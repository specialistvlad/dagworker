---
title: Choosing a backend
description: The honest comparison between in-memory, Redis, and PostgreSQL — including what each one actually guarantees, and what memcached was rejected for.
---

Every backend is driven through the identical `Manager` API — nothing in
your worker code changes based on which one you pick (see the
[Quickstart](/dagworker/guide/quickstart/) for the two-line swap). What
differs is durability, whether several processes can share one graph, and
the cost of each operation. This page is the honest version of that
comparison, including the parts a marketing page would leave out.

## The three backends

| | in-memory | Redis | PostgreSQL |
|---|---|---|---|
| survives a process restart | no | ~1s window¹ | yes |
| shared across processes | no | yes | yes |
| resumable durable event stream | yes | yes (Streams) | yes (outbox) |
| wake without polling | yes | yes (pub/sub) | yes (`LISTEN`) |
| module | *(core)* | `storage/redis` | `storage/postgres` |

¹ Redis replicates asynchronously by default, so a primary failover can lose
about a second of writes unless you opt into `WAIT`/`WAITAOF` per call.

**In-memory** is the default and the only backend with zero external
dependency. It's the right choice when every worker shares one process — a
CLI tool, a single-instance service, or your test suite — and the wrong
choice the moment you need two processes to see the same graph, because it
has no cross-process visibility at all. Data lives exactly as long as the
process does.

**Redis** gets you cross-process sharing with very low per-operation
latency, at the cost of a real, stated durability gap: Redis replicates
asynchronously to its replicas by default, and a primary failover during
that window can lose recently-written state. `WAIT`/`WAITAOF` closes that
gap per call at a latency cost you opt into deliberately, rather than one
the library imposes on every write whether you need it or not.

**PostgreSQL** gets you full WAL durability for nodes, edges, and the event
log — nothing is lost on crash, leases included.

There is no separate leases table to treat differently. A lease is three
columns on the node row — `epoch`, `deadline`, `worker` — which is what makes
a claim one write rather than two rows in one transaction
([ADR-0007](/dagworker/adr/0007-claim-atomicity-one-write-grants-ownership-and-records-the-deadline/)),
and those columns are exactly as durable as the node itself. A crash therefore
loses no lease, and the fencing design (see
[Concepts](/dagworker/guide/concepts/)) handles the case that matters anyway:
a worker that dies holding one has its node reclaimed at the deadline, and its
late write refused by the epoch.

## Durability, stated plainly

The spec requires every backend to publish exactly what it guarantees, with
no backend allowed to imply more than it delivers:

| Backend | Durability |
|---|---|
| in-memory | none — process lifetime only. Suitable when all workers share one process. |
| Redis | async replication by default: a primary failover can lose ~1s of writes. `WAIT`/`WAITAOF` is available as an opt-in per-call cost. |
| PostgreSQL | full WAL durability for nodes, edges, and events — and for leases, which are columns on the node row rather than a table of their own. |

None of this is a defect list. It's the same trade every mature storage
system asks you to make explicitly rather than papering over — the same
reason Redis itself ships `WAIT` as an opt-in rather than a default, and the
same reason a lease table has no business being crash-durable in the first
place.

## Measured cost per operation

n=30,000 nodes, containerized databases on the same laptop the rest of this
project's numbers come from:

| | in-memory | Redis | PostgreSQL |
|---|---|---|---|
| insert a node | 1.2 µs | 12 µs | 340 µs |
| claim + complete | 1.8 µs | 673 µs | 3.5 ms |

The networked figures are round-trip bound — one hop to a container here is
roughly 185 µs, so nothing single-shot beats that regardless of how the
query itself is written. PostgreSQL's insert cost was once roughly six
un-pipelined round trips per node; `pgx.Batch` took it to **2.06**, which is
asserted in CI by `storage/postgres/roundtrip_test.go` rather than left to a
wall clock. That is a constant factor either way, not growth with graph size.
See [Performance](/dagworker/guide/performance/)
for the same measurement repeated at a million nodes, and — more
importantly — for why the number that actually matters is a ratio, not
either of these absolute figures.

## What memcached was rejected for

The original design brief asked for Redis, memcached, and PostgreSQL.
Memcached is not one of the three backends, and that's a decision, not an
oversight — recorded in full in
[ADR-0017](/dagworker/adr/0017-memcached-rejected-as-storage-backend/). The
short version:

- **No multi-key atomicity, at any protocol version.** Every memcached verb
  — `set`, `cas`, `incr`, even the newer meta-protocol commands — is scoped
  to exactly one key. Adding a DAG edge touches at least two records (the
  successor's pending-predecessor count and the predecessor's own fan-out
  list); memcached has no way to make that one atomic unit under any
  circumstance.
- **No ordered structure.** There's no ZSET-equivalent for a priority-ordered
  ready set and no range-by-deadline index the lease sweeper could use — a
  deadline index would have to be a hand-maintained value under one key,
  subject to the same no-multi-key-atomicity problem.
- **No durable compare-and-swap across a restart or an eviction.** A CAS
  token's own documentation makes no claim that the token space survives a
  restart; a plain process restart can reissue a numerically identical
  token against a semantically different item — a textbook ABA hazard with
  no protocol-level fix.
- **Eviction is silent and indistinguishable from "never existed."** LRU
  eviction is slab-class-local, so an item can be evicted while plenty of
  RAM sits unused elsewhere. A node record — its status, its edges, its
  lease state — can vanish with no notification. A library whose core
  promise is durable, observable state transitions cannot make that promise
  on a substrate that can't tell eviction apart from absence.

None of that is disqualifying for what memcached is actually good at — a
read-through cache in front of a real store. If you want that layer, build
it yourself around `Manager.GetNode`; it's a legitimate pattern the library
simply doesn't ship, because doing so would mean carrying a fourth
backend-shaped module and a durability disclaimer for a benefit any host
program can already get in userspace with an ordinary cache-aside wrapper.

## Every backend passes the same suite

The comparison above is a tested contract, not documentation that quietly
goes stale: all three backends run through one shared conformance suite,
`dagstoretest.RunConformance`, roughly 65 named tests with stable IDs —
`T-CLAIM-ATOMIC`, `T-FENCE-STALE-ACK`, `T-EDGE-CYCLE-LEAVES-GRAPH-INTACT`
among them — referenced directly from each backend's own documentation. The
end-to-end suite goes further and runs every scenario against each backend
in turn, including two instances competing for one graph, which is what
actually exercises the cross-process claim story Redis and PostgreSQL both
promise and in-memory explicitly does not.

```go
func TestConformance(t *testing.T) {
	dagstoretest.RunConformance(t, dagstoretest.Harness{Name: "mine", New: myBackend.New})
}
```

If you're evaluating a fourth backend of your own, this suite is the actual
bar: a backend that cannot provide the four mandatory fenced primitives —
`Claim`, `Complete`, `Extend`, `Sweep` — doesn't implement the storage port
at all. There's no reduced-capability tier for those four; either the
backend gives you atomic claims and fenced writes, or it isn't a backend
this library can drive safely.
