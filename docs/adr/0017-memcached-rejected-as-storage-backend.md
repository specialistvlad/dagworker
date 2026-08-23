# ADR-0017: Memcached is rejected as a storage backend

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/06-memcached-and-storage-abstraction.md Part A (§A.1–§A.8)

## Context

The original brief asked for "Redis, Memcached, and PostgreSQL" backends. The design-synthesis
draft's answer (docs/research/00-synthesis.md §3, ADR-17 seed; 06 Part A) was not "yes" but "yes,
as a read-through `NodeCache` decorator that never implements `Store`." The project owner (AMD-5)
went further: memcached is **dropped entirely**. No `storage/memcached` module, no `NodeCache`
interface, no memcached service in `docker-compose.test.yml`, no row in the capability matrix. This
ADR records the full technical case for that rejection, because it is the decision in this set most
likely to be second-guessed against the literal wording of the original brief (docs/research/00
§11, open question 6).

**The protocol ceiling.** Memcached's classic protocol gives exactly eleven verbs — `set/add/
replace`, `append/prepend`, `cas`, `gets`, `delete`, `incr/decr`, `touch`/`gat` — and every one of
them is scoped to **exactly one key**
([protocol.txt](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)). There is no
verb that touches two keys atomically, at any protocol revision. Adding a DAG edge inherently means
touching at least two records (the successor's pending-predecessor count, the predecessor's
fan-out list) plus a scope index; memcached cannot make that one atomic unit under any
circumstance.

**The meta protocol (1.6+) does not lift the ceiling — it enriches single-key operations.** `mg`/
`ms`/`md`/`ma` add byte-efficient framing and expose internal item state (`c` for CAS token,
`C<token>` for conditional set/delete, `N`/`W`/`Z`/`X` stampede-control flags), but the spec is
explicit: "unlike `get`, metaget can only take a single key"
([protocol.txt](https://github.com/memcached/memcached/blob/master/doc/protocol.txt);
[Meta Text Protocol](https://docs.memcached.org/protocols/meta/)). There is no server-side
scripting analogous to Redis Lua or Redis Functions — no `Txn`, no `MULTI`/`EXEC` equivalent, at
any protocol version. The `N`/`W`/`Z` stampede tokens are a real, working single-key lease
primitive, but they carry no fencing token: if an item is deleted and recreated while the original
lease-winner is mid-flight, nothing keys that winner's write to a specific epoch, so it is
structurally weaker than the Chubby-sequencer/ZooKeeper-`zxid`/Temporal-`RangeID` fencing model
this library adopts everywhere else (ADR-0006).

**No ordered structure.** There is no ZSET analogue for a priority-ordered ready queue and no
range-by-deadline index for the timeout sweeper. A per-scope index or a deadline index would have
to be a hand-maintained value under one key — itself subject to the same no-multi-key-atomicity
problem, and, past roughly 1 MiB, unable to be stored at all without client-side sharding of the
index itself.

**No durable CAS across restart or eviction.** The protocol's own language is thin: a CAS token is
"a unique 64-bit value of an existing entry"
([protocol.txt](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)) — it does not
claim the token space survives a restart, and a plain process restart or an evict-then-recreate
cycle can reissue a numerically identical token against a semantically different item. This is a
textbook ABA hazard
([Stroustrup et al., "Understanding and Effectively Preventing the ABA Problem"](https://www.stroustrup.com/isorc2010.pdf);
[Lock-Free Concurrent Data Structures lecture notes](https://users.fmi.uni-jena.de/~nwk/LockFree.pdf))
with no protocol-level fix.

**Eviction is silent and adversarial to correctness.** LRU eviction is slab-class-local — an item
can be evicted while plenty of RAM sits unused in other slab classes
([new_lru.txt](https://github.com/memcached/memcached/blob/master/doc/new_lru.txt)) — and
`extstore`, memcached's own flash-storage extension, is explicitly non-durable: "data stored with
extstore is not durable — if the Memcached process crashes, data cannot be recovered"
([extstore flash storage docs](https://docs.memcached.org/features/flashstorage/)), and it evicts
from the in-RAM LRU tail "even when there is available space on disk" if the disk writer lags
([extstore eviction issue #922](https://github.com/memcached/memcached/issues/922)). A node record —
status, edges, lease state — can vanish with no notification, indistinguishable from "this node
never existed." A library whose headline promise is durable, observable state transitions cannot
make that promise on a substrate that cannot distinguish eviction from absence.

**The general pattern, confirmed by comparison.** Every genuinely CAS-capable distributed store
either widens the atomic unit to cover multiple keys (etcd's `Txn{Compare, Success, Failure}`
across N keys in one round trip; DynamoDB's `TransactWriteItems` across up to 100 items) or ties
its version/CAS token to a durable, monotonic revision that survives restarts (etcd
`mod_revision`; Cassandra's Paxos-backed LWTs within one partition). Memcached does neither — a
single-key, single-process-lifetime CAS token, strictly weaker than every other store this project
was ever asked to support, and weaker in precisely the dimension ("add an edge" is inherently a
two-record operation) that matters most for a DAG.

## Decision

**Memcached is rejected as a storage backend for dagworker, in every capacity — not merely
excluded from `Store`, but excluded from the codebase, the module layout, and the capability
matrix entirely.**

Concretely:

- There is no `storage/memcached` Go module and none will be added under this decision.
- There is no `dagstore.NodeCache` interface, no read-through/write-behind cache-decorator type,
  anywhere in the library.
- `docker-compose.test.yml` (ADR-0018) has no memcached service.
- No capability matrix, README table, or public doc comment lists memcached as a supported,
  partially-supported, or cache-tier backend. Memcached appears in dagworker's own documentation
  exactly once: in this ADR, as a rejected alternative, so the reasoning is discoverable rather
  than silently absent.
- The three supported backends are, and remain, exactly: in-memory (default), Redis, PostgreSQL.

A host program that wants a read-through cache in front of Redis or PostgreSQL reads is free to
build one in its own process, wrapping calls to `Manager.GetNode` with any memcached client it
likes — that pattern is entirely out of this library's scope, not forbidden to the host, just not
something dagworker ships, tests, or documents as one of its backends.

## Consequences

### Positive
- Removes an entire dependency surface (a memcached client — `pior/memcache` or
  `QuangTung97/go-memcache`, both comparatively young/niche relative to the ecosystem-standard
  clients for Redis/Postgres) and the module/CI/documentation burden that comes with it.
- Eliminates the risk of an operator reading "memcached: supported" in a capability table and
  configuring it somewhere durability actually matters — the single-most likely way a `NodeCache`
  decorator, however correctly scoped, gets misused in practice.
- One fewer moving part in `docker-compose.test.yml` and the conformance test matrix (ADR-0018).

### Negative
- This is a real, deliberate narrowing of the original brief's literal wording ("must also support
  Redis, Memcached, and PostgreSQL backends" — docs/research/00 §11 open question 6). Every dossier
  that touched memcached (03, 05, 06) converged independently on "cannot honestly be a durable
  backend for this workload," but the brief's literal ask is not met, and this must be stated
  plainly rather than glossed over.
- A host program that specifically wanted the shaved read-latency of a memcached tier now has to
  build that itself; dagworker offers no scaffolding for it.

### Neutral
- The rejection removes the memcached-derived 1 MiB item-size justification that partly motivated
  the 256 KiB default payload cap (ADR-0026 §5.3); that cap's other justification (the SQS
  precedent, independent of memcached) still stands on its own.

## Alternatives considered

**`NodeCache` read-through/write-behind decorator (the synthesis draft's own recommendation, 06
§A.6).** Seriously considered — technically sound as far as it went (invalidate-on-write, not
update-on-write, structurally defangs the ABA hazard because the cache is never authoritative) —
and rejected by the owner under AMD-5 specifically because the marginal benefit is thin at this
project's target latencies (in-memory has zero network hop by construction; Redis and Postgres both
sit under sub-millisecond loopback reads per docs/research/09 §4.2–§4.3's own measurement
methodology) against a real, ongoing cost: a fourth backend-shaped module, a newer/narrower client
dependency, and a permanent extra row in every capability explanation the project ever writes, for
a benefit a host program can already get in userspace with an ordinary cache-aside pattern around
`Manager.GetNode` — without teaching the library about memcached at all.

**Reduced-capability `Store` tier** (implement the mandatory methods, return `ErrCapability` for
`Lister`/timeout-sweep-shaped facets). Rejected per 06 §A.6's own argument: dishonest about
durability specifically — a caller using only the "supported" surface (single-node CAS reads/
writes) would still silently lose data on eviction or restart with zero API signal that anything
went wrong. Reduced capability without a durability disclaimer is a trap, not a mitigation.

**`Durable() bool` flag, host decides what to do with `false`.** Rejected: leaves the DAG topology,
ready-queue, and timeout sweep with no home at all if memcached were ever configured as the only
backend, forcing the library to refuse to start — a hard "no" nobody asked for, and one that
changes none of the underlying autopsy findings above.

**Meta-protocol conditional delete as a token single-facet gesture** (`md key C<token>` is, per 06
§B.4, the one facet memcached genuinely earns even under the old NodeCache framing). Rejected under
AMD-5's clean-break stance: even the one honestly-earned facet is not worth a module, a dependency,
and a documentation footnote in isolation, once the decorator it would have served is gone.

## References

- Memcached protocol spec — https://github.com/memcached/memcached/blob/master/doc/protocol.txt
- Meta Text Protocol — https://docs.memcached.org/protocols/meta/
- Max item size discussion — https://github.com/memcached/memcached/discussions/1066, https://github.com/memcached/memcached/issues/473
- Flash storage (extstore) durability — https://docs.memcached.org/features/flashstorage/
- Warm restart (graceful-only, not crash-safe) — https://docs.memcached.org/features/restart/
- LRU / eviction mechanics — https://github.com/memcached/memcached/blob/master/doc/new_lru.txt, https://github.com/memcached/memcached/issues/922, https://github.com/memcached/memcached/issues/881
- ABA problem — https://www.stroustrup.com/isorc2010.pdf, https://users.fmi.uni-jena.de/~nwk/LockFree.pdf
- etcd transactions — https://etcd.io/docs/v3.5/learning/api/, https://etcd.io/docs/v3.5/tutorials/how-to-transactional-write/
- DynamoDB `TransactWriteItems` — https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
- Cassandra lightweight transactions — https://cassandra.apache.org/doc/latest/cassandra/architecture/guarantees.html, https://axonops.com/blog/paxos-v2-and-lightweight-transactions/
- Sibling ADRs: ADR-0006 (fencing token), ADR-0016 (storage port shape), ADR-0018 (conformance
  suite), ADR-0026 (payload size cap)
