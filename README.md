# dagworker

**Hand a dynamic DAG of work to workers you already have.** dagworker owns the
graph, the readiness computation, and the lease protocol. Your program owns the
workers.

[![Go Reference](https://pkg.go.dev/badge/github.com/specialistvlad/dagworker.svg)](https://pkg.go.dev/github.com/specialistvlad/dagworker)
[![Go Report Card](https://goreportcard.com/badge/github.com/specialistvlad/dagworker)](https://goreportcard.com/report/github.com/specialistvlad/dagworker)
[![CI](https://github.com/specialistvlad/dagworker/actions/workflows/ci.yml/badge.svg)](https://github.com/specialistvlad/dagworker/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

```go
store := memory.New()
m, _ := dagworker.New(store)
defer m.Close(ctx)

// A graph. Edges may be added later, while it is running.
m.AddNodes(ctx, "deploy", []dagworker.NodeSpec{
    {ID: "build"},
    {ID: "test", Deps: []dagworker.NodeID{"build"}},
    {ID: "publish", Deps: []dagworker.NodeID{"test"}},
    {ID: "notify", Deps: []dagworker.NodeID{"publish"}, Trigger: dagworker.TriggerAllDone},
})

// A worker. Run as many as you like, in as many processes as you like.
for {
    lease, err := m.Claim(ctx, "deploy")   // blocks until something is ready
    if err != nil {
        return err
    }
    if err := doTheWork(lease.Node); err != nil {
        m.Nack(ctx, lease, err)            // retried, or failed, per policy
        continue
    }
    m.Ack(ctx, lease, result)              // releases whatever it unblocked
}
```

```
go get github.com/specialistvlad/dagworker
```

**The core module has no dependencies.** Not "few" — none. Importing dagworker
pulls in the standard library and nothing else. The PostgreSQL and Redis
backends, the gRPC and HTTP adapters, and the daemon are separate modules, so
you pay for exactly the ones you use.

## Why you might want this

You have work with dependencies between the items, workers that already exist,
and no appetite for a workflow engine with its own database, its own scheduler
process, and its own operational surface.

dagworker is a library. It embeds in your program. It computes which items are
ready, hands them out one lease at a time, and takes them back when a worker
dies. That is the whole job.

- **The graph is dynamic.** Add nodes and edges while it runs. An edge that
  would create a cycle is rejected at insert time, in O(1) for the common case.
- **A dead worker cannot lose work.** Every claim is a lease with a deadline and
  a fencing token. A worker that stalls and comes back finds its write refused
  rather than overwriting whoever took over.
- **Multiple processes, one graph.** Point several instances at the same Redis
  or PostgreSQL and they compete for work correctly, with no coordinator, no
  leader election, and no membership protocol.
- **Nothing is O(n).** Claiming, completing, inserting and querying cost the
  same at a million nodes as at a thousand. This is enforced by tests, not
  claimed in a README — see [Performance](#performance).

## Two cases it was built for

Both of these run end to end against all three backends in `test/e2e`, with
real worker pools claiming, failing, retrying and completing — not seeded
fixtures.

**A pipeline whose shape is decided while it runs.**
[`scenario_transcode_test.go`](test/e2e/scenario_transcode_test.go)

```
ingest ──▶ probe ──┬──▶ rendition:240p ──┐
                   ├──▶ rendition:720p ──┼──▶ manifest ──▶ publish
                   └──▶ rendition:1080p ─┘
                   ▲
                   └── these do not exist until probe has run
```

Probing the source decides how many renditions there are. Each is a separate
unit of work for a separate machine, and the manifest cannot be written until
every one of them is done. A task queue has no edges, so either the manifest
step polls or the probe blocks a worker until the renditions finish. A workflow
engine expresses it at the cost of running your code inside its runtime. Here
the fan-out is three lines in the probe handler: add the nodes, add the edges
into `manifest`, acknowledge. `manifest` was already waiting on dependencies
that did not exist when it was created.

**CI/CD orchestration over a runner fleet you already operate.**
[`scenario_release_test.go`](test/e2e/scenario_release_test.go)

```
checkout ──▶ build ──┬──▶ test:unit ────────┐
                     ├──▶ test:integration ─┼──▶ package ──▶ publish ──▶ notify
                     └──▶ lint ─────────────┘                              ▲
                          (notify uses all_done, so it runs even if publish fails)
```

Heterogeneous pools claim by `Kind` — builders are expensive and few, testers
are many, publishing is serialised to one. A flaky step retries with backoff. A
branch that is not taken is skipped, and skipping propagates. `notify` uses
`all_done` so a failed release still tells somebody. If your runners are
long-lived machines rather than pods, and you want the dependency graph without
adopting a control plane, this is the shape.

**Where it is the wrong tool:** work with no dependencies (use a queue), work
that must survive as *durable execution of your own functions* (use Temporal),
or a graph you want a UI and an operator to manage for you (use Argo).

## What it deliberately is not

- **Not a workflow engine.** There is no DSL, no retries-with-compensation, no
  durable execution of your Go functions. If you want Temporal, use Temporal.
- **Not a task queue.** A queue has no edges. If your work has no dependencies,
  use [River](https://github.com/riverqueue/river) or
  [Asynq](https://github.com/hibiken/asynq) — they are excellent and simpler.
- **Not exactly-once.** Delivery is at-least-once, with at-most-once *accepted
  effect* per lease. Exactly-once delivery to a process you do not control is
  not possible, and anything claiming otherwise is describing something else.

## Status

Four values, forever:

| Status | Meaning |
|---|---|
| `New` | exists, has not completed successfully yet |
| `InProgress` | a worker holds a valid lease |
| `Success` | terminal, succeeded |
| `Error` | terminal, did not succeed |

Why it failed is a separate closed `Reason`: `WorkerError`, `Timeout`,
`UpstreamFailed`, `Skipped`, `Cancelled`, `Removed`. A timeout is an error with
a reason, not a fifth status — every production system we surveyed converged on
that split, and the ones that did not have the issue trackers to prove it.

Blocked and ready are both `New`. Adding an edge can flip a node between them
with no worker and no caller involved, so exposing the difference would make a
scheduling artefact look like a status regression. `Manager.Inspect` shows it
for debugging.

## Trigger rules

By default a node runs when every predecessor succeeded. Four other rules cover
the cases that actually come up:

| Rule | Runs when |
|---|---|
| `TriggerAllSuccess` | every predecessor succeeded *(default)* |
| `TriggerAllDone` | every predecessor finished, however it finished |
| `TriggerNoneFailed` | every predecessor finished and none failed — a skip is fine |
| `TriggerNoneFailedMinOneSuccess` | as above, and at least one succeeded |
| `TriggerAlways` | immediately, ignoring predecessors |

A worker can report `Skip` instead of success or failure — "I looked, there was
nothing to do". That is the branch primitive, and it is why `NoneFailed` differs
from `AllSuccess`.

## Backends

| | in-memory | Redis | PostgreSQL |
|---|---|---|---|
| survives a restart | no | ~1s window¹ | yes |
| shared across processes | no | yes | yes |
| resumable event stream | yes | yes (Streams) | yes (outbox) |
| wake without polling | yes | yes (pub/sub) | yes (`LISTEN`) |
| module | *(core)* | `storage/redis` | `storage/postgres` |

Measured per-operation cost, n=30,000, containerised databases on one laptop:

| | in-memory | Redis | PostgreSQL |
|---|---|---|---|
| insert a node | 1.2 µs | 12 µs | 340 µs |
| claim + complete | 1.8 µs | 673 µs | 3.5 ms |

The networked figures are round-trip bound — one hop to a container here is
~185 µs, so nothing single-shot beats that. PostgreSQL's insert cost is roughly
six un-pipelined round trips per node; that is a constant factor rather than
growth, and `pgx.Batch` pipelining is the known fix.

¹ Redis replicates asynchronously by default, so a primary failover can lose
about a second of writes unless you opt into `WAIT`/`WAITAOF` per call. The
library documents this rather than implying ACID durability it cannot provide.

**Memcached is not supported**, and that is a decision rather than an omission:
it has no multi-key atomicity at any protocol version, no ordered structure to
sweep deadlines with, no durable compare-and-swap across a restart, and an LRU
that makes eviction indistinguishable from "never existed". A backend that can
silently delete your graph should not be offered.
See [ADR-0017](docs/adr/0017-memcached-rejected-as-storage-backend.md).

All three pass the same suite, and the end-to-end suite runs every scenario
against each of them -- including two instances competing for one graph.

Every backend passes the same suite:

```go
func TestConformance(t *testing.T) {
    dagstoretest.RunConformance(t, dagstoretest.Harness{Name: "mine", New: ...})
}
```

That is roughly 65 named tests — `T-CLAIM-ATOMIC`, `T-FENCE-STALE-ACK`,
`T-EDGE-CYCLE-LEAVES-GRAPH-INTACT` — which is what makes the table above a
tested contract rather than documentation that quietly goes stale.

## Workers that are not Go, or not in this process

The core library hands work to goroutines in your program. If your workers are
somewhere else, two optional adapters and a daemon put the same protocol on a
socket — and the core module still has no dependency on either.

```
dagworkerd --store=postgres --postgres-dsn-file=/run/secrets/dsn \
           --grpc-addr=:9090 --http-addr=:8080 \
           --auth-token-file=/run/secrets/tokens
```

The token file is not decoration: the daemon **refuses to start** if it is
asked to serve a non-loopback address with no credential configured. Both
adapters take a `WithAuthorizer` hook that runs before any handler, so a
deployment with real identities plugs in mTLS or its own token service instead;
`BearerToken` is just the smallest thing that is not an open port. See
[SECURITY.md](SECURITY.md).

| | gRPC | HTTP/JSON |
|---|---|---|
| taking work | unary long poll, one outstanding call per worker slot | blocking query with a `wait` parameter |
| events | server stream with a resume cursor | Server-Sent Events, `Last-Event-ID` is the cursor |
| errors | `google.rpc.Status` with `ErrorInfo` | RFC 9457 `application/problem+json` |
| schema | committed `.proto`, `buf` lint and breaking-change checks | hand-written OpenAPI 3.1 |
| module | `adapters/grpc` | `adapters/http` |

Dispatch is a **long poll**, not a push. Pushing work at a pool whose capacity
you cannot see is how a queue overwhelms its own workers; one outstanding claim
per execution slot makes HTTP/2's own stream limit the flow-control mechanism,
which is the shape Temporal's `PollActivityTaskQueue` settled on.

The one thing to know if you write your own client: **the lease deadline is not
the request deadline**. It lives in storage and outlives the call that granted
it, the connection it arrived on, and the daemon process. Reusing the claim
call's context for the acknowledgement expires it mid-job.

Both adapters and the daemon are separate modules. `go get` on the core pulls
in neither.

## Performance

Measured at **1,000,000 nodes** on every backend. Apple M-series, Go 1.26,
databases in containers on the same laptop:

| | in-memory | Redis | PostgreSQL |
|---|---|---|---|
| `ScopeStats` | 29 ns | 134 µs | 153 µs |
| `GetNode` | 458 ns | 156 µs | 175 µs |
| `Claim` + `Complete` | 1.7 µs | 649 µs | 3.5 ms |
| seed 1M nodes | 0.9 s | 33 s | 7 min 34 s |

One round trip to a container on this machine is ~150 µs — Docker Desktop on
macOS routes loopback through a VM — which is why both networked backends bottom
out where they do: nothing single-shot beats a round trip, and on Linux with a
local socket these numbers are several times smaller.

Seeding is the one place a per-node cost is visible, and it is round trips, not
work: PostgreSQL was six un-pipelined round trips per node and is now **2.06**,
measured deterministically by counting them through a `pgx` tracer rather than
by timing a loaded laptop. That is what took the 1M seed from 21 minutes to
7m34s. Claim is 10.0 round trips at 200 nodes and 10.0 at 10,000 — the count is
asserted in CI, so a regression that adds a query fails the build instead of
showing up as a slow afternoon.

The three backends are measured **one at a time**. They used to run as parallel
subtests, which meant Redis's per-operation costs were sampled while PostgreSQL
seeded a million rows through the same network stack on the same laptop: it
roughly doubled them and made Redis look slower than PostgreSQL at reading one
node. A measurement that reports the load from its own siblings is not a
measurement.

The number that matters is not any of those — it is that they do not change
with the size of the graph. CI asserts the *ratio* of per-operation cost between
a thousand nodes and a million:

```
Claim              1.12x
Claim+Complete     0.77x
AddNode (causal)   0.46x
ScopeStats         0.73x
GetNode            3.51x   (cache misses, not complexity)
```

An absolute threshold would be a promise about hardware, and a shared CI runner
breaks those for reasons unrelated to the code. A ratio cancels the machine out.
Over that span a linear scan shows up as ~1000x and even a `sqrt(n)` term as
~31x, so the 20x bound fails on a regression rather than on a noisy neighbour.

## How it works

Three ideas, and they are the whole design:

**Readiness is a counter, not a search.** Each node knows how many of its
dependencies are still unresolved. Completing a node decrements that counter on
its direct successors; any that reach zero become claimable. Nothing ever
rescans the graph. This is Kahn's algorithm, kept incremental.

**Cycles are rejected by an ordering, not a traversal.** Every node carries an
integer rank with the invariant that an edge always points from lower to higher.
An edge that already satisfies it is accepted in constant time, which is the
common case because callers build graphs roughly in causal order. Only a
genuinely out-of-order edge pays for a bounded search — and that search *is* the
cycle check. (Pearce–Kelly, JEA 2007.)

**A lease is only safe if writes are fenced.** Claiming a node bumps a
monotonic epoch. Every later write — the worker's acknowledgement, a heartbeat,
the reclaim of an expired lease — is a compare-and-swap on that epoch. Without
it, a worker that was merely paused rather than dead comes back and overwrites
whatever its replacement recorded. This is Kleppmann's fencing argument, and it
is not optional: a lease without it is not a weaker design, it is a different
and unsafe one.

## Documentation

- [`docs/spec/01-contract.md`](docs/spec/01-contract.md) — the normative
  contract: transition table, guarantees, complexity bounds
- [`docs/adr/`](docs/adr/) — 41 architecture decision records, one per decision
- [`docs/research/`](docs/research/) — the 15 primary-source research dossiers
  the design was derived from, and the synthesis that reconciled them

If you are wondering "why did they do it *that* way", the answer is in one of
those three places, with the paper or the post-mortem that argued for it.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). In short: `make check` must pass, new
behaviour needs a test that fails without it, and a design change needs an ADR.

Two suites, split by cost, and both budgets are constraints rather than hopes:

```
make check      tidy, lint, race, coverage.  No databases.   ~7s
make benchmark  integration, e2e, complexity, throughput.    ~3m30s
```

`make check` starts no container and opens no socket to a database, so it is
meant to be run constantly — and it stays honest only while it is fast. Run it
with the containers stopped and it must give the same answer in the same time.

## License

MIT — see [LICENSE](LICENSE).
