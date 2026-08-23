---
title: dagworker
description: A Go library that manages a dynamic DAG of work and hands ready items to workers you already have, under a fenced lease.
---

**Hand a dynamic DAG of work to workers you already have.** dagworker owns the
graph, the readiness computation, and the lease protocol. Your program owns
the workers.

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

**The core module has no dependencies.** Not "few" — none. Importing
dagworker pulls in the standard library and nothing else. The PostgreSQL and
Redis backends, the gRPC and HTTP adapters, and the daemon are separate
modules, so you pay for exactly the ones you use.

## Why you might want this

You have work with dependencies between the items, workers that already
exist, and no appetite for a workflow engine with its own database, its own
scheduler process, and its own operational surface.

dagworker is a library. It embeds in your program. It computes which items
are ready, hands them out one lease at a time, and takes them back when a
worker dies. That is the whole job.

- **The graph is dynamic.** Add nodes and edges while it runs. An edge that
  would create a cycle is rejected at insert time, in O(1) for the common
  case.
- **A dead worker cannot lose work.** Every claim is a lease with a deadline
  and a fencing token. A worker that stalls and comes back finds its write
  refused rather than overwriting whoever took over.
- **Multiple processes, one graph.** Point several instances at the same
  Redis or PostgreSQL and they compete for work correctly, with no
  coordinator, no leader election, and no membership protocol.
- **Nothing is O(n).** Claiming, completing, inserting and querying cost the
  same at a million nodes as at a thousand. This is enforced by tests, not
  claimed in a README — see [Performance](/dagworker/guide/performance/).

## Two cases it was built for

Both of these run end to end against all three backends in `test/e2e`, with
real worker pools claiming, failing, retrying and completing — not seeded
fixtures.

**A pipeline whose shape is decided while it runs.**
[`scenario_transcode_test.go`](https://github.com/specialistvlad/dagworker/blob/main/test/e2e/scenario_transcode_test.go)

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
[`scenario_release_test.go`](https://github.com/specialistvlad/dagworker/blob/main/test/e2e/scenario_release_test.go)

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

- **Not a workflow engine.** There is no DSL, no retries-with-compensation,
  no durable execution of your Go functions. If you want Temporal, use
  Temporal.
- **Not a task queue.** A queue has no edges. If your work has no
  dependencies, use [River](https://github.com/riverqueue/river) or
  [Asynq](https://github.com/hibiken/asynq) — they are excellent and
  simpler.
- **Not exactly-once.** Delivery is at-least-once, with at-most-once
  *accepted effect* per lease. Exactly-once delivery to a process you do not
  control is not possible, and anything claiming otherwise is describing
  something else.

## Backends, measured

| | in-memory | Redis | PostgreSQL |
|---|---|---|---|
| survives a restart | no | ~1s window¹ | yes |
| shared across processes | no | yes | yes |
| resumable event stream | yes | yes (Streams) | yes (outbox) |
| wake without polling | yes | yes (pub/sub) | yes (`LISTEN`) |

Measured at **1,000,000 nodes**, Apple M-series, Go 1.26, databases in
containers on the same laptop:

| | in-memory | Redis | PostgreSQL |
|---|---|---|---|
| `Claim` + `Complete` | 1.7 µs | 649 µs | 3.5 ms |
| seed 1M nodes | 0.9 s | 33 s | 7 min 34 s |

The number that matters is not any single figure — it is that none of them
change with the size of the graph. CI asserts the *ratio* of per-operation
cost between a thousand nodes and a million, not an absolute threshold. See
[Performance](/dagworker/guide/performance/) and [Choosing a backend](/dagworker/guide/backends/).

¹ Redis replicates asynchronously by default, so a primary failover can lose
about a second of writes unless you opt into `WAIT`/`WAITAOF` per call.

## Where to go next

- New to the library? Start with the [Quickstart](/dagworker/guide/quickstart/).
- Want the vocabulary first? Read [Concepts](/dagworker/guide/concepts/).
- Wiring up a non-Go worker? See [Writing workers](/dagworker/guide/workers/).
- Wondering why a decision was made a particular way? It is in the
  [Architecture Decisions](/dagworker/adr/), backed by a citation in the
  [Research Dossiers](/dagworker/research/).

## Documentation

- [Storage & manager contract](/dagworker/reference/contract/) — the normative contract:
  transition table, guarantees, complexity bounds.
- [Architecture Decisions](/dagworker/adr/) — one record per decision.
- [Research Dossiers](/dagworker/research/) — the primary-source research the design
  was derived from, and the synthesis that reconciled it.

If you are wondering "why did they do it *that* way", the answer is in one of
those three places, with the paper or the post-mortem that argued for it.

## Contributing

See [Contributing](/dagworker/contributing/). In short: `make check` must pass, new
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

MIT.
