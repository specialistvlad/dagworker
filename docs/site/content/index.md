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
| `Claim` + `Complete` | 1.7 µs | 797 µs | 3.6 ms |
| seed 1M nodes | 0.9 s | 34 s | 21 min |

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
behaviour needs a test that fails without it, and a design change needs an
ADR.

## License

MIT.
