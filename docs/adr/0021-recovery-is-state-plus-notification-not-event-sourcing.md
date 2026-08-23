# ADR-0021: Recovery is state-plus-notification, not event sourcing

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/10-event-bus-and-delivery-semantics.md §4, §6

## Context

Martin Fowler's Event Sourcing pattern treats the event log itself as the source of truth: "all
changes to the domain objects are initiated by the event objects," to the point that application
state can be "discard[ed]... completely and rebuild[t] by re-running the events from the event log
on an empty application." Temporal is the hardened production instance of this idea for a workflow
engine, and it only works because Temporal's SDK builds a **replay gateway** directly into every
workflow: activities are never re-invoked on replay, their previously recorded results are fed back
from history instead (10 §4.1). Fowler flags the exact trap this library must not walk into: "if
these events cause update messages to be sent to external systems, then things will go wrong
because those external systems don't know the difference between real processing and replays"
(10 §4.2).

dag-worker-go's entire purpose is to send "go do this" to external workers that live outside the
library and have no concept of "this is a replay." If the library ever rebuilt its dispatch state
by mechanically replaying the event log, every recovery, restart, or backfill would re-dispatch
every in-flight or completed node to a real worker a second time. Building Temporal's replay
gateway — activity-result caching keyed by history position, deterministic-execution checking — is
a large, ongoing surface area that directly contradicts this library's stated goal of a minimal
public surface, and nothing in the brief asks for workflow-replay semantics.

Separately, every durable pub/sub system that allows a subscriber to disconnect and later resume
faces the bounded-retention question: unlimited history is never free, so every system defines a
canonical "your cursor is now unreachable" condition rather than silently serving a wrong position
— etcd cancels the watch and reports `CompactRevision`; Kafka raises `OffsetOutOfRangeException`;
MongoDB Change Streams raise `ChangeStreamHistoryLost`; a lagging Postgres replication slot is
marked `wal_status = 'lost'` (10 §6). dag-worker-go needs the same discipline, and needs one
recovery procedure that is correct regardless of which backend produced the expiry.

## Decision

**Persisted node state is the single source of truth. The event stream is a best-effort,
derivative notification of transitions to that state — never the mechanism state is reconstructed
from.**

Concretely:

1. The authoritative write is the fenced storage mutation (`Claim`/`Complete`/`Extend`/`Sweep`,
   AMD-2) that changes a node's row/hash/key and bumps its `Seq` (ADR-0020) in one atomic
   operation. This is the fact. The event is emitted as a side effect of that write's commit
   succeeding — never before commit (ADR-0022's "emit after commit" rule) — and its own delivery to
   any given subscriber is decoupled from the fact's durability.
2. `Reserve`/`Claim` never trusts accumulated event history to decide eligibility. Every claim
   attempt re-derives "is this node currently eligible" from current storage state at the moment
   of the call (ADR-0019). A lost or duplicated readiness notification is therefore never a
   correctness problem, only a latency one — the same argument that makes a duplicated `SKIP
   LOCKED` poll harmless in Postgres-backed job queues.
3. Full event-log retention (durable, replayable history) is offered as an **opt-in add-on** for
   audit/observability — a subscriber that wants a durable materialized history table consumes the
   feed with `Durable: true` (ADR-0022) and builds it itself — but the library's own liveness and
   correctness never depend on that log surviving or being complete. The log is never load-bearing
   for dispatch.
4. Every backend defines a resume-cursor expiry condition and reports it uniformly:

```go
// ErrCursorExpired is returned by Subscribe, or delivered on Subscription.Err(),
// when the backend can no longer resume from the caller's From position —
// a Redis Stream trimmed past it, a Postgres outbox row garbage-collected,
// an in-memory ring buffer that has wrapped. One error, one recovery
// procedure, regardless of backend.
var ErrCursorExpired = errors.New("dagworker: resume cursor older than retained history")
```

5. **The one correct recovery procedure, for every backend, on `ErrCursorExpired`:**

```go
sub, err := m.Subscribe(ctx, opts)
if errors.Is(err, dagworker.ErrCursorExpired) {
    // (a) Read current state directly — the source of truth never went away.
    nodes, err := m.ListNodes(ctx, scope, cursor, limit) // requires Lister (ADR-0016)
    // ... reconcile local view against nodes ...

    // (b) Resubscribe from now. Do not attempt to "fill the gap" from the
    // event log — by construction, the gap is unrecoverable from events and
    // recoverable from state, which is the whole point of this ADR.
    opts.From = 0
    sub, err = m.Subscribe(ctx, opts)
}
```

A caller with no `Lister`-capable backend, or one that never subscribes at all, is equally correct:
polling `TryClaim`/`GetNode` on a timer is the same recovery path taken to its limit, and the
library never treats "no subscriber connected" as a degraded-correctness state — only a
degraded-latency one.

## Consequences

### Positive

- No replay-safety machinery is ever needed: no deterministic-execution checking, no
  activity-result cache keyed by history position, no "is this a replay" gateway. This directly
  sidesteps the burden Fowler names and Temporal had to build to carry (10 §4.2).
- Correctness of dispatch is independent of event-log durability, retention window, or subscriber
  presence — a host can run with zero subscribers, or with a subscriber that has been disconnected
  for a week, and neither condition ever produces an incorrect claim or a missed one (only a slower
  discovery of readiness while nobody is polling).
- One typed error and one documented recovery procedure applies across in-memory, Redis, and
  Postgres, despite their wildly different native retention mechanics (ring buffer wraparound,
  `MAXLEN` trim, outbox retention job) — callers write recovery code once, not per backend.

### Negative

- A subscriber cannot get a guaranteed-complete history of every transition that ever occurred
  purely from the event stream unless it opts into `Durable: true` and consumes fast enough to stay
  within retention — "the log is not truth" means a lapsed subscriber's only recourse is a fresh
  state read, which loses fine-grained "what happened while I was gone" detail (it answers "what is
  true now," not "what sequence of things led here").
- Hosts that want genuine workflow-style replay/audit (rebuild an exact history of a run for
  debugging or compliance) must build that themselves on top of the opt-in durable tier; the
  library does not provide it as a first-class feature, and retrofitting true event-sourced replay
  later would be a large, likely-breaking addition, not a natural extension of this design.

### Neutral

- This ADR governs *recovery*, not *ordering* (ADR-0020) or *backpressure* (ADR-0022) — it assumes
  those are already in place and specifies only what happens when a subscriber's position falls
  out of retained history, and what "truth" means when it does.

## Alternatives considered

**Pure event-sourced log as truth** (rebuild dispatch state by replaying history from an empty
application, Fowler's canonical shape). Rejected: requires a Temporal-grade replay gateway to avoid
re-dispatching live work to external processes on every recovery (10 §4.2) — a large, ongoing
surface area this library's minimal-public-surface goal explicitly does not want, for a guarantee
(exact historical replay) nothing in the brief asks for.

**Silent resume from "earliest available" or "latest" on cursor expiry** (Kafka's
`auto.offset.reset` shape). Rejected as the default: silently skipping an unknown amount of history
without telling the caller is exactly the "surprising per-backend divergence" this design set out to
avoid (10 §6) — an explicit typed error with one documented recovery path is strictly more honest
and no more code for the caller to write.

**Unbounded event retention** (never expire a cursor, keep the log forever). Rejected: unbounded
history is an unbounded storage bill and, per this ADR's own core claim, is never the source of
truth anyway — paying to retain it forever buys nothing dispatch correctness needs (10 §6).

## References

- docs/research/10-event-bus-and-delivery-semantics.md §4.1–§4.3, §6
- docs/research/00-synthesis.md §3 (ADR-21 seed), §4 (`ErrCursorExpired`)
- Fowler, "Event Sourcing" — https://martinfowler.com/eaaDev/EventSourcing.html
- Temporal, "Events and Event History" — https://docs.temporal.io/workflow-execution/event
- etcd Watch API (CompactRevision) — https://etcd.io/docs/v3.5/learning/api/
