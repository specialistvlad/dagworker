# ADR-0022: Subscriber backpressure never blocks the producer

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/10-event-bus-and-delivery-semantics.md §7; docs/research/08-go-api-and-concurrency-design.md §8

## Context

The producer of `NodeStatusChanged`/`NodeReady` events is the same code path that discovers newly
ready nodes and advances the DAG for every other worker in the scope. If emitting an event to one
slow, forgotten, or malicious subscriber can stall that discovery loop, one bad subscriber degrades
the entire scope for everyone — a textbook head-of-line-blocking failure. Dossier 10 §7.1 enumerates
five backpressure strategies (unbounded buffer, bounded drop-oldest, bounded drop-newest, bounded
block, disconnect-the-slow-subscriber) and disqualifies exactly one of them for anything on the
scheduling path: **bounded-block**, because it is the one strategy that lets a subscriber's own
slowness become the producer's problem. Every production system built for this exact shape agrees:
NATS tracks per-subscription pending limits and disconnects a subscriber that exceeds them rather
than let it stall the server (`slow_consumers`); etcd cancels a watch that falls behind compaction
rather than stalling other watchers; the Reactive Streams spec's entire reason for existing is to
put buffer bounds under the *subscriber's* declared demand, never the producer's blocking control
flow (10 §7.1).

The two event kinds this library emits (ADR-0019) additionally warrant different policies even once
"never block" is fixed as the constraint: `NodeStatusChanged` is a per-node history a subscriber
generally wants to see completely (drop-oldest is the acceptable compromise, since a subsequent
`Seq` comparison — ADR-0020 — always reveals a gap and a state read always recovers full accuracy —
ADR-0021); `NodeReady` carries no payload beyond "go call `Claim`," so multiple pending doorbells
for the same node are strictly redundant information and can be coalesced for free, something
`NodeStatusChanged` cannot do without losing real history.

A second, distinct concern this ADR must not conflate with the above: `Claim`'s own internal
wakeup path (ADR-0033) is **not a `Subscribe`-style fan-out** at all. It is a private, per-
`(scope, kind)` doorbell with no external subscriber, no configurable overflow policy, and no
`OverflowBlock` option — it is a counting signal consumed only by blocked `Claim` callers inside
the same process (or, for a networked backend, the `Manager`'s own listener goroutine), and it is
explicitly out of scope for the policy this ADR defines. Confusing the two would either force
`Claim`'s liveness through a policy designed for many independent, untrusted readers, or force
`Subscribe`'s fan-out through machinery designed for a single load-bearing wakeup signal.

## Decision

Every `Subscribe` fan-out point is a **bounded, per-subscriber channel** with an explicit,
caller-selectable `OverflowPolicy`. The producer's write path never blocks on any one subscriber's
channel being full, under any policy, ever.

```go
type OverflowPolicy int

const (
    // OverflowDropOldestAndMarkGap is the default. The oldest buffered event
    // is evicted to make room; the subscriber detects the gap via Seq
    // (ADR-0020) on its next delivery and recovers via a state read
    // (ADR-0021) if it needs to know what it missed.
    OverflowDropOldestAndMarkGap OverflowPolicy = iota

    // OverflowBlock blocks only the PRODUCER-SIDE PUMP GOROUTINE FEEDING THIS
    // ONE SUBSCRIBER'S CHANNEL — never the write path that performed the
    // storage mutation, and never any other subscriber. Use only when a
    // caller has capacity-planned for a guaranteed-complete feed and
    // accepts unbounded memory growth in the pump if it never drains.
    OverflowBlock

    // OverflowCloseSlow disconnects the subscriber once its buffer is full,
    // delivering ErrSubscriberLagged on Subscription.Err(). The subscriber
    // must resubscribe (ADR-0021) to resume.
    OverflowCloseSlow
)

type SubscribeOptions struct {
    Scope       Scope
    Filter      Filter
    From        Seq
    GlobalOrder bool
    BufferSize  int            // default 256 if zero
    Overflow    OverflowPolicy // default OverflowDropOldestAndMarkGap
    Durable     bool
}
```

The write path's obligation is exactly this, and no more: append the event to each live
subscription's own buffer (a bounded ring feeding a Go channel, per subscriber) under the same
critical section as the storage mutation's fan-out step, then return — it never waits on a
subscriber's consumer to drain. Per-subscriber delivery (draining the ring into the channel,
applying `Overflow` when the channel itself is full) happens on that subscription's own pump
goroutine (`sync.WaitGroup.Go`, ADR-0027 §9.4), never inline on the write path.

Per-backend mechanics, matching each backend's own native isolation primitive rather than emulating
one:

| Backend | Isolation mechanism | `NodeReady` doorbell |
|---|---|---|
| In-memory | Bounded Go channel per subscriber, ring buffer feeding it (a plain channel send would itself block if unbuffered and full — the ring is what makes drop-oldest possible without blocking) | Same channel, `map[NodeID]struct{}` coalescing gate — duplicate readiness for one node collapses to one pending entry |
| Redis | Stream + consumer group per named subscriber (`XREADGROUP`/`XACK`); a slow consumer falls behind in its own PEL without affecting any other consumer group | Plain pub/sub over the same stream, at-most-once, non-load-bearing (ADR-0019, ADR-0033) |
| PostgreSQL | Outbox table + per-subscriber `last_relayed_id` row; retention deletes rows older than every subscriber's own low-water mark | `LISTEN`/`NOTIFY`, at-most-once, non-load-bearing |

`OverflowBlock` is documented as **per-subscriber-slot-only**: it may cause that one subscription's
own pump goroutine to block indefinitely, which is the caller's explicit, capacity-planned choice,
but it must never be implemented by having the storage-mutation write path itself wait on any
channel send — the boundary between "the write commits and returns" and "subscribers get told
about it" is never crossed by a blocking call in either direction.

## Consequences

### Positive

- The engine's own liveness (discovering and dispatching ready nodes) is structurally independent
  of how many subscribers exist, how fast they consume, or whether any of them is misbehaving —
  matching the architecture diagram's own invariant that the public API "never blocks on I/O it
  doesn't own."
- `OverflowDropOldestAndMarkGap` as the default composes directly with ADR-0020 (`Seq` reveals the
  gap) and ADR-0021 (a state read closes it) — no subscriber-side special-casing is needed to
  recover correctly from a drop.
- Isolation is native per backend rather than emulated: Redis's PEL, Postgres's per-subscriber
  outbox cursor, and the in-memory bounded channel are each the backend's own idiomatic mechanism,
  not a lowest-common-denominator abstraction bolted on top.

### Negative

- `OverflowBlock` is a loaded footgun by design — a caller who selects it and then never drains
  will leak memory in that subscription's pump goroutine indefinitely. This is accepted because the
  alternative (refusing to offer it at all) removes a legitimate use case (a caller that genuinely
  needs a complete feed and controls its own consumption rate), and the risk is scoped to the
  caller who opted in, never to any other subscriber or to the producer.
- Three backends means three different native isolation mechanisms to implement, test, and keep
  behaviorally consistent under the shared conformance suite (ADR-0018) — more surface area than a
  single generic bounded-queue implementation shared across all backends would have been.

### Neutral

- This ADR governs `Subscribe`'s fan-out exclusively. `Claim`'s internal doorbell (ADR-0033) has no
  `OverflowPolicy`, no per-subscriber buffer, and is not configurable by `SubscribeOptions` — it is
  a separate, simpler mechanism by construction (ADR-0019), and nothing in this ADR applies to it.

## Alternatives considered

**Unbounded buffering for every subscriber, always.** Rejected outright per 10 §7.1: guarantees no
loss until the process OOMs, which is not a guarantee, it is a deferred outage, and it is never an
acceptable default for a library embedded in a host process the operator does not get to
capacity-plan on the library's behalf.

**A single global overflow policy, not per-subscription.** Rejected: `NodeStatusChanged` and
`NodeReady` genuinely warrant different tolerances (history-preserving drop-oldest vs.
information-free coalescing), and different callers of the same `Manager` legitimately want
different tradeoffs (a debug UI wants `DropOldestAndMarkGap`; a compliance audit log wants
`CloseSlow` so it never silently loses a record) — a single library-wide policy would force one of
these use cases to accept a policy it doesn't want.

**Bounded-block as the default, matching a naive "reliable delivery" instinct.** Rejected per the
Reactive Streams lesson and the NATS/etcd precedent (10 §7.1): defaulting to a policy that can stall
the scheduler on the very first slow subscriber is precisely the head-of-line-blocking failure this
ADR exists to prevent; `OverflowBlock` remains available as an explicit, informed opt-in, never the
zero-value behavior.

## References

- docs/research/10-event-bus-and-delivery-semantics.md §7.1, §7.2, §8
- docs/research/08-go-api-and-concurrency-design.md §8.1–§8.5
- docs/research/00-synthesis.md §3 (ADR-22 seed), §4 (`OverflowPolicy`, `SubscribeOptions`)
- Reactive Streams JVM spec — https://github.com/reactive-streams/reactive-streams-jvm/blob/master/README.md
- NATS Slow Consumers — https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers
