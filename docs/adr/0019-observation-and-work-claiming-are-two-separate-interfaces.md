# ADR-0019: Observation and work-claiming are two separate interfaces

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/10-event-bus-and-delivery-semantics.md §1, §4.3, §9; docs/research/01-prior-art-workflow-engines.md §13.1

## Context

Two structurally different signals leave the engine on every write: "here is what just happened to
this node" (an observation, read by any number of interested parties, tolerant of being durable)
and "a node in this scope may now be eligible for work" (a doorbell, consumed by exactly one
winner, tolerant of being lost or duplicated). Dossier 10 §1 is explicit that the entire design of
the reactive layer falls out of treating these as different problems instead of forcing one bus to
serve both. Conflating them — building one "subscribe and get handed work" channel — is the exact
failure the Reactive Streams initiative was created to fix: early push-based reactive libraries let
a fast producer overwhelm a slow consumer with no consumer-declared bound, and every later
production system that does real work-distribution (NATS JetStream's pivot from push to pull
consumers, SQS's pull-only `ReceiveMessage`, Postgres's `SKIP LOCKED` claim pattern) converges on
pull-for-the-claim, push-only-as-a-hint-on-top (10 §9.1).

The cost of getting this wrong is not stylistic. If "take this node" is modeled as delivery on a
subscription stream, then the stream's own guarantees — ordering, at-least-once redelivery,
backpressure policy (ADR-0022) — leak into the claim protocol's correctness story, and a subscriber
that merely wanted to *watch* status transitions for a dashboard ends up structurally entangled
with the single-winner competition semantics a real worker needs. Worse, it becomes an
un-doable breaking change to separate them later, because callers will have already built on
"receiving the event equals having the work."

The storage port compounds the reason to keep these separate: per AMD-1, every backend's fenced
mutation primitives (`Claim`, `Complete`, `Extend`, `Sweep` — AMD-2, see ADR-0007) are **mandatory
core** — a backend that cannot atomically grant single-winner ownership is not usable by this
library at all. The observation feed's backend facet (`dagstore.EventStream`) remains **optional**
and reports its durability tier truthfully via `CapabilitySet` (ADR-0016) — Postgres and Redis earn
a durable tier (outbox+relay, Streams+PEL); a backend that can only poll does not. A single
interface cannot have one method that is unconditionally mandatory and another that is
conditionally optional; they have to be two interfaces from the start.

## Decision

The event/reactive layer is exactly two Go-level contracts, never fused into one, at both the
public API and the storage port:

**Public API** (`dagworker`):

```go
// Observation — many readers, replayable per Seq (ADR-0020), recoverable via
// ErrCursorExpired (ADR-0021), backpressure-isolated per subscriber (ADR-0022).
// Receiving an event on this stream confers NO claim on any node.
func (m *Manager) Subscribe(ctx context.Context, opts SubscribeOptions) (*Subscription, error)
func (m *Manager) Handle(ctx context.Context, opts SubscribeOptions, fn func(Event)) (stop func(), err error)

// Work-claiming — single winner, atomic, fenced. The ONLY path that moves a
// node's Status to InProgress. Never reads Subscribe's event history to
// decide eligibility; always re-derives it from current storage state.
func (m *Manager) Claim(ctx context.Context, scope Scope, opts ...ClaimOption) (*Claim, error)
func (m *Manager) TryClaim(ctx context.Context, scope Scope, opts ...ClaimOption) (*Claim, error)
```

**Storage port** (`dagstore`, AMD-1/AMD-2 shapes):

```go
// Mandatory core — every backend implements this or is not a usable backend.
type Store interface {
    // ... CRUD + graph mutation ...
    Claim(ctx context.Context, scope string, req ClaimRequest) (Claimed, error)
    Complete(ctx context.Context, token ClaimToken, outcome Outcome) (readied []NodeRef, err error)
    Extend(ctx context.Context, token ClaimToken, d time.Duration) (deadline time.Time, err error)
    Sweep(ctx context.Context, scope string, limit int) (timedOut, readied []NodeRef, err error)
}

// Optional facet — durability tier reported via CapabilitySet (ADR-0016).
type EventStream interface {
    Subscribe(ctx context.Context, scope string) (<-chan Event, error)
}
```

`Claim` never consults `EventStream`. The `EventReady` notification that a completed `Complete`/
`Sweep` call emits (per the readied `[]NodeRef` it returns) is delivered to the *internal* doorbell
that the blocking `Claim` algorithm listens to (ADR-0033) — a private, uncapped-fan-in, coalescing
signal, not a subscriber channel — and, separately, may also be surfaced on the public `Subscribe`
stream as an `EventReady`-kind `Event` purely for observability or for a host building its own
custom dispatch loop directly on the doorbell. Both deliveries originate from the same commit; only
one of them (the internal doorbell) is load-bearing for `Claim`'s liveness, and losing the other
(a lagged or absent `Subscribe` fan-out) never changes claim correctness or eligibility.

A `Manager` implementation MUST NOT route `Claim`'s internal wakeup through the same bounded,
policy-governed channel that `Subscribe` (ADR-0022) uses — a subscriber under `OverflowBlock` or a
slow `Subscribe` consumer must never be able to delay the scheduler's own ability to notice new
work.

## Consequences

### Positive

- The claim path's correctness never depends on the observation path's delivery guarantees,
  buffer size, or backpressure policy — a host can run zero subscribers and still get every unit of
  work at full throughput.
- `EventStream` stays genuinely optional (ADR-0016's capability-facet pattern applies cleanly): a
  backend that can only poll for the observation feed is still a fully correct backend for claiming
  work, because claiming never routed through that facet.
- Each interface evolves independently. `Subscribe`'s filter/cursor/overflow surface (ADR-0020,
  ADR-0022) can grow without ever touching `Claim`'s signature, and `Claim`'s blocking algorithm
  (ADR-0033) can change its internal wakeup mechanism per backend without touching the public
  `Event`/`Subscription` types at all.
- Matches the pull-with-notification shape every production work-distribution system surveyed
  converges on once it has scale scars (NATS JetStream, SQS, `SKIP LOCKED` job queues — 10 §9.1),
  rather than reproducing a shape those systems specifically moved away from.

### Negative

- Two code paths to build, document, and test instead of one; a `Manager` implementation carries
  both a `Subscription` fan-out subsystem and an independent doorbell/wakeup subsystem, with their
  own goroutines, buffers, and shutdown ordering (ADR-0027 §9).
- The API surface must say loudly, in doc comments and not just in this ADR, that receiving
  `EventReady` on a `Subscription` confers no rights — a caller who builds a custom dispatch loop
  on top of `Subscribe` instead of calling `Claim` will observe races it did not expect unless it
  also implements its own fenced claim, which the public API deliberately does not expose a
  building block for outside of `Claim`/`TryClaim` itself.

### Neutral

- The wire `Event` type still carries `EventReady` as one of its two `EventKind` values (§10 of
  dossier 10) so that, for a single node, the ordering between its status change and its
  readiness doorbell is preserved by one stream if a caller chooses to observe both — this is a
  convenience for observability, not a claim mechanism, and does not contradict the two-interface
  decision above.

## Alternatives considered

**One fused "subscribe = get handed work" interface**, where taking a node is simply receiving an
event of kind `NodeReady` on the same channel every other subscriber uses. Rejected: this is
verbatim the Reactive Streams anti-pattern (10 §1, §9.1) — a slow or absent subscriber either stalls
every other subscriber (if the fan-out blocks) or silently drops real work opportunities (if it
doesn't), and there is no way to make exactly one of N identical subscribers "win" a `NodeReady`
delivery without inventing a second, ad hoc claim protocol layered on top — at which point the two
interfaces exist anyway, just entangled instead of separated.

**NATS core pub/sub as the sole take-a-node signal.** Rejected on the same evidence NATS's own team
used to justify JetStream's pull consumers: push-only distribution gives the broker no way to
respect a consumer's actual capacity, and NATS explicitly steers new work-queue designs toward pull
(10 §9.1).

**Kafka consumer-group rebalancing as the claim mechanism** (one partition assigned per consumer
stands in for "this consumer owns this work"). Rejected: partition assignment is a coarse,
whole-partition unit that does not compose with dag-worker-go's per-node fencing/lease/timeout model
(ADR-0006, ADR-0007) — a rebalance reassigns a partition wholesale, not one node's individual lease,
and Kafka's own guarantee is scoped to ordering within one partition (10 §3.2), not to a per-item
exactly-once-in-flight claim.

## References

- docs/research/10-event-bus-and-delivery-semantics.md §1, §4.3, §9.1, §9.2, §10
- docs/research/01-prior-art-workflow-engines.md §13.1, §15
- docs/research/00-synthesis.md §3 (ADR-19 seed), §4 (public API), §6 (storage port)
- Reactive Streams JVM spec — https://github.com/reactive-streams/reactive-streams-jvm/blob/master/README.md
- NATS JetStream pull consumers — https://docs.nats.io/nats-concepts/jetstream/consumers
