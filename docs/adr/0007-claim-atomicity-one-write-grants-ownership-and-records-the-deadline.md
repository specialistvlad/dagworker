# ADR-0007: Claim atomicity: one write grants ownership and records the deadline

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/01-prior-art-workflow-engines.md §11.2, §18; docs/research/03-leases-heartbeats-timeouts.md §5b-d; docs/research/08-go-api-and-concurrency-design.md §9.1 (Claim polling loop); docs/research/10-event-bus-and-delivery-semantics.md §9 (doorbell); AMD-1, AMD-2, AMD-4 (owner amendments)

## Context

Dossier 01's cross-system leasing survey (§18) makes the single strongest actionable finding in the
whole research series: the systems that need **no external sweeper process** — Faktory, Asynq (whose
recoverer ships built into the library, not bolted on by the operator), Kubernetes finalizers,
Nomad's plan-apply — are uniformly the ones where "the storage layer itself enforces the timeout as
part of the same atomic operation that granted the claim." The one mechanism surveyed that
structurally *requires* a bolted-on external sweeper — Sidekiq's base `BRPOPLPUSH` pattern — is also
"the one with the most documented production incidents in this survey." SQS reinforces the same
lesson from the other direction: it has no fencing token at all (03 §2.1), and Google's own Pub/Sub
documentation admits that "acknowledgment deadlines are not guaranteed to be respected unless you
enable exactly-once delivery" — i.e., even the deadline check itself is best-effort in the default
path, "the strongest argument in this entire survey for building the fencing token into the storage
layer rather than trusting 'the deadline hasn't passed yet' as a safety property on its own" (03
§2.2).

The original design synthesis (§6) modeled the mandatory storage core as generic CRUD plus a bare
`Transition(ctx, scope, id, from, to NodeStatus) error`, with the actual fenced claim/ack/sweep
mechanics living behind an *optional* `ReadyQueue`/`TimeoutSweeper` facet reached by type assertion.
The project owner rejected this shape (AMD-1, AMD-2): `Transition` carries no fencing token, no
structured `Outcome`, and — critically — cannot express the successor fan-out that must land in the
**same** atomic write as a completion (per the hot-path description, synthesis §2.3 steps 3-4: every
direct successor's pending-predecessor count is decremented, and any successor that hits zero is
pushed to the ready-set, inside the identical write that records the ack). A `Store` that cannot do
this atomically is not a backend this library can run against at all, so gating it behind an optional
facet produces an interface that type-checks and then fails at runtime — a category of bug this
project rejects outright.

## Decision

The storage port's mandatory core (binding on in-memory, Redis, and Postgres — memcached implements
none of this, ADR-0017) replaces the bare `Transition` primitive with four fenced operations:

```go
package dagstore

// Claim atomically selects one eligible node, bumps LeaseEpoch (ADR-0006),
// and records a deadline read from the backend's own clock (ADR-0008) — all
// in the SAME write. No backend may split "pick a node" from "set the
// deadline" into two operations.
func (s Store) Claim(ctx context.Context, scope string, req ClaimRequest) (Claimed, error)

// Complete is the only terminal-write path. Fenced on the presented token;
// in the SAME atomic operation it decrements every direct successor's
// pending-predecessor count (ADR-0003) and returns whichever successors
// just became ready, so the engine can emit EventReady with no second round
// trip. Ack and Nack (the public Manager surface) both compile down to one
// Complete call carrying a different Outcome.Reason.
func (s Store) Complete(ctx context.Context, token ClaimToken, outcome Outcome) (readied []NodeRef, err error)

// Extend resets the deadline under the identical fencing discipline (ADR-0010).
func (s Store) Extend(ctx context.Context, token ClaimToken, d time.Duration) (deadline time.Time, err error)

// Sweep finds every node whose deadline has elapsed, reclaims each one
// fenced on the epoch it observed, and returns both what timed out and what
// that unblocked — the identical "mutation plus fan-out in one write" shape
// Complete makes, so the sweep path and the claim path share one atomicity
// guarantee instead of two.
func (s Store) Sweep(ctx context.Context, scope string, limit int) (timedOut []NodeRef, readied []NodeRef, err error)
```

(Field-level shapes of `ClaimRequest`/`Claimed`/`NodeRef` are the implementation spec's job — this
ADR fixes the four-verb shape and the atomicity/fan-out guarantee each verb makes, not their exact
struct layout.) Per **AMD-1**, these four operations are part of the **mandatory** core alongside
node CRUD and graph mutation — never behind an optional `ReadyQueue`/`TimeoutSweeper` facet reached
by type assertion (superseding the classification sketched for ADR-0016 in the original synthesis).
The genuinely optional facets remain `Lister`, a durable-tier `EventStream`, `ConditionalDeleter`,
and `BatchClaim` (a batched multi-node `Claim`, an efficiency-only facet layered on the same fencing
rule).

Backend realizations: **Postgres** — one `SKIP LOCKED` CTE chained into `UPDATE ... RETURNING` for
`Claim`; `Complete`/`Sweep`'s fan-out is a chained CTE that also updates successors' `pending` column,
using a deterministic ascending-`node_id` lock order to avoid the fan-in deadlock class. **Redis** —
one Lua Function per verb, each performing the hash mutation and ZSET (deadline index) maintenance in
one `EVALSHA`. **In-memory** — one mutex-guarded operation per verb performing the ready-set pop or
push and the CSR out-edge walk.

**Blocking `Claim` at the public `Manager` layer (AMD-4).** The storage-level `Claim` above is
non-blocking — it returns immediately with `ErrScopeEmpty` when nothing is ready. `Manager.Claim`
(public API) layers a specified, non-implicit wakeup path on top, so it never degenerates into a
busy-loop or a hang:

```go
func (m *Manager) Claim(ctx context.Context, scope Scope, opts ...ClaimOption) (*Claim, error) {
    for {
        claim, err := m.store.Claim(ctx, scope, cfg)
        switch {
        case err == nil:
            return claim, nil
        case errors.Is(err, ErrScopeEmpty):
            select {
            case <-m.readyDoorbell(scope):      // EventReady (ADR-0019), best-effort
                continue                         // re-try the non-blocking claim immediately
            case <-time.After(jitteredPollInterval()): // fallback for best-effort doorbells
                continue
            case <-ctx.Done():
                return nil, fmt.Errorf("dagworker: Claim: %w", context.Cause(ctx))
            }
        default:
            return nil, fmt.Errorf("dagworker: Claim: %w", err)
        }
    }
}
```

The order is fixed: **(1)** an immediate non-blocking try, **(2)** wait on the `EventReady` doorbell
(ADR-0019), **(3)** a jittered poll as the fallback for backends whose doorbell is best-effort (every
backend's is, by ADR-0019's own design) — never only step 1+3 (a busy-loop) and never only step 1+2
(a hang on a backend whose doorbell drops the one notification that mattered). `TryClaim` is the
non-blocking variant that returns `ErrScopeEmpty` on the first miss instead of entering this loop.

## Consequences

### Positive

- Matches the only pattern surveyed needing zero external sweeper for correctness (01 §18); `Sweep`
  reuses the exact fenced primitive `Claim` would use for a reclaim, whether invoked inline on the
  claim path or on a ticker.
- Collapses "hand out work" and "fan out completions" to one round trip each (`Claim`, `Complete`)
  instead of a completion write followed by a separate successor-readiness scan — closing the exact
  race AMD-2 exists to prevent (a worker claiming a "not yet visible" successor in the gap between two
  writes).
- The specified doorbell-then-poll order (AMD-4) means every backend behaves identically from the
  caller's point of view regardless of whether its `EventReady` tier is durable or best-effort.

### Negative

- `Complete`'s signature is backend-heavy: implementing it requires knowing the DAG's out-edges, not
  just a node's own record — a backend cannot satisfy `Store` by wrapping a plain key-value store
  without also indexing edges, which is precisely why memcached cannot be a `Store` at all (ADR-0017).
- The fan-out-in-one-write requirement is materially more implementation work per backend than a
  naive "complete, then separately query and push successors" design would be.
- The jittered-poll fallback means a caller on a best-effort-doorbell backend still pays a bounded
  latency tail even when nothing is wrong — an accepted cost, not a bug, per AMD-4.

### Neutral

- `Claimed`/`NodeRef`/`ClaimRequest` are new types this ADR does not fully specify — that is the
  implementation spec's job, per AMD-2's own instruction that the ADR records the shape and the why.

## Alternatives considered

**Keep `Transition(from, to)` as the mandatory primitive, with claim/ack/sweep behind an optional
`ReadyQueue`/`TimeoutSweeper` facet** (the original synthesis draft). Rejected by owner decision
(AMD-1, AMD-2): carries no fencing token, no `Outcome`, and cannot express successor fan-out in one
write; an interface that compiles without these isn't a backend this library can actually run
against, which the project treats as worse than a compile-time refusal.

**Two separate round trips — a completion write, then a follow-up "list newly ready nodes"
query.** Rejected: reopens exactly the race AMD-2 exists to close (a worker claiming a successor in
the visibility gap between the two writes) and doubles storage round trips on the hottest path in the
system (03 §5, "hot path Ack/Nack").

**Model `Complete` after Redis Streams' `XACK` plus a separate readiness poll.** Rejected: Streams has
no facility for a caller-chosen per-claim deadline (03 §2.4) and no fan-out primitive at all — a
Streams-based design would still need a second, bespoke mechanism for successor push, buying nothing
over the unified design here.

**Background-only sweeper with no inline reclaim on the claim path.** Rejected: 01 §18's own
comparison table shows this is exactly Sidekiq's documented failure mode; `Sweep` must use the
identical fenced primitive `Claim` would use for a reclaim, whether run inline or on a ticker — never
a separate, independently-fallible cron-style process.

**Pure busy-loop or pure long-poll-and-hang for blocking `Claim`, with no specified fallback**
(leaving AMD-4 implicit, as the original synthesis did). Rejected: a busy-loop wastes CPU and hammers
storage under contention; a pure long-poll with no polling fallback hangs forever on any backend
whose doorbell delivery is merely best-effort — which, per ADR-0019, is every backend's `EventReady`
tier by design, making this the more dangerous of the two failure modes to leave unspecified.

## References

- Amazon SQS visibility timeout docs — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- Google Cloud Pub/Sub lease management — https://docs.cloud.google.com/pubsub/docs/lease-management
- `context.AfterFunc` — https://pkg.go.dev/context#AfterFunc
- docs/research/01-prior-art-workflow-engines.md §11.2, §18
- docs/research/03-leases-heartbeats-timeouts.md §5b-d
- docs/research/08-go-api-and-concurrency-design.md §9.1, Claim polling-loop example
- docs/research/10-event-bus-and-delivery-semantics.md §9.1-9.2
- docs/research/00-synthesis.md §2.1-2.3, ADR-07 seed
