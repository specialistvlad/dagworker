# ADR-0005: Predecessor satisfaction is tracked per-edge, not by a bare counter

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/02-incremental-topological-scheduling.md §8, docs/research/05-redis-backend.md §5, docs/research/04-postgres-backend.md, docs/research/00-synthesis.md §10.1

## Context

ADR-0003 decrements `pending[v]` on every predecessor completion. That decrement is atomic, but
atomicity is not idempotency: if the same logical event ("predecessor `u` satisfied its edge into
`v`") is delivered twice — a worker's ack retried after a timeout, a crashed instance replaying a
write-ahead log, an at-least-once queue redelivering — a raw `DECR` applies twice, and `v` becomes
ready one predecessor early, or, worse, ready while a *different*, real predecessor never finished
at all. The bug is silent and non-local: it manifests as a node running before its true dependencies
are satisfied, with no exception anywhere near the double-decrement that caused it. Every backend
this library targets is only ever at-least-once at the delivery layer — Redis Streams, Postgres
LISTEN/NOTIFY, and a crash-replay path all guarantee at-least-once, never exactly-once — so pushing
the idempotency problem to "the delivery layer will handle it" is not an option; something at the
counter layer itself must make the decrement safe.

Dossier 02 §8 surveys three structural answers. Strategy A (raw counter, idempotency pushed
entirely to the delivery layer) is free in storage but not actually a complete answer — it merely
relocates the problem to a subsystem that has to independently reinvent B or C's own mechanism to
provide the exactly-once guarantee it doesn't have natively. Strategy B (a per-edge "satisfied"
boolean, addressed by a dense slot assigned at `AddEdge` time and packed into a bitmap) is the
cheapest idempotent option by roughly three orders of magnitude at the project's target scale:
~366 KiB for a packed bitmap versus ~225–255 MB for a per-edge Postgres row or Redis key at
1,000,000 nodes/3,000,000 edges (out-degree 3). Strategy C (a per-node SET of not-yet-satisfied
predecessor IDs) costs roughly 80–115 MB in Redis's compact `intset` encoding at the same scale —
between A and packed-B, and it gets idempotency for free from `SADD`'s own no-op-on-existing-member
semantics rather than needing an explicit "already satisfied" branch.

This is where the research surfaced a genuine cross-dossier disagreement, resolved explicitly in
synthesis §10.1. Dossier 02 recommends the packed bitmap as the default, purely on the storage-cost
argument above. Dossier 05 (and, by the same reasoning, dossier 04's `edges.satisfied` boolean
column design) recommends the per-node-SET/per-edge-boolean shape instead — not primarily for
idempotency (the fencing-token-gated `Ack`/`Nack` CAS, ADR-0006, already makes the *terminal
transition* idempotent on its own) but because **dynamic edge removal needs addressable,
per-predecessor cancellation** that a slot-indexed packed bitmap cannot express safely without a
generation/slot-versioning scheme neither dossier fully designs — dossier 02's own Open Questions
section names this gap explicitly: "How does the packed-bitmap edge-slot scheme survive node/edge
removal?" Removing a predecessor edge from a node whose satisfaction is tracked by dense slot index
means either leaking the slot forever or reassigning it to a future edge, at which point a stale
delayed decrement for the *old* edge can corrupt the *new* one occupying its slot — exactly the kind
of ABA-shaped bug fencing tokens exist to prevent elsewhere in this design (ADR-0006).

## Decision

Predecessor satisfaction is tracked **per-edge, addressable by predecessor identity**, not by a bare
integer counter and not by a dense-slot-indexed packed bitmap. The concrete shape is backend-native:

- **Redis:** a `SET` per destination node containing the IDs of not-yet-satisfied predecessors.
  `SREM` on satisfaction; idempotent because `SREM` on an absent member is a defined no-op. `SCARD`
  gives `pending` directly — no separate counter field to keep in sync.
- **PostgreSQL:** an `edges.satisfied boolean NOT NULL DEFAULT false` column, addressed by the
  `(from_id, to_id)` primary key. A single `UPDATE edges SET satisfied = true WHERE from_id = $1 AND
  to_id = $2 AND satisfied = false RETURNING to_id`, chained into a `pending` decrement only for rows
  actually touched, is idempotent by construction — replay matches zero rows and is a correct silent
  no-op.
- **In-memory:** an adjacency structure plus a per-destination-node `pending` set (or equivalent
  bitset keyed by predecessor slot *that survives removal*, e.g. keyed by predecessor `NodeID`/handle
  rather than an insertion-order slot) — the decrement path checks and clears the specific
  predecessor's membership before touching the counter.

```go
// resolveEdge is the ONE idempotency-bearing primitive ADR-0003's decrement
// loop calls. It must check-and-clear atomically with any concurrent
// RemoveEdge (ADR-0036) touching the same (pred, target) pair.
func (s *Scheduler) resolveEdge(pred, target NodeID) (wasUnsatisfied bool) {
    // Backend-native check-and-set: Redis SREM's return value, Postgres's
    // UPDATE...WHERE satisfied=false row count, or an in-memory guarded
    // map-delete — never a blind DECR.
}
```

`RemoveEdge` (ADR-0036) uses this exact same structure to remove a specific, addressable
not-yet-satisfied predecessor — this is the capability the packed-bitmap alternative cannot
provide without an undesigned slot-versioning scheme, and it is why this ADR does not default to it.

The packed per-edge bitmap remains a valid **future** memory optimization and is explicitly
**deferred**, not rejected outright: it may be shipped as an opt-in v2 layout only once a
generation-counter or slot-versioning scheme that makes it safe under `RemoveEdge` is separately
designed and reviewed. It must never become the default while that gap remains open.

## Consequences

### Positive

- `RemoveEdge` (ADR-0036) can address and cancel a specific not-yet-satisfied predecessor
  unambiguously, by identity, at any time — the requirement that motivated this decision over the
  storage-cheaper alternative.
- Per-node observability comes for free: "what is this stuck node still waiting on" is a `SMEMBERS`
  call in Redis or a `WHERE satisfied = false` query in Postgres, with no separate debug index to
  build or maintain — a capability operators will need in production regardless of the idempotency
  question.
- The decrement is idempotent by construction (`SREM`-on-absent, `UPDATE...WHERE satisfied=false`)
  rather than by convention — there is no separate "check if already processed" step that could
  itself be implemented incorrectly.

### Negative

- Storage cost is materially higher than the packed-bitmap alternative: roughly 80–115 MB (Redis
  `intset`-encoded SETs) to ~225–255 MB (Postgres `edges.satisfied` rows) at 1,000,000 nodes/
  3,000,000 edges (out-degree 3), versus ~366 KiB for a packed bitmap — an 80–250× multiplier that
  is real, even though it is a single-digit percentage of dossier 04's own ~1–1.3 GB steady-state
  Postgres schema footprint at the same scale, and therefore not disqualifying at this project's
  target scale.
- Redis `SET`s for hub nodes with unusually high in-degree that exceed `set-max-intset-entries`/
  `set-max-listpack-entries` convert to hashtable encoding at 64–80 B/member, inflating the estimate
  further for skewed fan-in shapes — this must be documented as a known cost-model edge case, not
  silently absorbed into the "80–115 MB" figure above.

### Neutral

- This decision is orthogonal to the fencing-token mechanism (ADR-0006): the fencing token makes
  the *terminal Ack/Nack transition itself* safe against a stale worker; this ADR makes the
  *dependency-satisfaction bookkeeping downstream of that transition* safe against duplicate
  delivery of the same completion signal. Both are required; neither substitutes for the other.

## Alternatives considered

**Strategy A — raw counter, idempotency pushed to the delivery layer** (dossier 02 §8.2). Rejected:
storage-free, but not a complete answer — none of Redis Streams, Postgres LISTEN/NOTIFY, or a
crash-replay log provide native exactly-once delivery, so this strategy only relocates the problem
to a subsystem that would have to build strategy B or C anyway, at the messaging layer, with none of
the benefit of localizing the fix to the counter update itself.

**Strategy B — packed per-edge satisfied bitmap, dense slot assigned at `AddEdge` time** (dossier
02's own default recommendation, §8.6). Deferred rather than adopted as default, per synthesis
§10.1: it is ~3 orders of magnitude cheaper in storage, but it requires a stable, addressable slot
per predecessor that survives edge removal — a scheme neither dossier fully designs, and dossier
02's own Open Questions flags this gap explicitly. Addressing bits by `hash(predecessor_id) mod
width` instead of a dense slot is a false economy: a hash collision between two distinct
predecessors silently merges their satisfaction bits, and the node then waits forever on a
predecessor that will never separately report in.

**A per-node counter plus a separately-maintained hash-set purely for audit/observability, with the
counter as the sole correctness-bearing structure.** Rejected: this duplicates state (the counter
and the set can drift under a bug or a partial write) instead of making one structure do both jobs;
Strategy C's `SCARD`-as-`pending` design avoids this duplication entirely by construction.

## References

- docs/research/02-incremental-topological-scheduling.md §8.1-8.6 (including the 1M-node/
  out-degree-3 storage comparison table, §8.5)
- Redis SADD semantics (no-op on existing member): https://redis.io/docs/latest/commands/sadd/
- PostgreSQL explicit locking / row-level UPDATE semantics:
  https://www.postgresql.org/docs/current/explicit-locking.html
- docs/research/05-redis-backend.md §5
- docs/research/00-synthesis.md §10.1 (resolved contradiction), §3 (ADR-05 seed)
