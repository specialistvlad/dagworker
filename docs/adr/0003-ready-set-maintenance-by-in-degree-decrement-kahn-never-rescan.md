# ADR-0003: Ready-set maintenance by in-degree decrement (Kahn), never rescan

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/02-incremental-topological-scheduling.md §1, §5.1, §7, §8.6

## Context

The library's mandated scale is 1,000,000 nodes per scope. At that scale, any operation whose cost
is proportional to graph size rather than to the size of the *change* being processed is
disqualifying — dossier 09's performance discipline expresses this as CI-gated dimensionless
ratios, not absolute latencies, precisely because "does this stay fast as the graph grows" is the
actual question, not "is this fast today." Ready-set computation is the operation performed most
often in the system's lifetime — once per node completion, at minimum — so its asymptotic shape
determines the shape of the whole system.

Kahn's 1962 algorithm already contains the answer, if read for the right idea. The textbook
presentation runs Kahn from scratch — `O(V+E)` — to produce one full topological order. That is the
wrong thing to imitate literally in a live scheduler: re-running Kahn from `L ← ∅` after every
completion is exactly the naive-rescan design this ADR rejects. But the *inner loop* of Kahn's
algorithm — for the node just popped, walk its out-edges and decrement each successor's counter,
enqueueing any that hit zero — is already an amortized-O(1)-per-edge streaming algorithm once you
stop rebuilding the outer loop from scratch. Dossier 02 makes the amortized argument precisely: no
single completion is bounded better than `O(outdeg(n))` in the worst case (a fan-out hub), but no
adversary can make the *sum* across the DAG's entire life worse than `O(E)`, because the aggregate-
method argument charges each edge exactly once, at the moment its tail node finishes. This is the
entire content of "the sublinear ready-set requirement": treat every node completion as a single
Kahn "pop," never as a trigger to re-derive readiness by scanning.

Two forcing details keep this correct once the DAG becomes dynamic rather than static, both from
dossier 02 §1.3:

1. A node inserted after some of its declared predecessors have already finished must have its
   `pending` counter initialized *at insertion time* by inspecting each predecessor's current
   status — not initialized to raw in-degree and then blindly decremented later, and not assumed to
   start at the full predecessor count.
2. An edge inserted whose predecessor `u` is already terminal-success must be born *pre-satisfied*
   — never incremented at all — or the target waits forever for a signal that will never arrive
   because `u` has already fired its one completion event.

Both of these are "compare-and-decide" operations, not blind `INCR`/`DECR` calls — the general
principle that a raw counter update is not sufficient once delivery can be duplicated or graph
shape can change concurrently is developed fully in ADR-0005, which this ADR depends on for the
concrete decrement primitive's idempotency.

## Decision

Maintain one `pending[v]` counter per node — the count of not-yet-satisfied predecessor edges — as
long-lived state, and update the ready set through exactly one code path: the completion decrement.
No other code path ever adds a node to the ready set.

```go
// OnNodeTerminal is called exactly once per node reaching a terminal Status
// (Success or Error — a terminal Error still resolves its outgoing edges for
// dependency-counting purposes; trigger-rule evaluation, ADR-0030, decides
// separately whether each successor proceeds, is skipped, or fails).
// out(n) is n's adjacency list, CSR-encoded (dossier 02 §7.2).
func (s *Scheduler) OnNodeTerminal(n NodeID) (readyNow []NodeID) {
    for _, m := range s.out(n) {
        if s.resolveEdge(n, m) { // idempotent per-edge resolution, ADR-0005
            if s.pending[m].Add(-1) == 0 {
                readyNow = append(readyNow, m)
            }
        }
    }
    return readyNow
}
```

Node/edge insertion follows the compare-and-decide rule from §1.3 of the research, not a blind
increment:

- `AddNode` with predecessor `p`: if `p.Status == StatusSuccess`, the edge is born satisfied (no
  increment); if `p.Status == StatusError`, the new node is immediately `Error/UpstreamFailed`
  (T11) rather than `New` — it is never inserted onto the ready set only to be evicted a moment
  later; otherwise `pending++` and the edge is recorded unsatisfied.
- `AddEdge` into an already-`Ready` node (`pending == 0`) must pop the node back out of the ready
  set and increment `pending` in the **same atomic operation** as the edge insert (T5) — a worker
  claiming the node in the gap between "edge recorded" and "counter incremented" is a correctness
  bug, not a performance one, and every backend's storage primitive for this operation must
  guarantee it (cross-reference: the storage port's fenced mutation primitives, ADR-0016).

Scope-wide "is everything done" is answered by a *separate* O(1) counter (ADR-0024), never by
summing or scanning `pending[]` values — mixing the two questions turns an O(1) check into an O(n)
one for no benefit.

## Consequences

### Positive

- Ready-set maintenance costs `O(outdeg(n))` per completion and `O(E)` total across the DAG's whole
  life — the textbook amortized bound, and the entire content of the "sublinear ready-set"
  requirement stated in dossier 02 §1.2.
- The rule is uniform across every actor that can complete a node — worker `Ack`/`Nack`, sweeper
  timeout, engine-driven cancellation/upstream-failure propagation — because "decrement out-edges
  on terminal transition" is actor-agnostic; there is exactly one ready-set-mutation code path to
  audit for correctness.
- Because the decrement is folded into the same atomic write as the terminal-status transition
  (ADR-0002's state table, T7/T8/T10/T11/T12/T13), a successor becoming ready is never observably
  out of sync with its predecessor's completion — no window exists where the predecessor is
  terminal but the successor's counter hasn't yet reflected it.

### Negative

- A single high-fan-out "hub" node's completion is an `O(outdeg(n))` operation that cannot be
  amortized away for that one call — a node with 100,000 successors produces one large atomic write.
  This is accepted as the correct cost model (the aggregate bound is what matters, not
  worst-single-call latency) but must be documented as a known tail-latency shape, and the storage
  port's fan-out write (ADR-0016, AMD-2) must handle it as a bulk operation, not `N` individual
  round trips.
- The design requires every backend to implement the decrement as part of a single atomic
  mutation alongside the terminal-status write (never a separate round trip) — this is a mandatory
  backend obligation, not an optional optimization, and rules out any backend whose native
  primitives cannot express it.

### Neutral

- This ADR only fixes *how* the ready set is updated (the decrement rule and its atomicity
  requirement); the concrete idempotency mechanism for `resolveEdge` (per-edge boolean vs. bare
  counter vs. packed bitmap) is ADR-0005's decision, and the cycle-safety of edge insertion itself
  is ADR-0004's.

## Alternatives considered

**Full-graph rescan per event** (re-run Kahn from `L ← ∅` on every completion, or an equivalent
"recompute readiness for the whole scope" SQL aggregate query per completion). Rejected outright:
`O(V+E)` per event at 1,000,000-node scale is disqualifying on arithmetic alone, and it is exactly
the anti-pattern dossier 02 opens by warning against — imitating Kahn's *outer loop* rather than its
inner one.

**Poll-based readiness** (a periodic sweep that scans for `pending == 0` nodes not yet enqueued,
instead of an event-driven push on every decrement). Rejected: turns an O(1)-per-edge push into an
O(n)-per-tick scan, and introduces a latency floor (the poll interval) with no correctness benefit
over the push-based decrement, which dossier 02 §5.1 treats as strictly dominant once the decrement
is made idempotent (ADR-0005).

**Recompute `pending` from a live `COUNT` over unresolved edges on every readiness check** (rather
than maintaining a resident counter). Rejected: turns every single readiness check into a query
proportional to in-degree, defeating the entire point of caching the count — the resident-counter
design is what makes the check itself O(1) rather than merely making the *update* efficient.

## References

- Kahn, A. B. (1962). "Topological sorting of large networks." *Communications of the ACM*, 5(11),
  558–562. https://dl.acm.org/doi/10.1145/368996.369025
- docs/research/02-incremental-topological-scheduling.md §1, §5.1, §7, §8.6
- docs/research/00-synthesis.md §2.1 (hot path), §3 (ADR-03 seed), §5 (state-machine table, T3)
