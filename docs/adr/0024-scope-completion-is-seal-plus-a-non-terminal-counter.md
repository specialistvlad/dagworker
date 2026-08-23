# ADR-0024: Scope completion is Seal plus a non-terminal counter

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/12-dag-semantics-and-state-machine.md §2.1, §2.7, §6.3

## Context

"Is this scope done?" is undecidable from local information alone in a graph that can still grow.
A naive check — `SELECT COUNT(*) FROM nodes WHERE scope=? AND status IN (New, InProgress)` reaching
zero — races against a concurrent `AddNode`/`AddNodes` call in exactly the way distributed
termination detection exists to close: the count can hit zero for one instant while a caller
(commonly a worker that just `Ack`ed and is about to fan out into new children, per the dynamic
fan-out pattern ADR-0030's trigger rules are built to support) is *between* its `Ack` and its next
`AddNodes` call, with nothing in the data model recording that an obligation is still outstanding.
This is structurally identical to the problem Dijkstra and Scholten solved for detecting
termination of a "diffusing computation" — a dynamically-growing tree/DAG of active processes where
the question is exactly "has everything gone idle, permanently, with nothing in flight." Their
algorithm has each process track a `Deficit` — the imbalance between messages received and signals
sent — and only report itself terminated once that deficit is balanced on every edge. Safra's
ring/token algorithm solves the same general problem without needing a spanning tree, at the cost
of a full ring traversal's latency per check.

Both of those algorithms solve a strictly harder problem than dag-worker-go actually has. Dijkstra–
Scholten and Safra both assume an arbitrary communication topology of processes exchanging anonymous
messages with no shared, inspectable state. dag-worker-go's version of "is the computation done" is
easier by construction: nodes do not send messages to each other across an arbitrary topology — they
mutate a single shared, already-durable, transactionally-inspectable graph structure that the engine
itself owns. The general algorithms' full machinery (spanning-tree deficit bookkeeping, or a
circulating token) is solving a problem this project does not have; what dag-worker-go needs is the
*fixed point* of that idea, specialized to a system where the engine can atomically inspect and
update its own state.

The remaining piece the general algorithms cannot supply on their own: the "no more children will
ever be spawned" fact. In Dijkstra–Scholten, a process reports this about itself as part of the
protocol. In dag-worker-go, only the caller/host program can make this assertion with authority —
the library has no way to infer "the operator does not intend to call `AddNode` against this scope
again" from graph state alone, any more than a diffusing-computation process could infer another
process's intent to stay quiet. This must be an explicit, caller-driven signal, never an inferred
one, or the conservative default (never report complete) collapses into an unsound guess.

## Decision

A scope carries two pieces of state, both maintained transactionally by the storage backend as part
of every relevant node transition, never recomputed by scanning:

```go
// Scope-level completion state. Sealed is caller-driven (Manager.Seal),
// never inferred. notTerminalCount is maintained atomically by every T1-T14
// transition in the state-machine table (ADR-0001/ADR-0002) that changes a
// node's terminal/non-terminal status, INCLUDING node removal (ADR-0036) —
// see the removal-interaction note below.
type scopeState struct {
    Sealed           bool
    notTerminalCount int64 // atomic; never derived by scanning
}

// IsComplete is O(1) against maintained state — never a scan.
func (s *scopeState) IsComplete() bool {
    return s.Sealed && s.notTerminalCount == 0
}
```

`notTerminalCount` increments exactly once per node on first appearance (T1/T2 — a node is
non-terminal from the instant it exists) and decrements exactly once when that node reaches a
terminal `Status` (`StatusSuccess` or `StatusError`, via any of T7/T8/T9's eventual terminal
landing/T10/T11/T12/T13) **or** is removed while still non-terminal (ADR-0036: `RemoveNode` on a
`Blocked`/`Ready` node ends that node's outstanding obligation without it ever reaching a terminal
`Status`, and must decrement the counter for exactly the same reason a terminal transition would —
the counter tracks "nodes with an obligation still outstanding," and removal ends the obligation
just as completion does). A node that is *already* terminal when removed has already been
decremented at the point it became terminal; its removal has no further effect on the counter.

`Manager.Seal(ctx, scope)` sets `Sealed = true` and is the caller's one-time, explicit assertion "I
will not call `AddNode`/`AddNodes`/`AddEdge` against this scope again." It is irreversible for the
life of the scope, is idempotent (sealing an already-sealed scope is a no-op, not an error), and
never inferred from graph state, elapsed time, or a quiet period with no calls — an **open** scope
(the default, pre-`Seal`) never reports `IsComplete() == true` regardless of counter state, which
is the conservative, safe default matching how a diffusing computation with no explicit "no more
children" signal from every leaf can never be proven terminated by definition, not by a bug.
`Manager.IsComplete(ctx, scope)` reads this state directly; it never triggers retention/GC as a side
effect (ADR-0024 is about detecting completion, not about what happens after — retention runs on
its own TTL/low-water-mark schedule per the `ScopeConfig` retention policy, independent of `Sealed`).

`Scope.Health()` layers on top of `IsComplete()` as a reason-aware aggregate (per ADR-0001's
`Outcome.Reason`-carries-skip design): a scope can be complete while containing `ReasonSkipped`
nodes without that constituting failure, which a naive "any node has `StatusError`" check would
misreport.

## Consequences

### Positive

- Termination detection collapses to a single O(1) local check because the graph is one owned,
  transactionally-inspectable store — the general Dijkstra-Scholten/Safra machinery (spanning-tree
  deficit accounting, or a circulating ring token) is provably unnecessary here, not merely
  simplified away by assumption.
- The counter is maintained by the same atomic writes ADR-0001/ADR-0002's transition table already
  performs — no second pass, no separate reconciliation job, and no window where the counter can
  disagree with the set of actually-non-terminal nodes.
- Sealing is cheap (one boolean flip) and the conservative default (open, never reports complete)
  fails safe: a caller that forgets to seal simply never sees `IsComplete() == true`, which is
  observable and debuggable, rather than a false-positive completion signal that silently drops
  work a caller intended to add later.

### Negative

- A caller that forgets to call `Seal()` gets a scope that never reports complete, indefinitely —
  this is the correct behavior, not a bug, but it is a real operational footgun that must be
  documented prominently and made observable (e.g., a metric or `Scope.Health()` field surfacing
  "not sealed" distinctly from "sealed but incomplete") so it is diagnosable rather than a silent
  hang.
- `RemoveNode`'s interaction with `notTerminalCount` (decrementing on removal of a non-terminal
  node) is a detail this ADR introduces beyond the literal state-machine table in ADR-0001/ADR-0002
  — implementers must ensure every removal code path (ADR-0036) is wired into this counter exactly
  once, or the counter can drift and `IsComplete()` never reaches true for a scope whose graph is
  genuinely finished.

### Neutral

- This is a scope-wide counter, deliberately separate from the per-node `pending[]` counters
  ADR-0003 maintains for ready-set purposes — the two answer different questions ("is this one node
  ready" vs. "is the whole scope done") and must not be conflated into one structure or one update
  path, even though both are decremented by overlapping sets of transitions.

## Alternatives considered

**Naive live `COUNT(*) WHERE status IN (New, InProgress)` poll, no `Sealed` flag.** Rejected: races
against a concurrent `AddNode` in exactly the way Dijkstra-Scholten's diffusing-computation problem
exists to formalize — the count can transiently hit zero while a caller is between an `Ack` and its
next fan-out `AddNodes` call, producing a false-positive completion signal with no correctness
recourse.

**Full Dijkstra-Scholten deficit/spanning-tree bookkeeping**, tracking per-edge send/acknowledgment
balance rather than a single scope-wide counter. Rejected as solving a harder problem than this
system has: Dijkstra-Scholten's machinery exists to handle an arbitrary anonymous-message topology
with no shared inspectable state, which does not describe dag-worker-go's single-owned-store model
— building it anyway would add real implementation and verification cost for a generality this
project never needs.

**Safra's ring/token termination detection.** Rejected for the same reason plus an additional one:
it requires a full ring traversal's latency per termination check, which is the wrong performance
shape for a check this project wants to be O(1) and callable freely (`Manager.IsComplete` is
documented as never blocking on I/O it doesn't own, per the public API's own concurrency contract).

**Time-based inference of "done"** (e.g., declare a scope complete after N seconds with no new
`AddNode` calls). Rejected: this is exactly the unsound guess the conservative `Sealed`-gated design
is built to avoid — a legitimately slow but still-active caller (a worker doing real I/O before its
next fan-out call) would be indistinguishable from a caller that is genuinely finished, and no
timeout value can resolve that ambiguity safely.

## References

- Dijkstra, E. W., Scholten, C. S. (1980). "Termination detection for diffusing computations."
  https://en.wikipedia.org/wiki/Dijkstra%E2%80%93Scholten_algorithm
- Termination detection (Safra's algorithm survey): https://en.wikipedia.org/wiki/Termination_detection
- docs/research/12-dag-semantics-and-state-machine.md §2.1, §2.7, §6.3
- docs/research/00-synthesis.md §3 (ADR-24 seed), §5 (state table, "Sealed && notTerminalCount"
  invariant), §2.3 (hot path step 7)
