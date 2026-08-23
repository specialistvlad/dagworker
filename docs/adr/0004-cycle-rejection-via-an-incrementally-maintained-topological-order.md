# ADR-0004: Cycle rejection via an incrementally maintained topological order

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/02-incremental-topological-scheduling.md §2.4, §2.9, §3, docs/research/04-postgres-backend.md §14.4, docs/research/00-synthesis.md §10.3

## Context

`AddEdge` must reject a cycle-forming edge synchronously — a host program wiring a dependency needs
to know immediately whether the graph it just asked for is legal, per dossier 12's dynamic-DAG
requirement. Checked from scratch, "does `u → v` create a cycle" is a reachability query — is `v`
already reachable from... equivalently, can `u` be reached from `v` — computed via BFS/DFS at
`O(V+E)` per insertion. At 1,000,000-node scale this is exactly as disqualifying as the naive
ready-set rescan ADR-0003 rejects, for the identical reason: a per-mutation cost proportional to
total graph size, not to the size of the change.

Forty years of incremental-topological-order literature exist to answer this exact question
cheaper. Dossier 02 surveys AHRSZ (1990), MNR (1996), Pearce–Kelly (2004/2007), Katriel–Bodlaender
(2006), Ajwani–Friedrich (2007), Haeupler–Kavitha–Mathew–Sen–Tarjan (2012), and Bender–Fineman–
Gilbert(–Tarjan) (2009/2016). All of them maintain some notion of ordering and use it as a
pre-filter: if the existing order already implies the edge is safe, accept it for free; only an
edge that threatens the invariant requires a bounded local search, and that search doubles as the
cycle check — there is no separate reachability subroutine needed. The algorithms differ almost
entirely in the tightness of their worst-case bound and the complexity of the machinery needed to
achieve it: HKMST's two-way search needs a potential-function argument over pairs of arcs on a
common path to get its `O(m^{3/2})` bound; BFGT needs an asymmetric two-part level/index labeling
scheme; Katriel–Bodlaender needs a priority-queue-driven affected-region search with a nontrivial
amortized argument. None of the three has a widely-used open-source reference implementation.
Pearce–Kelly, by contrast, needs one integer per node, two bounded DFS walks, and one array splice
— and it is the only algorithm in the survey with a real production-adoption trail: derivatives ship
inside Google's Abseil C++ library, TensorFlow, the JGraphT Java graph library, and the MonoSAT SAT
solver.

A second, separate question the research flags as *unresolved between dossiers rather than settled
by either alone*: how much of Pearce–Kelly to build before the first release. Dossier 02
recommends implementing PK's full bounded bidirectional-DFS-plus-splice mechanism as the primary
mechanism from day one. Dossier 04's concrete PostgreSQL DDL (§14.4) ships a simplified
single-node rank-bump approximation in its slow-path fallback instead, and is explicit that this is
a deliberately lighter v1 than PK's own full affected-region renumbering, whose fast-path hit rate
"should be validated... before committing to it long-term." Synthesis §10.3 resolves this as a
sequencing question, not a disagreement: both dossiers agree PK is the right algorithm family; they
disagree only on how much of it to build before measuring real DAG shapes.

## Decision

Every node carries an integer `ord(v)` — its position in a maintained topological order. `AddEdge`
folds cycle rejection into the same walk that maintains this order; there is no separate
reachability check:

```go
// addEdgeChecked is called with the store's node-order index already loaded.
// It performs BOTH the cycle check and the order-repair in one pass — cycle
// detection is a side effect of the same bounded search, never a second query.
func (g *graph) addEdgeChecked(pred, target NodeID) error {
    if g.ord[pred] < g.ord[target] {
        // Fast path, O(1): the invariant already holds. This is the
        // overwhelmingly common case for a DAG built in roughly causal
        // order (parents registered before children).
        g.insertEdge(pred, target)
        return nil
    }
    // Slow path: ord[pred] >= ord[target]. Run a forward search from target
    // bounded to {ord < ord[pred]} and a backward search from pred bounded
    // to {ord > ord[target]}. If the forward search reaches pred, a path
    // target ⇝ pred already exists — the new edge would close a cycle.
    if g.forwardSearchReaches(target, pred) {
        return &CycleError{Scope: g.scope, Path: g.lastSearchPath()}
    }
    // No cycle: the searches found exactly Δ = the affected region whose
    // relative order must change. Re-splice only Δ's ord values; every node
    // outside Δ is untouched, regardless of total graph size.
    g.repairOrder(pred, target) // v0.1/v0.2: single-node rank-bump (see below)
    g.insertEdge(pred, target)
    return nil
}
```

**v0.1/v0.2 ship the single-node rank-bump approximation from dossier 04 §14.4** on every backend
(in-memory, Redis, Postgres alike, for a single order-maintenance semantics across the fleet), not
full Pearce–Kelly affected-region renumbering. Both preserve the one load-bearing invariant —
`ord(u) < ord(v)` for every live edge `(u,v)` — and the rank-bump variant is materially simpler to
get right under a deadline. Instrument the fast-path (`ord[pred] < ord[target]`) hit rate as a
metric from the first release. **Escalate to full Pearce–Kelly affected-region renumbering only if
that metric shows degradation on real DAG shapes** — this is a measurement-triggered upgrade, not a
speculative one, per this project's "benchmark before publishing tuned numbers" discipline
(dossier 04 §16). Full PK, and BFGT beyond it, remain the documented upgrade path (ADR-0004 does
not need to be re-litigated to take it — only the internal `internal/topo` implementation changes).

The bounded-backward-search-only shortcut (walk back at most `B` hops, accept optimistically if the
budget is exhausted without finding a cycle) is explicitly **not** an acceptable substitute for the
sound check above at any stage: a confirming cycle path longer than `B` hops is silently accepted,
and the ready-set counter in ADR-0003 will simply never fire for the affected nodes — the graph
deadlocks with no error anywhere near the cause. It may be used only as a cheap **pre-filter** ahead
of the sound check (catches the overwhelming majority of real "oops, accidental self-loop"
mistakes fast), never as a replacement for falling through to the full check on a pre-filter miss.

Node and edge removal (ADR-0036) interacts with `ord` only by leaving gaps: a removed node's slot is
never reused and is not renumbered into; because a deleted node participates in no live edge, the
invariant `ord(u) < ord(v)` for every remaining live edge is preserved with zero renumbering cost.

## Consequences

### Positive

- Cycle rejection is `O(1)` in the overwhelmingly common causal-insertion case and proportional to
  the actual affected region (never total graph size) in the uncommon reordering case — the correct
  cost model for a 1,000,000-node scope.
- Cycle detection requires no separate index or reachability structure beyond the `ord` values the
  ready-set/order machinery already needs — dossier 02 §3.4's explicit recommendation against
  building a dedicated reachability index (2-hop labels, interval labels) purely for this question.
- The v0.1 rank-bump choice ships a correct (invariant-preserving), measurable system quickly, with
  an escalation path that requires no public API or storage-port change — only an internal
  algorithm swap inside `internal/topo`.

### Negative

- The single-node rank-bump approximation has a real (if currently unmeasured) risk of degrading
  toward `O(n)`-ish behavior on adversarial or heavily out-of-causal-order insertion patterns before
  the fast-path-hit-rate metric would catch it in production; this is an accepted, monitored risk,
  not a hidden one.
- `ord` values are internal state with no external stability guarantee — they must never be exposed
  as a public sort key or serialized to a subscriber, since renumbering (rank-bump today, PK
  splicing after escalation) can change them for unrelated nodes as a side effect of an insertion.

### Neutral

- This ADR decides the *mechanism and sequencing*; it does not commit to a specific affected-region
  size threshold for escalation — that is an implementation/ops decision made against the
  fast-path-hit-rate metric this ADR mandates be built from v0.1.

## Alternatives considered

**Naive O(V+E) DFS reachability check per insertion.** Rejected on arithmetic alone at
1,000,000-node scale — identical reasoning to ADR-0003's rejection of full-graph rescan.

**Bounded-backward-search shortcut without a sound fallback.** Rejected: dossier 02 §3.3 is explicit
that this silently corrupts the DAG invariant — a cycle whose confirming path exceeds the search
budget is accepted, and the failure manifests as a permanently-deadlocked node with no error
surfaced anywhere near the cause. Acceptable only as a pre-filter ahead of the sound check, never
standalone.

**Full Pearce–Kelly from v0.1** (dossier 02's own primary recommendation). Deferred, not rejected:
synthesis §10.3 resolves this as a sequencing choice — ship the simpler rank-bump approximation
first, instrumented, and escalate only once real DAG shapes demonstrate the need, rather than paying
PK's full implementation cost against unmeasured assumptions about this project's actual
edge-arrival distribution.

**Katriel–Bodlaender, HKMST, or BFGT as the starting implementation.** Rejected for v1: each has a
better asymptotic worst-case bound than Pearce–Kelly but meaningfully harder implementation
machinery (a priority-queue-driven amortized argument, a potential-function-over-arc-pairs argument,
or an asymmetric two-part labeling scheme respectively) and, per the research, no widely-used
open-source reference implementation to build from — unlike PK's Abseil/TensorFlow/JGraphT lineage.
Reserved as the credible upgrade path if PK's affected-region cost ever shows up as a profiled
bottleneck.

## References

- Pearce, D. J., Kelly, P. H. J. (2007). "A Dynamic Topological Sort Algorithm for Directed Acyclic
  Graphs." *ACM JEA* 11, article 1.7. https://www.doc.ic.ac.uk/~phjk/Publications/DynamicTopoSortAlg-JEA-07.pdf
- Bender, M. A., Fineman, J. T., Gilbert, S., Tarjan, R. E. (2016). "A New Approach to Incremental
  Cycle Detection and Related Problems." *ACM TALG*. https://arxiv.org/pdf/1112.0784
- docs/research/02-incremental-topological-scheduling.md §2.4, §2.9, §3
- docs/research/04-postgres-backend.md §14.4
- docs/research/00-synthesis.md §2.1 (hot path step 4), §10.3
