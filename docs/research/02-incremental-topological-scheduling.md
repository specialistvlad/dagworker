# Incremental Topological Scheduling: Maintaining a Ready-Set Over a Dynamic DAG in Sublinear Time

Scope: this dossier covers the *algorithms and math* side of the ready-set problem for
`dag-worker-go` — how to know, after any single mutation (a node completes, an edge is
added, a node is cancelled), which nodes just became eligible for a worker to pick up,
without ever re-scanning the whole graph. It deliberately stays below the distributed-systems
layer (leases, partitioning, multi-instance coordination belong in a different research
file) and instead answers: what is the cheapest correct data structure and update rule,
what do the 40 years of incremental-topological-order literature actually buy you over
naive Kahn, and how does all of it degrade gracefully onto a KV store that only offers
`GET`/`SET`/`INCR`/atomic-scripts instead of pointers.

---

## 1. Baseline: Kahn's algorithm and the in-degree-counter trick

### 1.1 Static Kahn (1962)

Kahn's algorithm is the origin of every "ready set" scheduler in this space. It topologically
sorts a DAG by repeatedly stripping source vertices (in-degree 0) [Kahn, A. B. (1962). "Topological sorting of large networks." *Communications of the ACM*, 5(11), 558–562](https://dl.acm.org/doi/10.1145/368996.369025):

```
L ← empty list                       // the output order
S ← { v ∈ V : indeg(v) = 0 }         // the ready set
while S ≠ ∅:
    remove a node n from S
    append n to L
    for each edge (n, m):
        indeg(m) ← indeg(m) - 1
        if indeg(m) = 0:
            insert m into S
if |L| < |V|: report a cycle          // leftover in-degree > 0 everywhere ⇒ cycle
```

Run once, from scratch, this is `O(V + E)` — every vertex is dequeued once, every edge is
examined once for the decrement. That bound is optimal for *producing a full order once*.
It is the wrong algorithm to re-run on every event in a live scheduler, and the wrong thing
to imitate literally — the useful idea is not the full-graph scan, it is what happens
**inside** the `for each edge (n, m)` loop.

### 1.2 The trick: never rescan, only decrement

A scheduler doesn't need a global order at all — it needs the *ready set*, refreshed
incrementally. Kahn's algorithm already computes that increment for free: the only work
triggered by finishing node `n` is walking `n`'s **out-edges** and decrementing each
successor's counter. Nothing else in the graph is touched. So instead of re-running Kahn
from `L ← ∅` after every completion, you keep the `indeg[]` array (renamed `pending[]` once
edges start being added dynamically — see §1.3) resident as long-lived state and treat each
node completion as a single Kahn "pop":

```go
// OnNodeSucceeded is called exactly once per node completion.
// out(n) is n's adjacency list — see §7.2 for the CSR encoding.
func (s *Scheduler) OnNodeSucceeded(n NodeID) []NodeID {
    var readyNow []NodeID
    for _, m := range s.out(n) {
        if s.pending[m].Add(-1) == 0 {   // atomic decrement, see §8
            readyNow = append(readyNow, m)
        }
    }
    return readyNow
}
```

The cost of this operation is **`O(outdeg(n))`**, not `O(V+E)`. That is the entire content
of "the in-degree-counter trick": Kahn's algorithm is already an amortized-`O(1)`-per-edge
streaming algorithm if you stop rebuilding it from scratch. Summed over the DAG's whole
life, `Σ outdeg(n) = E`, so the *total* work done by ready-set maintenance across every
completion that will ever happen is `O(E)` — each edge is charged exactly once, at the
moment its tail finishes. This is a textbook amortized-analysis argument (aggregate
method): no single completion is bounded better than `O(outdeg(n))` worst case (a fan-out
hub node), but no adversary can make the *sum* worse than `O(E)`, because each edge can only
ever be traversed once by this rule.

### 1.3 What changes when the DAG is dynamic

Two things break the static picture, and both matter for `dag-worker-go`:

1. **Edges/nodes arrive after some ancestors have already finished.** A newly inserted node
   `v` must start with `pending[v] = indeg(v)` computed *at insertion time*, not at
   graph-birth time — trivial as long as node creation is the only writer of the initial
   counter (§8.5 revisits this once retries enter the picture).
2. **A newly inserted edge `(u, v)` where `u` is already done.** If you insert the edge and
   then blindly set `pending[v] += 1`, `v` will wait forever for a predecessor that already
   finished. The insertion path must therefore check `u`'s current status atomically as part
   of the same operation: if `u` is terminal-success, do **not** increment `pending[v]`
   (the edge is born already-satisfied); if `u` is pending, increment normally. This is a
   compare-and-decide, not a blind `INCR`, and it is the first of several places in this
   design where "just call INCR/DECR" is the wrong primitive — see §8.

### 1.4 Data structures for the baseline

| Structure | Purpose | Shape |
|---|---|---|
| `pending[v]` | remaining unsatisfied predecessors | array/hash `NodeID → uint32`, one atomic counter per node |
| `out(v)` | successors to notify on completion | CSR adjacency (§7.2) or per-node adjacency list |
| ready set | nodes with `pending = 0` awaiting a worker | FIFO queue / bitset / heap (§7.3) |
| status | new / in-progress / success / error | one byte per node, or folded into the same record as `pending` |

Everything past this point in the dossier is about (a) what to do when edges can also be
**added** after the graph is already running — which perturbs a maintained order, not just a
counter — and (b) how to make the counter update in §1.2 survive at-least-once delivery,
crashes, and concurrent writers across multiple `dag-worker-go` instances.

---

## 2. Incremental / online topological-order maintenance

Kahn's counter trick is sufematically enough to answer "who is ready to run" — it needs no
topological *order*, only in-degree. But `dag-worker-go` also needs to **reject or detect
cycles on edge insertion** (§3) and, in some designs, to expose a consistent order for
tie-breaking, priority, or debugging. That requires maintaining an actual topological
ordering online, as an adversary inserts edges one at a time and the algorithm must either
update the order or report that the new edge closes a cycle. This is a 35-year-old research
line; the papers differ enormously in whether they are "read the abstract and implement it
this afternoon" or "read the abstract and hire the authors."

### 2.1 Problem statement

Maintain a strict total order `ord : V → {1..n}` consistent with all edges seen so far
(`(u,v) ∈ E ⇒ ord(u) < ord(v)`), online, as edges are inserted one at a time, minimizing the
total work over a sequence of `m` insertions. Every algorithm below is judged on the same
axis: total time for `m` insertions into an initially edgeless graph on `n` vertices, i.e. an
**amortized** cost, because the worst single insertion in *every* known algorithm can be made
`Θ(n)` by an adversary (an edge from the current last vertex to the current first vertex
forces a full re-numbering) — the question is only how bad the *sum* over `m` insertions gets.

### 2.2 Alpern–Hoover–Rosen–Sachs–Zadeck (AHRSZ), 1990 — the ancestor

Alpern, Hoover, Rosen, Sachs, and Zadeck (SODA 1990, "Incremental evaluation of computational
circuits") is the earliest algorithm in this family and the one every later paper compares
itself to (Pearce and Kelly's own comparison table explicitly ranks it as having "a strictly
tighter bound on its runtime than PK" — i.e. AHRSZ theoretically dominates Pearce–Kelly, it
just isn't what practitioners reach for) [Pearce & Kelly, *A Dynamic Topological Sort
Algorithm for Directed Acyclic Graphs*, JEA 11 (2007)](https://www.doc.ic.ac.uk/~phjk/Publications/DynamicTopoSortAlg-JEA-07.pdf).
AHRSZ works by discovering the affected region with a bounded bidirectional search (forward
from the edge's head, backward from its tail) and renumbering only that region — the same
core idea every subsequent paper (PK, KB, HKMST, BFGT) reuses in some form. Treat it as the
"first draft" of the technique below rather than something to implement directly; nobody in
the applied literature ships raw AHRSZ, they ship Pearce–Kelly's simplification of it.

### 2.3 Marchetti-Spaccamela–Nanni–Rohnert (MNR), 1996 — the other classical baseline

MNR is the second pre-2000 algorithm cited as a practical baseline in the Pearce–Kelly
comparison; the JEA paper's own empirical section notes "comparing MNR and PK is more
subtle, since neither achieves a strictly tighter bound than the other" — i.e. MNR and PK
are incomparable in the worst case, each winning on different graph shapes, and both are
DFS-based affected-region algorithms of essentially the same flavor as AHRSZ.

### 2.4 Pearce and Kelly (WEA 2004 / JEA 2007) — the one to actually implement

**"A Dynamic Topological Sort Algorithm for Directed Acyclic Graphs"**, D. J. Pearce and
P. H. J. Kelly, *ACM Journal of Experimental Algorithmics* 11 (2007), article 1.7
[[PDF]](https://www.doc.ic.ac.uk/~phjk/Publications/DynamicTopoSortAlg-JEA-07.pdf),
conference precursor WEA 2004 [[PDF]](https://www.doc.ic.ac.uk/~phjk/Publications/DynTopoSortWEA2004.pdf).

**Mechanism.** Every vertex carries an integer `ord(v)` — its position in the maintained
topological order, stored so the order can be read off directly with no extra pass. To
insert edge `(x, y)`:

1. If `ord(x) < ord(y)` already, the invariant holds — accept the edge, **no search at all**.
   This is the fast path and, for a scheduler where edges are usually added in roughly
   causal order (a parent added before its child), it is the *overwhelmingly common* case.
2. Otherwise (`ord(x) ≥ ord(y)`), the edge threatens the invariant. Run a forward DFS from
   `y` restricted to vertices with `ord < ord(x)` (the set `Δᶠ`, vertices that might need to
   move after `x`) and a backward DFS from `x` restricted to vertices with `ord > ord(y)`
   (the set `Δᵇ`). **If the forward search reaches `x` itself, the graph has a cycle** —
   reject the edge. Reachability discovery *is* cycle detection; there is no separate check.
3. If no cycle is found, `Δᶠ ∪ Δᵇ` is exactly the set of vertices whose relative order must
   change. Extract their existing `ord` values, and re-assign them the same set of integer
   slots but in the new required sequence (`Δᵇ` vertices first, in their old relative order,
   then `Δᶠ` vertices, in their old relative order). Every vertex *outside* this affected
   region keeps its `ord` untouched.

**Cost.** Work per insertion is proportional to `|Δ| = |Δᶠ| + |Δᵇ|`, the *affected region* —
not `n`, not `m`. Pearce and Kelly are explicit that they are not chasing the best worst-case
bound: their own comparison shows AHRSZ is strictly better in the worst case, and MNR is
incomparable. What makes PK the right default is that `|Δ|` tracks *locality of disruption*
rather than graph size — an edge that only crosses a small neighborhood only perturbs that
neighborhood, however large `n` is elsewhere in the DAG. The JEA paper's empirical section
(random DAGs, thousands of vertices) found PK fastest on sparse digraphs and only a constant
factor behind the best on dense ones.

**Why this is the one to build.** It is a few hundred lines: two bounded DFS traversals plus
an array splice, no balanced trees, no potential-function bookkeeping, no randomization. It
is also the one with a real adoption track record — implementations of Pearce–Kelly (or
direct derivatives) ship inside Google's **Abseil** C++ library, **TensorFlow**, the
**JGraphT** Java graph library, and the **MonoSAT** SAT solver, per the paper's own citing
literature and the maintained reference implementations on GitHub
([metapragma/pearce-kelly](https://github.com/metapragma/pearce-kelly),
[noncrab/pearce-kelly](https://github.com/noncrab/pearce-kelly)). That is a strong practical
signal: production systems facing exactly this problem (maintain a topo order, reject
cycles, do it fast in the common case) converged on PK, not on the asymptotically superior
algorithms below.

### 2.5 Katriel and Bodlaender (TALG 2006) — first real amortized improvement over `O(mn)`

**"Online topological ordering"**, I. Katriel and H. L. Bodlaender, *ACM Transactions on
Algorithms* 2(3), 2006, 364–379
[[TCS companion tight-analysis paper]](https://www.sciencedirect.com/science/article/pii/S0304397507006573).
Before this paper the standard bound for `m` online insertions was the trivial `O(mn)` (each
insertion re-does an `O(n)`-ish local fix). Katriel–Bodlaender give
**`O(min{m^{3/2} log n, m^{3/2} + n^2 log n})`** total time — a genuine asymptotic
improvement, not just a constant-factor practical win. They also modified an earlier
heap-based algorithm of Alpern et al. and analyzed the special case of bounded treewidth
`k`, getting `O(mk log^2 n)`, which becomes `O(n log n)` (optimal) on trees. A later paper by
Liu and Chao gives a **tight** analysis of the same algorithm: `Θ(m^{3/2} + m n^{1/2} log n)`
— i.e. Katriel–Bodlaender's own stated bound was not tight, and the true behavior is slightly
worse on the `n^{1/2} log n` term.

**Implementability.** This is a heap-based algorithm (a priority queue keyed on `ord` drives
the affected-region search so the smallest-first exploration terminates as early as
possible) with a nontrivial amortized argument behind the bound. It is meaningfully harder
to get right than Pearce–Kelly — expect low hundreds to low thousands of lines depending on
how much of a generic priority-queue/graph library you can lean on — and the payoff (a
better worst-case *asymptotic* bound) only shows up on adversarially structured insertion
sequences. For a workflow DAG where edges mostly arrive in causal, roughly-forward order,
this is asymptotics you are unlikely to ever collect on.

### 2.6 Ajwani, Friedrich (and Meyer) — average-case analysis, and a worse worst case

Ajwani and Friedrich's line of work (building on Ajwani–Friedrich–Meyer) contributes two
distinct results, both useful for calibrating expectations rather than for direct
implementation:

- **"An O(n^2.75) algorithm for online topological ordering"** (Ajwani & Friedrich,
  ESA 2007 / TALG follow-up) gives a *worse* worst-case bound than Katriel–Bodlaender for
  general graphs, trading it for simpler machinery — of the family in this section, it is
  the one with the least favorable worst case, which is why it isn't recommended over
  §2.5/§2.7/§2.8 for a from-scratch implementation.
- **"Average-Case Analysis of Online Topological Ordering"** (Ajwani & Friedrich, ISAAC
  2007) is the more load-bearing result for a systems designer: they prove that when edges
  of a complete DAG are inserted **in random order**, several of these algorithms (including
  variants of PK and KB) run in **expected `O(n^2 polylog n)`** time total, dramatically
  better than their adversarial worst case. This matters directly for `dag-worker-go`: it is
  the closest thing in the literature to a formal argument that "worst case is a pathological
  adversary, typical DAG-construction order is fine," which is exactly the assumption the
  Pearce–Kelly recommendation in §2.4 leans on informally.

### 2.7 Haeupler, Kavitha, Mathew, Sen, and Tarjan (TALG 2012) — the sparse-graph state of the art for a while

**"Incremental cycle detection, topological ordering, and strong component maintenance"**,
B. Haeupler, T. Kavitha, R. Mathew, S. Sen, R. E. Tarjan, *ACM Transactions on Algorithms*
8(1), article 3, 2012 [[arXiv:1105.2397]](https://arxiv.org/pdf/1105.2397).

They give **two** algorithms:

1. A **two-way search** algorithm handling `m` arc additions in **`O(m^{3/2})`** total —
   for sparse graphs (`m = O(n)`) this improves the prior best by a `log n` factor and, per
   the paper's own Theorem 3.5, the total number of arc traversals across the whole insertion
   sequence is bounded by `4·m^{3/2} + m + 1` — an unusually explicit constant for a paper in
   this area. The mechanism: to insert `(v, w)` with `v` currently ordered after `w`, run a
   forward search from `w` and a backward search from `v` simultaneously, but only along
   pairs of arcs that are "compatible" (a forward-traversed arc `(u,x)` and a
   backward-traversed arc `(y,z)` must keep `u < z` in the *current* order) — if a forward
   arc lands in the backward-visited set (or vice versa), that closes a cycle; otherwise the
   two frontiers are spliced into the new local order exactly as in Pearce–Kelly. What makes
   the amortized bound work is a potential function over **pairs of arcs that lie on a common
   path** ("related" pairs): each search that does non-trivial work strictly increases the
   number of related pairs, and there are at most `m^2/2` such pairs total, which pays for
   `O(m^{3/2})` aggregate traversal work once the searches are bucketed into "small"
   (`≤ 2√m` steps) and "big" categories.
2. A **search-order** algorithm for denser graphs handling an arbitrary insertion sequence in
   **`O(n^{5/2})`** total, improving the prior best (Katriel–Bodlaender's dense-graph
   variant) by a polynomial factor on sufficiently dense inputs — though the authors are
   candid that this bound "may be far from tight," showing only a much weaker
   `Ω(n^2 · 2^{√(2 lg n)})` lower bound for their own algorithm, tied to the unresolved
   "k-levels" problem in combinatorial geometry.
3. A genuinely useful **lower bound**: any algorithm that only reorders vertices strictly
   "between" the endpoints of the affected edge in the current order (a natural formalization
   of what PK/AHRSZ/KB all do, called the *locality property*) must take **`Ω(m^{3/2})`**
   time in the worst case for sparse graphs — so the `O(m^{3/2})` bound above is not an
   accident of a clever algorithm, it is close to the best any "local" algorithm can do.
   Katriel additionally showed even local algorithms need `Ω(n^2)` when `m = Θ(n)` for a
   *different* cost measure (total vertices moved), underscoring that PK-family algorithms
   trade average-case locality for a provably worse adversarial case.

**Implementability.** This is a research artifact, not a weekend build. The two-way search
does eliminate heap operations (no priority queue, "soft-threshold" search bounds instead),
which the authors present as an implementation *simplification* relative to Katriel–
Bodlaender — but correctly implementing the compatibility test between forward/backward arcs
and getting the amortized bound to actually manifest requires careful bookkeeping absent
from PK. Reserve this for a v2 if `dag-worker-go` ever needs to defend against adversarial
insertion patterns at very large `m`; it is not where you start.

### 2.8 Bender, Fineman, Gilbert (SODA 2009) and Bender–Fineman–Gilbert–Tarjan (TALG 2016) — simpler machinery, comparable bounds

**"A New Approach to Incremental Topological Ordering"**, M. A. Bender, J. T. Fineman, S.
Gilbert [[PDF]](https://www.comp.nus.edu.sg/~gilbert/pubs/SODA09.pdf), gives an
`O(n^2 log n)` total-time algorithm for dense graphs using "completely different techniques"
from AHRSZ/PK/KB — described elsewhere in the literature as avoiding the heavier
data-structure machinery of Katriel–Bodlaender.

The extended journal result, **"A New Approach to Incremental Cycle Detection and Related
Problems"**, M. A. Bender, J. T. Fineman, S. Gilbert, R. E. Tarjan, *ACM TALG* 2016
[[arXiv:1112.0784]](https://arxiv.org/pdf/1112.0784), sharpens this into two regimes:

- **Sparse:** `O(min{√m, n^{2/3}} · m)` total time for `m` insertions.
- **Dense:** `O(n^2 log n)` total time, improving on HKMST's `O(n^{5/2})` dense bound by a
  polynomial factor.

**Mechanism.** Like every algorithm in this family, insertion of `(v, w)` (with `v` currently
ordered after `w`) triggers a bidirectional search — but BFGT's is deliberately *asymmetric*:
each vertex carries a two-part label (a coarse **level** and a fine **index within the
level**), and the **backward** search is the one that is explicitly bounded by
`Δ = min{√m, n^{2/3}}` steps — "each backward search proceeds entirely within a level; if it
takes too long, stop it and increase the level of a vertex" rather than letting an unbounded
search run and paying for it with a global potential argument the way HKMST does. The
authors state plainly that the result "is considerably simpler than [HKMST]'s `O(m^{3/2})`
algorithm," explicitly because it needs **no dynamic ordered-list data structure and no
random sampling/selection** — every structure involved is a plain array or counter. That is
the single most actionable sentence in this whole literature for a Go implementer: BFGT is
the theoretically-competitive algorithm that was *designed* to be simple to implement, at the
cost of an asymmetric, slightly fiddlier two-part labeling scheme than PK's single integer
`ord`.

**Implementability.** Meaningfully closer to buildable than HKMST — "simple data structures"
is the authors' own framing — but the two-part level/index labeling and the level-promotion
rule are still more moving parts than Pearce–Kelly's single splice, and there is (as of this
research) no widely-used open-source reference implementation to crib from, unlike PK's
Abseil/TensorFlow/JGraphT lineage. Treat it as the credible "if PK's practical
`O(|Δ|)`-per-edge cost ever shows up in a profile as the bottleneck" upgrade path, not the
starting point.

### 2.9 Comparative table

| Algorithm | Year | Total cost, `m` insertions | Technique | Implementable in a few hundred lines? |
|---|---|---|---|---|
| Naive re-topo-sort | — | `O(m·(V+E))` | full Kahn/DFS per insertion | yes, but don't |
| AHRSZ | 1990 | best worst case among the classics (unspecified closed form; strictly dominates PK) | bidirectional bounded search, renumber affected region | reference only — nobody ships it raw |
| MNR | 1996 | incomparable to PK | bidirectional DFS, renumber | reference only |
| **Pearce–Kelly** | 2004/2007 | `O(\|Δ\|·f(\|Δ\|))`, `Δ` = affected region (data-dependent, not a fixed poly in `m,n`) | single-integer `ord`, fwd/bwd DFS, array splice | **yes — recommended default** |
| Katriel–Bodlaender | 2006 | `O(m^{3/2} + m n^{1/2} log n)` tight (Liu–Chao) | heap-driven affected-region search | moderate — heap + amortized bookkeeping |
| Ajwani–Friedrich | 2007 | `O(n^{2.75})` worst case; `O(n^2 polylog n)` expected on random insertion order | search-order variant | moderate, mainly useful for its average-case theorem |
| Haeupler–Kavitha–Mathew–Sen–Tarjan | 2012 | `O(m^{3/2})` sparse, `O(n^{5/2})` dense | two-way "compatible" search, potential function over related arc pairs | **research artifact** — correct amortized accounting is the hard part |
| Bender–Fineman–Gilbert(–Tarjan) | 2009/2016 | `O(min{√m,n^{2/3}}·m)` sparse, `O(n^2 log n)` dense | asymmetric two-part level/index labels, bounded backward search per level | harder than PK, easier than HKMST — "simple data structures" by design |

**Verdict for `dag-worker-go`:** ship Pearce–Kelly. It is the only algorithm in this table
with (a) a bound that adapts to how locally-disruptive real insertions are rather than a
fixed worst-case polynomial, (b) production adoption you can point to, and (c) an
implementation surface — two bounded DFS walks and an array splice — that a few hundred
lines of Go and a solid test suite can actually get right. Revisit BFGT only if profiling on
adversarial or synthetic dense-insertion workloads shows PK's `|Δ|` blowing up in practice.

---

## 3. Cycle prevention on edge insertion

`dag-worker-go` must decide, at `AddEdge(u, v)` time, whether to accept the edge or reject it
as cycle-forming — synchronously, because a host program adding a dependency needs to know
immediately whether the graph it just asked for is legal.

### 3.1 What an exact check costs

Rejecting cycle-forming edges exactly is equivalent to a reachability query: `(u, v)` is safe
iff `v` cannot already reach `u` (equivalently, `u` cannot already reach `v` and `v` cannot
reach `u` in the other direction depending on orientation convention — the operative test is
"does the new edge's head already reach the new edge's tail"). Computed from scratch via
BFS/DFS this is `O(V+E)` **per insertion**, which is exactly what a "keep re-running Kahn"
design costs on the ready-set side too — unacceptable at the node counts this project
targets. Two moves make it fast:

### 3.2 The maintained-order fast path — sub-`O(1)`-ish in the common case

If you are already running Pearce–Kelly (§2.4) to maintain `ord()`, the cycle check is
**free**, folded into the insertion procedure rather than bolted onto it:

- **Fast accept, `O(1)`:** if `ord(u) < ord(v)` already, the edge cannot create a cycle —
  accept immediately, no traversal at all. For a DAG built in roughly causal order (parents
  registered before children, which is the overwhelmingly common shape for a task-dependency
  graph) this is the path taken essentially every time.
- **Bounded search, cycle iff the searches meet:** if `ord(u) ≥ ord(v)`, run the PK
  forward/backward DFS described in §2.4. The searches are bounded to the region between the
  two orders, and they encounter `u` (from the forward side, having started at `v`) **iff**
  a path `v ⇝ u` already exists, i.e. **iff** the new edge would close a cycle. There is no
  separate reachability subroutine — cycle detection is the failure mode of the same walk
  that computes the affected region for reordering. This is the same principle underlying
  every algorithm in §2: "reject edges that violate a maintained topological order" is not an
  alternative to incremental order maintenance, it *is* what incremental order maintenance
  computes as a side effect.

### 3.3 The cheap-but-unsound shortcut, and why it needs a sound fallback

A tempting cost-cutting move is a **bounded backward search**: from `u`, walk backward
(along reversed edges) for at most `B` steps looking for `v`; if found, reject; if the budget
runs out first, *accept optimistically*. This is `O(B)` worst case regardless of graph size,
which is attractive — but it is **not sound**: a cycle whose confirming path from `v` back to
`u` is longer than `B` hops is silently accepted, corrupting the DAG invariant a scheduler
depends on (a "ready" node could now be waiting, transitively, on itself, and Kahn's counter
trick in §1 will simply never fire it — the graph deadlocks silently with no error surfaced).
A bounded search is only safe to use as a **pre-filter**: run it first for the common case of
a nearby cycle (cheap, catches most real programmer mistakes — accidental self-loops and
short cycles are the overwhelming majority of "oops" cases in a hand-built task graph), and
if it exhausts its budget without finding `v`, fall through to the exact, sound check in
§3.2 rather than accepting. That composition is sound and fast on average; skipping the
fallback is a correctness bug dressed up as an optimization.

### 3.4 Recommendation

Maintain `ord()` via Pearce–Kelly as the primary mechanism (§2.4); this makes cycle rejection
`O(1)` in the fast-accept case and `O(|Δ|)` in the reordering case, with no additional data
structure or index required beyond what the ready-set already needs for tie-breaking. Do not
build a separate reachability index (§4) purely to answer "would this edge cycle" — that
question is already answered essentially for free by order maintenance, and a general
reachability index (2-hop labels, interval labels) is the wrong tool for a question that only
ever needs a *yes/no* against one specific pair at insertion time, not repeated queries
against an evolving label set.

---

## 4. Reachability queries for cancellation and failure propagation

A different query shape: "node `x` failed (or was cancelled) — which already-scheduled or
future nodes are downstream of it and must also be cancelled/failed?" This is a
**descendant** query (forward reachability from `x`), potentially fired far less often than
edge insertions or completions, but potentially touching a large fraction of the graph when
it does (a root-node failure can invalidate everything below it).

### 4.1 Transitive closure — ruled out by arithmetic alone

Precomputing and storing the full reachability relation as an `n × n` bit matrix gives
`O(1)` query time but `O(n^2)` bits of storage and `O(n·m)` (or `O(n^ω)`, `ω < 2.3716`, via
fast Boolean matrix multiplication) to build and to keep updated after every edge insertion.
At `n = 10^6`, `n^2` bits is `10^{12}` bits ≈ **125 GB** — before even accounting for the cost
of invalidating/recomputing rows every time an edge is added to a *dynamic* graph. This is
disqualified by the problem's own scale requirement, not by any subtlety.

### 4.2 On-the-fly BFS/DFS — the default, and it is not actually expensive here

Doing a plain BFS/DFS over `out()` edges starting at the cancelled node, with no
precomputed index at all, costs `O(V_d + E_d)` where `V_d, E_d` are the size of `x`'s
**descendant subgraph**, not the whole DAG. Crucially, this cost is not overhead layered on
top of the cancellation work — it is *exactly* the set of nodes that must be visited anyway
to mark them cancelled/failed and to fire status-transition events for each of them. There is
no way to cancel `k` downstream nodes in less than `Ω(k)` work regardless of index, because
each one needs an individual status write and an individual event emission. This makes
on-the-fly traversal, using the same CSR/adjacency structure the ready-set already
maintains (§7.2), the correct default: no separate index, no separate storage cost, and
asymptotically optimal for the work the operation must do regardless.

### 4.3 2-hop labeling — powerful, but solving a different problem

**"Reachability and distance queries via 2-hop labels"**, E. Cohen, E. Halperin, H. Kaplan,
U. Zwick, SODA 2002 / *SIAM J. Computing* 32 (2003), 1338–1355
[[PDF]](https://web.cs.ucla.edu/~ehalperin/cozygene/publications/papers/labels.pdf). Each
vertex `v` is assigned a label `L(v) = (L_in(v), L_out(v))` — sets of "hub" vertices such
that `u` reaches `v` iff `L_out(u) ∩ L_in(v) ≠ ∅`. Query cost drops to a label-intersection
(`O(|L_out(u)| + |L_in(v)|)`, or `O(1)`-ish with hashing) independent of graph size — but
finding the **minimum** 2-hop cover is **NP-hard**, so practical systems use greedy
approximations (typically an `O(log n)`-factor guarantee via a set-cover reduction), and
label sizes can still be `O(√m)` per vertex in the worst case for general graphs before
practical DAG-specific reductions bring it down. This is the right tool when you need **many
repeated reachability queries** against a graph that changes rarely relative to how often it
is queried — i.e. an analytics/audit workload, not a live scheduler whose graph is churning
by construction. Building and maintaining a 2-hop cover incrementally as edges/nodes are
added is itself an open, harder problem than the static case; it is not something to build
for a first cut of `dag-worker-go`.

### 4.4 Interval / tree-cover labeling and GRAIL — DAG-specific, and their real limitation

The classic tree-cover technique: pick a spanning tree of the DAG, do a postorder traversal,
and assign each vertex `v` the interval `[min postorder-number of any descendant of v,
postorder-number of v]`. Reachability along tree edges reduces to **interval containment**:
`u` reaches `v` (via the tree) iff `v`'s interval ⊆ `u`'s interval — `O(n)` total space,
`O(1)` query. The catch, confirmed across the indexing-technique literature, is exactly what
you'd expect from covering a general DAG with one spanning tree: **"labels cannot distinguish
reachability on non-tree edges"** — any path that leaves the chosen spanning tree even once
is invisible to a single interval pair, producing false negatives that must be patched by
additional labels (in the worst case, one extra label per non-tree edge, which for a DAG with
significant edge density beyond its spanning-tree skeleton erodes the `O(n)` space guarantee
back toward `O(m)` or worse) [Indexing Techniques for Graph Reachability Queries, *ACM
Computing Surveys*, 2024–25 survey](https://arxiv.org/pdf/2311.03542).

**GRAIL** ("Graph Reachability Indexing via RAndomized Interval Labeling," Yıldırım, Chaoji,
Zaki, *VLDB* 2010) generalizes this by keeping `d` **independent random** interval labels per
vertex (from `d` random DFS/BFS traversal orders) instead of one canonical spanning tree.
A "no" from *any* of the `d` intervals is a sound proof of non-reachability (fast rejection);
graphs where GRAIL's design goal is met use this to prune the overwhelming majority of
negative queries in `O(d)` time without ever touching the graph itself, falling back to an
explicit traversal only for the (rare, with `d` in the single digits) surviving candidates.
Space and construction time are both **linear** — `O(dn)` — which the paper's own framing
emphasizes is *the* reason GRAIL, unlike more precise 2-hop-labeling schemes, scales to
multi-million-vertex graphs at all: more sophisticated indexes win on small graphs but do not
scale, and GRAIL is presented specifically as the one that does.

**PathTree** (Microsoft Research, *TODS*) is a related refinement that decomposes the DAG
into a small number of vertex-disjoint paths (rather than one spanning tree) and assigns each
vertex a vector of path-position labels — better precision than a single interval at the cost
of a slightly heavier label per vertex; treat it as a point on the same
precision/space/build-cost trade-off curve as GRAIL rather than a different paradigm.

### 4.5 Recommendation

Do not build a reachability index for cancellation in `dag-worker-go` v1. §4.2's on-the-fly
traversal is asymptotically tied to the unavoidable cost of the cancellation work itself, adds
zero index-maintenance burden to every edge insertion (which a 2-hop or interval label would
impose), and reuses the adjacency structure the scheduler needs regardless. Revisit GRAIL-style
labeling only if a product requirement emerges for **repeated, index-style** reachability
queries decoupled from actually touching the downstream nodes (e.g., "is X blocked on Y" UI
queries fired far more often than cancellations happen) — at that point GRAIL is the
right-shaped answer because it is the one member of this family explicitly designed to survive
contact with a million-plus-vertex graph.

---

## 5. Counting-based completion detection

Three distinct "is X done" questions show up in a DAG scheduler, and they want three
different counters, not one:

### 5.1 Per-node readiness — this is just §1's `pending[]` counter

Already covered: `pending[v]` starts at `indeg(v)`, decremented once per satisfied
predecessor, `v` becomes ready at `pending[v] = 0`. `O(1)` amortized per edge over the DAG's
life (§1.2).

### 5.2 Whole-scope completion — a single global counter, not a graph walk

"Has every node in this scope reached a terminal state" should never be answered by walking
the graph. Maintain one additional atomic counter per scope, `outstanding`, initialized to 0
and `INCR`'d once per node creation, `DECR`'d exactly once per node's *first* transition into
a terminal state (success, error, or timeout-error — see the state-machine research file for
the exact vocabulary). The scope is complete the instant `outstanding` reaches 0. This is
`O(1)` per transition and needs no reachability or traversal machinery at all — it is the
classic **counting semaphore** pattern, and it composes trivially with the per-node counter
in §5.1 (they are different counters serving different questions, updated at different
events: `pending[v]` reacts to *predecessor* completions, `outstanding` reacts to *this
scope's own* completions).

### 5.3 Subgraph / "did this branch finish" completion

A finer-grained variant — "notify me when node `x` and everything transitively depending on
`x` for its result has finished" — is a fan-in join, and it is answered the same way modern
dataflow systems answer it: attach a local counter to the join point, seeded with the number
of direct inputs it is waiting on (exactly `indeg` again), decremented by each input's
completion. This is fractally the same primitive as §1 applied at whatever granularity the
host program cares about — there is no need for a distinct algorithm, only for the scheduler
to expose "give me a counter that fires when these `k` specific nodes are all terminal" as a
composable primitive on top of the per-edge counters it already maintains.

---

## 6. Reference counting for GC of finished subgraphs

Once a node is terminal and every downstream consumer has observed that fact (decremented
its `pending[]` counter because of it), the completed node's own bookkeeping — its status
record, and especially any result payload it handed to the library for delivery to
successors — is garbage. A DAG has no cycles, so simple reference counting is not merely
adequate here, it is **complete**: unlike general-purpose refcounting garbage collectors,
which need a cycle collector because arbitrary object graphs can contain reference cycles
[classic result, Weizenbaum 1963 and the whole subsequent GC literature], a DAG is acyclic by
definition, so a plain refcount can never get stuck at a nonzero floor the way a cyclic
garbage structure does. There is no analogue of "unreachable cycle" to worry about.

### 6.1 The production analogue: Naiad's distributed reference counting

The clearest existing engineering precedent for "reference-count your way to knowing when a
piece of dataflow state is safe to discard, across a distributed system, without a global
stop-the-world scan" is Naiad's **progress-tracking protocol** ("Naiad: A Timely Dataflow
System," Murray et al., SOSP 2013
[[PDF]](https://sigops.org/s/conferences/sosp/2013/papers/p439-murray.pdf)). Naiad's own
description of the mechanism: *"Progress tracking in Naiad is essentially distributed
reference counting"* — each worker maintains, per **pointstamp** (a location-in-the-dataflow
× logical-timestamp pair, structurally analogous to "this node, at this point in the DAG's
causal history"), an **occurrence count** of messages it believes are still live for that
pointstamp. When a worker retires a message it does not decrement its own counter directly;
it **broadcasts** a `-1` update for that pointstamp to every other worker, and all workers
(including the retiring one) apply updates only from these broadcasts. A pointstamp — and
everything gated behind it — is known to be permanently finished exactly when every worker's
occurrence count for it has reached zero and stays there, at which point the frontier of "in
play" timestamps advances and any state pinned to that pointstamp can be released.

### 6.2 Applying the pattern to `dag-worker-go`

Map this onto the scheduler directly: give each node's stored payload/result a **consumer
refcount**, seeded at edge-insertion time with the number of *distinct successors registered
against it so far* (which can itself grow, since the DAG is dynamic — an increment on every
new outgoing edge added after the node already succeeded is the dynamic-DAG analogue of
Naiad broadcasting updates rather than assuming a fixed count up front). Each successor's
consumption of the payload (i.e., the moment its own `pending[]` decrement in §1.2 fires
because of *this specific* predecessor) issues a `-1` against the refcount. The payload
becomes eligible for GC/eviction from storage the instant the refcount reaches zero **and**
no further out-edges can still be added from that node (which, if node mutation is closed
once a node reaches a terminal state — a reasonable invariant to adopt — is simply "node is
terminal and refcount is zero"). This is `O(1)` amortized per edge, exactly like §1.2's
readiness decrement, and can piggyback on the *same* atomic decrement operation described in
§8 rather than requiring a second round-trip to storage: one idempotent "predecessor `u`
satisfied for successor `v`" event can simultaneously (a) decrement `v`'s `pending` counter
and (b) decrement `u`'s consumer-refcount, inside the same atomic script.

### 6.3 Why no cycle collector is needed, restated precisely

The only reason general refcounting GC needs a backup tracer is that a reference cycle keeps
every member's count above zero forever even though the whole cycle is externally
unreachable. `dag-worker-go`'s edges are, by the DAG invariant §2–§3 exist specifically to
enforce, never part of a cycle — so a node's consumer-refcount reaching zero is both
necessary *and sufficient* for "nothing will ever decrement it again," with no possibility of
a live-locked residual cycle. This is one of the few places in the whole design where the
acyclicity constraint pays for itself directly in implementation simplicity, not just in
scheduling semantics.

---

## 7. Concrete data structures, and their KV-store shape

### 7.1 In-degree / pending-dependency counters

Logically: `NodeID → uint32`. Physically, three reasonable encodings depending on backend:

- **In-memory (default backend):** a plain `sync/atomic`-backed slice or `map[NodeID]*uint32`,
  `Add(-1)` is a single atomic instruction — genuinely `O(1)`, no contention beyond the
  cache-line bounce.
- **Redis:** either one `INCR`-friendly string key per node (`node:{id}:pending`), or — far
  cheaper at scale — packed into a single `BITFIELD`-addressable string per scope, one
  fixed-width sub-integer field per node, mutated with `BITFIELD ... INCRBY`. The packed form
  trades per-key overhead (see §8.5's arithmetic) for a slightly less ergonomic API.
- **PostgreSQL:** a plain `pending int4 not null` column on the `nodes` row — this counter is
  free, it rides along on a row you need to store anyway for status (§5).

### 7.2 Adjacency in CSR form, and why "dynamic" fights it

Compressed Sparse Row is the right *steady-state* shape for `out()`: a `row_ptr[n+1]` array of
offsets plus a flat `col_idx[m]` array of successor IDs, so node `v`'s successors are exactly
`col_idx[row_ptr[v] : row_ptr[v+1]]` — `O(outdeg(v))` to enumerate, `O(1)` to locate the
slice, and `O(m + n)` total space with none of the per-edge pointer/object overhead a
linked adjacency list would carry
[CSR format overview](https://en.wikipedia.org/wiki/Sparse_matrix#Compressed_sparse_row_(CSR,_CRS_or_Yale_format)).
The problem: CSR is naturally an **immutable, batch-built** structure — appending one edge to
row `v` in a naive CSR means shifting every row after `v` in `col_idx`, an `O(m)` disaster on
a graph that is *by requirement* taking edge insertions continuously. Two standard fixes, in
increasing order of engineering effort:

1. **Per-row slack + amortized doubling.** Over-allocate each row's slice with spare capacity
   (classic dynamic-array doubling: grow a row's backing capacity by `2×` when it fills,
   amortized `O(1)` per append — the same textbook argument that makes `append()` to a Go
   slice amortized `O(1)`). Rows still live in one flat backing array or arena; a row that
   outgrows its slack gets relocated to a fresh, larger slot (rare, and each relocation is
   charged to the `O(log outdeg(v))` doublings that row will ever undergo, not to every future
   insert).
2. **Generational CSR.** Build an immutable CSR snapshot in bulk at DAG-construction or at
   periodic compaction points; route edges added *after* a snapshot through a small
   overflow structure (a per-node dynamic slice, or a per-node Redis `SET`/list) that is
   consulted in addition to the CSR slice, and fold the overflow back into a fresh CSR
   snapshot on the next compaction. This is the right shape when most edges arrive in a
   predictable "build the graph" burst with a long tail of sparser dynamic additions — which
   matches the way most real task DAGs are actually constructed (a big upfront plan, plus
   occasional dynamic re-planning).

On a KV store with no pointers at all, the CSR row concept degenerates to "the successor list
for node `v` is whatever the store's native ordered/unordered collection type gives you" —
a Redis `SET` (`SADD`/`SMEMBERS`, `O(1)` insert per member) or a Postgres `edges(from_id,
to_id)` table with a btree index on `from_id` (`O(log m)` insert, `O(log m + outdeg(v))` scan)
— trading CSR's cache-friendly contiguity for the store's native atomicity and durability
guarantees. That trade is almost always worth it once the adjacency lives outside a single
process's memory.

### 7.3 The ready set: FIFO, bitset, or heap

| Representation | Push | Pop / pick | Membership test | When to use |
|---|---|---|---|---|
| FIFO queue | `O(1)` | `O(1)` | not supported natively | plain work distribution, no priority — default for `dag-worker-go` |
| Bitset (one bit per node) | `O(1)` set | `O(n/w)` scan for next set bit (`w` = word width), or `O(1)` with an auxiliary free-list of set positions | `O(1)` | workers pull in bulk / batch scans; cheap existence checks for "is this node currently ready" |
| Heap / priority queue | `O(log n)` | `O(log n)` | not supported natively | priority scheduling (deadline, weight, retry-backoff ordering) — an extension point, not needed for the minimal state machine |

For the KV mapping: a FIFO ready-queue is a Redis `LIST` (`RPUSH`/`LPOP`, both `O(1)`) or a
Redis `STREAM` when you additionally want durable, replayable, consumer-group-based delivery
to competing worker instances (the multi-instance distribution question is out of scope here
— see the coordination research file — but the *data structure* choice is squarely in scope:
a Stream gives you the FIFO semantics of a List plus at-least-once delivery bookkeeping for
free, which is exactly the primitive §8 needs to reason about idempotent consumption). In
PostgreSQL, the equivalent is a `status = 'ready'` partial index combined with
`SELECT ... FOR UPDATE SKIP LOCKED` to hand out rows to competing workers without contention
— logically a FIFO-ish set (no strict ordering guarantee unless you also `ORDER BY` a
sequence column) with `O(log n)` insert/pop via the index rather than `O(1)`, which is the
price of getting transactional guarantees "for free" from a relational engine instead of
crafting them by hand as in §8.

---

## 8. Idempotent in-degree decrement under at-least-once delivery

This is the crux of the practical section, and it is where "just use `INCR`/`DECR`" quietly
breaks correctness. State the failure mode precisely first.

### 8.1 Why a raw counter is not idempotent

`pending[v].Add(-1)` is atomic — no two concurrent callers can race and lose an update — but
atomicity is not the same property as idempotency. If the *same logical event* ("predecessor
`u` has satisfied its edge into `v`") is delivered twice — because a worker's ack timed out
and was retried, because a message queue guarantees only at-least-once delivery, because a
crashed `dag-worker-go` instance replays its write-ahead log — a raw `DECR` applies the
decrement **twice**, and `v` becomes ready one predecessor early (or, if `v` has exactly two
remaining predecessors and one fires twice, `v` becomes ready while the *other*, real
predecessor never finished at all). This is silent and non-local: the bug shows up as a node
running before its true dependencies are satisfied, with no exception anywhere near the
double-decrement itself. Any design that maps the in-degree trick from §1.2 directly onto
`INCR`/`DECR` without an accompanying idempotency mechanism has this bug.

### 8.2 Strategy A — raw counter, idempotency pushed to the delivery layer

Keep the single integer counter from §1 as-is, and make the *event source* responsible for
exactly-once delivery of the decrement — e.g. a message queue with consumer-side
deduplication by a stable `(edge, delivery-attempt-independent) ID`, or a Redis Stream
consumer group with an explicit "processed IDs" acknowledgment log the consumer checks before
applying each entry. **Storage cost:** effectively zero beyond the counter itself — the
`pending` field is a column/value you need to store regardless for §1, so this strategy adds
no per-edge state at all. **The catch:** correctness now depends entirely on a *different*
subsystem's exactly-once guarantee, which either doesn't exist natively (Redis Streams and
Postgres LISTEN/NOTIFY are at-least-once, not exactly-once, by design) or has to be built —
at which point you have built strategy B or C anyway, just at the messaging layer instead of
the counter layer, with none of the benefit of localizing the fix to the counter update
itself.

### 8.3 Strategy B — per-edge "satisfied" flag

Make the *edge itself* the idempotency key: pair the decrement with a check-and-set on a
per-edge boolean, inside one atomic operation, so a replay finds the flag already set and
no-ops before ever touching the counter.

**Redis (Lua, atomic via `EVAL`** — Redis guarantees a script runs to completion with no
other command interleaved, per the official scripting semantics
[[redis.io: EVAL](https://redis.io/docs/latest/commands/eval/)]):

```lua
-- KEYS[1] = per-destination-node bitmap of satisfied predecessor slots
-- KEYS[2] = pending-count key for the destination node
-- KEYS[3] = ready-queue key (a Redis LIST or STREAM) to push to if this completes it
-- ARGV[1] = this edge's pre-assigned bit offset within KEYS[1] (assigned at AddEdge time)
-- ARGV[2] = the destination node id, to enqueue if it becomes ready
local already = redis.call('GETBIT', KEYS[1], ARGV[1])
if already == 1 then
    return -1                       -- duplicate delivery: no-op, report as such
end
redis.call('SETBIT', KEYS[1], ARGV[1], 1)
local remaining = redis.call('DECR', KEYS[2])
if remaining == 0 then
    redis.call('RPUSH', KEYS[3], ARGV[2])
end
return remaining
```

`GETBIT`/`SETBIT`/`DECR` are each documented as `O(1)`
[[redis.io: INCR](https://redis.io/docs/latest/commands/incr/)] and the whole script executes
as one atomic unit, so this is a single round trip with no lock, no CAS retry loop, and no
window for a concurrent second delivery of the same edge to both pass the `already == 1`
check.

**PostgreSQL (single statement, transactionally atomic by default):**

```sql
-- edges(from_id bigint, to_id bigint, satisfied boolean not null default false,
--       primary key (from_id, to_id))
-- nodes(id bigint primary key, pending int not null)

WITH marked AS (
    UPDATE edges
       SET satisfied = true
     WHERE from_id = $1 AND to_id = $2 AND satisfied = false
    RETURNING to_id
)
UPDATE nodes
   SET pending = pending - 1
  FROM marked
 WHERE nodes.id = marked.to_id
RETURNING nodes.pending;
```

If the edge was already `satisfied = true`, the CTE's `UPDATE ... WHERE satisfied = false`
matches zero rows, `marked` is empty, the outer `UPDATE ... FROM marked` therefore also
touches zero rows, and the statement is a correct, silent no-op on replay — idempotency
falls out of the `WHERE satisfied = false` guard, not out of any application-level retry
logic, using nothing more exotic than standard row locking semantics
[[PostgreSQL: Explicit Locking](https://www.postgresql.org/docs/current/explicit-locking.html)].

**Memcached** has no scripting and no multi-key transactions
[[memcached protocol spec](https://raw.githubusercontent.com/memcached/memcached/master/doc/protocol.txt)],
but its `add` command — store only if the key does **not** already exist — is *itself* a
perfect, native idempotency primitive for exactly this flag, with no CAS loop required:

```
add edge:{u}:{v}:satisfied 0 0 1
1                                   # first delivery: STORED, proceed to decrement
add edge:{u}:{v}:satisfied 0 0 1
NOT_STORED                          # replay: key already exists, skip the decrement
```

only when the `add` reports success does the caller proceed to `decr node:{v}:pending 1`;
because `decr` itself floors at zero rather than going negative, and because the `add` gate
already prevents a second decrement from ever being issued for this edge, the pair is safe
without Memcached-side atomicity across the two commands — the flag `add` is the sole
correctness-bearing operation, and it is Memcached's one genuinely atomic idempotent
primitive.

### 8.4 Strategy C — per-node set of satisfied predecessor IDs

Instead of one flag per edge, keep, per destination node, the **set** of predecessor IDs that
have already reported in; a node is ready when that set's cardinality equals its recorded
in-degree.

```lua
-- KEYS[1] = set of satisfied predecessor ids for this destination node
-- KEYS[2] = ready-queue key
-- ARGV[1] = predecessor node id reporting completion
-- ARGV[2] = this node's total in-degree (recorded at node-creation time)
-- ARGV[3] = this node's own id, to enqueue if now ready
local added = redis.call('SADD', KEYS[1], ARGV[1])
if added == 0 then
    return -1                       -- duplicate: SADD is a no-op on an existing member
end
if redis.call('SCARD', KEYS[1]) == tonumber(ARGV[2]) then
    redis.call('RPUSH', KEYS[2], ARGV[3])
end
return added
```

Idempotency here is a direct consequence of `SADD`'s own semantics (adding an existing member
is defined to be a no-op, `O(1)` per element regardless
[[redis.io: SADD](https://redis.io/docs/latest/commands/sadd/)]) — no explicit "already
satisfied" branch is even needed, which is a genuine ergonomic win over Strategy B. The
trade-off is what it costs to store a *member ID* per edge instead of one *bit* per edge,
quantified next.

### 8.5 Quantified storage comparison, 1,000,000 nodes at out-degree 3

Out-degree 3 on 1M nodes ⇒ **`m = 3,000,000` edges**. Figures below are engineering
estimates from each store's documented data-layout behavior, not measurements — treat them
as order-of-magnitude, and re-benchmark against your actual Redis/Postgres version before
committing to one (flagged again in "Open questions").

| Strategy | What's stored | Redis (approx.) | PostgreSQL (approx.) |
|---|---|---|---|
| **A. raw counter only** (no idempotency built in) | 1 integer per node | ~4 MB packed via `BITFIELD` (20-bit fields × 1M), or ~50–100 MB if each node is its own key (per-key hashtable overhead dominates a 4-byte payload) | **~0 extra** — `pending int4` rides on the `nodes` row you store anyway |
| **B. per-edge satisfied bit** | 1 bit per edge, addressed by a slot assigned at `AddEdge` time | **~366 KiB** total as a packed bitmap (3,000,000 bits ÷ 8) — a single string per node or per scope, essentially free | one `edges` row per edge: `HeapTupleHeaderData` (23 B) + line pointer (4 B) + `from_id`+`to_id`+`satisfied` (17 B, padded to 24 B for 8-byte alignment) ≈ **51 B/row**, plus a btree PK entry (~24–32 B) ⇒ **~75–85 B/edge** → **≈ 225–255 MB** at 3M edges [[storage-page-layout.html]](https://www.postgresql.org/docs/current/storage-page-layout.html) |
| **B. per-edge satisfied flag as its own Redis key** (no packing) | 1 key per edge | per-key hashtable overhead (commonly tens-to-~90 bytes even for a 1-byte value, version-dependent) × 3M ⇒ **~170–270 MB** — same order as Postgres, ~1000× worse than the packed bitmap | n/a |
| **C. per-node set of satisfied predecessor IDs** | 3,000,000 total set-member entries spread over up to 1,000,000 sets | integer-only sets ≤512 members use Redis's compact `intset` encoding (~8 B/member) ⇒ ~24 MB of member data + ~1M × key overhead (~56–90 B) ⇒ **≈ 80–115 MB**; sets that exceed the intset/listpack thresholds (default `set-max-intset-entries`/`set-max-listpack-entries`, i.e. hub nodes with unusually high in-degree) convert to hashtable encoding at ~64–80 B/member, inflating this estimate for skewed fan-in | a `satisfied_predecessors(to_id, from_id)` table has the same row shape as strategy B ⇒ **~same ≈ 225–255 MB**, with strictly worse ergonomics than B (no native atomic "add-if-absent that also reports success" without the same `WHERE ... = false` pattern) |

**Reading the table:** the packed per-edge bitmap (B, Redis, packed) is the cheapest
*idempotent* option by roughly three orders of magnitude over anything that spends a whole
key or a whole row per edge — but it requires assigning each edge a **dense, stable slot
index at insertion time** (store the slot on the edge record itself, or have the worker echo
it back in its completion ack) rather than addressing bits by the predecessor's own ID;
addressing by `hash(predecessor_id) mod width` instead is a false economy — a hash collision
between two real, distinct predecessors silently merges their bits, and the node then waits
forever on a predecessor that will never separately report in, because its slot was already
marked satisfied by a different node. Strategy C is the right choice when you want the
*satisfied-predecessor set itself* to be independently useful for observability ("which
specific predecessors is this node still waiting on" is a `SDIFF` away) and can tolerate an
~80–250× storage multiplier over the bitmap for that visibility. Strategy A is free but
is not actually a complete answer to the question this section asks — it only becomes
correct once paired with an idempotency mechanism that is, structurally, a restatement of B
or C at the message-delivery layer instead of the counter layer.

### 8.6 Recommendation

Default to **Strategy B, packed bitmap**, for the hot path (§1.2's decrement): assign each
edge a per-destination-node slot index at `AddEdge` time (an `O(1)` allocation — just the
node's current in-degree at the moment the edge is added), store the slot alongside the edge
record so the worker's completion ack can carry it back, and gate every decrement behind the
`GETBIT`/`SETBIT` check in §8.3's Lua script (or the equivalent `WHERE satisfied = false`
guard in Postgres). Reserve Strategy C for cases where per-predecessor observability is a
product requirement in its own right, not as the default idempotency mechanism — the
storage multiplier is too large to pay for a property (individual predecessor visibility)
most call sites don't need.

---

## Recommendations for dag-worker-go

1. **Ready-set maintenance is Kahn's in-degree trick, full stop** — never re-scan the graph
   on a completion event; decrement `pending[]` for exactly the completed node's out-edges
   and enqueue any that hit zero. This alone is the entire "sublinear ready-set" requirement;
   everything else in this dossier is either making that decrement safe (§8) or handling the
   two operations Kahn's algorithm doesn't cover — dynamic edge insertion (§2–3) and
   cancellation (§4).
2. **Implement Pearce–Kelly (§2.4), not a fancier incremental-order algorithm**, for
   maintaining the order used in cycle rejection. It is the only algorithm in the literature
   surveyed here with real production adoption (Abseil, TensorFlow, JGraphT, MonoSAT), a
   cost that tracks how locally disruptive an insertion actually is rather than a fixed
   worst-case polynomial, and an implementation surface — one integer per node, two bounded
   DFS walks, one array splice — realistically achievable in a few hundred lines with strong
   test coverage. Budget the Bender–Fineman–Gilbert(–Tarjan) two-part labeling scheme (§2.8)
   as the upgrade path if profiling on real or adversarial workloads ever shows PK's affected
   region growing unacceptably large; do not start there.
3. **Fold cycle rejection into the order-maintenance walk itself (§3.2)** rather than running
   a separate reachability check — `ord(u) < ord(v)` is a free `O(1)` accept for the common
   case, and the bounded DFS that handles the uncommon case *is* the cycle check. Never ship
   the bounded-backward-search shortcut (§3.3) without its sound fallback; it silently
   corrupts the DAG invariant otherwise, in a way that manifests as a deadlocked node with no
   error anywhere near the cause.
4. **Do not build a reachability index (2-hop labels, interval labels, GRAIL) for
   cancellation.** Plain on-the-fly BFS over the same adjacency structure the ready-set uses
   is asymptotically tied to the unavoidable per-node work cancellation must do anyway (§4.2),
   and every indexing scheme surveyed here (§4.3, §4.4) imposes a nontrivial *insertion-time*
   maintenance cost on a graph whose whole premise is dynamic edge insertion. Revisit only if
   a product requirement demands repeated reachability queries decoupled from actually
   cancelling nodes.
5. **Give completion detection two counters, not one**: `pending[v]` per node (§1, §5.1) and
   one global `outstanding` per scope (§5.2) for "is the whole scope done" — do not try to
   answer the scope-level question by walking or aggregating the per-node counters, it is a
   separate `O(1)`-per-transition counter with a different update trigger.
6. **Make GC of finished node payloads a consumer-refcount, decremented in the same atomic
   operation as the ready-set decrement (§6.2)** — one idempotent "edge satisfied" event
   should update both `pending[v]` and the predecessor's refcount in a single round trip.
   Because the graph is acyclic, this refcount needs no cycle collector (§6.3): reaching zero
   is both necessary and sufficient for safe eviction.
7. **Never wire a raw `INCR`/`DECR` directly to an at-least-once delivery path.** Default to
   the packed per-edge satisfied-bit strategy (§8.3, §8.6) as the idempotency mechanism for
   the hot-path decrement: assign a dense slot per edge at insertion time, gate every
   decrement behind a `GETBIT`/`SETBIT`-guarded script (Redis) or a `WHERE satisfied = false`
   guarded update (Postgres), or Memcached's native `add`-if-absent. This is ~3 orders of
   magnitude cheaper in storage than a per-edge key/row/set-member at the 1M-node,
   out-degree-3 scale this project targets (§8.5), and it is the only one of the three
   strategies surveyed that is idempotent *by construction* rather than by relying on a
   delivery layer's own exactly-once guarantee, which none of Redis Streams, Postgres
   LISTEN/NOTIFY, or Memcached natively provide.
8. **Use CSR-with-slack (§7.2), not pointer-based adjacency lists, for the in-memory
   backend**, and mirror the same "one contiguous successor collection per node" shape onto
   Redis `SET`s or a `(from_id, to_id)` Postgres table for the pluggable backends — this
   keeps `out(v)` enumeration at `O(outdeg(v))` on every backend, which is what makes §1.2's
   per-completion cost genuinely proportional to out-degree rather than to `n`.

---

## Open questions

- **Exact worst-case constant for Pearce–Kelly's per-insertion cost.** The JEA paper's own
  comparison states the bound only relative to AHRSZ/MNR ("AHRSZ strictly tighter," "MNR
  incomparable") rather than as a clean closed-form in `|Δ|`; before committing to PK as
  the *only* order-maintenance mechanism, reproduce the paper's own benchmark methodology
  against `dag-worker-go`'s actual edge-arrival distribution (mostly-causal vs. adversarial)
  to confirm `|Δ|` stays small in practice, rather than trusting the literature's random-DAG
  benchmarks to transfer.
- **Whether Bender–Fineman–Gilbert–Tarjan is worth building at all**, given no existing
  open-source reference implementation was found during this research (contrast Pearce–
  Kelly's multiple GitHub implementations and production adopters) — a from-scratch BFGT
  implementation carries real correctness risk with no reference to differential-test
  against; decide this only after PK's affected-region size is actually measured as a
  problem, not preemptively.
- **The exact per-key/per-row overhead constants used in §8.5 are estimates from documented
  encoding behavior, not measurements** — before finalizing the idempotency strategy,
  benchmark actual `MEMORY USAGE` (Redis) and `pg_column_size`/`pg_total_relation_size`
  (Postgres) figures against the specific versions this project pins, since Redis's
  listpack/intset conversion thresholds and per-key overhead have changed across major
  versions.
- **How slot assignment for the packed-bitmap strategy (§8.3/§8.6) survives node/edge
  *removal***, which the brief flags as a "possibly" requirement — freeing and reusing a bit
  slot safely (so a new edge doesn't inherit a stale "satisfied" bit from a deleted one)
  needs either slot versioning (a generation counter alongside the slot index) or a
  never-reuse policy that lets the bitmap grow monotonically; neither is worked out here and
  both interact with the GC/refcounting design in §6.
- **Multi-instance contention on the same per-node counter/bitmap** (two `dag-worker-go`
  processes racing to be the one whose decrement flips `pending[v]` to zero and enqueues it)
  is deliberately out of scope for this dossier — the Lua/SQL snippets in §8 are atomic
  *per store*, but the broader question of how competing instances agree on who dequeues the
  resulting ready node belongs with the partitioning/lease-stealing research this file
  explicitly does not cover.
