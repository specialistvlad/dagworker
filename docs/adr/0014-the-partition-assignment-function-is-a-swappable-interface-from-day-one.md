# ADR-0014: The partition assignment function is a swappable interface from day one

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/07-work-distribution-across-instances.md §3.2, §3.3, §3.4,
  §7.1, §7.2, §7.3

## Context

ADR-0013 ships v1 as pure pull-based competition, which has a real, quantified throughput ceiling
and no per-instance locality (07 §6, §1.2). The documented upgrade path (07 §7.1) fixes this with
two independent hashing layers wrapped around the same fenced claim primitive: **node → virtual
partition**, using jump consistent hash, because that assignment is append-only and instance-
agnostic (07 §3.3); and **partition → instance**, using Highest Random Weight (HRW) hashing,
because instances leave and rejoin at arbitrary points, not just at the end of a sequence, and
jump consistent hash's own paper states plainly that only the highest-numbered bucket can be
retired cleanly — using it for instance assignment "makes it more suitable for data storage
applications than for distributed web caching" and is the wrong tool where crash-and-rejoin at
arbitrary index is the normal case (07 §3.3, §7.2). Conflating the two layers — using one algorithm
for both — reintroduces exactly the failure mode jump hash's own paper warns against.

The two-layer design is not needed on day one: v0.1-v0.4 ship `P = 1` (ADR-0013's literal absence
of partitioning), and the naive `partition mod live_instance_count` rule is enough to validate the
plumbing (fencing epoch per partition, membership heartbeats, partition-scoped claim queries)
before investing in HRW's math (07 §7.3, v0.2 step). What *is* needed on day one is the seam: if
the assignment logic is hardcoded inline at every claim call site now, replacing it later touches
every call site instead of one internal implementation. Kafka's own KIP-848 rebalance-protocol
rewrite is the industrial precedent for this exact shape — it replaced a "stop the world"
synchronized-barrier rebalance with per-member independent reconciliation as a protocol-version
upgrade specifically because the *what* (claim/assign) stayed stable while the *how* (rebalance
mechanics) was swapped underneath, and existing clients kept working unmodified (07 §7.3).

## Decision

**The engine calls through one internal, unexported interface for every partition-routing
decision, from the first commit that touches claim routing — never a hardcoded modulo or hash
inline at a call site, even while v0.1's implementation is the trivial single-partition case.**

```go
// internal/distribution — never imported outside the engine; never part of
// the public claim API's shape at any point (07 §7.3, §7.2).
type Assigner interface {
    // Partition maps a node to one of p virtual partitions. Pure function of
    // (nodeID, p); append-only and instance-agnostic — jump consistent hash
    // is the intended implementation from v0.2 on (07 §3.3).
    Partition(nodeID string, p int) int

    // Owner maps a partition to the instance that should service it, given
    // the current live-membership view. Pure function of (partitionID,
    // live) — no shared state, no network round trip, no coordination
    // between instances computing it. HRW is the intended implementation
    // from v0.3 on (07 §3.2, §7.1 Layer 3).
    Owner(partitionID int, live []InstanceID) InstanceID
}
```

`Owner`'s answer is **advisory routing, never a correctness boundary** — a wrong or stale answer
during a membership transition costs at most one wasted claim attempt against a partition another
instance already owns; the underlying claim is still gated by the mandatory fencing epoch
(ADR-0006), which is the only thing status-write correctness ever depends on (07 §7.3).
This is why the interface can be swapped without a version bump to the public API.

`P`, the virtual partition count, is fixed **per scope at scope-creation time** (07 §7.1 Layer 2)
and is a `ScopeConfig` field (ADR-0034) — it is not global, and it is not renegotiated after nodes
exist in the scope (see ADR-0034's in-flight-change semantics for why).

**Staged rollout, each stage an internal swap only:**
- **v0.1-v0.4:** `Assigner` is not exercised at all beyond the constant `P = 1` case — every node
  maps to partition 0, every instance is the owner (equivalent to ADR-0013's pure competition).
- **v0.2:** `Partition` becomes jump consistent hash over `P` (from `ScopeConfig.PartitionCount`);
  `Owner` starts as `live_sorted_by_id[partition % len(live)]` — a two-line function, deliberately
  worse under churn than HRW, that exists to validate every other piece of the plumbing first.
- **v0.3:** `Owner`'s implementation is swapped for HRW plus bounded-load capping (`c ≈ 1.25-1.5`,
  07 §3.5) with a stealing fallback for locally-starved instances (07 §7.1 Layer 5). `Partition`'s
  jump-hash implementation is untouched. No public API changes in either step — `Claim`'s signature
  never grows a partition parameter.

## Consequences

### Positive
- The v0.2 → v0.3 upgrade (naive modulo → HRW) is an internal refactor, not an API break, because
  the seam already exists — this is the entire point of the ADR and is load-bearing for the phased
  plan's "no public API break in a later phase" constraint.
- v0.2 can validate fencing-per-partition, membership heartbeats, and partition-scoped claim
  queries against a two-line `Owner` implementation before any HRW code is written, catching
  plumbing bugs cheaply.
- Testing the assignment logic in isolation is possible from day one (`Assigner` is a plain
  interface with no I/O), independent of whichever backend is under test.

### Negative
- One interface call is added to the claim-routing path even in v0.1's trivial case — cheap (a
  single method call, a plausible inlining candidate) but real, non-zero indirection on a hot path.
- Carrying an unused abstraction for four phases (v0.1-v0.4) has a real cost in reviewer attention
  and the risk that its shape is wrong before real membership churn ever exercises it — mitigated
  by keeping the interface's surface (two pure functions) small enough that a wrong guess is cheap
  to fix before v0.3 ships.

### Neutral
- `Owner`'s advisory-only status means a bug in a future `Assigner` implementation degrades
  throughput (redundant scanning, wasted claims), never correctness — this is a deliberate
  consequence of where the interface sits in the architecture, not an incidental property.

## Alternatives considered

- **Hardcode the naive modulo rule now, introduce the interface only when HRW is built**: rejected
  — this is structurally the "stop the world" rebalance Kafka's own KIP-848 replaced; retrofitting
  an interface boundary after call sites already assume a concrete function means touching every
  call site instead of one implementation swap, exactly the cost this ADR exists to avoid (07 §7.3).
- **Use jump consistent hash for both layers**: rejected for the `Owner` role specifically — the
  algorithm's own paper limits clean removal to the highest-numbered bucket only, and instances
  leave at arbitrary index as the normal case, not the exception (07 §3.3).
- **Maglev-style precomputed lookup table for `Owner`**: rejected — Maglev optimizes for a regime
  (huge, rarely-changing backend sets, a table of ~65,537 entries) that does not match dag-worker-
  go's regime of tens of instances with genuine membership churn; rebuilding an `O(M)` table on
  every join/leave is disproportionate machinery here (07 §3.4, §7.2).
- **Pool virtual partitions across scopes on one shared ring** instead of per-scope: left as an
  open question in the source dossier itself (07 open questions) rather than decided now; per-scope
  namespacing is simpler to reason about for the v0.2/v0.3 rollout and is what ADR-0034's
  per-scope `PartitionCount` field assumes — cross-scope pooling remains a documented, deferred
  option, not foreclosed by this interface's shape.

## References

- [Thaler & Ravishankar — Highest Random Weight hashing, IEEE/ACM ToN 6(1), 1998](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/02/HRW98.pdf)
- [Lamping & Veach — Jump Consistent Hash, arXiv:1406.2294](https://arxiv.org/pdf/1406.2294)
- [Eisenbud et al. — Maglev, NSDI 2016](https://www.usenix.org/conference/nsdi16/technical-sessions/presentation/eisenbud)
- [KIP-848 — The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- docs/research/07-work-distribution-across-instances.md §3, §7
- ADR-0013 (the correctness floor this interface sits above); ADR-0034 (`ScopeConfig.PartitionCount`)
