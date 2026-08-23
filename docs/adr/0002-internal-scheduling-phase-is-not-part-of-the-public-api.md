# ADR-0002: Internal scheduling Phase is not part of the public API

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/12-dag-semantics-and-state-machine.md §1.4, §1.5.1, §1.6, §1.7, §2.2

## Context

ADR-0001 fixes `Status` at four values by moving everything else off the top-level enum. `Phase` is
where the *scheduling* half of that everything-else lives: the internal question of exactly where a
node sits in the engine's own bookkeeping — blocked on a dependency, sitting ready for a worker,
claimed and being worked, or one of several ways it reached `StatusError`. Dossier 12 identifies
nine such internal states (`PhaseBlocked`, `PhaseReady`, `PhaseClaimed`, `PhaseSucceeded`,
`PhaseFailed`, `PhaseTimedOut`, `PhaseUpstreamFailed`, `PhaseSkipped`, `PhaseCancelled`) and is
explicit that collapsing this axis into `Status` would reproduce Airflow's 14-value sprawl one
level down — the exact failure mode ADR-0001 exists to avoid.

The forcing function specific to *this* library, not present in any static-DAG system surveyed, is
that dag-worker-go's DAG is dynamic: a node can flip `PhaseBlocked → PhaseReady` and back to
`PhaseBlocked` purely as a side effect of graph mutation (a caller inserts a new, unresolved
predecessor edge into a node that was already sitting ready — T5 in the transition table, discussed
fully at 12 §2.2), with no worker or caller action against *that node* involved at all. If
`Blocked`/`Ready` were public and subscribed, this flip would look, from a subscriber's point of
view, exactly like a status regression on an existing node — the DAG equivalent of a job un-failing
itself. Every subscriber would need bespoke handling for "this isn't really a regression, ignore
it," which is exactly the kind of invariant-weakening special case ADR-0022's bounded-channel event
design is built to keep out of the wire contract.

The systems that *do* expose a Blocked/Ready-shaped split at the top level — Airflow's
`none`/`scheduled`/`queued` progression is the clearest example — do so because a human is staring
at a Gantt chart and wants to see queueing delay. That is a debugging-UI requirement, not an
orchestration requirement, and dag-worker-go is a library embedded in a host process, not a UI.
Conflating the two needs into one public enum is how Airflow arrived at 14 values in the first
place.

## Decision

`Phase` is an internal `uint8` enum, never serialized to the wire, never present on `Event`, and
carrying no cross-minor-version stability promise:

```go
// Phase is INTERNAL scheduling detail. Never on the wire, never on the
// subscription stream. Exposed read-only for debug/admin tooling only.
type Phase uint8

const (
    PhaseBlocked        Phase = iota // Status=New; unmet dependencies
    PhaseReady                       // Status=New; dependencies met, sitting in the ready set
    PhaseClaimed                     // Status=InProgress; a worker holds the lease
    PhaseSucceeded                   // Status=Success
    PhaseFailed                      // Status=Error; Outcome.Reason=ReasonWorkerError
    PhaseTimedOut                    // Status=Error; Outcome.Reason=ReasonTimeout
    PhaseUpstreamFailed              // Status=Error; Outcome.Reason=ReasonUpstreamFailed
    PhaseSkipped                     // Status=Error; Outcome.Reason=ReasonSkipped
    PhaseCancelled                   // Status=Error; Outcome.Reason=ReasonCancelled
)

// Debug/admin accessor only — no stability promise across minor versions.
func (n Node) Phase() Phase
```

Every `Phase` value maps onto exactly one `(Status, Outcome.Reason)` pair — `Phase` never carries
information `Status`+`Outcome` cannot already reconstruct; it exists purely so engine code and
storage backends have a place to put scheduling bookkeeping (specifically, the Blocked/Ready
distinction Kahn's algorithm needs, ADR-0003) without that bookkeeping polluting the public enum.

The event bus enforces the boundary structurally, not by convention: `EventTransition` fires only
on rows of the state-machine table whose `Status` column changes value (T1/T2 as first appearance;
T6 through T14 as terminal or in-progress transitions) — never on `Phase`-only churn. Internal
`Blocked ↔ Ready` movement (T3, T4, T5) is invisible on `EventTransition` entirely; it surfaces only
through the separate `EventReady` doorbell (ADR-0019), which fires once per moment a node enters
`PhaseReady` and is deliberately best-effort and re-derivable from storage — never a status claim.
This means `StatusNew → StatusNew` is the only "self-loop" possible in the public model, and it is
never emitted as an event at all.

## Consequences

### Positive

- Subscribers never see a false regression: a node that becomes ready, gets re-blocked by a new
  predecessor edge, and later becomes ready again produces zero `EventTransition` events for that
  churn — only `EventReady` doorbells, which are documented as re-derivable and lossy-tolerant by
  design (ADR-0019), so a missed or duplicated one is a latency problem, never a correctness one.
- Engine and storage code get a stable, typed place (`Phase`) to store "why is this node not yet
  ready" without inventing ad-hoc sentinel values against the public `Status`/`Outcome` pair.
- `Phase` can grow, shrink, or be renumbered across minor versions with zero compatibility
  obligation, because the ADR-0001 contract only promises `Status`/`Outcome` stability.

### Negative

- A caller that wants queueing-delay metrics ("how long did this node sit blocked before becoming
  ready") cannot get them from the subscription stream at all — it must call `node.Phase()`
  directly or rely on a metrics hook, which is strictly more work than reading an event.
- Because `Phase` carries no stability promise, any admin/debug tooling built against specific
  `Phase` values is implicitly coupled to the current minor version and must be treated as
  such — this is a documented trade, not an oversight, but it is a real constraint on tooling.

### Neutral

- `Phase`'s nine values are a strict refinement of `Status`'s four plus `Outcome.Reason`'s seven —
  no new information exists at this layer that isn't already derivable from the public pair; this
  ADR is entirely about *where* that information lives, not what information exists.

## Alternatives considered

**Expose `Blocked`/`Ready` as public sub-states** (the Airflow Gantt-chart precedent). Rejected:
creates spurious regression-shaped events on every dynamic-edge-into-a-ready-node case (T5), which
is a named, expected operation in this library (12 §2.2), not a rare edge case — making it look like
a fault in the public stream is a design defect, not a minor wart.

**Fold `Phase` into `Status` as additional enum values** (the "just make it 6 or 9 values" option).
Rejected per ADR-0001's own reasoning: this is the literal mechanism by which Airflow reached 14
values, and it reopens the exact compatibility problem ADR-0001 closes — a public enum growing
without bound as scheduling detail accumulates.

**Expose scheduling phase as an untyped string label** (freeform "current phase" field). Rejected:
not stable or typed enough for retry-policy selection or dashboards to switch on programmatically,
and provides no compile-time exhaustiveness checking — the same objection dossier 12 raises against
folding free text into `Status` applies equally to a string standing in for `Phase`.

**No internal phase concept at all — derive everything on demand from stored dependency counters.**
Rejected: the ready-set maintenance in ADR-0003 needs a first-class place to record "this node is
currently sitting in the ready set" versus "blocked," and re-deriving that fact on every debug
query by re-scanning dependency state defeats the O(1)/O(log n) performance discipline this project
is built around (dossier 09).

## References

- Airflow trigger rules / task states (Gantt-chart-oriented status granularity):
  https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
- Kubernetes Pod `phase` vs. `conditions` (two-level precedent for internal vs. observed state):
  the canonical precedent discussed in docs/research/12-dag-semantics-and-state-machine.md §1.2
- docs/research/12-dag-semantics-and-state-machine.md §1.4-1.7, §2.2
- docs/research/00-synthesis.md §5 (state-machine transition table), §4 (public API `Phase` type)
