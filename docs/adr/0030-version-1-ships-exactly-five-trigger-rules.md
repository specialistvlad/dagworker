# ADR-0030: Version 1 ships exactly five trigger rules

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/12-dag-semantics-and-state-machine.md §1.5.4, §3.1–§3.2

## Context

A trigger rule decides, per node, when its predecessors' outcomes make it eligible to run — not
merely "have all predecessors finished" but "given how they finished, should this node run, be
skipped, or fail." Airflow ships thirteen such rules after a decade of production use, and its own
documentation names a footgun its default (`all_success`) creates: "Skipped tasks will cascade
through trigger rules `all_success` and `all_failed`, and cause them to skip as well" — an
accidental, transitive skip storm through a diamond-shaped DAG that the rule set's later additions
exist specifically to let a DAG author opt out of (12 §1.2, §3.2).

dag-worker-go's closed, four-value `Status` (ADR-0001) folds `skipped`/`upstream_failed`/`timeout`/
`cancelled` into `Outcome.Reason` under a single `StatusError`. This is precise on purpose (12
§1.5.4): trigger-rule evaluation needs to distinguish "predecessor did not run because a rule
decided against it" (`ReasonSkipped`) from "predecessor actually failed" (`ReasonWorkerError`,
`ReasonTimeout`, `ReasonUpstreamFailed`, `ReasonCancelled`) even though both report `StatusError` —
otherwise the exact cascading-skip bug Airflow's own docs warn about reproduces here, since a naive
"any Error predecessor blocks me" rule cannot tell a benign skip from a real failure.

The engineering reason to ship a *subset* of Airflow's thirteen rules, rather than all of them or
an arbitrary smaller set, is not "narrow use case" for most of the deferred ones — it is that a
family of rules (`one_failed`, `one_success`, `one_done`) requires early-fire semantics: a node
becomes ready while some of its declared predecessors are still `InProgress`, which means those
predecessors' eventual completion has to be handled as a no-op against an already-scheduled
successor. That is a second code path through every row of the state-transition table (synthesis
§5) that has not had production mileage yet, and it does not compose with the incremental
pending-counter model (ADR-0003) the way every rule below does (12 §3.2).

## Decision

Ship exactly five trigger rules in v1, each evaluable **incrementally**, as each predecessor
independently reaches a terminal state, with no rule ever needing to inspect a predecessor that
has not yet terminated:

```go
type TriggerRule uint8

const (
    TriggerAllSuccess              TriggerRule = iota // default
    TriggerAllDone
    TriggerNoneFailed
    TriggerNoneFailedMinOneSuccess
    TriggerAlways
)

// NodeOption; default is TriggerAllSuccess if unset.
func WithTriggerRule(r TriggerRule) NodeOption
```

Every rule except `Always` is evaluated against three incrementally-maintained per-node counters —
`total` (static, set at node creation from the declared predecessor count), `successCount`, and
`failedCount` — where `notTerminal := total - successCount - failedCount`. A predecessor
transitioning to `StatusSuccess` increments `successCount`; a predecessor transitioning to
`StatusError` increments `failedCount` **unless** its `Outcome.Reason == ReasonSkipped`, in which
case neither counter moves and the predecessor is counted only in `notTerminal`'s implicit third
bucket — this is the exact mechanism that stops a benign skip from cascading as a failure, per 12
§1.5.4's own resolution:

| Rule | Short-circuits to `Error/UpstreamFailed` when | Fires (`New/Ready`) when |
|---|---|---|
| `all_success` (default) | `failedCount > 0` **or** any predecessor is `Skipped` (a skip also breaks `all_success`, matching Airflow) | `successCount == total` |
| `all_done` | never | `notTerminal == 0`, any mix of success/failure/skip |
| `none_failed` | `failedCount > 0` (a `Skipped` predecessor does **not** count against this rule — this is the documented fix for the cascading-skip footgun) | `notTerminal == 0` (equivalently `failedCount == 0` at that point, by the short-circuit above) |
| `none_failed_min_one_success` | `failedCount > 0`, same exemption for `Skipped` | `notTerminal == 0 && successCount >= 1`; if `notTerminal == 0 && successCount == 0` (every non-failed predecessor was skipped) the node resolves `Error/Skipped`, not `Error/UpstreamFailed` — there was no failure, only an absence of any success to build on |
| `always` | never | immediately at insertion (T2) — **not gated by predecessor state at all**, even predecessors declared but not yet terminal |

`always` is deliberately not equivalent to `all_done`: an `always` node's declared edges still exist
for graph-structure and observability purposes, but they never gate its readiness — it becomes
`New/Ready` the moment it is inserted with zero unmet non-blocking preconditions from its *own*
non-`always`-governed dependency semantics. A host wanting "run after predecessors finish,
regardless of outcome" — the actual cleanup-hook pattern — uses `all_done`, not `always`; this
distinction is called out explicitly in the package doc comment for `TriggerAlways` to prevent the
two from being reached for interchangeably.

`ReasonSkipped` on `StatusError`, `Outcome.Reason == ReasonUpstreamFailed` for the short-circuit
path above, matching the transition table's existing T11/T12 rows (synthesis §5) — this ADR adds no
new `Reason` value, it only specifies precisely which `Reason`s each rule treats as "counts against
me."

The deferred `one_*` family (`one_failed`, `one_success`, `one_done`) and the narrower Airflow rules
(`all_failed`, `all_done_setup_success`, `all_done_min_one_success`, `all_skipped`, `none_skipped`)
are explicitly **not implemented** in v1 — not stubbed, not silently accepted and ignored.
`WithTriggerRule` with any value outside the five above returns `ErrInvalidConfig` from `AddNode`,
never a silent fallback to `all_success`.

## Consequences

### Positive

- All five rules compose directly with the existing incremental pending-counter machinery
  (ADR-0003, ADR-0005) — no rule requires a second "provisionally ready while predecessors are
  still running" concept, so the state-transition table (synthesis §5) needs no new rows to support
  this ADR, only a precise reading of which existing rows (T3, T11, T12) each rule triggers.
- `none_failed` and `none_failed_min_one_success` directly close the exact production footgun
  Airflow's own documentation names (12 §1.2) — a host can build branch-then-rejoin DAGs where a
  conditionally-skipped branch does not poison its downstream join node, without waiting for a
  later release.
- The five-rule set is small enough to exhaustively conformance-test (ADR-0018) per rule, per
  `Outcome.Reason` combination, before the DAG-semantics engine has any production mileage — the
  explicit goal 12 §3.2 states for deferring the early-fire family.

### Negative

- A host that genuinely needs early-fire semantics (dispatch a fan-in join the instant the first
  branch succeeds, without waiting for slower sibling branches) has no v1 primitive for it and must
  build an approximation on top of `Subscribe` (ADR-0019) plus its own bookkeeping, or wait for a
  later version. This is a real capability gap, accepted deliberately rather than shipped half-built
  against an unproven state-machine extension.
- The `Skipped`-exemption rule for `none_failed`/`none_failed_min_one_success` is a subtle enough
  distinction (a `StatusError` predecessor that does *not* count as failed) that it must be
  documented prominently in the `TriggerRule` doc comments, not left to be inferred from behavior —
  a host reading only the `Status` enum and not `Outcome.Reason` will misjudge these two rules.

### Neutral

- This ADR only fixes which rules ship and their exact firing/short-circuit conditions; the
  mechanics of how a predecessor's terminal transition reaches this counter update (the fenced
  write path, AMD-2) are ADR-0007's and ADR-0003's territory, unchanged by this decision.

## Alternatives considered

**Ship all thirteen Airflow rules for parity with the most mature prior-art system surveyed.**
Rejected: the `one_*`/early-fire family requires a second, race-prone "provisionally ready" concept
threaded through every transition-table row (12 §3.2) that the core state machine has not yet
proven correct under production load; shipping it now trades a proven, small surface for an
unproven, large one for a v1 release.

**Ship only `all_success`, defer every other rule.** Rejected: `none_failed` and
`none_failed_min_one_success` are cheap — "a boolean OR maintained per-node, same shape as the
existing dep-counter machinery" (12 §3.2) — and they are the documented fix for a real, named
production failure mode (Airflow's cascading skip). Deferring them to a later version ships a
known footgun for no engineering-cost reason.

**Model `Skipped` as a fifth public `Status` instead of an `Outcome.Reason`, so trigger rules could
branch on `Status` alone without inspecting `Reason`.** Rejected by ADR-0001/ADR-0002 already — this
ADR inherits that decision rather than reopening it, and the counter design above shows the
`Reason`-aware approach is fully sufficient to implement every v1 rule correctly without a fifth
status value.

## References

- docs/research/12-dag-semantics-and-state-machine.md §1.2, §1.5.4, §3.1–§3.2
- docs/research/00-synthesis.md §3 (ADR-30 seed), §5 (state-machine table, T3/T11/T12)
- Airflow trigger rules — https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
