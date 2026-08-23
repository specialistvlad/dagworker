# ADR-0001: Public status vocabulary is exactly four values

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/01-prior-art-workflow-engines.md §17, docs/research/12-dag-semantics-and-state-machine.md §1.1-1.4, §1.5.1-1.5.4, docs/research/00-synthesis.md §10.5

## Context

Every workflow/queue engine surveyed in dossier 01 accumulates a public status enum under
production pressure, and the size of that enum tracks how much it was allowed to grow rather than
how much information the domain actually needs at the top level. Airflow's `TaskInstanceState` has
grown to 14 values in Airflow 3.x. River ships 8. Kubernetes Pod `phase` has only 5 but immediately
had to bolt on an open-ended `conditions` set beside it because 5 wasn't enough. The dossier's
cross-system table (12 §1.3) makes the pattern explicit across eight systems — Airflow, Temporal,
Argo, Kubernetes, River, Sidekiq, Step Functions, Cloud Tasks — and every one of them, once it had
production scars, converged on the same three-part shape: a small closed "where in the lifecycle"
set, a separate "whose fault, if any" axis, and timeout modeled as a *reason under failure*, never
a sibling top-level state. Not one of the eight makes timeout a peer of success/failure at the
top-level status: Step Functions carries `States.Timeout` as an error *name* inside its
`Retry`/`Catch` machinery; Cloud Tasks surfaces it as a `responseStatus` code, not a task state;
Temporal's four timeout *kinds* all collapse to the workflow-level `Failed`/`TimedOut`.

This matters concretely for dag-worker-go because the public `Status` is the one piece of surface
every subscriber, every retry-policy selector, and every dashboard built on top of the library will
switch on for years — dossier 08's API-shape research treats a library's public enum as effectively
permanent once embedded, on par with a wire format. Growing it later without a major version bump
is not possible once external code has exhaustive `switch` statements over it; growing it *with* a
major version bump means the whole embedding host and every downstream consumer must be
recompiled. The cost of getting this wrong compounds every year the library stays in production,
which is exactly the failure mode dossier 01's survey documents Airflow, Temporal, and River all
living with today.

There was a genuine internal disagreement to resolve here, not just an external survey to read.
Dossier 01's own early executive framing (written before the dedicated state-machine dossier
existed) proposed a 5-value vocabulary — `new / in-progress / success / error / error-timeout` —
keeping timeout as an explicit peer of error. Dossier 12, whose entire scope is the state machine,
re-reads 01's own survey evidence and concludes 4 values, with timeout folded into a reason field.
Synthesis §10.5 resolves this in 12's favor: 01's own comparison table is the evidence *for* the
4-value design once read carefully, and 12 is authoritative on state-machine questions specifically
because that survey-then-synthesize-then-second-guess sequence is exactly how the field itself
arrived at its converged answer.

The remaining design pressure is where to put everything the 4-value enum can no longer carry:
which attempt this is, whose lease this was, why a node failed, what internal scheduling phase
produced this state. All of that needs a real home — see ADR-0002 for the internal-`Phase` half of
that answer.

## Decision

`Status` is a closed `uint8` enum with exactly four values, defined once and never extended:

```go
type Status uint8

const (
    StatusNew        Status = iota // exists, not yet acked by a worker (blocked or ready — Phase, ADR-0002)
    StatusInProgress               // claimed by a worker, ack pending
    StatusSuccess                  // terminal, succeeded
    StatusError                    // terminal, did not succeed — see Outcome.Reason for why
)

func (s Status) Terminal() bool { return s == StatusSuccess || s == StatusError }
```

Every fact the four values cannot express — why a node failed, which attempt this was, whose lease
it held, when the outcome was recorded — rides on a closed, structured `Outcome` written in the
same atomic operation as every transition that reaches a terminal `Status` (per the state-machine
transition table, ADR-0002/ADR-0006):

```go
type Reason uint8

const (
    ReasonNone           Reason = iota // Status == StatusSuccess
    ReasonWorkerError                  // worker explicitly Nacked
    ReasonTimeout                      // ack deadline elapsed, no response — never a peer status
    ReasonUpstreamFailed                // a required predecessor ended in Error
    ReasonSkipped                       // trigger-rule evaluation decided this node cannot run
    ReasonCancelled                      // caller- or engine-initiated cancellation
    ReasonRemoved                        // predecessor removed out from under this node (ADR-0036)
)

type Outcome struct {
    Reason    Reason
    Message   string    // free text lives ONLY here, size-capped
    Attempt   uint32    // 1-based; also the fencing epoch (ADR-0006/ADR-0011)
    Timestamp time.Time
}
```

`Reason` is itself closed and small (7 values) — not a free-text bag — because programmatic
consumers (retry-policy selection, alerting, `Scope.Health()`) must be able to `switch` on it.
Free-form text is permitted only in `Outcome.Message`, which carries no contract and must never be
parsed by a caller for control flow. `Status` growing past four values, or `Reason` growing without
a documented compatibility review, requires a major version bump under Go's own import-compatibility
rule (cross-reference ADR-0031); this is a permanent constraint on the type, not a v1-only rule.

## Consequences

### Positive

- The public switch surface every subscriber and worker SDK writes against stays at four arms
  forever — the single biggest lever against the Airflow-14-value/River-8-value sprawl the survey
  documents as the natural failure mode of *not* deciding this up front.
- Timeout, cancellation, upstream-failure, and skip are all trivially distinguishable by a
  consumer that cares (retry-policy selection needs "did the worker actively refuse or silently
  vanish" — `Reason` answers this without string-matching `Outcome.Message`).
- `Outcome.Attempt` doubling as the retry-count and (per ADR-0006/ADR-0011) the fencing epoch means
  no second field is needed to answer "which attempt produced this outcome."

### Negative

- A caller computing "did this scope succeed" by checking "any node has `StatusError`" gets a false
  alarm on every conditionally-skipped branch (`ReasonSkipped`) unless it is reason-aware. This is a
  real, documented trade (12 §1.5.4) — mitigated, not eliminated, by shipping `Scope.Health()` as a
  reason-aware aggregate from v1 so this is not homework left to every caller.
- Any future status-shaped need (e.g. a genuine "paused" state some host wants) must be expressed
  through `Reason` plus `Status` semantics that already exist, or wait for a major version — there
  is no escape hatch that doesn't touch the version number.

### Neutral

- `Outcome.Attempt`/`Reason` together reproduce the *information* of Airflow's `upstream_failed`,
  `skipped`, and Temporal's four timeout kinds — the survey's information content is fully
  preserved, only its placement (field vs. top-level enum) changes.

## Alternatives considered

**5-value vocabulary with `error-timeout` as a peer of `error`** (dossier 01's own early framing).
Rejected per synthesis §10.5: 01's own later, more careful reading of its own survey data — Step
Functions' `States.Timeout` as an error *name*, Cloud Tasks' `DEADLINE_EXCEEDED` as a *response
status* not a task state, Temporal's four timeout *kinds* all resolving to workflow-level
`Failed`/`TimedOut` — supports the 4-value conclusion, not the 5-value one it started from.

**Airflow's 14-value `TaskInstanceState`.** Rejected as the negative case study this ADR is
designed to avoid: `queued`, `scheduled`, `up_for_retry`, `up_for_reschedule`, `restarting`,
`shutdown`, `removed`, etc. mix scheduling detail with business outcome in one enum, which is
exactly the sprawl ADR-0002 is written to keep out of the public surface entirely.

**Kubernetes Pod `phase` (5 values) plus an open `conditions` list.** Rejected as a bolt-on
precedent: Kubernetes itself needed to add `conditions` because `phase` alone was insufficient,
which is evidence *against* a small top-level enum plus an unstructured escape hatch — dag-worker-go
instead ships one *closed*, structured `Outcome`, not an open condition set, precisely to avoid
recreating this need for a second axis later.

**Temporal's two-independent-state-spaces model** (7-value workflow status + 4 independent
activity-timeout kinds). Rejected as *public* surface: Temporal's split is real and useful
internally, but exposing two independent enums to every subscriber doubles the switch surface for
no benefit dag-worker-go's simpler node model needs — the timeout-kind distinction Temporal makes
(schedule-to-start, start-to-close, etc.) collapses to a single `ReasonTimeout` here because
dag-worker-go ships one flat per-node lease timeout (ADR-0010), not Temporal's four-timeout model.

## References

- Airflow task states: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html
- Temporal Workflows (status model): https://docs.temporal.io/workflows
- AWS Step Functions error handling (`States.Timeout` as an error name): https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html
- River `rivertype` package (8-value job state enum): https://pkg.go.dev/github.com/riverqueue/river/rivertype
- docs/research/01-prior-art-workflow-engines.md §17
- docs/research/12-dag-semantics-and-state-machine.md §1.1-1.5
- docs/research/00-synthesis.md §10.5
