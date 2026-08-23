# 12 — DAG Semantics and State Machine

Scope: the semantic design of the dag-worker-go DAG itself — the public status vocabulary,
dynamic mutation rules, failure propagation, scopes, node identity/payload, and retention. This
is the dossier that shapes the public Go API, so every recommendation below is written as if it
were going straight into `types.go`.

---

## 1. The state machine

### 1.1 Why this is hard: two failure modes seen in every prior-art system

Every mature job/task/workflow engine converges on the same tension:

- **Too few states** → users can't tell "this never ran because its parent failed" from "this ran
  and failed," so they build ad-hoc side channels (tags, log-scraping) to recover the distinction.
- **Too many public states** → the status becomes a leaky abstraction of the *scheduler's*
  internal bookkeeping, and every consumer of the status enum has to relearn scheduler internals
  to build a UI or an alert.

The systems below all made this trade differently. The recurring, load-bearing lesson — stated
explicitly by the Airflow docs themselves — is that **`skipped` and `upstream_failed` are not the
same as `failed`, and collapsing them loses information downstream consumers rely on**:
"[Task] enters `upstream_failed` [only] when a preceding task fails and trigger rules prevent
continuation," which is a materially different operational fact than "the task's own code threw."
[Airflow — Task Instances](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)

### 1.2 Survey of prior art

#### Apache Airflow — `TaskInstanceState` (14 values in Airflow 3.x)

| State | Meaning | Terminal? |
|---|---|---|
| `none` | Dependencies not yet met, not queued | no |
| `scheduled` | Scheduler determined dependencies are met | no |
| `queued` | Assigned to an executor, awaiting a worker slot | no |
| `running` | Executing on a worker | no |
| `restarting` | Externally requested restart while running | no |
| `up_for_retry` | Failed, retries remain, will be rescheduled | no |
| `up_for_reschedule` | Sensor in `reschedule` mode, releasing the worker slot between pokes | no |
| `deferred` | Handed off to a triggerer (async wait, e.g. long external-event wait) | no |
| `awaiting_input` | Waiting on a human response (human-in-the-loop) | no |
| `success` | Finished without error | **yes** |
| `failed` | Own execution errored, no retries left | **yes** |
| `skipped` | Deliberately bypassed — branching, `LatestOnly`, trigger rule | **yes** |
| `upstream_failed` | An upstream task failed and the trigger rule requires it | **yes** |
| `removed` | Task vanished from the DAG definition since the run started | **yes** |

[Airflow — Task Instances](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)

The critical design fact: Airflow ships **four distinct terminal-failure-shaped states**
(`failed`, `skipped`, `upstream_failed`, `removed`) instead of one. Each answers a different
operator question:
- `failed` → "look at this task's logs, it broke."
- `upstream_failed` → "don't look at this task, look upstream."
- `skipped` → "this was never supposed to run, that's fine."
- `removed` → "the DAG source changed out from under a live run."

Airflow's trigger-rule docs also record a genuine footgun worth inheriting as a *warning* rather
than a feature: "Skipped tasks will cascade through trigger rules `all_success` and `all_failed`,
and cause them to skip as well" — an accidental, transitive skip storm through a diamond, which
is exactly the kind of behavior a minimal-vocabulary design must decide on explicitly rather than
back into by accident.
[Airflow — DAGs / trigger rules](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)

Airflow also separates **liveness detection** from the state enum: `task_instance_heartbeat_timeout`
(scheduler config) marks a task `failed` if it stops heartbeating, independent of any
`on_failure_callback`/`on_retry_callback` hook that only fires on an already-committed state
change.
[Airflow — configuration reference](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html)

#### Temporal — Workflow status vs. Activity timeout taxonomy (two independent state spaces)

Temporal deliberately does **not** have one shared enum for workflows and activities — they are
different objects with different failure semantics, and conflating them is exactly the mistake a
minimal public vocabulary must avoid.

Workflow Execution Status (open: `Running`, `Paused`; closed: `Completed`, `Failed`, `Cancelled`,
`Terminated`, `Continued-As-New`, `Timed Out`).
[Temporal — Workflows](https://docs.temporal.io/workflows)

Activities have **no status enum at all** — instead they have four *timeout* dimensions that
compose to define failure, because an activity is a leaf unit of work whose only interesting
transitions are "started," "finished," and "the four ways it can go dark":

| Timeout | Definition | Detects |
|---|---|---|
| `ScheduleToStartTimeout` | scheduled → a worker picks it up | worker pool starved / crashed |
| `StartToCloseTimeout` | single attempt's execution wall-clock | one attempt hanging |
| `ScheduleToCloseTimeout` | first schedule → final closed status, spanning all retries | total budget exceeded |
| `HeartbeatTimeout` | max gap between `RecordActivityHeartbeat` calls | worker died mid-attempt without crashing the process |

"If heartbeats cease within the specified timeout window, the server recognizes the worker has
likely crashed and can promptly initiate recovery procedures rather than waiting for the broader
Start-To-Close timeout to expire."
[Temporal — Detecting Activity Failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures)

This is the strongest prior-art argument for dag-worker-go's own "worker ack timeout, settable
per node at claim time" requirement: Temporal proves that a *coarse* timeout (schedule-to-close)
and a *fine* one (heartbeat) are complementary, not redundant, and that both live below the
public status line — the workflow/DAG consumer only ever sees `Failed`/`TimedOut`.

#### Argo Workflows — `NodeStatus.phase`

`Pending | Running | Succeeded | Failed | Error | Skipped | Omitted`.
[Argo Workflows — Fields](https://argo-workflows.readthedocs.io/en/latest/fields/#nodestatus)

Argo keeps `Failed` (the pod ran and its main container exited non-zero) distinct from `Error`
(the *controller* couldn't even run the step — infra fault, not a code fault) — the same
own-fault vs. infra-fault split dag-worker-go needs between "worker returned error" and "worker
never answered." Argo's `retryStrategy.retryPolicy` names this explicitly with `OnFailure` vs.
`OnError` as separate retry triggers.
[Argo Workflows — Retries](https://argo-workflows.readthedocs.io/en/latest/retries/)

Argo's DAG template also has a **fail-fast toggle at the DAG level**, not per node: "By default,
DAGs fail fast: when one task fails, no new tasks will be scheduled" once already-running tasks
finish; `failFast: false` lets "all branches run to completion, regardless of failures in other
branches."
[Argo Workflows — DAG walk-through](https://argo-workflows.readthedocs.io/en/latest/walk-through/dag/)

#### Kubernetes — Pod `phase` vs. `conditions` (the canonical two-level precedent)

Phase: `Pending | Running | Succeeded | Failed | Unknown` — five values, always present, always a
simple linear-ish summary. Kubernetes' own docs are explicit that phase is *not* the real state
machine: "The phase is not intended to be a comprehensive rollup of observations of container or
Pod state, nor is it intended to be a comprehensive state machine."
[Kubernetes — Pod Lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)

Underneath, `conditions` is an open-ended, independently-flippable set of booleans
(`PodScheduled`, `Initialized`, `ContainersReady`, `Ready`, plus custom readiness gates) that
controllers actually key their logic on. This is the direct model for the public/internal split
recommended in §1.10: a small closed `phase`-like enum for humans and simple consumers, plus a
richer, extensible, structured detail underneath for programmatic consumers — but *not* an
open boolean-condition-set, because a DAG node's internal lifecycle is linear enough that a
richer *enum* (not a condition set) is the right shape, argued in §1.10.

Kubernetes Jobs add the retry/timeout vocabulary at the *pod-attempt* level: `backoffLimit`
(attempt cap) and `activeDeadlineSeconds` (absolute wall-clock timeout for the whole Job),
with retry spacing of "10s, 20s, 40s ... capped at 6 minutes" between pod restarts.
[Kubernetes — Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/)

#### Job queues: Sidekiq, River — the "distinguish infra state from business outcome" pattern

River's `rivertype.JobState` (Go, directly comparable prior art since it's also a Go library):

| State | Doc comment |
|---|---|
| `available` | "immediately eligible to be worked" |
| `scheduled` | "scheduled for the future"; scheduler flips it to `available` |
| `retryable` | "errored, but will be retried"; scheduler flips it to `available` when due |
| `running` | actively running; **"If River can't update state of a running job, that job will be left as `running` and will require a pass by the job rescuer service"** |
| `completed` | succeeded; reaped after a retention window (default 24h) |
| `discarded` | exhausted retries; "manual user intervention is required" |
| `cancelled` | user-cancelled; reaped after retention window (default 24h) |
| `pending` | "parked while waiting for some external action... will never be worked or deleted unless moved out of this state by the user" |

[River — `rivertype` package](https://pkg.go.dev/github.com/riverqueue/river/rivertype)

Two details are directly reusable:
1. River's `running` state has an explicit escape hatch for the exact failure mode this project
   must handle — a worker that took a job and never came back — via a background "rescuer" pass,
   which is the architectural precedent for a timeout sweeper rather than a synchronous
   deadline check (see §1.11, row *Ack timeout expiry*).
2. `pending` (blocked-on-external-action, never auto-picked) is a **different state from
   `scheduled`** (blocked-on-time, auto-transitions) — directly informative for §1.12's
   PENDING/BLOCKED-vs-READY question below.

Sidekiq's retry ladder is the standard "job queue" backoff citation: delay `= retry_count**4 + 15
+ rand(10 * (retry_count + 1))`, ~25 default attempts spanning about 20 days, after which the job
moves to a capped **Dead set** (10,000 jobs or 6 months, whichever binds first) that requires
manual replay.
[Sidekiq — Error Handling](https://github.com/sidekiq/sidekiq/wiki/Error-Handling)

#### AWS Step Functions — state-machine-level `Retry`/`Catch`, not a job-status enum

Step Functions doesn't expose a task status enum to the *caller* at all — it exposes execution
status (`RUNNING | SUCCEEDED | FAILED | TIMED_OUT | ABORTED`) plus, per `Task`/`Parallel`/`Map`
state, declarative `Retry` and `Catch` blocks keyed on structured **error names**
(`States.Timeout`, `States.TaskFailed`, `States.HeartbeatTimeout`, `States.ALL`, ...).
[AWS Step Functions — error handling](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html)

This is the strongest existing precedent for the "reason/outcome as a separate typed field"
recommendation in §1.10: Step Functions' `Retry` entries are `{ErrorEquals, IntervalSeconds,
MaxAttempts, BackoffRate, MaxDelaySeconds, JitterStrategy}` — i.e., the *retry policy itself*
is a first-class value keyed by error classification, not a side effect of which enum value got
set. Their built-in `States.Timeout` (task ran past `TimeoutSeconds` or missed a heartbeat) is
carried as an **error name**, not a distinct top-level execution status — direct prior art for
answering "is TIMEOUT its own status?" with "no, it's a reason under Failed" (§1.12).
[AWS Step Functions — states reference](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-states.html)

#### Google Cloud Tasks — attempt-level telemetry, not a state enum

Cloud Tasks' `Task` resource has no `status` field at all — only `dispatchCount`/`responseCount`
counters and a `firstAttempt`/`lastAttempt` pair of `{scheduleTime, dispatchTime, responseTime,
responseStatus}` structs, plus a per-task `dispatchDeadline` ("the deadline for requests sent to
the worker" — HTTP default 10 min, range 15s–30min) whose expiry surfaces as a `DEADLINE_EXCEEDED`
`responseStatus`, which is functionally identical to the ack-timeout requirement in this project's
brief.
[Cloud Tasks — Task reference](https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks#Task)

Queue-level retry config (`maxAttempts`, `minBackoff`, `maxBackoff`, `maxDoublings`,
`maxRetryDuration`) computes delay as: double `minBackoff` up to `maxDoublings` times, then grow
linearly by `2^maxDoublings × minBackoff` until `maxBackoff` is hit, then hold at `maxBackoff`
(worked example: `minBackoff=10s, maxBackoff=300s, maxDoublings=3` → `10, 20, 40, 80, 160, 240,
300, 300, ...`).
[Cloud Tasks — configuring queues](https://docs.cloud.google.com/tasks/docs/configuring-queues)

### 1.3 Cross-system comparison table

| System | Public surface | Own-fault vs. infra-fault split? | "Never should have run" state? | Timeout modeled as |
|---|---|---|---|---|
| Airflow | 14-value enum | yes (`failed` vs `upstream_failed`) | yes (`skipped`, `removed`) | zombie heartbeat → `failed` |
| Temporal | 7-value workflow status + 4 activity timeout kinds | yes (`Failed` vs 4 timeout kinds) | no (workflow-level `Terminated` only) | own dimension, not folded into status |
| Argo | 7-value node phase | yes (`Failed` vs `Error`) | yes (`Skipped`, `Omitted`) | `Error` + retryPolicy `OnError` |
| Kubernetes Pod | 5-value phase + open condition set | partially (`Failed` reason field) | no | separate `activeDeadlineSeconds`/probe machinery |
| River | 8-value enum | yes (`running`-stuck needs a rescuer) | no | rescuer service, not a state |
| Sidekiq | 4 conceptual buckets (queued/retry/dead/done) | no | no | none built in |
| AWS Step Functions | 5-value execution status + structured error names | yes (error name taxonomy) | no | `States.Timeout` as an error name |
| Google Cloud Tasks | no status enum, attempt telemetry only | yes (`responseStatus` code) | no | `dispatchDeadline` → `DEADLINE_EXCEEDED` |

**Conclusion drawn from the table:** every system that has lived long enough to accumulate
production scars ends up with (a) a small closed set for "where are we in the lifecycle," (b) a
separate way to say "whose fault, if any," and (c) timeout as a *reason under failure*, never a
sibling top-level state. No production system reviewed makes timeout its own peer of
success/failure — that alone answers one of the six explicit questions in the brief.

### 1.4 Proposed model: Public `Status`, internal `Phase`, and a structured `Outcome`

Three layers, not two, because the survey shows two independent axes need separating even inside
"internal": *where in the pipeline* (queued vs claimed vs waiting-on-timeout-sweep) and *why it
ended the way it did* (which trigger rule fired, which error class). Collapsing those two into
one internal enum reproduces Airflow's 14-value sprawl one level down.

```go
// Status is the PUBLIC vocabulary. Four values. This is the entire contract
// exposed to subscribers and to the worker ack API. Never add a fifth value
// without a major version bump — this enum is the API's load-bearing wall.
type Status uint8

const (
    StatusNew        Status = iota // exists, not yet ready (blocked or not yet dispatched)
    StatusInProgress               // claimed by a worker, ack pending
    StatusSuccess                  // terminal, succeeded
    StatusError                    // terminal, did not succeed — see Outcome for why
)

// Phase is INTERNAL scheduling detail. Never serialized to subscribers directly;
// it exists so the engine's own code (and storage backends) has a place to put
// bookkeeping state without polluting Status. Exposed read-only via Node.Phase()
// for debugging/admin tooling, but no public API contract promises its values
// are stable across minor versions.
type Phase uint8

const (
    PhaseBlocked   Phase = iota // Status=New; unmet dependencies
    PhaseReady                  // Status=New; dependencies met, sitting in the ready set
    PhaseClaimed                // Status=InProgress; a worker holds the lease
    PhaseSucceeded               // Status=Success
    PhaseFailed                  // Status=Error; Outcome.Reason=ReasonWorkerError
    PhaseTimedOut                // Status=Error; Outcome.Reason=ReasonTimeout
    PhaseUpstreamFailed          // Status=Error; Outcome.Reason=ReasonUpstreamFailed
    PhaseSkipped                 // Status=Error; Outcome.Reason=ReasonSkipped (see 1.5.4 note)
    PhaseCancelled                // Status=Error; Outcome.Reason=ReasonCancelled
)

// Outcome is populated once a node reaches a terminal Status. It is the
// structured "reason" field the brief asks for, modeled directly on AWS Step
// Functions' error-name-plus-metadata pattern (concepts-error-handling.html)
// rather than folding reasons into more top-level statuses.
type Outcome struct {
    Reason    Reason    // closed enum, see below
    Message   string    // worker- or engine-supplied human text, size-capped
    Attempt   uint32    // 1-based attempt number this outcome belongs to
    Timestamp time.Time
}

type Reason uint8

const (
    ReasonNone           Reason = iota // Status=Success
    ReasonWorkerError                  // worker explicitly Nacked/returned error
    ReasonTimeout                      // ack deadline elapsed with no response
    ReasonUpstreamFailed                // a required predecessor ended in Error
    ReasonSkipped                       // trigger rule decided this node should not run
    ReasonCancelled                      // engine- or caller-initiated cancellation
    ReasonRemoved                        // node/edge deleted out from under the run
)
```

Design notes tying this back to the survey:

- **`StatusNew` covers both `PENDING`/`BLOCKED` and `READY`.** See §1.5.1 for why this is a
  deliberate collapse, not an oversight.
- **`StatusInProgress` is the "claimed" leaf**, directly answering the brief's "probably also
  in-progress" — every surveyed system needs this because "assigned but not yet acked" is a
  distinct, actionable state (it is exactly what a lease-expiry sweeper watches).
- **`Outcome.Reason` is what carries `skipped`/`upstream_failed`/`timeout`/`cancelled`** without
  growing `Status`. This directly implements the recurring lesson from §1.1: the *information*
  Airflow's four failure-shaped states carry is preserved, but it rides on a field, not on the
  status enum, so the public vocabulary stays at four values forever.
- **`Reason` is still closed and small (7 values)**, not a free-text bag, because programmatic
  consumers (retry-policy selection, dashboards) need to switch on it. Free text lives in
  `Outcome.Message` only.

### 1.5 Explicit answers to the brief's pointed questions

#### 1.5.1 Is PENDING/BLOCKED distinct from READY in the public API? — **No.**

Recommendation: **collapse both into `StatusNew`.** Reasoning:

- The distinction is a *scheduling* fact, not a *business-outcome* fact — from the perspective of
  "does the host program's operator need to act," blocked-on-a-dependency and
  ready-to-be-claimed-any-millisecond are the same thing: "not started yet, nothing to do."
- Every system that *does* expose this split (Airflow's `none`/`scheduled`/`queued`) does so
  because *humans stare at a Gantt chart* and want to see queueing delay — a debugging UI need,
  not an orchestration need. dag-worker-go is a library, not a UI; give the *internal* `Phase`
  (`PhaseBlocked`/`PhaseReady`) to any admin/debug surface instead of inflating the wire-level
  status subscribers must switch on.
- It also sidesteps a nasty invariant problem: in a *dynamic* DAG, a node can flip from Blocked to
  Ready and back to Blocked (a new predecessor edge is inserted — see §2.2) purely as a side
  effect of graph mutation, with no worker or caller action involved. Making that flip a public,
  subscribed status transition means every subscriber must handle a new-node-added event that
  looks exactly like a state regression on an existing node. Keeping it internal removes the
  regression entirely: `StatusNew` never "regresses," it just keeps being new.
- Cost of this choice: a consumer that *wants* queueing-delay metrics must ask the engine
  directly (`node.Phase()` or a metrics hook), not infer it from the subscription stream. That's
  an acceptable trade for keeping the stream's invariants simple (see the transition table below
  — `StatusNew → StatusNew` is the only self-loop, and it is never emitted as an event).

#### 1.5.2 Is TIMEOUT an error with a reason, or its own status? — **A reason.**

Every mature system agrees on this even where they disagree on everything else: Step Functions
carries `States.Timeout` as an **error name** matched inside the same `Retry`/`Catch` machinery as
any other failure, not a sibling of `SUCCEEDED`/`FAILED`
([AWS docs](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html));
Cloud Tasks surfaces deadline expiry as a `responseStatus` code (`DEADLINE_EXCEEDED`), not a task
state; Airflow's heartbeat timeout resolves directly to `failed`; Temporal's four timeout *kinds*
all resolve to the workflow-level `Failed`/`TimedOut` closed status, never to an activity-level
peer of "completed." dag-worker-go should do the same: `Status = StatusError`,
`Outcome.Reason = ReasonTimeout`. This also means a node that timed out and a node whose worker
called `Nack(err)` are trivially distinguishable by a consumer that cares (retry-policy selection
needs to know: did the worker actively refuse, or silently vanish?) without adding a fifth public
status.

#### 1.5.3 Is CANCELLED public? — **Yes, as a `Reason`, not as a fifth `Status`.**

Argument for including it at all (unlike `Removed`, which is arguably too obscure for v1 — see
open questions): cancellation is caller-initiated and must be distinguishable from
worker-initiated failure for the same reason timeout must be — a retry policy and an alerting
rule both need "did this fail because someone told it to stop" as a first-class fact, not
something inferred from `Outcome.Message` string-matching. River makes `cancelled` a full
top-level state distinct from `discarded`
([rivertype docs](https://pkg.go.dev/github.com/riverqueue/river/rivertype)); Temporal makes
`Cancelled` a full closed-status peer of `Failed`
([Temporal Workflows](https://docs.temporal.io/workflows)). dag-worker-go doesn't need a fifth
public status to get this distinction — `ReasonCancelled` under `StatusError` is sufficient and
keeps the promise that `Status` never grows.

#### 1.5.4 Where does `skipped` go, given "everything else internal"?

This is the one place where collapsing into `StatusError` is debatable and worth flagging as an
explicit trade rather than hiding it. Airflow treats `skipped` as *not a failure* — a
`none_failed_min_one_success` trigger rule downstream of a skip is satisfied, not blocked
([Airflow trigger rules](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)).
If dag-worker-go reports a skipped node as `StatusError`, a naive subscriber computing
"did the DAG succeed" by checking "any node has `StatusError`" will get a false alarm on every
conditionally-skipped branch. Two resolutions, and the recommendation:

- **(a)** Keep `StatusError` for skipped, but require any DAG-level completion/health check to be
  reason-aware (`Reason != ReasonSkipped` counts as real failure). This is the recommendation —
  it keeps the four-value promise absolute — **provided** the library also ships a first-class
  `Scope.Health()`/`Run.Outcome()` aggregate that already excludes skips, so "did the naive
  subscriber get it wrong" is not left as a homework problem for every caller.
- **(b)** Add a fifth public status `StatusSkipped`. Rejected for v1: it's the exact sprawl this
  project's owner explicitly wants to avoid, and it only matters if v1 supports conditional
  trigger rules at all — which §3.2 recommends deferring past `all_success`/`all_done`.

### 1.6 Full transition table

Legend for **Actor**: `Engine` = library-internal scheduler logic (no external call), `Caller` =
host program via public API (`AddNode`, `Cancel`, ...), `Worker` = external worker via
`Ack`/`Nack`, `Sweeper` = background timeout-detection goroutine/service.

| # | From (Status/Phase) | To (Status/Phase) | Actor | Trigger | Storage operation |
|---|---|---|---|---|---|
| T1 | — | New/Blocked | Caller | `AddNode` with unmet deps, or deps not yet added | `INSERT node`; per-dependency counter init |
| T2 | — | New/Ready | Caller | `AddNode` with zero dependencies | `INSERT node`; `PUSH ready-set` |
| T3 | New/Blocked | New/Ready | Engine | last unmet predecessor edge resolves (predecessor reaches Success, or is removed, or trigger rule is satisfied) | decrement dep-counter to 0 (atomic); `PUSH ready-set` |
| T4 | New/Blocked | New/Blocked | Caller | `AddEdge` inserting a new predecessor into an already-blocked node | increment dep-counter; `INSERT edge` |
| T5 | New/Ready | New/Blocked | Caller | `AddEdge` inserting a new, unresolved predecessor into a currently-ready node (see §2.2) | atomic: `POP ready-set` + increment dep-counter + `INSERT edge` |
| T6 | New/Ready | InProgress/Claimed | Engine (on behalf of Worker via `Claim`) | worker calls `Claim(scope, kind?)` | atomic `POP ready-set` (or lease-CAS); `SET status=InProgress, lease_deadline=now+timeout, attempt+=1` |
| T7 | InProgress/Claimed | Success | Worker | `Ack(nodeID, attempt, result)` | `SET status=Success, outcome={}`; for each successor: run T3-equivalent dep-resolution |
| T8 | InProgress/Claimed | Error/Failed | Worker | `Nack(nodeID, attempt, err)` | `SET status=Error, outcome={ReasonWorkerError,...}`; run T9/T10/T11 fan-out per failure policy |
| T9 | InProgress/Claimed | New/Ready (retry) | Engine | `Nack` and `attempt < maxAttempts` | atomic: `SET attempt` unchanged (see §3.4), `SET status=New`, schedule re-ready after backoff delay; `PUSH ready-set` (delayed) |
| T10 | InProgress/Claimed | Error/TimedOut | Sweeper | `lease_deadline` elapsed, no `Ack`/`Nack` received | `CAS status=Error WHERE lease_id=<fenced token> AND status=InProgress`; `outcome={ReasonTimeout,...}` |
| T11 | New/Blocked | Error/UpstreamFailed | Engine | a required predecessor reaches Error and the node's trigger rule (default `all_success`) is now unsatisfiable | `SET status=Error, outcome={ReasonUpstreamFailed,...}`; recurse to this node's successors |
| T12 | New/Blocked or New/Ready | Error/Skipped | Engine | trigger-rule evaluation determines the node can never satisfy its rule (e.g. `all_success` with a skipped/failed predecessor and no override) | `SET status=Error, outcome={ReasonSkipped,...}`; recurse |
| T13 | New/\* or InProgress/Claimed | Error/Cancelled | Caller | `Cancel(nodeID)` or `CancelScope(scopeID)` | `CAS status IN (New,InProgress) -> Error`; `outcome={ReasonCancelled,...}`; if fail-fast policy, recurse to successors as T11-equivalent with `ReasonCancelled` |
| T14 | any node in a scope | Error/Removed | Caller | `RemoveNode`/`RemoveEdge` targeting a node with live successors still depending on it (see §2.4) | `SET status=Error, outcome={ReasonRemoved,...}` on affected successors only if their dep can no longer resolve; the removed node itself is hard-deleted, not transitioned |
| T15 | Success or Error (terminal) | — | Caller | GC/retention sweep (§6) | `DELETE node` + tombstone/cursor bookkeeping |

Notes on invariants this table is designed to preserve:

- **Every row that changes `Status` writes exactly one `Outcome` or clears it** — there is no
  transition that leaves `Outcome` stale relative to `Status`, which is what makes the two safe
  to read non-atomically by a subscriber (read `Status` first, then `Outcome`; a torn read at
  worst shows an old-but-still-consistent-with-a-prior-Status pair, never a mismatched one, if the
  storage layer writes them in the same row/transaction — mandatory for every backend, see the
  storage dossier).
- **T6 and T10 are the only transitions that use compare-and-swap keyed on a fencing token**
  (lease id / attempt number) rather than a plain state check, because they are exactly the two
  places two actors can race for the same node: a worker's late `Ack` racing the sweeper's
  timeout, and two engine instances (different processes, shared storage — see the concurrency
  dossier) racing to claim the same ready node. This directly follows Kleppmann's fencing-token
  argument — "the storage service must actively validate tokens and reject any requests with ...
  a non-increasing value"
  ([How to do Distributed Locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html))
  — applied with `attempt` doubling as the token (§3.4).
- **No row transitions directly from `Success` back to anything.** Once succeeded, a node is
  immutable except for deletion (T15). This matches every surveyed system — none of Airflow,
  Temporal, Argo, River, or Step Functions allow a completed unit of work to un-complete; "retry"
  always means a *new* attempt on a node that is not yet in a terminal state, never a mutation of
  a terminal one.

### 1.7 The public `Status` subscription stream contract

Given the table above, the event stream (requirement: "every node status transition") should only
emit on rows that change `Status`, not `Phase` — i.e., T3/T4/T5 (internal Ready/Blocked churn) are
**not** emitted as status-transition events, only as the separate "ready for processing" signal
the brief calls out as event class (b). This keeps the two announced event classes crisp:

1. **Status-transition events** — fire on T1/T2 (New created), T6 (→ InProgress), T7 (→ Success),
   T8/T9/T10/T11/T12/T13/T14 (→ Error, with `Outcome.Reason` attached). Exactly one event per row
   in the table above that has a `Status` in the "To" column different from "From," or is a
   node's first appearance.
2. **"Take this for processing" events** — fire once per T6-eligible moment, i.e., whenever a node
   enters `New/Ready` (T2, T3, or T9-after-backoff), *not* when it's claimed. This is what lets
   multiple worker-pull loops race for the same signal without the signal itself being
   consumed-once (claiming is a separate, CAS'd `Claim` call — the notification is "go look," not
   "yours").

---

## 2. Dynamic mutation semantics

### 2.1 The five hard cases the brief names, framed as one underlying question

All five cases the brief lists reduce to: **"what does the engine do when the fact 'this node has
no more predecessors that can affect it' turns out to be false after the engine already acted on
it as if it were true?"** That's a graph problem (cycle safety, dependency-count correctness) and
a distributed-systems problem (has-this-DAG-terminated) at once. §2.7 below treats the second
half directly via Dijkstra–Scholten/Safra; this section treats the graph-mechanics half.

### 2.2 Adding an edge into a node that is already running or already succeeded

This is the retroactive-dependency case and the one genuinely hard design decision in this
dossier, because no surveyed system needs to answer it — Airflow, Argo, and Step Functions all
require the full graph shape to be knowable before a run starts (Airflow's dynamic task mapping
still expands a *known template* at runtime, it doesn't let a caller wire a brand-new edge into an
already-*running* task instance; Argo's `withParam` generates sibling nodes, not new predecessors
of an existing node). dag-worker-go's brief explicitly requires exactly this, so the recommendation
here is original design, argued from first principles plus the closest analogues (transaction
isolation semantics, and Temporal's "history is append-only, decisions are made from a snapshot"
model):

| Target node's current status | Adding edge `pred → target` | Recommendation |
|---|---|---|
| `New/Blocked` or `New/Ready` | `pred` not yet terminal | **Allowed, ordinary case.** `target` moves (or stays) Blocked; if it was Ready, T5 applies (pop it back out of the ready set — see below for why this must be atomic). |
| `InProgress/Claimed` | `pred` not yet terminal | **Allowed, but non-retroactive: does not interrupt the in-flight attempt.** The edge is recorded for the *next* attempt (retry) or for downstream consumers that read the graph shape, but the worker holding the current lease already received its input snapshot and keeps running to completion. Rationale: interrupting live work because the graph shape changed underneath it violates the same "isolation" expectation transactional systems give — a running attempt should see a consistent snapshot of its inputs, not a torn one. This is a deliberate, documented **non-goal**: dag-worker-go does not support mid-flight cancellation-on-dependency-change; the caller who needs that must `Cancel` explicitly (T13). |
| `Success` (terminal) | new edge `pred → target`, and `pred` is not yet terminal | **Rejected by default, with an explicit opt-in.** Default: `AddEdge` returns a typed `ErrAlreadySucceeded` — a completed node's success is a fact about the past, and silently re-blocking it would mean "success" stopped being a terminal, trustworthy signal, breaking the single invariant §1.6 leans on hardest (no transition out of Success). Opt-in: an explicit `AddEdge(..., WithReopen())` that performs `Success → New/Blocked` as a documented, rare, audited operation (its own `Outcome.Reason = ReasonReopened` would need to be added if this ships — flagged as an open question, §Open questions, since it's the one place the "closed enum" promise strains). |
| `Error` (terminal, any reason) | new edge `pred → target` | **Rejected**, same default as Success, same opt-in shape. An already-failed/skipped/cancelled node re-entering the graph as blocked is even less obviously correct than reopening a success, since its `Outcome.Reason` no longer describes reality once it can run again. |

**The concurrency-critical detail inside "allowed" (rows 1–2):** the transition from Ready back to
Blocked (T5) must be atomic with the edge insert, or a worker can `Claim` the node in the gap
between "edge recorded" and "dep-counter incremented," running it before its new dependency is
honored. Every backend needs a single atomic operation for "insert edge AND (if target was Ready)
remove it from the ready set AND increment its dep-counter" — in Redis this is one Lua script (see
the storage dossier for the full script); in PostgreSQL one `SERIALIZABLE` or advisory-lock-guarded
transaction; in-memory one mutex-guarded critical section. This is the single most important
concurrency-correctness requirement to fall out of this dossier's analysis and should be flagged
to whichever dossier owns storage-backend design.

### 2.3 Adding an edge that would create a cycle

**Recommendation: reject synchronously at insert time with a typed `ErrCycle` error, at
O(1)-amortized cost using a topological-order (not per-edge DFS) invariant.**

Naive cost: checking "does adding edge `u → v` create a cycle" by DFS from `v` looking for a path
back to `u` costs O(V+E) per insert in the worst case — the standard cycle-detection bound, since
"a topological ordering is possible if and only if the graph has no directed cycles," and both
Kahn's algorithm and DFS-based topological sort run in O(|V|+|E|)
([Topological sorting — Wikipedia](https://en.wikipedia.org/wiki/Topological_sorting)). Doing a
full O(V+E) walk on every single edge insert directly violates this project's headline O(1)/O(log
n) performance goal at 1M-node scale.

The standard trick used by incremental-topological-order maintenance (Pearce & Kelly,
*"A Dynamic Topological Sort Algorithm for Directed Acyclic Graphs"*, ACM JEA 2006, the
canonical reference for this exact problem) is to **maintain an explicit topological order number
(`ord[v]`) per node** and use it as a cheap pre-filter:

```go
// Cheap path: if ord[pred] < ord[target] already, the edge cannot create a
// cycle without also implying one already existed (topological invariant
// holds), so this is the common case and it's O(1).
func (g *graph) addEdgeChecked(pred, target NodeID) error {
    if g.ord[pred] < g.ord[target] {
        g.insertEdge(pred, target) // O(1): append to adjacency, bump dep-counter
        return nil
    }
    // Slow path: ord[pred] >= ord[target]. The order must be repaired before
    // we know whether a cycle exists. Bounded local re-topo-sort touching only
    // the "affected region" between ord[target] and ord[pred] — Pearce/Kelly
    // bound this at O((V+E) log V) worst case across a whole batch, but for a
    // single edge it is proportional to the size of the affected region, not
    // the whole graph, and the affected region is empty (i.e., a true cycle)
    // exactly when target can already reach pred.
    if g.canReach(target, pred) { // bounded BFS within the affected window
        return ErrCycle{Pred: pred, Target: target}
    }
    g.repairOrder(pred, target) // re-number the affected region
    g.insertEdge(pred, target)
    return nil
}
```

Cost model to put in the perf dossier's cross-reference: **O(1) amortized for the overwhelmingly
common case** (new edges added in roughly causal order, which is true for the fan-out and
retroactive-dependency patterns this library actually needs to support — a new node is normally
wired to *recently* created nodes, not to ones deep in the topological past), degrading to
work proportional to the *affected region size* (never the whole graph) only when an edge is
inserted "against the grain" of current topological order. This is the honest cost, and it should
be explicitly benchmarked (adversarial insert order vs. causal insert order) in the performance
dossier rather than asserted as O(1) unconditionally.

Cheaper, coarser alternative worth naming for v1 if implementing Pearce–Kelly correctly is judged
too much engineering risk up front: **per-scope generation counters plus a same-scope-only cycle
check limited to a bounded neighborhood** (e.g., reject any edge whose target is within k hops
upstream of pred via a bounded reverse-BFS, and fall back to a full check only past that bound,
logging it as a slow-path metric). This trades a small risk of missing a very-long-range cycle
check being fast for guaranteed worst-case bounds; not recommended as the final answer, but a
legitimate fallback if full incremental topo-order maintenance slips the schedule.

### 2.4 Deleting a node or edge

| Operation | Effect | Recommendation |
|---|---|---|
| `RemoveEdge(pred, target)` | `target`'s dep-counter decrements | If this was the last unmet dependency, `target` becomes Ready immediately (identical mechanics to T3) — deleting a blocking edge is dependency-resolution-by-removal, not a special case. |
| `RemoveNode(id)` where `id` is `New`/Blocked or Ready, no successors yet claimed | full delete | Straightforward: cascade-decrement all successors' dep-counters as if a `RemoveEdge` fired for every outgoing edge. |
| `RemoveNode(id)` where `id` is `InProgress` | **reject by default** (`ErrNodeInFlight`), require `Cancel` first | Deleting live work out from under a worker holding a lease is a correctness hazard identical to the retroactive-edit-of-a-running-node problem in §2.2 — force the caller through the explicit, auditable `Cancel` (T13) path instead of overloading delete with cancel semantics. |
| `RemoveNode(id)` where `id` is terminal (`Success`/`Error`) and has live successors depending on it | full delete, successors marked per T14 | The successor's `ReasonRemoved` distinguishes "my dependency vanished" from `ReasonUpstreamFailed` ("my dependency ran and failed") — a materially different operational fact worth the extra `Reason` value, directly mirroring Airflow's dedicated `removed` state, which exists for exactly this "the DAG changed out from under a live run" scenario ([Airflow task states](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)). |

### 2.5 Adding a successor to an already-completed node — becomes ready immediately

This is the load-bearing dynamic-DAG case (it's what makes fan-out and streaming pipelines work at
all) and should be treated as the *default*, unsurprising path, not an edge case:
`AddNode(newNode, dependsOn=[completedPred])` — if `completedPred.Status == Success`, `newNode`'s
dep-counter for that edge starts pre-satisfied; if `newNode` has no other unmet deps, it is Ready
the instant it's inserted (T2-with-a-precomputed-satisfied-edge, not T1-then-T3 — no intermediate
Blocked state should ever be observable). If `completedPred.Status == Error`, the recommendation
in §3 is that `newNode` is immediately `Error/UpstreamFailed` (T11) rather than Ready — a node
should never be scheduled onto the ready set as a live consequence of wiring it to an
already-failed predecessor, since that predecessor will never re-run to unblock it.

### 2.6 Dynamic fan-out: survey and the pattern to adopt

| System | Mechanism | When task count is known | Cap/limit |
|---|---|---|---|
| Airflow ≥2.3 | `task.expand(x=iterable)` / `partial().expand()` | at scheduling time, from a literal or from an upstream task's *return value* ("task-generated mapping") | `max_map_length` (default **1024**); `max_active_tis_per_dag` throttles concurrency [Airflow dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html) |
| Prefect | `task.map(iterable)` returns a list of `PrefectFuture`s | at call time, from any Python iterable, including one produced earlier in the same flow | none built-in; governed by the task runner's concurrency limits [Prefect — run work concurrently](https://docs.prefect.io/v3/how-to-guides/workflows/run-work-concurrently) |
| Argo Workflows | `withItems` (static list) vs. `withParam` (JSON array from a prior step's stdout) | `withItems`: parse time; `withParam`: **runtime**, "you can generate the JSON in another step ... so creating a dynamic workflow" | none named in docs; bounded by controller resource limits in practice [Argo — loops](https://argo-workflows.readthedocs.io/en/latest/walk-through/loops/) |
| Dask distributed | `dask.delayed` recursion, or `get_client()`/`secede()`/`rejoin()`/`worker_client()` to submit new futures **from inside a running task** | fully dynamic — a task can decide, using its own computed result, to spawn more tasks, with no pre-declared shape at all | none — bounded only by cluster resources; `secede`/`rejoin` exist specifically to avoid worker-thread deadlock while a task blocks on children it just spawned [Dask distributed — task launch](https://distributed.dask.org/en/stable/task-launch.html) |
| Ray | any remote function can call another remote function and return/chain `ObjectRef`s ("nested tasks"); dependencies are expressed as futures resolved at runtime | fully dynamic, same class as Dask | governed by cluster scheduling, not a declared cap |

Two families emerge: **template-expansion-at-schedule-time** (Airflow, Argo `withParam`, Prefect
`.map`) where the *set* of new nodes is computed once, from one piece of data, and inserted as a
batch; and **fully organic runtime spawning** (Dask, Ray) where any running unit of work can
insert more nodes at any time, including nodes that depend on itself.

**Recommendation for dag-worker-go: support the organic model as the primitive, and let
template-expansion be a caller-side pattern built on top, not a separate library feature.**
Reasoning: the library's public API is already "any caller can `AddNode`/`AddEdge` at any time"
(that's the dynamic-DAG requirement itself) — a worker acking a node's completion is just another
caller, so `Ack(nodeID, result)` plus, in the same logical operation, a batch `AddNodes(children...)`
call gives the host program the Dask/Ray "spawn from inside a task" capability for free, with no
bespoke "expand" API surface to design, version, or benchmark separately. The one thing worth
adding explicitly, because none of the surveyed systems' analogue is directly liftable into a
concurrent-multi-instance setting: **a batch-insert API (`AddNodes([]NodeSpec, edges []EdgeSpec)
→ error`) that is atomic per scope** — either all nodes and edges in the batch land or none do —
so a worker fanning out into 10,000 children under Airflow's own `max_map_length`-style cap (v1
recommendation: a configurable per-call and per-scope batch-size cap, defaulting to something in
the low thousands, mirroring Airflow's 1024 default) doesn't leave the graph half-expanded if it
crashes mid-batch.

### 2.7 Sealed vs. open nodes/scopes, and termination detection

**The problem, stated precisely:** "is this DAG/scope done?" is undecidable from local
information alone in a graph that can still grow, for exactly the reason distributed termination
detection exists: a scope with zero currently-`New`/`InProgress` nodes is not necessarily
finished — some already-succeeded node might, a moment later, be handed a new successor by a
caller that hasn't made its next `AddNode` call yet. This is *structurally* the same problem
Dijkstra and Scholten solved for detecting termination of a "diffusing computation": a
computation is a (possibly growing) tree/DAG of active processes, and the question is exactly
"has every process in this dynamically-shaped computation gone idle, permanently, with nothing in
flight."

**Dijkstra–Scholten**, the paper's own model: a diffusing computation starts at one initiator and
spreads by processes sending messages that spawn or activate other processes; the algorithm has
each process count outstanding "signals" it's owed and only report itself terminated up its
spanning-tree parent once its own subtree has balanced every send with an acknowledgment —
"[for DAGs] a `Deficit` attribute on edges measur[es] the imbalance between received messages and
sent signals... nodes can only terminate after balancing their deficits on both incoming and
outgoing edges," and for general (cyclic-capable) graphs the algorithm "implicitly creates a
spanning tree of the graph by having each node record the first edge through which it receives a
message."
[Dijkstra–Scholten algorithm — Wikipedia](https://en.wikipedia.org/wiki/Dijkstra%E2%80%93Scholten_algorithm)

**Safra's algorithm** (the ring-based alternative, commonly paired with Chandy–Misra–Haas-style
credit/weight schemes and popularized via Mattern's presentations) takes a different, decentralized
approach: a **token circulates a logical ring** of processes accumulating a count of
messages-sent-minus-messages-received across the whole system; when the token returns to the
initiator having circulated once with a net-zero counter and every process was idle when it
forwarded the token, termination is declared. Where Dijkstra–Scholten needs a maintained
spanning tree (good fit when the "spawn" relationship — parent/child — is itself the natural tree
to hang bookkeeping off of), Safra's ring/token approach needs no such structure and tolerates a
much more tangled communication graph, at the cost of a full ring traversal's latency per
termination check.
[Termination detection — Wikipedia](https://en.wikipedia.org/wiki/Termination_detection)

**Why a plain "count of not-yet-terminal nodes" is insufficient for dag-worker-go:** the
naive check `SELECT COUNT(*) FROM nodes WHERE scope=? AND status IN (New,InProgress)` racing
against a concurrent `AddNode` is exactly the diffusing-computation race Dijkstra–Scholten exists
to close — the count can hit zero for one instant while a caller (possibly a worker that just
Acked and is about to fan out, i.e. exactly the pattern in §2.6) is *between* its Ack and its next
AddNodes call, with nothing in the data model recording "there is still an obligation outstanding
here."

**Recommendation: a `Sealed`/`Open` flag, exactly as the brief proposes, modeled as the
project-specific fixed point of Dijkstra–Scholten's "deficit" idea rather than a full
general-purpose termination-detection algorithm** — because dag-worker-go's version of the problem
is *simpler* than the fully general one both algorithms solve: nodes don't send anonymous
messages to each other across an arbitrary communication topology; they mutate a single shared,
already-durable graph structure the engine itself owns and can inspect transactionally. The
"deficit" that must reach zero is not messages-in-flight, it's exactly: **(a) no node in the scope
is `New`/`InProgress`, AND (b) the scope (or the specific node whose completion might trigger new
successors) has been explicitly marked `Sealed` by the caller.** `Sealed` is the caller's
assertion "I am done calling `AddNode`/`AddEdge` against this scope/node" — the equivalent of a
Dijkstra–Scholten process explicitly reporting it will spawn no more children, which collapses the
distributed-detection problem back into a simple, cheap, storage-local check:

```go
// Scope-level completion check — O(1) against a maintained counter, not a
// scan, once Sealed is tracked as a boolean alongside a live not-terminal
// counter maintained by every T1..T14 transition above.
func (s *Scope) IsComplete() bool {
    return s.Sealed && s.notTerminalCount.Load() == 0
}
```

This makes the sealing decision the caller's responsibility (matching the brief's own framing:
"no more successors will be added" is a statement only the caller/host program can make with
authority — the library cannot infer it), while the *counting* stays O(1) via an atomically
maintained live counter per scope, updated on every one of the T1–T15 transitions rather than
recomputed by scanning. An **open** scope (the default) simply never reports `IsComplete()==true`
regardless of counter state, which is the conservative, safe default — it costs nothing but a
`Scope.Seal()` call the caller must remember to make, exactly mirroring how a diffusing computation
without an explicit "no more children" signal from every leaf can never be proven terminated by
definition, not by a bug.

---

## 3. Failure propagation and retry

### 3.1 Two coarse policies, named consistently with the brief

- **Fail-fast**: one node's terminal `Error` immediately cancels every not-yet-terminal descendant
  (T13-style cascade with `ReasonCancelled`, or a dedicated `ReasonUpstreamFailed` — see the
  trigger-rule table below for why `ReasonUpstreamFailed` is the more precise choice for pure
  dependency failures vs. `ReasonCancelled` for an explicit stop-the-world). This is Argo's
  default (`failFast: true`) and Kubernetes Job's implicit behavior once `backoffLimit` is
  exhausted.
- **Continue**: descendants are marked `Error/UpstreamFailed` (or `Error/Skipped`, depending on
  trigger rule) only when *their own* trigger rule becomes unsatisfiable, while unrelated branches
  of the DAG keep running unaffected. This is Argo's `failFast: false` and Airflow's default
  per-task trigger-rule evaluation (each task's rule is checked independently against its own
  immediate predecessors, not against a whole-DAG kill switch).

### 3.2 Airflow's trigger-rule set, tabulated, with the v1 subset recommendation

| Trigger rule | Fires when | Recommend for v1? |
|---|---|---|
| `all_success` (default) | every predecessor is `Success` | **Yes — the only default, ships first** |
| `all_failed` | every predecessor is `Failed`/`upstream_failed` | No — narrow use case (failure-handler nodes), defer |
| `all_done` | every predecessor reached *any* terminal state | **Yes** — the standard "cleanup/finally" pattern, cheap to implement (just checks terminality, not outcome) |
| `all_done_setup_success` | like `all_done`, plus ≥1 setup predecessor succeeded | No — depends on an Airflow-specific "setup/teardown" task concept out of scope for v1 |
| `all_done_min_one_success` | all non-skipped predecessors done, ≥1 succeeded | No — defer, composes two conditions v1 doesn't need yet |
| `all_skipped` | every predecessor is `Skipped` | No — narrow |
| `one_failed` | ≥1 predecessor `Failed`, doesn't wait for the rest | No for v1 (needs "doesn't wait" semantics — a node becoming ready *before* all predecessors are terminal, which complicates the dep-counter model in §2 non-trivially: it requires a second, race-prone kind of "ready," see open questions) |
| `one_success` | ≥1 predecessor `Success`, doesn't wait for the rest | No for v1, same reason as `one_failed` |
| `one_done` | ≥1 predecessor terminal, doesn't wait | No for v1, same reason |
| `none_failed` | zero predecessors in `Failed`/`upstream_failed` | **Yes** — cheap (a boolean OR maintained per-node, same shape as the existing dep-counter machinery) and it is the documented fix for Airflow's own cascading-skip footgun, so shipping it in v1 directly addresses the failure mode called out in §1.2 |
| `none_failed_min_one_success` | no failures AND ≥1 success | **Yes** — Airflow's own recommended default for anything downstream of a branch, and it's `none_failed` plus a trivial extra AND |
| `none_skipped` | zero predecessors `Skipped` | No — defer, narrow |
| `always` | ignore predecessors entirely | **Yes** — trivial to implement (a node with `always` simply isn't gated by dep-counting at all) and it's the primitive fail-fast-cancellation and cleanup-hook patterns are built from |

**v1 recommendation: `all_success` (default), `all_done`, `none_failed`,
`none_failed_min_one_success`, `always`.** All five share the property that they can be evaluated
*incrementally*, as each predecessor reaches a terminal state, without ever needing to look at a
predecessor that hasn't finished yet and without needing the "doesn't wait for the rest" early-fire
semantics that `one_*` rules require — which is the actual engineering reason to defer the
`one_*` family, not merely "narrow use case": early-fire trigger rules mean a node can become
Ready while some of its declared predecessors are still `InProgress`, which means those
predecessors' eventual completion must be handled as a no-op against an already-scheduled
successor — a second code path through every transition in §1.6's table that v1 should not take on
before the core state machine has production mileage.
[Airflow — DAGs / trigger rules](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)

### 3.3 Retries: attempts, backoff, jitter, budgets

**Backoff formula: full jitter, per the AWS Architecture Blog's own head-to-head testing.** The
post defines three candidate formulas —

```
FullJitter        = random_between(0, min(cap, base * 2^attempt))
EqualJitter        = half = min(cap, base * 2^attempt) / 2
                     random_between(half, half + half)   // i.e. half + random(0, half)
DecorrelatedJitter = min(cap, random_between(base, prev_sleep * 3))
```

— and concludes Full Jitter is the recommendation: it "uses less work" (fewer total retry
attempts consumed across a fleet under contention) than Equal Jitter, and performs comparably to
Decorrelated Jitter while being simpler to reason about and implement, because "adding jitter
addresses the clustering problem where exponentially backed-off calls happen [simultaneously]...
without jitter, exponential backoff alone creates gaps and clusters of calls, whereas jittered
approaches achieve an approximately constant rate of calls."
[AWS Architecture Blog — Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)

```go
// Full Jitter, per AWS's recommendation, as the library default.
func fullJitterBackoff(attempt uint32, base, cap time.Duration) time.Duration {
    exp := base << attempt // base * 2^attempt, saturating before overflow
    if exp > cap || exp < base /* overflow */ {
        exp = cap
    }
    return time.Duration(rand.Int63n(int64(exp) + 1))
}
```

Cross-checking against the three other systems surveyed:
- Cloud Tasks' `minBackoff`/`maxBackoff`/`maxDoublings` scheme (§1.2) has **no jitter term at
  all** — it's a pure deterministic exponential-then-linear ramp. Worth flagging as the weaker
  precedent precisely because it predates (or ignores) the jitter literature; not a
  counter-argument to Full Jitter, just evidence that "ship deterministic backoff" is a real,
  shippable-but-inferior choice other systems made.
- Step Functions' `Retry` block supports `JitterStrategy: FULL | NONE` directly — i.e., AWS's own
  product team took their blog's own recommendation and shipped it as a literal enum value years
  later, which is about as strong an internal-consistency endorsement as a design decision gets.
  [Step Functions error handling](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html)
- Sidekiq's `count**4 + 15 + rand(10*(count+1))` is closer to Equal Jitter in shape (a
  deterministic floor plus a bounded random addition) than to Full Jitter, and its polynomial
  (not exponential) growth is a deliberate choice for a *human-facing* dead-letter timeline (~20
  days to exhaust 25 attempts) rather than a machine-facing thundering-herd concern — not directly
  transferable to a library whose retry timeline should be configurable per node, not baked into
  one global curve.
  [Sidekiq Error Handling](https://github.com/sidekiq/sidekiq/wiki/Error-Handling)

**Recommendation: Full Jitter as the library-wide default, with `base`, `cap`, and `maxAttempts`
settable per node at claim time** (mirroring the brief's own per-node-timeout requirement — the
same `Claim`-time options struct should carry both the ack deadline and the retry policy, since
both are "how should the engine treat this node if the worker doesn't come back cleanly").

**Retry budgets / circuit breaking:** none of the eight systems surveyed implement a cross-node
"retry budget" (a shared cap on total retries-in-flight across a whole scope to stop a systemic
outage from retry-storming a downstream dependency) as a *DAG-engine* feature — it shows up one
layer down, as a property of the *worker's own* HTTP/RPC client (e.g., gRPC's or a service mesh's
retry budget, unrelated to any system surveyed here). **Recommendation: out of scope for
dag-worker-go v1.** The library's job is per-node retry scheduling with jittered backoff; a
cross-node budget is a policy the host program can build on top by watching the status-transition
stream (count `ReasonWorkerError`/`ReasonTimeout` events per scope per window) and calling
`CancelScope` if a threshold trips — exposing the primitive (the stream) is sufficient, building
the policy in is scope creep the brief doesn't ask for.

### 3.4 Retry as a new attempt on the same node, with attempt-as-fencing-token

**Recommendation, matching the brief's own lean: yes, retry is a new attempt on the same
`NodeID`, and the attempt counter doubles as the fencing token.** This directly follows River's
own documented failure mode (§1.2: a `running` job whose engine can't update its state "will be
left as `running` and will require a pass by the job rescuer service") and Kleppmann's fencing
argument (§1.6): the danger case is a worker from **attempt N** finally responding (a very slow
`Ack`) *after* the sweeper has already declared attempt N timed out and the engine has dispatched
attempt N+1 to a different worker. Without a fencing check, the late `Ack` from the stale worker
could incorrectly mark the node `Success` after a second worker is already redoing the work (or
worse, both attempts write conflicting results).

```go
// Ack/Nack must be fenced against the current attempt number, not merely
// against "is this node currently InProgress" — a plain status check is not
// enough, exactly per Kleppmann: "the storage server [must remember] that it
// has already processed a write with a higher token number" and reject the
// stale one.
func (e *Engine) Ack(nodeID NodeID, attempt uint32, result []byte) error {
    // Single atomic CAS at the storage layer:
    //   UPDATE nodes SET status='success', outcome=...
    //   WHERE id=$1 AND status='in_progress' AND attempt=$2
    // A 0-row update means either the node moved on already (timeout fired,
    // attempt N+1 dispatched) or this Ack is stale — both map to the same
    // caller-visible error, ErrStaleAttempt, so a worker can't distinguish
    // "I was too slow" from "someone else already handled this," which is
    // the correct level of ignorance for a worker to have.
    n, err := e.store.CASSuccess(nodeID, attempt, result)
    if err != nil {
        return err
    }
    if n == 0 {
        return ErrStaleAttempt
    }
    return nil
}
```

This makes `attempt` do double duty as (a) the retry-count the backoff/max-attempts policy reads
and (b) the fencing token the CAS keys on — one field, two jobs, and no separate lease-UUID scheme
needed, because attempt numbers are already monotonic per node by construction (T6 increments on
every claim) and already durable (they're part of the row a retry mutates anyway).

---

## 4. Scopes

### 4.1 What a scope is, mapped onto five distinct responsibilities

The brief lists five things a scope must be a unit of; the survey shows these five responsibilities
are usually split across *different* mechanisms in other systems (Kubernetes splits them across
Namespace + ResourceQuota + RBAC + a separate GC controller), which is itself the argument for
`dag-worker-go` deliberately fusing them into one concept rather than four:

| Responsibility | Kubernetes' analogue | dag-worker-go's `Scope` |
|---|---|---|
| Namespace (naming) | `Namespace` object, DNS-safe name | scope ID is an opaque caller-chosen string, implicitly created on first use — no `CreateScope` call required, exactly mirroring "namespaces are created implicitly" in the brief |
| Isolation | RBAC bound to a namespace | key-prefixing (below) — a scope's data is physically partitioned in every backend's key space, so cross-scope access is a bug class prevented by key shape, not by an access-control check the library would have to enforce at runtime |
| Ownership | namespace `labels`/`annotations`, external convention | out of scope for the *library* — no built-in ACL system; ownership is a host-program concern layered on top, same as Kubernetes leaves "who may touch this namespace" to RBAC, a separate subsystem, not core namespace semantics |
| Concurrency limit / quota | `ResourceQuota` object, "enforce[d]... at the namespace level," rejected at admission with `403 Forbidden` on violation ([Kubernetes ResourceQuota](https://kubernetes.io/docs/concepts/policy/resource-quotas/)) | a per-scope `MaxConcurrentInProgress` (and optionally `MaxNodesTotal`) enforced at `Claim` time / `AddNode` time by the same atomic op that would otherwise unconditionally admit — reject with a typed `ErrScopeQuotaExceeded`, same admission-time-rejection shape Kubernetes uses |
| GC/retention unit | no first-class per-namespace TTL in core Kubernetes (left to controllers) | first-class: `Scope.RetentionPolicy` (§6) — this is one thing dag-worker-go should do *better* than the Kubernetes analogue, since "when do old DAG runs get deleted" is squarely inside this library's job in a way "when do old namespaces get deleted" was never Kubernetes core's job |

### 4.2 Key-prefix design

Every storage backend keys all scope-owned data off a `scope:<scopeID>:` prefix, so that:
- Redis: `SCAN`/`KEYS` restricted to a scope is a prefix match, and a Lua script enforcing
  quota/atomicity (§2.2, §3.4) only ever touches keys under one prefix, never needing cross-prefix
  coordination.
- PostgreSQL: `scope_id` as a leading column in every composite primary key and every index, so
  every query the engine issues is naturally scope-scoped and index-local, and a `DROP` of a whole
  scope's rows is a single indexed range delete, not a full-table scan.
- Memcached: since it has no native namespacing or scan primitive, scope prefixing is the *only*
  mechanism available and must additionally maintain an explicit per-scope node-ID index (a
  separate key) since Memcached cannot enumerate keys by prefix at all — a concrete design
  constraint this dossier flags for the storage-backend dossier rather than solving here, since
  Memcached's complete absence of a "list keys matching X" primitive means several O(1)/O(log n)
  operations trivial in Redis/Postgres (e.g., "list all Ready nodes in this scope") need a
  Memcached-specific auxiliary index structure to stay off O(n).

### 4.3 May edges cross scopes? — **No.**

**Recommendation: reject `AddEdge` (and multi-node `AddNodes`+edges batches) that reference a node
in a different scope, with a typed `ErrCrossScopeEdge`.** What this buys, concretely:

1. **The O(1)/O(log n) performance goal stays achievable per-scope.** If edges could cross scopes,
   "is this DAG done" (§2.7) and cycle detection (§2.3) would both need to reason about a graph
   that spans key-prefix boundaries, defeating the entire point of prefixing (§4.2) as a
   performance mechanism — every cross-scope-capable operation degrades to needing global
   coordination exactly where per-scope prefixing was supposed to let backends shard/partition
   independently in a future clustered mode.
2. **Deletion/GC (§6) stays scope-local.** A scope can be torn down (all its keys deleted) without
   a global "is anyone else still pointing at one of my nodes" check — which is precisely the kind
   of global liveness question §2.7 shows is expensive and race-prone. Kubernetes' own namespace
   deletion has to grapple with exactly this for cross-namespace owner references; not needing to
   is a real simplification, not merely a restriction.
3. **The quota/concurrency-limit mechanism (§4.1) stays meaningful.** A `MaxConcurrentInProgress`
   quota on scope A is trivially bypassable if scope B can hold a node that fans out into scope A
   via a cross-scope edge dependency chain most of whose accounting lands on B.
4. **Cost of the restriction:** a caller who genuinely needs one causal chain spanning two logical
   namespaces must either put both halves in one scope (using the *labels* mechanism, §5.4, to
   still get logical partitioning within it for selective subscription) or explicitly bridge them
   at the application layer (scope A's terminal node's `Ack` triggers a scope-B `AddNode` call from
   host-program code, i.e., the same "worker fans out" pattern from §2.6, just crossing a scope
   boundary through caller code rather than through the graph itself). This is a real, but small
   and rare, cost — most reported no-scope-boundaries-should-exist use cases are actually just
   "one scope with labels."

---

## 5. Node payload and identity

### 5.1 Caller-supplied IDs vs. library-generated, and idempotent insert

**Recommendation: caller-supplied string IDs, required, with idempotent insert defined as
same-ID-same-payload = no-op, same-ID-different-payload = conflict error.** This mirrors the
industry-standard idempotency-key pattern directly: Stripe's API "saves the resulting status code
and body of the first request made for any given idempotency key... [and] compares incoming
parameters to those of the original request and errors if they're not the same to prevent
accidental misuse."
[Stripe — Idempotent requests](https://docs.stripe.com/api/idempotent_requests)

```go
// AddNode is idempotent keyed on caller-supplied ID. Same id + byte-identical
// spec (payload, deps, labels) is a silent no-op returning the existing node's
// current view — critical for at-least-once callers (a host program retrying
// its own AddNode call after a network blip must not create a duplicate node
// or, worse, silently fork the DAG into two shapes depending on which retry
// "won").
func (s *Scope) AddNode(id NodeID, spec NodeSpec) (Node, error) {
    existing, ok := s.store.Get(id)
    if ok {
        if !specEqual(existing.Spec, spec) {
            return Node{}, ErrIDConflict{ID: id} // Stripe's "parameters ... not the same" case
        }
        return existing, nil // idempotent no-op
    }
    return s.store.Insert(id, spec)
}
```

Why caller-supplied rather than library-generated (e.g., a UUID/ULID returned from `AddNode`):
a library-generated ID makes idempotent retry *impossible* to express correctly — if the host
program's `AddNode` call itself times out before the response (with the new ID) arrives, the
caller has no way to know whether to retry (risking a duplicate node under a new ID) or not
(risking having silently dropped the node). Caller-supplied IDs push the idempotency key to where
the caller already has one naturally (most host programs modeling "one node per business
work-item" already have a natural key — an order ID, a request ID — for that work-item). Library
ID generation should still be offered as a convenience helper (`GenerateID() NodeID`, e.g. a
ULID for the lexical-sortability property, unrelated to idempotency) for callers who genuinely
have no natural key, but never as the *only* option.

**Comparison to how other surveyed systems handle this:** Airflow, Argo, and Step Functions all
use caller/definition-supplied names for tasks/states (a DAG is Python/YAML/JSON source naming its
own nodes) — none of them generate task identity for the caller, because a human author needs a
stable name to reference across DAG edits. That precedent supports caller-supplied IDs even setting
the idempotency argument aside. River and Sidekiq, by contrast, generate job IDs (UUIDs) because
their unit of work has no natural pre-existing external key and — importantly — neither system
promises idempotent insert at all (a duplicate `Insert` call there does create two jobs, modulo
River's separate opt-in `UniqueOpts` feature); this project's brief explicitly wants idempotent
insert, which settles the comparison in favor of caller-supplied IDs.

### 5.2 Payload type: opaque `[]byte` vs. a generic type parameter

**Recommendation: `[]byte` at the storage/wire boundary, with a generic convenience wrapper on
top for in-process ergonomics — not a generic type parameter threaded through the core engine.**

Reasoning:
- Every pluggable backend this project must support (Redis, Memcached, PostgreSQL `bytea`/`jsonb`)
  is fundamentally a bytes store at the wire protocol level — a `Storage[T any]` interface would
  force every backend adapter to own (de)serialization for an unbounded set of `T`s, or force the
  library to fix serialization (JSON? gob? protobuf?) as a hidden, hard-to-change default baked
  into the storage contract itself. Keeping the core `Node.Payload []byte` keeps that choice where
  it belongs: with the caller, per node, changeable without touching storage-adapter code.
- A generic `Node[T]` *can* still be layered on top as a thin, optional convenience type
  (`type TypedNode[T any] struct { Node; decode func([]byte) (T, error) }`) for host programs that
  want compile-time payload types and are willing to fix one serialization format for their whole
  DAG — but it should not be the primitive the engine, the storage interface, or the event stream
  are defined in terms of, precisely because the event stream (subscribers "anyone can subscribe
  to") crosses process/language boundaries in the general case (a Redis- or Postgres-backed
  install's subscribers are not guaranteed to be the same Go binary that inserted the node), where
  a Go generic type parameter has no meaning at all — it cannot survive serialization onto a wire.

### 5.3 Size limits

No surveyed system leaves payload size unbounded; the numbers below are the real-world floor a
"support Redis, Memcached, and PostgreSQL" library must design under, since the library's declared
limit can never exceed the tightest backend's hard limit without silently breaking on that
backend:

| System | Limit | Source |
|---|---|---|
| Memcached | default max item size **1 MiB** (`item_size_max`, tunable via `-I`) | [Memcached — Configuring the Server](https://github.com/memcached/memcached/wiki/ConfiguringServer) |
| AWS SQS | message body **256 KiB** (1,048,576 bytes is actually SQS's *newer* raised limit per the fetched quotas page — treat 256 KiB as the traditionally-cited figure and confirm against the current AWS quotas page at implementation time, since AWS has changed this limit over the product's lifetime) | [AWS SQS — message quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html) |
| Redis | value size limited by `proto-max-bulk-len` (default 512 MiB), far above what a DAG node payload should ever need | general Redis operational knowledge, not independently re-verified against current docs in this research pass — flag for verification |
| PostgreSQL | `bytea`/`text` column, practically bounded by TOAST at ~1 GiB per value, again far above a sane node-payload budget | general PostgreSQL operational knowledge — flag for verification |

**Recommendation: a library-default hard cap of 256 KiB per node payload** (matching the
traditionally-cited SQS figure, chosen specifically *because* it's a widely-battle-tested "small
enough to always fit comfortably in Memcached's 1 MiB ceiling with room for framing/metadata, big
enough for the overwhelming majority of real work-item payloads" number, not because SQS is
otherwise relevant to this project), **configurable down** per scope for backends or deployments
that need a tighter bound, and **never configurable up past what the memcached backend's
configured `item_size_max` allows** when that backend is in use — the library should read the
active backend's real limit where introspectable and take the minimum of (library default, caller
override, backend real limit), failing `AddNode` with a typed `ErrPayloadTooLarge` rather than
silently truncating or erroring deep inside a backend driver. The recommended pattern for anything
genuinely payload-heavy (large files, big JSON blobs) is the same one AWS ships for exactly this
case in SQS's own extended-client pattern: **store the large blob in an out-of-band object store
and put a reference (a key/URL) in the node payload** — this project should document that pattern
rather than try to raise its own size ceiling to accommodate it.

### 5.4 Labels/metadata and worker capability matching — argue for it

**Recommendation: yes, ship labels (`map[string]string`, small, indexed) and a `kind`-partitioned
`Claim(scope, opts ClaimOpts{Kind string, Labels map[string]string})` — this is not a nice-to-have,
it is close to load-bearing, and it is cheap specifically because the ready-set can be
kind-partitioned at write time rather than filtered at read time.**

Argument for necessity: a host program embedding this library almost never has one homogeneous
worker pool — the moment there are two kinds of external worker (a GPU-bound worker vs. a
CPU-bound one, a worker that only knows how to call vendor A's API vs. vendor B's), a
`Claim()` that hands back an arbitrary ready node forces the host program to implement its own
filtering-and-requeue loop on top (claim, inspect kind, if wrong kind release-and-retry) — which
both wastes a claim/lease cycle per mismatch and reintroduces exactly the kind of
worker-competing-for-work coordination problem this library exists to solve, just one layer up in
application code instead of inside the library where it belongs. Kubernetes' entire node-affinity /
taint-toleration system and Cloud Tasks' separate named queues both exist because "not every
worker can run every unit of work" is close to universal in practice, not a niche need.

Argument for cheapness ("it looks essential and is cheap if the ready-set is partitioned by
kind," per the brief's own framing, which this dossier endorses): the ready set (§1.6, the thing
`Claim` pops from) does not have to be one structure — it can be one ready-queue-per-`(scope,
kind)` pair, so `Claim(scope, kind="gpu")` is a direct O(1)/O(log n) pop from *that* queue, with
zero filtering cost, rather than a scan-and-skip over a mixed queue. `Labels` beyond `kind` (an
open `map[string]string` for finer-grained capability matching — region, vendor, size class) are
inherently more expensive to serve as O(1) pops (arbitrary label predicates don't partition into a
small fixed set of queues the way a single `kind` string does), so the recommendation is
**two-tier**: `Kind string` gets the fast, structurally-partitioned treatment (a required or
defaulted field, since it's cheap and covers the majority need), while `Labels
map[string]string` is available for selective *subscription filtering* (a subscriber says "only
notify me about nodes matching these labels," filtered at the fan-out point where the cost is
already being paid to distribute the event, not at the `Claim` hot path) rather than promised as
an indexed `Claim`-time predicate in v1.

---

## 6. Retention and GC

### 6.1 When is a finished node deleted?

**Recommendation: never automatically, immediately, on completion — always after a configurable
retention window measured from the terminal-transition timestamp, per scope, with a floor
enforced by unread-subscriber-cursor position (§6.2).** This directly mirrors River's own default
(`completed`/`cancelled` jobs "reaped by the job cleaner service after a configured amount of time
(default 24 hours)") rather than Sidekiq's approach of not retaining successful jobs at all past
the queue they moved through, since dag-worker-go's stronger read-after-write and
subscription-replay requirements (the brief's "receive every node status transition") need a
window River-style systems don't have to provide (a plain job queue has no promise that a
consumer, once notified once, ever needs to re-read the job's final state).
[River — `rivertype` package](https://pkg.go.dev/github.com/riverqueue/river/rivertype)

### 6.2 Interaction with subscribers that have not caught up

This is the actual hard part, and none of the eight systems surveyed is a clean precedent because
none of them combine *durable, replayable, multi-subscriber* event delivery with *per-item TTL
deletion* the way this brief's requirements do simultaneously — the closest real analogue is Redis
Streams' own consumer-group model, which separately solves "how do we know a consumer hasn't
finished with this yet" via the **Pending Entries List (PEL)**: a message stays "pending" (not
GC'd from the group's bookkeeping) from `XREADGROUP` delivery until `XACK`, and `XPENDING`/
`XCLAIM`/`XAUTOCLAIM` exist specifically to recover from a consumer that took a message and never
acked it — structurally the same "don't delete/reclaim state a subscriber might still need" problem
as a node whose status-transition event some subscriber hasn't yet consumed.
[Redis — Streams](https://redis.io/docs/latest/develop/data-types/streams/)

**Recommendation, adapted from that model:**

1. **Track a low-water mark per scope: the oldest position any registered subscriber has not yet
   acknowledged consuming**, exactly analogous to a Streams consumer group's oldest pending entry.
   A terminal node is eligible for deletion only once (a) its retention window has elapsed *and*
   (b) the scope's low-water mark has advanced past its terminal-transition event — i.e., every
   currently-registered subscriber has already seen (or explicitly skipped) the transition that
   made this node terminal.
2. **A subscriber that never checkpoints/acks is a leak, by design, not a silent correctness bug**
   — exactly as an un-acked Streams PEL entry blocks that stream's own trimming. The library should
   expose this as an observable metric (`Scope.OldestUnackedAge()`/`Scope.PendingSubscriberLag()`)
   and, critically, **must not let a single wedged subscriber block retention forever by default**
   — a configurable maximum lag (time-based, e.g. "no subscriber may hold back GC for more than
   72h") after which the low-water mark is forcibly advanced, the stuck subscriber is dropped, and
   a distinguished "you missed events, resync from a full snapshot" signal is delivered to it if
   it reconnects (mirroring how Redis Streams itself expects a consumer that fell too far behind
   to eventually be reaped by `XCLAIM`/`XAUTOCLAIM` from another consumer, or for the stream to be
   trimmed by `MAXLEN` regardless of PEL state if the operator configures it that way — Redis does
   not make unbounded blocking the only option, and neither should this library).
3. **Deletion is always of the *node*, never a silent truncation of undelivered events** — the
   event stream and the node-table lifecycle are two different retention policies that happen to
   be coupled through the low-water mark, not the same mechanism. A backend that can cheaply
   support "keep a small tombstone/last-known-outcome even after the full node payload is GC'd"
   (e.g., Postgres: null out the `payload` column but keep the row with `status`/`outcome` for a
   longer secondary window) should do so, so a very-late subscriber query gets "this node finished
   with outcome X, payload no longer retained" rather than "node not found, indistinguishable from
   never having existed" — the latter is indistinguishable from a bug from the caller's point of
   view and should be avoided wherever the backend makes it cheap to avoid.

### 6.3 Recommended default policy, concretely

- Per-scope configurable `RetentionPolicy{TerminalTTL time.Duration, MaxSubscriberLag
  time.Duration}`, library defaults `TerminalTTL = 24h` (matching River's own default exactly,
  since it's a reasonable, field-tested number for "how long might an operator want to look at a
  finished job") and `MaxSubscriberLag = 72h` (three times the TTL — long enough that a
  subscriber offline over a long weekend isn't punished, short enough that a genuinely-abandoned
  subscriber doesn't pin storage indefinitely).
- GC runs as a background sweep per scope (same architectural family as River's "job cleaner
  service" and the sweeper already required for timeout detection in §1.6 T10 — these two sweeps
  can plausibly share one background-worker abstraction in the implementation, though that's an
  implementation detail for a later dossier, not a semantic one).
- `Scope.Seal()` (§2.7) does not itself trigger GC — a sealed, complete scope still respects the
  same TTL/lag policy as an open one, since "the DAG is logically done" and "operators/subscribers
  are done looking at it" are different facts, exactly the same distinction Argo and Airflow both
  respect by keeping finished DAG-run history around for a separately-configured retention window
  rather than deleting a run's records the instant its last node finishes.

---

## Recommendations for dag-worker-go

1. **Ship a four-value public `Status` enum — `New, InProgress, Success, Error` — and never grow
   it.** Carry everything else (skip, upstream-failure, timeout, cancellation, removal) in a
   closed 7-value `Outcome.Reason` field populated only when `Status` reaches a terminal value.
   This is the single highest-leverage decision in this dossier and is validated by every system
   surveyed converging on "small closed top-level status + richer reason/detail field" once it
   left academic/greenfield status and hit production scars.
2. **Collapse PENDING/BLOCKED into READY at the public layer** (§1.5.1) — both surface as
   `StatusNew`; expose the Blocked/Ready split only via an internal `Phase` accessor for
   debugging, not via the subscription stream, specifically because dynamic mutation (§2.2) can
   flip a node between them with no worker or caller action, which would otherwise force every
   subscriber to treat a scheduling artifact as a state regression.
3. **Model TIMEOUT and CANCELLED as `Outcome.Reason` values, never as top-level statuses** (§1.5.2,
   §1.5.3) — this is unanimous across every system surveyed (Step Functions' `States.Timeout` as
   an error name, Cloud Tasks' `DEADLINE_EXCEEDED` response status, Temporal's timeout kinds all
   resolving to workflow `Failed`/`TimedOut`).
4. **Make the ack-timeout sweep a compare-and-swap keyed on the attempt number**, treating
   `attempt` as a Kleppmann-style fencing token, and use the identical CAS discipline for `Ack`
   and `Nack` — this closes the specific race (a late `Ack` from a timed-out worker) that every
   at-least-once job system (River's own "rescuer service" language, Redis Streams' `XCLAIM`)
   builds explicit machinery to handle.
5. **Reject cross-scope edges outright (`ErrCrossScopeEdge`)** — it is the one restriction that
   keeps §2's cycle-check, §2.7's completion-check, and §6's retention/GC all provably scope-local,
   which is what makes the O(1)/O(log n) performance goal achievable per-operation rather than
   contingent on total graph size.
6. **Reject retroactive edges into `Success`/`Error` nodes by default** (`ErrAlreadySucceeded` /
   equivalent), with an explicit, separately-named opt-in (`WithReopen()`) for the rare case that
   needs it — do not make re-blocking a completed node the default behavior of `AddEdge`, since it
   breaks the one invariant (§1.6) every transition table in this dossier leans on: nothing leaves
   `Success` except deletion.
7. **Maintain a topological-order number per node (Pearce–Kelly-style) rather than doing a fresh
   DFS per `AddEdge`**, to keep cycle rejection at insert time O(1)-amortized for the causal (most
   common) insertion order and bounded-by-affected-region rather than by total graph size in the
   worst case — and benchmark both the causal and adversarial insertion orders explicitly in the
   performance dossier, since this is the one place this dossier's O(1) claim is conditional
   rather than unconditional.
8. **Implement scope completion (`IsComplete`) as `Sealed && notTerminalCount == 0` with an
   atomically-maintained O(1) counter**, not a scan and not a general Dijkstra–Scholten/Safra
   implementation — the fully general termination-detection algorithms solve a harder problem
   (arbitrary message-passing topology) than this library actually has (a single owned,
   transactionally-inspectable graph store), and an explicit caller-driven `Seal()` collapses the
   real problem to a cheap counter check.
9. **Ship exactly five trigger rules in v1**: `all_success` (default), `all_done`, `none_failed`,
   `none_failed_min_one_success`, `always` — all five evaluable incrementally as predecessors
   complete, none requiring "become ready before all predecessors are terminal" semantics. Defer
   the `one_*` family explicitly, and document why (§3.2) rather than silently omitting it.
10. **Default backoff = Full Jitter** (`random(0, min(cap, base*2^attempt))`), per node
    `base`/`cap`/`maxAttempts` settable at claim time alongside the ack-timeout deadline in the
    same options struct — both are "what should the engine do if this worker misbehaves" settings
    and belong together in the API surface.
11. **Caller-supplied string `NodeID`, required; idempotent `AddNode` defined as
    same-ID-same-payload = no-op, same-ID-different-payload = typed conflict error** — the Stripe
    idempotency-key pattern, chosen over library-generated IDs specifically because generated IDs
    make correct retry-after-timeout impossible for the caller to express.
12. **`[]byte` payload at the storage/engine boundary; a generic typed wrapper only as an optional
    convenience layer on top**, never as the primitive the storage interface or event stream are
    defined in terms of, because the event stream must remain meaningful to subscribers in a
    different process (or language) than the inserting caller.
13. **256 KiB default payload size cap**, enforced as `min(library default, caller override,
    backend real limit)` with a typed `ErrPayloadTooLarge`, chosen to sit comfortably under
    Memcached's 1 MiB default `item_size_max` — document the "put a reference to an external blob
    store in the payload" pattern rather than raising the ceiling to accommodate large payloads.
14. **Ship `Kind string` as a first-class, ready-set-partitioning field on every node, with
    `Claim(scope, kind)` an O(1)/O(log n) pop from a per-`(scope,kind)` queue**; ship free-form
    `Labels map[string]string` for subscription-time filtering only, not as an indexed `Claim`
    predicate in v1.
15. **Retention: default 24h terminal-node TTL (matching River), gated additionally on a per-scope
    subscriber low-water mark with a default 72h maximum-lag override** that forcibly advances and
    flags a lagging subscriber for resync rather than blocking GC indefinitely — modeled on Redis
    Streams' PEL/`XCLAIM`/`MAXLEN` combination, since it is the closest real precedent for coupling
    durable multi-consumer delivery with bounded retention.

---

## Open questions

- **Reopening a completed node (`WithReopen()`, §2.2, §6):** if this ships, does `Outcome.Reason`
  need an eighth value (`ReasonReopened`) to describe a node that is no longer in its original
  terminal state, and does that node's *history* (was it ever `Success`, and when) need to be
  retained as an audit trail distinct from its current `Outcome` — this dossier flags the need but
  does not design the audit-log shape, which likely belongs in whichever dossier covers storage
  schema/versioning.
- **Batch-insert atomicity at scale (§2.6):** a per-scope-atomic `AddNodes` batch is recommended,
  but the cap on batch size (Airflow uses 1024 as its `max_map_length` default) interacts with
  each backend's own transaction-size practicalities differently — PostgreSQL can comfortably
  batch thousands of rows in one transaction, but a single Redis Lua script or a Memcached-backed
  implementation may need a materially smaller cap or a different (non-single-transaction) batching
  strategy to stay performant at 1M-node scale; this needs backend-specific benchmarking, not a
  single cross-backend constant.
- **The cost model for adversarial-order edge insertion (§2.3)** is stated qualitatively
  ("proportional to affected region size") but not derived with a tight worst-case bound for this
  project's specific access pattern; a literature review of Pearce & Kelly's amortized bounds
  across a realistic workload (mostly-causal inserts with occasional cross-scope-in-time fan-in)
  should happen before committing to a specific implementation, and is deferred to whichever
  dossier covers the core graph data structure and its benchmarks.
- **`one_*` "early-fire" trigger rules (§3.2):** deferred from v1, but if a future version adds
  them, they require a second notion of "ready" (ready-because-a-quorum-of-predecessors-resolved,
  while others are still `InProgress`) that this dossier's transition table (§1.6) does not model
  — worth a dedicated design pass rather than a bolt-on when the time comes.
- **Exact current numeric limits for Redis (`proto-max-bulk-len`) and PostgreSQL (TOAST/row size)
  cited in §5.3 were not independently re-verified against current upstream docs in this research
  pass** (both are far above the recommended 256 KiB default so they don't change the
  recommendation, but they should be pinned to primary-source citations before this dossier's
  numbers are treated as final).
- **Cross-instance claim contention** (multiple library instances in different processes racing on
  the same `Claim`) is referenced in §1.6/§3.4 as the reason CAS-with-fencing is mandatory, but the
  *distribution strategy* question the brief poses (partition-per-scope vs. consistent hashing vs.
  pure pull-based competition vs. lease stealing) is explicitly out of this dossier's scope and
  belongs to the storage/concurrency dossier — flagged here only so that dossier inherits the
  fencing-token requirement as a hard constraint on whatever distribution strategy it recommends.
