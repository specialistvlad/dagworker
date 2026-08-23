# Prior Art in Workflow / DAG / Job Orchestration Engines

Research dossier for `dag-worker-go`. Scope: extract concrete state machines, leasing
mechanisms, timeout handling, anti-double-dispatch techniques, and event/notification
designs from the systems named in the brief, plus what each got wrong in production. All
claims are sourced inline; where sources disagree a position is taken.

---

## 1. Temporal and its predecessor Cadence

### 1.1 Lineage

Cadence was created at Uber by Maxim Fateev and Samar Abbas as an open-sourced,
distributed, durable orchestration engine for long-running business logic
[cadenceworkflow.io](https://cadenceworkflow.io/). In 2018 Fateev and Abbas left Uber and
founded Temporal, forking Cadence in a **non-backwards-compatible** way: Uber had run
Cadence for four years without breaking changes and had accumulated technical debt the
founders wanted to shed, so the fork spent roughly a year in private development before its
production release
[temporal.io/blog](https://temporal.io/blog/building-resilient-workflows-from-azure-to-cadence-to-temporal),
[Contrary Research](https://research.contrary.com/company/temporal-technologies). Concrete
changes in the fork: Thrift → Protocol Buffers, HTTP/Thrift RPC → gRPC, native TLS, and
SDKs beyond Java/Go (TypeScript, PHP, Python)
[temporal.io/temporal-versus/cadence](https://temporal.io/temporal-versus/cadence). The two
projects remain API-similar but diverged enough that most of the original Cadence team and
its user base migrated to Temporal — the canonical "why we moved off X" case for this
research, except the "X" and its successor were built by the same two people.

### 1.2 Event sourcing and the determinism constraint

A Temporal **Workflow Execution** is not a process with in-memory state that gets
checkpointed — it is an append-only **Event History**. State changes are persisted as
events *before* being applied, so no state is lost on a server crash
[Temporal docs](https://docs.temporal.io/workflows). Recovery does not restore a snapshot:
the Worker re-runs the workflow function from the top and replays the Event History
step-by-step, using the recorded events to short-circuit any operation that already
happened (rather than re-executing side effects). This only works if the workflow function
is **deterministic**: on replay it must reproduce the exact same sequence of Commands as
the original run. During replay, the SDK diffs the freshly-generated Commands against the
stored History; a mismatch (e.g. non-deterministic branching, a changed loop count, direct
system-clock reads) makes replay fail outright
[Event History walkthrough, Go SDK](https://docs.temporal.io/encyclopedia/event-history/event-history-go).
This is the central design bet of Temporal: **the durable log is the source of truth, and
the workflow function is a pure projection over it.** Any dag-worker-go analog that lets
hosts run arbitrary node-processing code does not need this constraint (activities in
Temporal terms are exempt from determinism — only the orchestrating "decider" code is
constrained), but the event-log-as-truth pattern is directly reusable for the DAG's own
status transitions.

### 1.3 Task queues, matching, and how double-dispatch is avoided

Work handoff to external workers happens through **Task Queues**. A Task Queue is a
lightweight, dynamically-created queue name that any number of Workers can long-poll; a
dedicated **Matching Service** owns queue state and pairs incoming poll requests with
incoming tasks, doing a **sync match** (deliver directly to a waiting poller, no persistence
round-trip) whenever possible and falling back to an **async match** (persist the task,
wait for the next poller) otherwise
[Matching Service architecture doc](https://github.com/temporalio/temporal/blob/main/docs/architecture/matching-service.md).
Double-dispatch is avoided the same way a durable queue avoids it everywhere in this
survey: a task is handed to exactly one poll response, and it is not deleted until the
activity or workflow task completes — if the worker never responds, the **task's own
schedule-to-start / start-to-close timeout** fires and the Matching Service reschedules it,
which is a lease, not a broadcast. For throughput, Task Queues are split into
**partitions** (default 4, tunable) forming a tree; pollers and tasks can be "forwarded"
from a starved child partition up to the root so that an idle poller on one partition can
still receive a task queued on another — this is Temporal's answer to the "how does a
single logical queue scale past one lock" problem
[Matching Service architecture doc](https://github.com/temporalio/temporal/blob/main/docs/architecture/matching-service.md).

### 1.4 Sticky execution — a cache, not a correctness mechanism

**Sticky Execution** binds the *next* Workflow Task for a given Workflow Execution to the
Worker that handled the *previous* one, via an auto-generated **Sticky Queue** unique to
that worker process. The payoff is that the sticky worker already has the workflow's state
machine cached in memory and does not need to replay the full Event History; the cost is
worker memory pinned per open workflow
[Sticky Execution docs](https://docs.temporal.io/sticky-execution). Crucially, sticky
queues carry their own **schedule-to-start timeout, defaulting to 5 seconds** — far
shorter than a normal task queue's — because if the sticky worker is gone, waiting the
normal timeout would stall the workflow for no reason. When that 5s timeout fires,
stickiness for that execution is dropped and the task falls back to the regular (non-sticky)
task queue, at the cost of a full-history replay by whichever worker picks it up next
[Sticky Execution docs](https://docs.temporal.io/sticky-execution). This is a clean pattern
worth stealing narrowly: **affinity is a performance hint with a short, separate timeout,
never the mechanism that guarantees the task is eventually served.**

### 1.5 Heartbeats, timeouts, and long-running activity liveness

Temporal activities (the unit closest to dag-worker-go's "node handed to an external
worker") support four independently-configurable timeouts: Schedule-To-Start (queue wait),
Start-To-Close (execution), Schedule-To-Close (end to end), and **Heartbeat**
[Temporal blog, activity timeouts](https://temporal.io/blog/activity-timeouts). Heartbeat
is the liveness signal for long activities: the worker periodically calls
`RecordActivityHeartbeat`; if the server does not see a heartbeat within
`HeartbeatTimeout`, the activity is considered failed and retried per the retry policy —
independent of the (necessarily much longer) Start-To-Close timeout
[Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures).
On retry, any heartbeat *details* payload from the failed attempt is delivered with the new
attempt so the activity can resume from a checkpoint instead of restarting from zero —
`activity.GetHeartbeatDetails()` in the Go SDK
[activity package docs](https://pkg.go.dev/go.temporal.io/sdk/activity). In Go:

```go
activityoptions := workflow.ActivityOptions{
    StartToCloseTimeout: 10 * time.Minute,
    HeartbeatTimeout:    10 * time.Second,
    RetryPolicy: &temporal.RetryPolicy{
        InitialInterval: time.Second,
        MaximumAttempts: 5,
    },
}
ctx = workflow.WithActivityOptions(ctx, activityoptions)
```

The pattern is directly transplantable to dag-worker-go's per-node settable timeout
requirement: **the timeout that detects a dead worker should be short and heartbeat-driven,
decoupled from the (much longer, business-defined) deadline for the whole unit of work.**

### 1.6 What Temporal got wrong / hard limits

- **Event History is not infinite.** The service logs a warning past **10,240 events** and
  hard-**terminates** the workflow execution past **51,200 total events**, or past **2,000
  updates**, or past **10,000 signals**
  [Event History docs](https://docs.temporal.io/workflow-execution/event). The prescribed
  fix, `ContinueAsNew`, atomically closes the current execution and starts a fresh one
  under the same Workflow ID with a reset history — effectively a manual, developer-owned
  "compact the log" operation
  [Continue-As-New, Go SDK](https://docs.temporal.io/develop/go/continue-as-new). This is a
  hard reminder that an append-only-log-as-truth design needs an explicit truncation/
  compaction story from day one, or it becomes a footgun that only appears at scale.
- **Sticky-queue timeout tuning was itself a multi-year bug hunt.** Issues
  [#2363](https://github.com/temporalio/temporal/issues/2363) and PR
  [#2811](https://github.com/temporalio/temporal/pull/2811) track a **5-second delay after
  a worker disappears** before its sticky tasks fall back to the general queue — the
  fallback timeout was originally too coarse-grained, a reminder that "fall back to a
  general queue" needs its own tight timeout, not the workflow-task default.
- Sticky execution interacting with worker shutdown produced repeated "Workflow Task Timed
  Out" reports across SDKs, e.g.
  [sdk-python#783](https://github.com/temporalio/sdk-python/issues/783) — affinity-to-a-
  dead-process is an evergreen source of spurious timeouts; any sticky/affinity feature
  needs an explicit, fast "worker gone" signal (heartbeat or connection drop), not just a
  timer.

---

## 2. Netflix Conductor

Conductor is an **RPC/pull orchestrator**: task workers live on separate machines from the
server and communicate over HTTP, polling their designated queues for work rather than
being pushed to
[Technical Details](https://conductor.netflix.com/devguide/architecture/technicaldetails.html).
This is architecturally the closest match in this survey to dag-worker-go's "external
workers poll/receive ready nodes" model.

**Task state machine** (task, not workflow, level):

`SCHEDULED → IN_PROGRESS → {COMPLETED | FAILED | FAILED_WITH_TERMINAL_ERROR | TIMED_OUT | COMPLETED_WITH_ERRORS}`,
with `{FAILED, TIMED_OUT} → SCHEDULED` as an automatic, delayed retry, and `CANCELED` /
`SKIPPED` as workflow-driven side transitions
[Task Lifecycle](https://conductor-oss.github.io/conductor/devguide/architecture/tasklifecycle.html).
A task sits in `SCHEDULED` in a queue until a worker polls it, at which point the server
marks it `IN_PROGRESS`
[Task Lifecycle](https://conductor-oss.github.io/conductor/devguide/architecture/tasklifecycle.html).

**Four independent timeouts**, all server-enforced, none worker-trusted:

| Timeout | Fires when | Effect |
|---|---|---|
| `pollTimeoutSeconds` | no worker polls the task at all | task → `TIMED_OUT` |
| `responseTimeoutSeconds` (default 600s) | worker polled but never reported back | task → `TIMED_OUT` |
| `timeoutSeconds` | overall SLA for a single attempt | task → `TIMED_OUT` |
| `totalTimeoutSeconds` | hard budget across *all* retries | no further retries scheduled, regardless of remaining retry count |

[Task Lifecycle](https://conductor-oss.github.io/conductor/devguide/architecture/tasklifecycle.html)

The split between "never polled" and "polled but silent" is worth stealing directly: it
distinguishes a starved queue (no capacity) from a dead worker (had capacity, then
vanished), which are different operational problems and should alert differently. Workers
can also extend their own deadline mid-flight by reporting progress with a
`callbackAfterSeconds` value — a lightweight heartbeat that resets the response-timeout
clock without a full heartbeat RPC
[Task Lifecycle](https://conductor-oss.github.io/conductor/devguide/architecture/tasklifecycle.html).

Orchestration itself is driven by a **"decide"** pass — Conductor's server-side loop
recursively evaluates a running workflow's state, schedules the next ready tasks, persists
checkpoints, and re-runs whenever a task transitions
[Netflix Tech Blog, original architecture](https://netflixtechblog.com/netflix-conductor-a-microservices-orchestrator-2e8d4771bf40).
This is architecturally a **level-triggered reconciler over a persisted graph**, the same
family as Kubernetes controllers (§13) and Argo Workflows (§6), and a strong precedent for
treating "which nodes are now ready" as a pure function recomputed from persisted state
rather than as edge-triggered event chaining.

Netflix later contributed Conductor to the OSS community; **Conductor OSS**, maintained
primarily by Orkes, is the continuation of that repository
[Conductor OSS FAQ](https://conductor-oss.github.io/conductor/devguide/faq.html). Public
documentation does not detail the original scaling pain that motivated the handoff, so no
claim is made there beyond the fact of the transition.

---

## 3. Apache Airflow

### 3.1 DagRun / TaskInstance state machines and the scheduler loop

Airflow separates **DagRun** (one graph execution) from **TaskInstance** (one node's
execution within that run). A TaskInstance has 14 distinct states in current Airflow:

| State | Meaning |
|---|---|
| `none` | dependencies not yet met, not queued |
| `scheduled` | scheduler determined dependencies are met |
| `queued` | assigned to an Executor, awaiting a worker slot |
| `running` | actively executing on a worker |
| `restarting` | externally asked to restart while running |
| `success` | finished without error |
| `failed` | errored, no retries left (or none configured) |
| `skipped` | skipped by branching / LatestOnly / trigger rule |
| `upstream_failed` | an upstream failed and the trigger rule requires it |
| `up_for_retry` | failed, retries remain, will be rescheduled |
| `up_for_reschedule` | a Sensor in reschedule mode, releasing its worker slot between polls |
| `deferred` | handed off to a trigger (async wait, e.g. `defer()`) |
| `awaiting_input` | waiting on a human response, no worker held |
| `removed` | task vanished from the DAG definition since the run started |

[Airflow docs, Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)

The happy path is `none → scheduled → queued → running → success`. Two states are
notable design choices worth stealing: **`up_for_reschedule`** (a sensor releases its
worker slot while "polling" rather than blocking it — decoupling "waiting for an external
condition" from "occupying execution capacity") and **`deferred`** (the same idea
generalized: hand the wait to a lightweight async trigger process instead of a worker at
all). Both encode the idea that *waiting* and *executing* are different resource classes
and should not compete for the same slot.

### 3.2 Scheduler HA via database row locking — no consensus system

Airflow's scheduler runs a repeating **`_do_scheduling()`** pass: create new DagRuns,
promote queued DagRuns to running, and — the performance-critical part — a **critical
section** that moves TaskInstances from `scheduled` to `queued`/dispatched to the executor
while respecting pool/concurrency limits
[Scheduler docs](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html).
That critical section is protected by taking a **row-level write lock on every row of the
`Pool` table**, conceptually `SELECT * FROM slot_pool FOR UPDATE NOWAIT` (the real query
differs slightly)
[Scheduler docs](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html).
Multiple scheduler processes can run **active-active** against the same Postgres/MySQL
database with **no leader election, no Raft/Paxos, no ZooKeeper/Consul** — coordination is
entirely through the shared metadata database's locking primitives, an explicit design
choice to keep "operational surface area" minimal
[Scheduler docs](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html),
[AIP-15](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=103092651). Each
scheduler is fully active — not a hot standby — and does its own task-error-handling,
health-check, and email work
[Astronomer, Benefits of the Airflow 2.0 Scheduler](https://www.astronomer.io/blog/airflow-2-scheduler/).

This only works if the database supports the right SQL locking clauses:

| Backend | Multi-scheduler support | Why |
|---|---|---|
| PostgreSQL 12+ | Full | native `SELECT ... FOR UPDATE [SKIP LOCKED\|NOWAIT]` |
| MySQL 8.0+ | Full | added `SKIP LOCKED`/`NOWAIT` in 8.0 |
| MariaDB 10.6+ | Full (after 10.6.0) | `SKIP LOCKED`/`NOWAIT` landed in 10.6.0; earlier versions deadlock |
| MySQL 5.x | **Unsupported** | no `SKIP LOCKED`/`NOWAIT`; prone to false deadlock detection |
| SQLite | **Unsupported** | no real row locking; single-writer only |

[Scheduler docs](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html)

This is a direct, load-bearing precedent for the requirement that dag-worker-go support a
Postgres backend for multi-instance operation, and it shows the exact mechanism
(`SKIP LOCKED`/`NOWAIT` row-claiming, no external coordinator) that scales this pattern.

### 3.3 Where the locking design bit them

Even with row locks, Airflow's scheduler has repeatedly deadlocked in production because
two code paths lock **DagRun** and **TaskInstance** rows in different orders. Issue
[#27473](https://github.com/apache/airflow/issues/27473) traces a concrete deadlock: one
path (`DagRun.update_state()`) writes `dag_run.last_scheduling_decision` while another
(`DagRun.schedule_tis()`) bulk-updates `task_instance.state = 'scheduled'`; under
concurrent schedulers this produces a classic "Process A waits for lock held by B; B waits
for a lock held by A" cycle. The maintainers' own guidance is that whenever a scheduling
path needs locks on multiple tables, it must always take them in the same global order
(DagRun before TaskInstance), because Postgres's deadlock detector will otherwise fire
under load — the issue itself was closed "not planned" in the tracker, i.e. accepted as a
recurring cost of the design rather than fully fixed
[GH #27473](https://github.com/apache/airflow/issues/27473). **Takeaway for
dag-worker-go: define and document a single, global lock-acquisition order across every
multi-table transaction from day one** — this is exactly the kind of bug that is cheap to
prevent up front and expensive to hunt down after the fact.

Airflow's own scheduler doc admits the operational cost bluntly: even where multi-scheduler
is "supported," the Postgres/MySQL 8 requirement locks out anyone running an older or
embedded-friendly database, and community reports (e.g.
[#53491](https://github.com/apache/airflow/issues/53491), 30–60 minute slowdowns from DB
lock contention under HA) show the row-lock approach still degrades under enough
concurrent scheduler load — it is a scaling technique with a ceiling, not a free lunch.

### 3.4 Dynamic task mapping (AIP-42) — the closest analog to a dynamic DAG

Airflow's static-DAG-as-Python-code model could not express "spawn N parallel tasks where N
is only known at run time" until **AIP-42 Dynamic Task Mapping**
([proposal](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-42+Dynamic+Task+Mapping)).
The design:

- A TaskInstance gets a new integer `mapping_id` column, default `-1` (unmapped); the
  primary key expands from `(dag_id, task_id, run_id)` to
  `(dag_id, task_id, run_id, mapping_id)`. Related tables (`TaskFail`,
  `TaskReschedule`, rendered-fields cache) get the same treatment.
- A new `TaskMap` table stores only the **shape** (type, length, dict keys) of a mapped
  upstream's output, specifically so the scheduler never has to load the full (potentially
  huge) XCom payload just to know how many mapped instances to create.
- `.expand()` over multiple iterables takes their **Cartesian product**, with a
  deterministic index ordering derived from Python 3.6+ dict insertion order (e.g.
  expanding over a length-5 and a length-3 iterable yields indices 0–14 in a fixed,
  reproducible order).
- Expansion happens lazily: the scheduler only materializes the N concrete TaskInstances
  once the mapped task's dependencies are actually satisfied, not at DAG-parse time.
- A `maximum_map_size` config caps N to prevent an accidental combinatorial explosion from
  a bad upstream value taking down the metadata database.
- The authors explicitly benchmarked the schema migration cost before shipping: adding the
  `mapping_id` column and widening the primary key took **1 minute 7 seconds on a 35-million-
  row table**, versus 9–28 minutes for other Airflow migrations around the same era — i.e.
  they treated "can this ship without a multi-hour outage on large installs" as a hard
  gate.

[AIP-42](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-42+Dynamic+Task+Mapping)

The lesson generalizes directly to dag-worker-go's "dynamic DAG, nodes/edges added while
running" requirement: **store the *shape* of dynamically generated fan-out separately from
its payload**, cap fan-out with a hard configurable ceiling, and expand lazily against
readiness rather than eagerly at graph-definition time.

---

## 4. Prefect 2 / 3

Prefect's orchestration model treats every state transition as a **negotiation**: the
execution engine *proposes* a state to the Prefect API/server, and server-side
**orchestration rules** (retry policy, concurrency limits, caching) can accept, reject, or
rewrite that proposal before it is committed
[Prefect docs, States](https://docs.prefect.io/v3/concepts/states). This inverts the
typical "worker owns its own state transitions" model: the source of truth about whether a
transition is *allowed* lives centrally, not in the worker process. Prefect's own docs
frame the key modeling insight as **state *type* drives orchestration logic; state *name*
is just a human label** — a task that is retrying still has type `RUNNING` even though its
displayed name is "Retrying," so downstream logic never has to special-case retry-vs-first-
attempt [Prefect docs, States](https://docs.prefect.io/v3/concepts/states). Concretely,
Prefect 3 defines roughly 20 named states across 7 types:

| Type | Example names | Terminal? |
|---|---|---|
| `SCHEDULED` | Scheduled, Late, AwaitingRetry, AwaitingConcurrencySlot, Resuming | no |
| `PENDING` | Pending, Submitting, InfrastructurePending | no |
| `RUNNING` | Running, Retrying | no |
| `PAUSED` | Paused, Suspended | no |
| `CANCELLING` | Cancelling | no |
| `CANCELLED` | Cancelled | **yes** |
| `COMPLETED` | Completed, Cached, RolledBack | **yes** |
| `FAILED` | Failed, TimedOut | **yes** |
| `CRASHED` | Crashed | **yes** |

[Prefect docs, States](https://docs.prefect.io/v3/concepts/states)

Note the explicit `CRASHED` type distinct from `FAILED`: a `FAILED` run raised an
application exception, a `CRASHED` run was killed by the *infrastructure* (OS signal,
`SIGTERM`, OOM) — i.e. Prefect encodes "the worker itself died" as a first-class outcome
different from "the worker ran and reported an error," which is precisely the
error-vs-timeout distinction the dag-worker-go brief calls for ("error-with-timeout" as
its own bucket).

**Execution/leasing model (Prefect 3):** work is queued server-side in **work pools**,
optionally subdivided into **work queues** for priority/concurrency partitioning; long-
running **workers** poll a work pool (matched by infra type) for scheduled flow runs, then
provision infrastructure and execute
[Prefect docs, Workers](https://docs.prefect.io/v3/concepts/workers). In the hosted-cloud
deployment mode, the control plane **never opens an inbound connection into the customer's
network** — the worker only ever makes outbound polls and posts back status — a
zero-inbound-firewall design that is worth keeping in mind for any dag-worker-go deployment
guidance, even though the library itself is embedded rather than SaaS
[Prefect, Hybrid Work Pool Architecture](https://www.prefect.io/learn/diagram-hybrid-pool).

Prefect's own public criticism of Airflow (its direct predecessor problem) is that
"Airflow remains bottlenecked by its own scheduler, which takes 10 seconds to run any
task, meaning no matter how big your cluster, Airflow will still only ask it to run a task
every 10 seconds" — a fixed polling-interval scheduler loop caps throughput independent of
available worker capacity
[Prefect Blog, Why Not Airflow?](https://medium.com/the-prefect-blog/why-not-airflow-4cfa423299c4).
This is exactly the failure mode a `LISTEN/NOTIFY`- or heartbeat-driven design (§12, River)
avoids: **a scheduler loop with a fixed sleep interval puts a hard ceiling on latency that
no amount of worker parallelism can buy down.**

---

## 5. Dagster

Dagster's execution path has four cooperating pieces:

1. **Run Coordinator** — everything submitted from the UI/CLI passes through it first; it
   applies deployment-wide limits and prioritization (e.g. `QueuedRunCoordinator` caps
   total concurrent runs) before handing off.
2. **Run Launcher** — the actual interface to compute (local process, Kubernetes Job,
   Docker container, ECS task, …).
3. **`dagster-daemon` process** — a single long-running process that itself orchestrates
   several sub-daemons: the schedule daemon, sensor daemon, run-queue daemon, and
   run-monitoring daemon; it also does periodic janitorial work (expiring stale runs).
4. **Asset Daemon** — continuously evaluates each asset's `AutomationCondition` (the
   successor to the older `AutoMaterializePolicy`) and launches materialization runs when
   conditions are met.

[Dagster docs, Run Launcher/Coordinator](https://atlan.com/dagster-data-orchestration/),
[Asset Daemon and Tick System](https://deepwiki.com/dagster-io/dagster/4.3-asset-daemon-and-tick-system),
[Dagster daemon docs](https://docs.dagster.io/deployment/execution/dagster-daemon).
Runs triggered by a schedule or sensor go through the identical Run Coordinator → Run
Launcher pipeline as a manually-launched run; only the *first* caller differs (the daemon
instead of the webserver) — the same "one funnel regardless of trigger source" discipline
seen in Kubernetes's reconciliation model (§13). Publicly documented Dagster material does
not specify whether `QueuedRunCoordinator`'s concurrency accounting uses Postgres row locks
or an in-memory/queue-table scheme internally; that implementation detail was not
confirmed by primary sources during this research and should not be assumed.

---

## 6. Argo Workflows

Argo represents a workflow as a **Kubernetes Custom Resource**; the workflow-controller is
a standard Kubernetes controller reconciling that resource against the state of the Pods
it spawns. **NodePhase** — the state of one DAG step — has 7 values:

| Phase | Meaning |
|---|---|
| `Pending` | node is waiting to run |
| `Running` | node is running |
| `Succeeded` | node finished with exit code 0 |
| `Skipped` | node was skipped (e.g. `when:` condition false) |
| `Failed` | node (or a child of it) exited non-zero |
| `Error` | node had an error *other than* a non-zero exit (e.g. pod eviction, image pull failure) |
| `Omitted` | node's `depends` condition was never satisfied |

[argo-workflows source, `workflow_types.go`](https://github.com/argoproj/argo-workflows/blob/main/pkg/apis/workflow/v1alpha1/workflow_types.go)

The `Failed` vs `Error` split is a genuinely useful distinction most systems in this survey
collapse into one bucket: **the unit of work ran and reported failure** vs **the
infrastructure never let the unit of work finish at all** — directly analogous to the
Prefect `FAILED`/`CRASHED` split (§4) and to the "error" vs "error-with-timeout" split
dag-worker-go's brief already calls for.

**Storage ceiling and its fix.** Because a Workflow is an actual Kubernetes object living
in etcd, and etcd enforces a hard **1 MB per-object size limit**, a workflow with enough
nodes eventually cannot fit its own `/status/nodes` map in the resource. Argo's answer is a
two-stage escape hatch: first compress `status.nodes` into `status.compressedNodes` (gzip
by default; `zstd`/`brotli` selectable via `WORKFLOW_COMPRESSION_ALGORITHM`), and if that
is *still* too big, offload node status entirely into an external Postgres/MySQL/MariaDB
table via `nodeStatusOffLoad: true`
[Offloading Large Workflows](https://argo-workflows.readthedocs.io/en/latest/offloading-large-workflows/).
This is a direct precedent for a scaling failure mode dag-worker-go must design around from
the start rather than retrofit: **a DAG's per-node status must not be forced to live inside
a single record whose backend has a hard size ceiling** — this argues for status being
one row per node in a store designed for that access pattern (as the brief's "pluggable
storage" already implies), not a single blob column.

Community issues repeatedly report workflows getting stuck as **"workflow never
reconciled"** — a liveness-probe failure category where the controller stops making
progress on a resource, generally under apiserver load or after a controller restart loses
its in-memory work queue state
([discussion #13460](https://github.com/argoproj/argo-workflows/discussions/13460),
[issue #11051](https://github.com/argoproj/argo-workflows/issues/11051)). The generalizable
lesson: a reconciliation loop's *liveness* depends on the health of the API it reconciles
against, and a controller needs to distinguish "nothing to do" from "stuck trying to do
something" in its own health signal, or the two look identical from outside.

---

## 7. Luigi

Luigi predates all of the above and shows what the *absence* of the mechanisms above costs
in production. It has a single **central scheduler** process; workers hold a keep-alive
thread that reports liveness, and task status flows
`PENDING → RUNNING → DONE`, `RUNNING → FAILED`, and — after enough consecutive failures —
`FAILED → DISABLED`, with a timer eventually returning a disabled task to `PENDING`
[Luigi scheduler internals, DeepWiki](https://deepwiki.com/spotify/luigi/2.4-scheduler).
Because the scheduler is a **single stateful process with no persistence guarantees
described in its own docs**, three failure classes recur across its issue tracker:

- Tasks get stuck permanently `RUNNING` even after their output already exists, because
  the scheduler's record of "who is running this" and the worker's actual liveness drift
  apart with no independent timeout to reconcile them
  ([issue #434](https://github.com/spotify/luigi/issues/434),
  [issue #2200](https://github.com/spotify/luigi/issues/2200)).
- If a worker does blocking I/O on its main thread in single-process mode, its keep-alive
  heartbeat thread starves too, and the scheduler — with no independent way to check
  liveness — declares the worker disconnected even though it is still working
  ([issue #2070](https://github.com/spotify/luigi/issues/2070)).
- A task whose dependency is satisfied by a *different* worker than the one that originally
  claimed it can be left permanently `PENDING`, because ownership and readiness are not
  cleanly separated ([issue #3049](https://github.com/spotify/luigi/issues/3049)).

The unifying root cause across all three: **liveness detection (heartbeat) and cooperative
scheduling both ran on the same thread/process as user task code, with no independent
timeout authority.** Every system later in this survey that gets this right (Faktory,
Sidekiq Pro, Temporal, Conductor) puts the timeout clock in the *server*, driven by an
explicit heartbeat or lease-expiry check, never trusting the worker's own thread scheduling
to notice its own death.

---

## 8. Nextflow

Nextflow implements the **dataflow programming model**: a pipeline is a DAG of
**Processes** that communicate exclusively through typed, asynchronous **Channels**; a
process fires automatically whenever all of its input channels have a value available, and
the runtime parallelizes independent processes without any explicit fork/join code from the
pipeline author
[Nextflow docs, cache-and-resume](https://www.nextflow.io/docs/latest/cache-and-resume.html),
[23andMe Engineering, Introduction to Nextflow](https://medium.com/23andme-engineering/introduction-to-nextflow-4d0e3b6768d1).
Execution is fully decoupled from *where* it runs — the same pipeline definition targets a
local executor, an HPC scheduler (SLURM/PBS/HTCondor), or a cloud batch service purely by
changing the configured **Executor**.

The mechanism most relevant to dag-worker-go is **content-addressed task caching**: every
task execution is unconditionally recorded into a task cache keyed by a **hash of its
inputs** (script text, input file content hashes, parameter values); on a subsequent
`-resume` run, Nextflow recomputes that hash for each task and, if a matching cache entry
exists *and* the task's declared output files are still physically present in its work
directory, skips re-execution entirely and reuses the recorded outputs — otherwise it
re-executes
[Nextflow docs, cache-and-resume](https://www.nextflow.io/docs/latest/cache-and-resume.html).
This is a strong, directly-portable idea for a dynamic DAG library: **idempotent
re-submission of a node is "free" if the node's content hash and its physical output are
both still valid**, which gives hosts a resume/replay story without the library needing an
event-sourced history at all — a lighter-weight alternative to Temporal's approach (§1.2)
that trades "replay produces bit-identical history" for "replay skips work whose inputs
provably haven't changed."

---

## 9. HTCondor DAGMan

DAGMan is the workflow layer over HTCondor's batch scheduler and is the closest thing in
this survey to a battle-tested **crash-recovery protocol for a DAG scheduler process
itself** (as opposed to recovering individual worker tasks). Each DAG node has exactly six
states:

| Status | Value | Meaning |
|---|---|---|
| `STATUS_NOT_READY` | 0 | ≥1 parent unfinished (or node is a FINAL node) |
| `STATUS_READY` | 1 | all parents finished, not yet running |
| `STATUS_PRERUN` | 2 | the node's PRE script is executing |
| `STATUS_SUBMITTED` | 3 | the node's HTCondor job(s) are queued/running |
| `STATUS_POSTRUN` | 4 | the node's POST script is executing |
| `STATUS_DONE` | 5 | node completed successfully |

[HTCondor manual, DAGMan node status](https://htcondor.readthedocs.io/en/latest/automated-workflows/dagman-information-files.html)

Two recovery mechanisms matter:

- **Rescue DAG.** When DAGMan exits from a *failed* run, it writes a `.rescue001` file
  (incrementing `.rescue002`, `.rescue003`, … on repeated failures) marking every
  already-succeeded node with the `DONE` keyword. Resubmitting that rescue file skips every
  `DONE` node and resumes exactly at the first unfinished one
  [DAGMan Applications manual](https://htcondor.readthedocs.io/en/v8_8/users-manual/dagman-applications.html).
- **Recovery mode.** If the DAGMan *process itself* dies before it can write a Rescue DAG
  (killed, `condor_hold`), resubmitting/releasing the same job causes DAGMan to
  reconstruct the state it should have persisted by replaying the `*.nodes.log` file — an
  append-only job-event log HTCondor was already writing for unrelated reasons — and only
  then resumes scheduling
  [HTCondorWiki, DagRecovery](https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=DagRecovery).

The second point is the load-bearing idea: **DAGMan's crash recovery for the *scheduler
process itself* is bootstrapped off a log it was already writing for a different purpose
(job accounting), not a separate WAL built specifically for scheduler recovery.** For
dag-worker-go, this argues that node-status transitions, once persisted for the "everyone
can subscribe to a stream of transitions" requirement, are automatically sufficient to
reconstruct scheduler state after a crash — a second, separate recovery log is redundant if
the transition log is itself durable and replayable.

---

## 10. Celery + Canvas

Celery's **Canvas** primitives compose async tasks: **`chain`** runs tasks strictly in
sequence, piping each result into the next; **`group`** fans a set of tasks out for
concurrent execution and collects results; **`chord`** is a `group` plus a callback that
fires only once every member of the group has completed
[Celery docs, Canvas](https://docs.celeryq.dev/en/stable/userguide/canvas.html). `chord` is
implemented via a `chord_unlock` sentinel task that polls the result backend until the
group is complete, then dispatches the callback — polling, not a push notification, is the
mechanism, and it inherits every problem of poll-based completion detection.

Reliability is bolted on via broker semantics, and it shows:

- **Redis broker + `acks_late` + `task_reject_on_worker_lost` does not actually work**:
  the setting to re-queue a task whose worker was killed mid-execution is documented as
  broken specifically on the Redis broker, so a killed worker silently loses the task
  instead of releasing it back to the queue
  ([issue #3541](https://github.com/celery/celery/issues/3541)).
- **Chords can hang forever** if a member task dies via `SIGKILL`/`SystemExit` (commonly
  the OOM killer) — the unlock/`link_error` callback that would normally fire is never
  invoked for that failure mode, so the whole chord stalls waiting for a member that will
  never report in
  ([issue #2911](https://github.com/celery/celery/issues/2911)).
- **Warm shutdown drops "successful" tasks under `acks_late=True`**: a task that finishes
  and returns success *after* the worker received `SIGTERM` may never get acked, so on
  restart it is redelivered and re-executed despite having already succeeded — an
  at-least-once violation of the acking contract at exactly the moment (graceful shutdown)
  it's supposed to be safest
  ([issue #3802](https://github.com/celery/celery/issues/3802)).
- Celery's own FAQ concedes the point: Redis "won't perform as well as" an AMQP broker for
  strict reliability, and RabbitMQ (or another real AMQP broker) is the recommended choice
  when delivery guarantees matter
  [Celery FAQ](https://docs.celeryq.dev/en/stable/faq.html).

**Takeaway:** a `chord`-style "wait for N siblings, then continue" join is exactly the
dynamic-fan-in shape a DAG library needs, but Celery's implementation shows the two ways to
get it wrong — polling for completion instead of being notified, and letting the
ack/lease contract silently break under process-kill signals — both of which
dag-worker-go's own explicit ack-with-timeout design (per the brief) is meant to avoid.

---

## 11. Sidekiq and Faktory

### 11.1 Sidekiq — the "just Redis" baseline, and its exact failure mode

Sidekiq's default fetch is a plain `BRPOP`: pop-and-delete in one atomic step. That is
maximally simple and fast, but if the worker process dies **between** the pop and finishing
the job, the job is gone — Redis never had a second copy of it
[Sidekiq Reliability wiki](https://github.com/sidekiq/sidekiq/wiki/Reliability). The
documented fix pattern is `BRPOPLPUSH` (now `BLMOVE` as of Redis 6.2, since `BRPOPLPUSH` is
deprecated): atomically pop from the ready queue **and** push the same payload onto a
private "in-progress" list *in the same command*; only once the job is confirmed done is it
removed from that in-progress list — this is the textbook Redis-native at-least-once
reliable-queue pattern
[Sidekiq Reliability wiki](https://github.com/sidekiq/sidekiq/wiki/Reliability). Note the
Redis-list version of this pattern (unlike a lease/TTL scheme) requires an *external* sweep
to notice a list entry whose owning process died — the entry itself carries no expiry.

Sidekiq Pro's `super_fetch` builds exactly that sweep on top of `LMOVE`: each process
emits a heartbeat that **expires after 60 seconds**; on startup (and periodically), a
process scans for other processes' heartbeats that have expired and, if at least a minute
has passed since the last such check, adopts their orphaned in-progress lists back onto the
ready queue. To avoid hammering Redis, a **full `SCAN` of the keyspace for orphaned queues
runs only once per hour**
[thoughtbot, super_fetch](https://thoughtbot.com/blog/enhancing-job-reliability-with-sidekiq-pro-s-super-fetch-strategy),
[BigBinary, super_fetch](https://www.bigbinary.com/blog/increase-reliability-of-background-job-processing-using-super_fetch-of-sidekiq-pro).
A job recovered as orphaned **three times within 72 hours** is treated as a poison pill and
force-moved to the Dead set rather than retried a fourth time
[BigBinary, super_fetch](https://www.bigbinary.com/blog/increase-reliability-of-background-job-processing-using-super_fetch-of-sidekiq-pro).
Shutdown handling is signal-specific and load-bearing: `SIGTERM`/`SIGINT` let Sidekiq push
its unfinished jobs back to the queue cleanly before exiting; `SIGKILL` gives it no chance
to do that and in-flight job data for that process is lost outright unless a
super_fetch-style external sweep exists
[Sidekiq Reliability wiki](https://github.com/sidekiq/sidekiq/wiki/Reliability).

### 11.2 Faktory — the language-agnostic, protocol-first version of the same idea

Faktory generalizes Sidekiq's pattern into an explicit wire protocol so *any* language can
be a worker, not just Ruby. Jobs move through five states:

`SCHEDULED → ENQUEUED → WORKING → {done, implicit} | RETRIES → {ENQUEUED again | DEAD}`

[Faktory protocol spec](https://github.com/contribsys/faktory/blob/main/docs/protocol-specification.md)

The verbs are `PUSH` (producer enqueues), `FETCH` (consumer reserves — blocks up to 2
seconds if the named queues are empty), and exactly one of `ACK` or `FAIL` from the
consumer once it is done. `FETCH` is a **lease, not a delete**: it starts a `reserve_for`
timer (default **1800s**, minimum **60s**, configurable per-job) during which the consumer
must respond; if that timer expires with no `ACK`/`FAIL`, the server itself moves the job to
`RETRIES` and it becomes fetchable again — no external sweep process required, because the
lease expiry is enforced server-side by construction, not bolted on afterward
[Faktory protocol spec](https://github.com/contribsys/faktory/blob/main/docs/protocol-specification.md).
This is a materially cleaner design than Sidekiq's own (list-based, sweep-dependent)
mechanism, and closely matches what dag-worker-go's brief already specifies for its own
worker-ack-with-timeout requirement — **Faktory's `FETCH`/`reserve_for`/auto-retry-on-
expiry is close to a reference implementation of exactly the mechanism the brief asks
for**, generalized: a per-job configurable reservation timeout, set at fetch/take time,
with a library-wide default (1800s here) — an extremely close match to "settable per node
at the moment the worker takes it, with a library default."

---

## 12. Go-native job queues

### 12.1 River (PostgreSQL)

River claims jobs with Postgres's `FOR UPDATE SKIP LOCKED`, added in Postgres 9.5
specifically so that concurrent claimants can skip rows another transaction already has
locked instead of blocking on them
[brandur.org/river](https://brandur.org/river). River's producer/executor split matters for
contention: **one producer per process** consolidates the `SKIP LOCKED` claim for *all* of
that process's internal goroutine executors in a single query, so only inter-process
contention (not intra-process) ever touches the lock — "many fewer" real contenders than a
naive one-lock-per-worker-goroutine design
[brandur.org/river](https://brandur.org/river). River layers Postgres `LISTEN`/`NOTIFY` on
top purely as a latency optimization: the moment a job becomes ready, `NOTIFY` can wake an
idle poller immediately, pushing mean dispatch latency down to sub-millisecond instead of
waiting for the next poll tick
[brandur.org/river](https://brandur.org/river). Because enqueue is a normal SQL `INSERT`,
jobs enqueued inside a caller's own transaction are **transactionally consistent with
whatever else that transaction did** — a job is visible to workers if and only if the
enqueuing transaction commits, eliminating the classic "wrote to the DB, then crashed
before publishing to the queue" dual-write hole entirely
[brandur.org/river](https://brandur.org/river). The author reports roughly 10k trivial
jobs/sec on a commodity laptop but explicitly declines to publish it as a real benchmark
number [brandur.org/river](https://brandur.org/river) — a good discipline for
dag-worker-go's own 1M-node benchmark suite to imitate: report methodology and hardware
alongside every number, or don't publish the number.

```sql
-- River-style claim: one producer, N jobs at once, no blocking on contested rows
WITH claimed AS (
  SELECT id
  FROM river_job
  WHERE state = 'available'
    AND queue = $1
    AND scheduled_at <= now()
  ORDER BY priority DESC, id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT $2
)
UPDATE river_job
SET state = 'running', attempted_at = now(), attempt = attempt + 1
FROM claimed
WHERE river_job.id = claimed.id
RETURNING river_job.*;
```

### 12.2 Asynq (Redis)

Asynq's task lifecycle is `pending → active → {completed | retry → pending | archived}`,
with a separate `scheduled` state for delayed tasks; it promises "guaranteed at least one
execution" and "automatic recovery of tasks in the event of a worker crash" via a
lease/deadline model — an active task carries a deadline, and an independent recoverer
process requeues any active task whose deadline has passed without completion
[hibiken/asynq README](https://github.com/hibiken/asynq/blob/master/README.md). This is
architecturally Faktory's `reserve_for` idea reimplemented over Redis sorted sets rather
than a bespoke wire protocol — same mechanism, different storage substrate, which is itself
a useful data point: **the lease-with-deadline pattern is storage-agnostic** and should be
implementable identically over Redis, Postgres, or Memcached in dag-worker-go, differing
only in which primitive expresses "atomically claim and set an expiry" (`SKIP LOCKED` +
`UPDATE` in Postgres; a Lua script combining `ZREM`+`ZADD` in Redis; `CAS`/`gets`+`cas` with
a TTL in Memcached).

### 12.3 Machinery and gocelery — Celery's shape, ported, and largely abandoned

Both are broker-agnostic (RabbitMQ/SQS/Redis) Go reimplementations of Celery's model,
supporting `group`/`chord`/`chain`, JSON task envelopes, and Fibonacci-backoff retries
[Medium, Task orchestration in Go Machinery](https://medium.com/swlh/task-orchestration-in-go-machinery-66a0ddcda548),
[gocelery/gocelery](https://github.com/gocelery/gocelery). At the time of the survey used
for this dossier, Machinery's most recent commit dated to December 2021
[go.libhunt.com, gocelery alternatives](https://go.libhunt.com/gocelery-alternatives) — a
cautionary data point that "port Celery's API surface into Go" has not historically
attracted long-term maintenance, likely because none of Celery's actual reliability
problems (§10) disappear just by porting the API to a different language; the interesting
engineering (leasing, ack semantics, chord-completion notification) has to be redesigned,
not translated.

### 12.4 Temporal Go SDK

Covered in depth in §1.5; the concrete Go-specific API surface — `ActivityOptions{
HeartbeatTimeout, StartToCloseTimeout, RetryPolicy }`, `activity.RecordHeartbeat(ctx,
details)`, `activity.GetHeartbeatDetails()` on resume — is a directly reusable API shape
for dag-worker-go's own "worker takes a node, may set a timeout, may report progress"
contract, minus Temporal's determinism/replay machinery, which dag-worker-go's external
(non-replayed) worker model does not need.

---

## 13. Kubernetes Job / CronJob controller reconciliation

### 13.1 Level-triggered reconciliation, not edge-triggered event handling

Every Kubernetes controller — including the Job and CronJob controllers — is built on one
non-negotiable design rule: **reconciliation is level-triggered.** An edge-triggered
design would have to replay every event in order to know what to do next; a level-triggered
controller ignores the event that woke it up entirely and instead re-derives the answer
from scratch by comparing *current observed state* to *desired state* — "are there 4
healthy Pods? If not, create or delete until there are"
[HackerNoon, Level Triggering and Reconciliation in Kubernetes](https://medium.com/hackernoon/level-triggering-and-reconciliation-in-kubernetes-1f17fe30333d).
In practice Kubernetes runs a **hybrid**: watch events are only ever used as a cheap "go
look again now" *hint* that something might have changed; they never carry the logic for
*what* to do — that always comes from a fresh read of current state
[golinuxcloud, Kubernetes Reconcile Loop Explained](https://www.golinuxcloud.com/kubernetes-reconcile-loop-explained/).
The practical payoff: a controller that crashes and restarts, or that missed an event
entirely because it was down, self-heals for free on its next reconcile pass — there is no
missed-event failure mode to reason about, because no code path depends on having seen
every event
[golinuxcloud, Kubernetes Reconcile Loop Explained](https://www.golinuxcloud.com/kubernetes-reconcile-loop-explained/).
**This is arguably the single most important idea in this entire survey for
dag-worker-go's "reactive, subscribe to a stream" requirement**: the stream of transitions
should be a *convenience/notification* channel for subscribers, never the sole source of
truth for "what needs to happen next" — that answer must always be independently
recomputable by re-reading current DAG state, or a missed message on the stream becomes a
permanently stuck node.

### 13.2 Job controller: exactly-once-effective dispatch via finalizers, not locks

The Job controller's core problem is structurally identical to dag-worker-go's "hand a
ready node to exactly one worker": don't create more Pods than `parallelism` allows, and
don't lose track of a Pod's outcome. Its mechanism is **finalizers**, not a lock: every Pod
the Job controller creates gets a tracking finalizer attached, and that finalizer is only
removed once the controller has durably recorded the Pod's terminal status in the Job's own
`status` — as of Kubernetes 1.27+, this finalizer-based tracking is the *only* mode (no
fallback to counting live Pods), specifically because counting live Pods is racy under
apiserver list/watch staleness
[Kubernetes docs, Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/).
Failed Pods are recreated with **exponential backoff — 10s, 20s, 40s, … capped at 6
minutes** — until `backoffLimit` is hit, at which point the Job itself is marked failed
[Kubernetes docs, Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/). A
subtlety fixed only in 1.28: when a `podFailurePolicy` is configured, the controller
recreates a **terminating** Pod's replacement only after that Pod actually reaches the
terminal `Failed` phase, not the instant termination is requested — otherwise a
double-count of "how many Pods are trying to do this work" is briefly possible during the
termination window
[Kubernetes docs, Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/).
**The generalizable pattern: "the work is claimed" and "the work's outcome is durably
recorded" are two different facts, and the claim must not be released/retried until the
second fact is confirmed** — a finalizer is just a durable flag co-located with the claimed
resource that enforces this ordering; dag-worker-go's storage layer needs an equivalent
(e.g. a node cannot be re-offered to another worker while its previous claim's outcome is
still unconfirmed, even if the claim's lease has expired — the expiry should trigger
*investigation*, not silent instant reassignment, if there is any chance the original
worker is still finishing up. In practice this is a tradeoff most systems here resolve
in favor of availability over this stricter safety — see §16 on fencing tokens for the
correct general fix).

### 13.3 CronJob controller: idempotent scheduling, not a timer thread

CronJobs are a thin, purely time-driven layer over Jobs: on each reconcile, the controller
computes what run *should* exist given the cron schedule, `concurrencyPolicy` (Allow /
Forbid / Replace), `startingDeadlineSeconds`, and history limits (**default 3 successful /
1 failed** retained), and creates or skips a Job accordingly — again level-triggered, so a
controller restart never double-fires a scheduled run, it just re-derives "is a Job for
this time slot missing" from the Job objects that already exist
[Tanmay Batham, What Happens When You Create a CronJob](https://tanmaybatham.medium.com/what-happens-when-you-create-a-cronjob-in-kubernetes-a-deep-internal-level-breakdown-70797edda81b).

### 13.4 Leader election as a Lease object — the *other* valid answer to "avoid double-dispatch"

Where Airflow (§3.2) chose row-locking with *no* leader election, core Kubernetes
components (`kube-controller-manager`, `kube-scheduler`) choose the opposite: **exactly one
active instance**, enforced by a `coordination.k8s.io/v1` **Lease** object. The current
holder periodically overwrites `spec.renewTime`; if `leaseDurationSeconds` elapses with no
renewal, any standby can atomically take over via a conditional write to the same object —
correctness rests entirely on the apiserver's own compare-and-swap semantics, not on wall
clocks agreeing across machines
[Kubernetes docs, Leases](https://kubernetes.io/docs/concepts/architecture/leases/). This
is a legitimate alternative to Airflow-style row-locking for the "avoid double-dispatch
across instances" problem, but it trades *throughput* (only one instance ever does the
scheduling work) for *simplicity of reasoning* — the opposite tradeoff from active-active
row-locking, which gets all instances working but pays for it in lock contention and
lock-ordering bugs (§3.3). Both are legitimate; dag-worker-go should pick per-operation, not
globally (see §15 for the actual recommendation).

---

## 14. Nomad and Mesos: two very different answers to "who gets the work"

### 14.1 Mesos — two-level, offer-based, framework decides

Mesos splits scheduling into two levels. The **Mesos master** tracks free resources
cluster-wide and periodically makes **resource offers** to registered frameworks according
to a fairness policy (Dominant Resource Fairness, DRF, in the original design); each
**framework's own scheduler** then decides, for each offer, to accept it (and bind
specific tasks to it) or decline
[Mesos paper review, Salem Alqahtani](https://salemal.medium.com/paper-review-of-mesos-a-platform-for-fine-grained-resource-sharing-in-the-data-center-55d44fecb243),
[Datastrophic, DRF explained](https://datastrophic.io/resource-allocation-in-mesos-dominant-resource-fairness-explained/).
The master never inspects task semantics — it only ever hands out fungible resource
bundles and lets each framework apply its own task-placement logic, which is what lets
wildly different frameworks (a batch scheduler, a long-running-service scheduler, a
big-data framework) coexist on one cluster without the master needing to understand any of
them.

### 14.2 Nomad — single scheduler, optimistic concurrency, plan-apply

Nomad instead runs its own schedulers **optimistically concurrent**: multiple scheduler
worker goroutines evaluate different pending jobs *in parallel*, with **no locking or
reservation** between them while they compute a *plan* — meaning two schedulers can
legitimately propose conflicting allocations onto the same node at the same time
[Nomad docs, How Scheduling Works](https://developer.hashicorp.com/nomad/docs/concepts/scheduling/how-scheduling-works).
Conflicts are resolved at a single serialization point: every computed plan is submitted to
a **plan queue** and applied one at a time by the (Raft) leader, which checks the plan
against the *actual current* cluster state and does a partial or complete rejection if
resources were consumed by another plan in the meantime; a rejected scheduler re-evaluates
against a fresh state snapshot and resubmits
[Nomad architecture, eval-triggers](https://github.com/hashicorp/nomad/blob/main/contributing/architecture-eval-triggers.md).
Every **Evaluation** — the unit of "something happened that might require rescheduling"
(17 distinct trigger types: job register/deregister, node state change, deployment
watcher ticks, periodic-job firing, etc.) — is itself durably written to Raft before being
handed to a scheduler worker, so a scheduler crash mid-evaluation loses no work; the
evaluation simply gets redelivered
[Nomad architecture, eval-triggers](https://github.com/hashicorp/nomad/blob/main/contributing/architecture-eval-triggers.md).
Nomad's own docs credit Google's **Omega** paper for this optimistic-concurrency pattern —
schedule speculatively and in parallel, reconcile with a cheap atomic check at commit time,
rather than serializing all scheduling decisions up front
[Datastrophic/related coverage cites Omega as Nomad's inspiration]. **This is the
single-writer-serialization-point pattern generalized**: it is structurally the same idea
as Postgres `SKIP LOCKED` (many claimants race, one storage-layer primitive picks a
winner atomically) and as the Kubernetes optimistic-concurrency resourceVersion check on
every `PUT`/`PATCH` — three different systems converging on "let many actors compute
speculatively, make the *commit* atomic and conflict-checked, retry losers" as the
general answer to distributed double-dispatch.

---

## 15. Cross-cutting: work distribution across multiple instances

The brief flags this as an open design question — partition-per-scope? consistent
hashing? pure pull-based competition? lease stealing? — and the survey above gives
concrete, tested answers for each option:

| Strategy | Who uses it | Mechanism | Failure mode if instance dies |
|---|---|---|---|
| **Pure pull-based competition** ("pull queue") | Faktory, Sidekiq, Asynq, River, Celery | Every instance polls the same shared queue; storage-layer atomicity (`SKIP LOCKED`, `LMOVE`, `FETCH`+lease) picks exactly one claimant per item | Only the claimed-but-unacked items are affected; lease/reservation timeout releases them automatically |
| **Row-lock active-active, no partitioning** | Airflow scheduler | Every scheduler instance repeatedly contends for the same `Pool`-table row lock to run its own critical section | Lock is released with the crashed transaction; another instance's next loop iteration just gets it |
| **Leader election (single active)** | kube-controller-manager, kube-scheduler | `Lease` object with TTL; standbys idle-poll for the lease to free up | Bounded failover latency = lease TTL; zero throughput from standbys meanwhile |
| **Partition ownership, dynamically rebalanced** | Temporal Task Queue partitioning (§1.3), Kafka consumer groups (not surveyed directly but the same family) | Logical queue split into N partitions; a partition is "owned" (loaded) by one matching-service shard at a time, with forwarding to cover gaps | Requires an explicit partition-ownership protocol; more moving parts than pull-based competition |
| **Two-level offer/accept** | Mesos | Central resource tracker offers bundles; frameworks accept/decline | Framework crash mid-decision just times the offer out; master resource state is authoritative |
| **Optimistic parallel scheduling + serialized commit** | Nomad | Every scheduler computes speculatively in parallel; a single plan-apply point does the atomic conflict check | Rejected plans are cheap to redo against fresh state; no locks held during computation |
| **Consistent hashing over a fixed key space** | Not directly used by name in any system surveyed above (Cadence/Temporal use tree-partitioning instead), but the standard general-purpose answer for "which of N nodes owns key K" — see [DEV.to, Consistent Hashing](https://dev.to/arslan_ah/how-to-use-consistent-hashing-in-a-system-design-interview-33ge) | A hash ring; a key's owner is the first node walking clockwise from the key's hash position; adding/removing a node moves only ~k/N keys | Needs virtual nodes (multiple ring positions per physical node) to avoid uneven load, since raw consistent hashing alone measurably skews distribution across nodes |

**Position taken:** for dag-worker-go, **pure pull-based competition with a per-node
reservation lease (Faktory's model) is the right default**, because it requires zero
coordination protocol beyond what the storage backend already provides
(`SKIP LOCKED`/`LMOVE`/`gets`+`cas`+TTL), it degrades gracefully (a dead instance simply
stops claiming work; nothing needs rebalancing), and it is the only option in the table
above that all four required backends (in-memory, Redis, Memcached, PostgreSQL) can
implement natively without bolting on an external coordinator. **Scope-based partitioning
should be layered on top as an optional optimization** (route claims for scope X
preferentially to instances that already have scope X's hot data cached — Temporal's
sticky-execution idea, §1.4 — never as the mechanism that guarantees a node is eventually
claimed). Consistent hashing and leader election both solve *different* problems (routing
to a data owner; guaranteeing single-writer for expensive coordination like DagRun
creation) and are worth keeping as secondary tools, not the primary work-distribution
answer, given the O(1)/O(log n) and 1M-node performance mandate — hash-ring rebalancing and
Raft-backed leader election both add latency and moving parts pure pull-based competition
avoids entirely.

---

## 16. Distributed-locking correctness: what a lease is actually allowed to assume

Every "lease with a timeout" mechanism surveyed above (Faktory's `reserve_for`, Sidekiq
Pro's heartbeat expiry, Asynq's deadline, Nomad's plan-apply check) implicitly assumes a
**bounded clock skew and bounded pause time**: if the *original* claimant is merely paused
(GC, scheduler preemption, a slow disk write) rather than dead, and it wakes up and
finishes its write *after* another claimant has already taken over the same work, both
writes can land — a correctness violation, not just a performance blip. Martin Kleppmann's
widely-cited critique of the Redlock algorithm formalizes exactly this failure mode:
Redlock's safety argument implicitly requires a synchronous system model (bounded network
delay, bounded process pause, bounded clock drift) that real systems routinely violate — a
GC pause can run to "several minutes," far longer than any sane lease duration, and Redis's
own clock source (`gettimeofday`) is not even monotonic, so it can jump backward on NTP
correction
[Kleppmann, How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html).
His fix is **fencing tokens**: the lock/lease service hands out a monotonically increasing
number with every grant, and the *protected resource itself* (not the lock service) must
reject any write tagged with a token older than one it has already seen — this pushes the
correctness check to the last possible point (the actual write), where it can't be
undermined by a stale holder waking up late. Kleppmann's explicit verdict: Redlock-style
locks are fine when a lock's only job is an *efficiency* optimization (avoid duplicate work,
tolerable if it occasionally fails), but are **not sufficient on their own for
correctness-critical mutual exclusion** — for that, either add fencing tokens at the
protected resource, or use a lock service backed by a real consensus protocol (ZooKeeper,
etcd/Raft) whose session/lease semantics are actually verified against these failure modes
[Kleppmann, How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html).

**This directly bears on dag-worker-go's Redis/Memcached backends.** A node's "claimed by
worker W until time T" record is a lease of exactly this kind. If two instances of the
library, or two workers under the same instance, can ever both believe they hold a valid
claim on the same node (slow worker + expired lease + late-arriving completion report), the
storage write for "mark node success" needs a **fencing check**: attach the lease's
generation/version number to the ack, and reject (or at minimum flag for reconciliation) an
ack whose version does not match the node's *current* lease generation — this is a small,
cheap addition (one extra integer compare in the same atomic update that already claims the
node) and it is the difference between "occasionally re-does work" (acceptable) and
"occasionally corrupts final state by accepting two conflicting acks" (not acceptable for a
system whose whole job is tracking node status correctly).

---

## 17. Comparison table — node/task state machines

| System | Public/core states | Distinguishes infra-failure from app-failure? | Distinguishes "not yet started" from "waiting on external condition"? | Retry modeled as | Terminal states |
|---|---|---|---|---|---|
| **Temporal/Cadence** (workflow) | Running, Completed, Failed, Canceled, Terminated, ContinuedAsNew, TimedOut | Yes — Activity failure vs Workflow Task timeout are different event types in History | Yes — Timers, Signals, `defer` all first-class History event kinds | New Activity Task Execution per retry policy attempt | Completed, Failed, Canceled, Terminated, TimedOut |
| **Netflix Conductor** (task) | SCHEDULED, IN_PROGRESS, COMPLETED, FAILED, FAILED_WITH_TERMINAL_ERROR, TIMED_OUT, CANCELED, SKIPPED, COMPLETED_WITH_ERRORS | Partially — `TIMED_OUT` is separate from `FAILED`, but both infra and app timeouts collapse into `TIMED_OUT` | No — no dedicated "waiting" state distinct from `SCHEDULED` | `{FAILED,TIMED_OUT} → SCHEDULED` | COMPLETED, FAILED_WITH_TERMINAL_ERROR, CANCELED, SKIPPED |
| **Airflow** (TaskInstance) | none, scheduled, queued, running, success, failed, skipped, upstream_failed, up_for_retry, up_for_reschedule, deferred, awaiting_input, removed, restarting | No — no separate "infra killed my worker" state; shows up as `failed` or `up_for_retry` | **Yes**, uniquely well: `up_for_reschedule` (sensor) and `deferred` (async trigger) both mean "waiting, not occupying a worker" | `failed → up_for_retry → scheduled` | success, failed, skipped, upstream_failed, removed |
| **Prefect 3** | 20 names / 7 types: Scheduled, Pending, Running, Paused, Cancelling, Cancelled, Completed, Failed, Crashed (+ variants) | **Yes, explicitly**: `CRASHED` type (infra/signal killed it) vs `FAILED` type (app raised) | Yes — `Paused`/`Suspended` types are distinct from `Pending` | `Failed → AwaitingRetry → Retrying (type RUNNING)` | Cancelled, Completed, Failed, Crashed |
| **Argo Workflows** (Node) | Pending, Running, Succeeded, Skipped, Failed, Error, Omitted | **Yes, explicitly**: `Error` (infra) vs `Failed` (non-zero exit) | Partially — `Omitted` covers unmet `depends`, but no distinct "polling" state | Controlled by `retryStrategy`, not a distinct phase | Succeeded, Skipped, Failed, Error, Omitted |
| **Luigi** | PENDING, RUNNING, DONE, FAILED, DISABLED | No | No | `FAILED → DISABLED → (timer) → PENDING` | DONE (DISABLED is a soft-terminal with auto-recovery) |
| **HTCondor DAGMan** (node) | NOT_READY, READY, PRERUN, SUBMITTED, POSTRUN, DONE | No (failure isn't a node status value at all — it's out-of-band, driving Rescue-DAG generation) | Yes — `READY` (deps met, not started) distinct from `PRERUN`/`SUBMITTED` (actually executing) | Whole-DAG level (Rescue DAG resubmission), not per-node | DONE |
| **Faktory** (job) | Scheduled, Enqueued, Working, Retries, Dead | No (server-side; app vs infra failure is opaque to the state machine, both produce `FAIL`) | Yes — `Scheduled` (future `at`) distinct from `Enqueued` (ready now) | `Working → Retries → Enqueued`, capped by `retry` count | Dead (after exhausting `retry`, unless negative) |
| **Kubernetes Job/Pod** | Pod: Pending, Running, Succeeded, Failed, Unknown; Job (derived): Active, Complete, Failed | **Yes** via `podFailurePolicy` (distinguishes exit-code classes and disruption-caused failures from app failures) | Yes — `Pending` (image pull, scheduling) is distinct from `Running` | New Pod per `backoffLimit` attempt, exponential 10s→6min | Job: Complete, Failed |

**Consensus reading across the table:** the systems that age best (Prefect, Argo, K8s) all
independently arrived at a **3-axis minimum**: (1) is it done or not, (2) if done, did the
*application* fail or did the *infrastructure* fail it, (3) if not done, is it actively
running or genuinely waiting on something external. Systems that collapse (2) into a single
`FAILED`/`error` bucket (Conductor, Luigi, Faktory) all show up elsewhere in this survey with
operator complaints about ambiguous alerting or manual log-diving to tell the two apart.
This is a strong argument for dag-worker-go's minimal public vocabulary being **new →
in-progress → success → error → error-timeout** (5 states) rather than collapsing timeout
into plain `error`, exactly as the brief already leans toward — the prior art overwhelmingly
supports keeping that distinction even while keeping everything else internal.

---

## 18. Comparison table — leasing / work-claiming mechanisms

| System | Claim primitive | Atomicity guarantee | Lease/timeout owner | What happens on expiry | Requires external sweeper? |
|---|---|---|---|---|---|
| **Temporal Matching Service** | Sync/async match on a Task Queue (partitioned) | Server-mediated single-delivery per poll response | Server (Schedule-To-Start / Heartbeat timeouts) | Task rescheduled to the queue (or history replayed on a non-sticky worker) | No |
| **Netflix Conductor** | Long-poll `FETCH` moves task to `IN_PROGRESS` | Server-side state transition on poll | Server (`responseTimeoutSeconds`, extendable via `callbackAfterSeconds`) | Task → `TIMED_OUT`, then auto-`SCHEDULED` retry | No |
| **Airflow scheduler dispatch** | `SELECT ... FOR UPDATE [NOWAIT]` on Pool table row | Postgres/MySQL row lock | N/A — dispatch itself, not a worker lease (worker liveness is separate, via heartbeat) | Lock released on transaction end (commit or crash) | No, for the dispatch step itself |
| **Faktory** | `FETCH` verb | Server-side; job moves to `WORKING`, timer starts | Server (`reserve_for`, default 1800s, min 60s) | Auto-moved to `RETRIES` by server itself | **No** — expiry enforced server-side, no separate process needed |
| **Sidekiq (plain)** | `BRPOP` | Redis atomic pop | None (no lease at all) | **Job lost** if worker dies mid-processing | N/A (this is the reliability gap) |
| **Sidekiq (`BRPOPLPUSH`/`BLMOVE` pattern)** | Atomic pop-and-push to private list | Redis atomic list move | None inherent — needs external sweep | Job sits in the dead worker's private list forever unless swept | **Yes**, always |
| **Sidekiq Pro `super_fetch`** | `LMOVE` + heartbeat key | Redis atomic list move + separate heartbeat key with TTL | Heartbeat (60s expiry) + orphan-scan process | Orphan-scan (throttled to ≥1/min per check, full `SCAN` ≤1/hr) requeues it | **Yes**, but built-in |
| **River (Postgres)** | `SELECT ... FOR UPDATE SKIP LOCKED` | Postgres MVCC row lock, skip-on-contention | Application-level retry/attempt tracking; no separate lease timer documented | Depends on caller's own attempt/timeout logic on top of River's job states | Not for claiming; yes if modeling worker-crash detection |
| **Asynq (Redis)** | Move to `active` ZSET with a deadline score | Redis atomic (Lua script) | Deadline embedded in the sorted-set score | Independent recoverer process requeues tasks past deadline | **Yes**, built-in recoverer |
| **Kubernetes Job controller** | Finalizer attached at Pod creation | apiserver optimistic-concurrency (`resourceVersion` CAS) | No timeout on the finalizer itself — bounded by Pod lifecycle + `activeDeadlineSeconds` if set | Finalizer removed only after terminal Pod phase is durably observed | No — reconciliation loop *is* the sweeper, running continuously |
| **Nomad plan-apply** | Plan submitted to single-leader plan queue | Raft-serialized apply with fresh-state conflict check | N/A — not a lease, a speculate-then-commit-or-reject model | Rejected plan triggers a new Evaluation against fresh state | No — rejection *is* the recovery path |
| **Mesos** | Framework accepts a resource offer | Master tracks resource accounting per offer; offer has a short implicit validity window | Framework-side (offer can simply be declined or ignored) | Offer rescinded/reoffered if unused | No |
| **Kubernetes leader election** | Conditional write to a `Lease` object | apiserver CAS on the Lease resource | `leaseDurationSeconds`, renewed by the current holder | Any standby may take over once expired | No — the Lease object itself carries the timeout |

**Consensus reading:** the mechanisms that need **no external sweeper process** — Faktory,
Asynq (recoverer is *built into* the library, not bolted on separately by the operator),
Kubernetes finalizers, Nomad's plan-apply — are uniformly the ones where the storage layer
itself enforces the timeout as part of the same atomic operation that granted the claim
(a TTL/deadline stored *with* the claim, checked by the same code path that would service a
new claim request). The one mechanism that structurally *requires* a bolted-on external
sweeper — Sidekiq's base `BRPOPLPUSH` pattern — is also the one with the most
documented production incidents in this survey (§11.1). **This is the strongest single
actionable finding in this whole dossier: dag-worker-go's lease/claim operation must be a
single atomic primitive that both claims the node and records its expiry, on every backend,
and node reclamation must be driven by that same expiry being checked inline by the next
claim attempt — never by a separate cron-style sweep process that can itself fail, fall
behind, or be forgotten in a deployment.**

---

## Recommendations for dag-worker-go

1. **Public status vocabulary: `new → in-progress → success | error | error-timeout`,
   nothing else public.** Every mature system surveyed independently converges on needing
   the app-failure/infra-failure split (§17) — do not collapse `error` and `error-timeout`
   into one bucket the way Conductor and Faktory do; the operational cost of that
   collapse (ambiguous alerting, manual log correlation) is visible across multiple issue
   trackers in this survey. Keep retry count, attempt history, lease generation, and
   partition/queue placement entirely internal, matching the brief's "minimal public,
   everything else internal" instruction and matching Prefect's state-type-vs-state-name
   split (§4) as the cleanest precedent for how to structure the internal/public boundary
   in code (an internal richer enum that *maps onto* the small public one, never the
   reverse).

2. **Make the claim operation a single atomic primitive on every backend, with the lease
   deadline stored as part of the same write that grants the claim** (§18's strongest
   finding). Concretely: in-memory — a mutex-guarded map entry with `(workerID, leaseUntil,
   fenceToken)`; Postgres — `UPDATE ... SET status='in-progress', worker_id=$1,
   lease_until=now()+$2, fence_token=fence_token+1 WHERE id IN (SELECT id FROM nodes WHERE
   status='ready' ... FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *`; Redis — a single Lua
   script doing the ready-set removal, in-progress-ZSET insert (score = deadline), and
   fence-token increment atomically; Memcached — `gets`/`cas` on a per-node key encoding
   status+lease+fence, retried on CAS failure. **Never implement lease reclamation as a
   separate cron/sweeper job** — make expiry-checking part of the same code path that
   services a new claim (whoever asks "give me ready work" also atomically reclaims
   anything whose lease has passed, in the same query/script), matching Faktory and the
   Kubernetes reconcile loop rather than base Sidekiq.

3. **Adopt Faktory's exact lease-timeout contract**: a library-wide default reservation
   timeout, overridable per node at the moment a worker takes it (`reserve_for` in
   Faktory's terms) — this is already explicitly requested in the brief and Faktory's
   protocol spec (§11.2) is close to a reference implementation to copy the shape of,
   including the sane bounds (a minimum floor on the timeout, e.g. Faktory's 60s, to
   prevent a misconfigured caller from creating a reclaim storm).

4. **Add fencing tokens to every ack, from day one, on every backend** (§16). The ack
   handler must compare the fence token on the incoming ack to the node's current fence
   token and reject (routing to a "late/conflicting ack" internal state for
   observability, not silently dropping) any ack whose token is stale. This is one integer
   column/field and one comparison; it is the difference between "an occasional double-
   claim redoes idempotent work" (fine) and "an occasional double-claim corrupts which
   worker's result is recorded as final" (not fine), and per Kleppmann (§16) this is *not*
   optional once Redis or Memcached are in scope as real backends, since neither offers
   the consensus guarantees that would make it safe to skip.

5. **Default to pure pull-based competition for cross-instance work distribution** (§15's
   position), because it is the only strategy in the comparison table implementable
   natively and identically on all four required backends without introducing an external
   coordination service the brief never asked for. Offer scope-affinity as an optional,
   best-effort routing hint layered on top (a claim query can prefer nodes in scopes the
   calling instance has recently touched, exactly as Temporal's sticky execution prefers
   but never requires the previous worker, §1.4) — never make correctness depend on the
   hint. Do not build partition ownership, consistent hashing, or leader election into the
   critical path for v1; revisit only if pull-based claim contention is measured (not
   assumed) to be a bottleneck at the 1M-node benchmark scale.

6. **Reconciliation over the DAG's readiness graph must be level-triggered and independent
   of the event stream** (§13.1's central lesson, reinforced by Netflix Conductor's
   "decide" pass, §2). "Which nodes are now ready to dispatch" must always be answerable by
   a fresh query against persisted state, never by replaying a log of past transitions —
   the pub/sub stream the brief requires is a *notification convenience* for subscribers,
   and must not be a dependency for correctness. Concretely: a crashed instance restarting,
   or a subscriber that missed messages, must both be able to recover to a fully correct
   view purely by re-querying current node/edge state.

7. **Give dynamically-added fan-out its own shape/payload split, à la Airflow's AIP-42**
   (§3.4): when a node's processing dynamically creates N new child nodes, store the count/
   shape of that expansion separately from any large payload driving it, cap N with a
   hard configurable ceiling to prevent a combinatorial blow-up from taking down the shared
   store, and expand lazily (only materialize child node rows once something actually
   needs to know about them), not eagerly the instant the parent completes.

8. **Design the storage schema so no single node's status, or any node's history, is ever
   forced into one oversized field/row/object** (§6's etcd-1MB lesson from Argo). Node
   status belongs in a table/keyspace designed for wide row counts and point lookups (one
   row per node), never serialized as a single blob that grows with total DAG size — this
   is exactly the pluggable-storage, O(1)-per-node access pattern the brief already
   requires, but Argo's postmortem is concrete evidence for *why* it matters at scale, not
   just an abstract nicety.

9. **Enforce and document one global multi-table lock-acquisition order from the first
   commit**, per Airflow's still-open deadlock class (§3.3). If a single transaction ever
   needs to touch both a "node" row and a "scope"/"dag-run" row (or their equivalents),
   pick and document the order once (e.g. always scope before node) and enforce it in code
   review / lint, because this exact bug class recurs independently at Airflow's scale and
   is far cheaper to prevent by convention than to debug in production later.

10. **Bound and document the event/history log's growth, and provide an explicit
    compaction/retention story before it is needed**, per Temporal's hard 51,200-event
    termination ceiling (§1.6). Even though dag-worker-go's per-node history is unlikely to
    approach Temporal's per-workflow scale, the *pattern* — an unbounded append-only log
    backing "subscribe to every status transition" — needs an explicit answer for "what
    happens after 1M nodes have each transitioned 3 times" from the design phase, not
    discovered as an incident later. Nextflow's content-hash-based idempotent-skip model
    (§8) is a good complementary idea for allowing large graphs to be safely re-submitted/
    resumed without needing Temporal-grade replay machinery.

## Open questions

- **Should the fencing-token check live in the storage adapter interface (so every backend
  implementation is forced to support it) or be optional per backend?** Making it mandatory
  is the correctness-safe choice per §16, but adds implementation burden to a Memcached
  adapter (no native CAS-with-struct support beyond raw `gets`/`cas` on an opaque blob) —
  needs a concrete Memcached data-layout decision before this can be answered definitively.
- **What is the actual maximum useful Task-Queue-style partition count for dag-worker-go's
  claim query at 1M nodes**, i.e. at what concurrent-claimant count does `SKIP LOCKED`
  contention (Postgres) or a single Lua script (Redis) stop scaling linearly, and does that
  threshold arrive before or after the point where sharding the ready-set by scope becomes
  worth its added complexity? This needs to be answered empirically by the benchmark suite
  the brief already mandates, not by further literature research.
- **Should scope-affinity routing (recommendation 5) be implemented at all in v1, or
  deferred entirely until a measured bottleneck justifies it?** Temporal's own sticky-queue
  design took multiple bug-fix cycles to get the fallback timeout right (§1.6); building it
  prematurely risks importing that same class of bug before there's demonstrated need.
- **How should node-level heartbeats (for long-running external work, analogous to
  Temporal's Activity Heartbeat / Conductor's `callbackAfterSeconds`) interact with the
  per-node settable timeout** — is a heartbeat a first-class part of the public worker-ack
  API from v1, or an internal-only extension point added once a real long-running-work use
  case appears? The brief specifies only a flat "timeout, then error-with-timeout,"
  which is closer to Faktory's single `reserve_for` than to Temporal's four-timeout model;
  worth confirming this simpler model is intentional before building it, since retrofitting
  heartbeat-based deadline extension later touches the same lease primitive recommendation 2
  already locks in.
- **Netflix's own rationale for spinning Conductor out to Orkes/OSS stewardship** (scaling
  pain with its original Dynomite/Cassandra/Elasticsearch dependency stack, or purely a
  business decision) was not confirmed by any primary source found during this research;
  if a deeper Conductor post-mortem exists it was not surfaced by the queries run here and
  is worth a targeted follow-up if Conductor's storage-layer choices become directly
  relevant to a design decision.
