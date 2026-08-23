# PostgreSQL as the Durable Backend for a DAG + Job Queue at 1M+ Rows

Scope: this dossier evaluates PostgreSQL as one pluggable storage backend for dag-worker-go —
a library that tracks a dynamic DAG of nodes, decides which nodes are ready, hands them to
external workers under a lease, and reacts to completion/timeout — at a target scale of
1,000,000+ nodes per scope, multiple library instances competing for the same storage.

## 1. `SELECT … FOR UPDATE SKIP LOCKED`: the canonical queue pattern

### 1.1 What it does, precisely

`FOR UPDATE` is a locking clause on `SELECT`: it takes the same row locks an `UPDATE` would,
without doing the update. Ordinarily, if a `SELECT … FOR UPDATE` hits a row already locked by
another transaction, it blocks until that transaction ends (or errors immediately with `NOWAIT`).
`SKIP LOCKED` changes this: rows that cannot be locked immediately are silently excluded from
the result set instead of blocking or erroring — [PostgreSQL SELECT
docs](https://www.postgresql.org/docs/current/sql-select.html). The documentation is explicit
that this "provides an inconsistent view of the data" and is unsuitable for general-purpose
work, but calls out exactly one sanctioned use case: "avoid[ing] lock contention with multiple
consumers accessing a queue-like table."

The feature shipped in PostgreSQL 9.5 (2016) after a multi-year mailing-list debate; 2ndQuadrant
(Craig Ringer) wrote the reference explainer, archived at
[jaytaylor.com](https://jaytaylor.com/notes/node/1540867485000.html), built around this query
shape:

```sql
DELETE FROM queue
WHERE itemid = (
  SELECT itemid
  FROM queue
  ORDER BY itemid
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *;
```

The post frames the problem precisely as dag-worker-go will face it: "how do I find the first
row in a queue table that nobody else has claimed, and claim it for myself, such that each item
gets processed exactly once — none skipped, none processed twice — while allowing genuine
concurrency between consumers?" Before 9.5, the only correct answers were (a) plain `FOR UPDATE`,
which serializes every claim behind the slowest in-flight worker, or (b) advisory locks with
manual bookkeeping of which IDs are "in use," which pushes the concurrency problem into
application code. `SKIP LOCKED` moves the semantics for "claim one of many equivalent candidates"
into the row-lock manager itself, which is where it belongs: lock acquisition and the "no two
workers get the same row" guarantee become atomic and free of races.

### 1.2 Why it composes with a CTE for claim-and-mark-in-one-round-trip

Because `FOR UPDATE SKIP LOCKED` is just a locking clause on a `SELECT`, it can live inside a
`WITH … AS (SELECT … FOR UPDATE SKIP LOCKED)` and be joined into an `UPDATE … FROM` in the same
statement — one network round trip, one transaction, and the rows stay locked for the CTE's
whole scope so no other worker can see them as "unlocked" in between the `SELECT` and the
`UPDATE`. This is the pattern every production queue library in §2 converges on, and it is the
pattern proposed for dag-worker-go in §13.

### 1.3 Caveats sourced from the docs and from operational reports

- **Ordering is best-effort under skew.** `ORDER BY priority, id … LIMIT N FOR UPDATE SKIP
  LOCKED` does not guarantee strict priority order across concurrent claimers: if the
  highest-priority row is locked by another transaction, it is skipped, and a lower-priority row
  is claimed instead. This is a correct trade — the alternative is blocking — but it means
  "priority" is soft under contention, not a hard guarantee. Document this in the public API.
- **Interaction with `LIMIT`.** `SKIP LOCKED` is evaluated per-row during the scan; the `LIMIT`
  is applied after skip-filtering, so a worker asking for 50 rows gets up to 50 *unlocked*
  matching rows, scanning as many rows as it needs to find them. If contention is high (many
  workers claiming from a small ready-set), the useful-work-per-scanned-row ratio drops and CPU
  cost rises even though the query returns instantly and never blocks.
- **Not a substitute for `NOWAIT`'s error semantics.** `SKIP LOCKED` never raises an error and
  never reports "how many rows were skipped" — a worker cannot distinguish "no ready work" from
  "there was ready work but it was all locked by faster competitors" without a separate count
  query, which matters for autoscaling/backpressure decisions in the host program.
- **MySQL added the equivalent in 8.0 (2018), long after Postgres**; the technique below is
  Postgres/Oracle/SQL-Server-specific and does not exist in older MySQL or in SQLite.

## 2. How six real systems structure their queue tables

Fetched directly from each project's schema/migration source on GitHub. All six independently
converge on the same shape — status column, priority + scheduled-time ordering, `FOR UPDATE
SKIP LOCKED`, JSON payload column — which is strong evidence this is close to a canonical
design, not a Go-specific or Ruby-specific idiom.

| System | Language | Table(s) | Status representation | Claim mechanism | Reactive push |
|---|---|---|---|---|---|
| [River](https://github.com/riverqueue/river) | Go | `river_job` (+ `river_leader` for leader election) | `river_job_state` enum: `available, cancelled, completed, discarded, pending, retryable, running, scheduled` | `SELECT … WHERE state='available' AND queue=$1 AND scheduled_at<=now() ORDER BY priority, scheduled_at, id FOR UPDATE SKIP LOCKED LIMIT $n` inside a CTE, then `UPDATE` to `running` in the same statement | Polling by default; `LISTEN/NOTIFY` supported as a low-latency wake-up hint, not the source of truth |
| [Que](https://github.com/que-rb/que) | Ruby | `que_jobs`, `que_lockers` (UNLOGGED), `que_values` | Derived, not stored directly: `que_determine_job_state()` computes `expired / finished / errored / scheduled / ready` from `expired_at`, `finished_at`, `error_count`, `run_at` | Historically **session-level `pg_advisory_lock`** keyed by job id (not row locks) so a lock survives across a worker's held connection without an open transaction; `que_poll_idx` partial-style index on `(queue, priority, run_at, id)` | `pg_notify` on the `que_state` and job-insert channels via `que_job_notify()` / `que_state_notify()` trigger functions — see [migration 4](https://github.com/que-rb/que/blob/master/lib/que/migrations/4/up.sql) |
| [Oban](https://github.com/oban-bg/oban) (Elixir) | Elixir | `oban_jobs` | `oban_job_state` enum: `available, suspended, scheduled, executing, retryable, completed, discarded, cancelled` | `FOR UPDATE SKIP LOCKED` over a partial-index-friendly `(queue), (state), (scheduled_at)` index set — see [v01.ex](https://github.com/oban-bg/oban/blob/main/lib/oban/migrations/postgres/v01.ex) | `oban_jobs_notify()` trigger function fires `pg_notify` on insert/update, filtered to jobs actually due |
| [pgqueuer](https://github.com/janbjorge/pgqueuer) | Python | single job table + status enum | explicit status column | `FOR UPDATE SKIP LOCKED`, described in the README as "never double-processed" | `NOTIFY` wakes workers "the moment a job lands," with polling as an explicit fallback for missed notifications |
| [graphile-worker](https://github.com/graphile/worker) | Node/TS | `jobs` + `job_queues` (queue-level lock, not row-level) | no explicit status column — presence in `jobs` plus `locked_at`/`locked_by` on the *queue* row is the state | `get_job()` PL/pgSQL function does `… FOR UPDATE OF job_queues SKIP LOCKED LIMIT 1` — it locks the **queue**, not the job, so all jobs in one named queue are strictly serialized relative to each other | `pg_notify('jobs:insert','')` fired once per statement via `tg_jobs__notify_new_jobs()` trigger — see [sql/000001.sql](https://github.com/graphile/worker/blob/main/sql/000001.sql) |
| [Hatchet](https://github.com/hatchet-dev/hatchet) | Go | `v1_task` (partitioned) + `v1_queue_item` + `v1_task_event` | `v1_task_initial_state` plus separate lifecycle columns (`schedule_timeout_at`, `retry_count`) | Same SKIP LOCKED family, applied against `v1_queue_item`, a slim projection table decoupled from the wide `v1_task` row | Custom event bus fed from `v1_task_event`, decoupled from the queue table itself |

Design lessons worth lifting directly:

1. **Separate the wide "task" row from the thin "queue" row.** Hatchet's `v1_queue_item`
   duplicates only the columns a claim query needs (`tenant_id, queue, priority, task_id`) and
   leaves the heavy `input jsonb`, `additional_metadata jsonb`, etc. on `v1_task`. This keeps the
   hot, frequently-locked, frequently-vacuumed table narrow — directly attacking the bloat
   problem in §8 — at the cost of a join to fetch the payload after claiming. dag-worker-go's
   `nodes` table should follow this split (§13).
2. **graphile-worker's queue-level lock is a cautionary tale, not a model.** By locking the
   `job_queues` row instead of the job row, two jobs in the same named queue can never be worked
   concurrently even on different workers — a deliberate choice for graphile-worker's ordering
   guarantees, but exactly the anti-pattern dag-worker-go must avoid for the DAG's ready-set,
   where many unrelated nodes in the same scope must be claimable in parallel.
3. **Que's advisory-lock history is the reason session vs. transaction scoping matters (§5).**
   Que originally chose session-level advisory locks specifically because a Ruby worker holds a
   job across a long-running unit of work without wrapping it in one open SQL transaction —
   `pg_advisory_lock` survives that; a `FOR UPDATE` row lock would not (it is released at
   transaction end, and you don't want an open transaction held for the duration of an
   externally-executed job).
4. **Every system uses `NOTIFY` as a latency optimization on top of polling, never as the sole
   delivery mechanism** — see §3 for why that split is forced by NOTIFY's semantics, not just
   caution.
5. **Hatchet's own writeup of *why* they partition** ([hatchet.run/blog/postgres-partitioning](https://hatchet.run/blog/postgres-partitioning))
   is the single most relevant real-world war story for dag-worker-go and is covered in depth in
   §10.

## 3. `LISTEN` / `NOTIFY` as the reactive push channel

dag-worker-go's requirement is "anyone can subscribe to a stream and receive every status
transition and every ready-for-pickup event." `LISTEN`/`NOTIFY` is the obvious native mechanism,
but it has four hard limits that must shape the design, all from the primary docs
([NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html)):

1. **Transactional, commit-gated delivery.** A `NOTIFY` issued inside a transaction is queued and
   delivered to listeners only once that transaction commits; if it rolls back, the notification
   is discarded entirely. This is exactly the semantics dag-worker-go wants — a status-transition
   notification must never be observed for a transition that didn't actually happen — but it
   means notify-latency includes commit latency, and a long-running transaction that also holds a
   `LISTEN` session open will delay delivery of everyone's notifications, not just its own.
2. **8000-byte payload ceiling**, hard-compiled (`NOTIFY_PAYLOAD_MAX_LENGTH`), not a GUC. The
   payload must therefore carry only identifiers (`scope`, `node_id`, `event_id`, new status) —
   never the node's JSON `payload` column. Consumers that need the full row do a follow-up read
   keyed by the identifier, which is also what protects them from missing an update if the
   payload changed again between notify and read (see point 4).
3. **The 8GB global asynchronous notification queue, shared across the whole cluster, and its
   failure mode.** All pending, not-yet-consumed notifications for every channel and every
   listener live in one shared area; `pg_notification_queue_usage()` reports the fraction full,
   and the docs warn that once it passes 50% full, the log starts naming the session that is
   blocking cleanup — invariably a session that issued `LISTEN` and then sat in a long
   transaction or simply stopped calling `NOTIFY`/checking for notifications, since the queue can
   only be trimmed up to the slowest listener's read position. If the queue fills completely, the
   documented behavior is that **any transaction that calls `NOTIFY` fails at commit** — a
   completely unrelated writer, having nothing to do with the stalled listener, is the one who
   gets the error. This is a poisoning failure mode: one wedged consumer breaks writes for the
   whole cluster. Mitigation for dag-worker-go: never let library-internal `LISTEN` sessions be
   long-lived without also continuously calling `PQconsumeInput`/draining (pgx's `Conn.WaitForNotification`
   loop does this correctly); treat a stuck listener connection as fatal and reconnect rather
   than let it linger; alert on `pg_notification_queue_usage() > 0.5` in operational guidance.
4. **Does not survive disconnect, and is not itself durable.** A session must re-issue `LISTEN`
   after every reconnect, and if the connection drops between a status change committing and the
   listener consuming the notification, that event is gone — `NOTIFY` is a wake-up bell, not a
   log. **This single fact dictates the architecture**: the events table (§13, `dagw.events`)
   is the durable, replayable source of truth for "every status transition," and `NOTIFY` is
   purely a latency shortcut telling an already-caught-up listener "don't wait for your next poll
   interval, there's something new — go read the events table (or the ready-set) now." Every
   library surveyed in §2 uses exactly this split (poll-as-ground-truth, notify-as-optimization),
   and dag-worker-go should not deviate: a subscriber that has been disconnected re-subscribes by
   resuming from the last `event_id` it processed, never by trusting that it received every
   `NOTIFY`.

## 4. Logical decoding and `wal2json` as an alternative event source

Rather than application code calling `pg_notify()` on every transition, PostgreSQL can stream
every committed row change out of the write-ahead log itself via **logical decoding**: a
replication slot plus an *output plugin* (built-in `pgoutput`, or the widely-used third-party
[wal2json](https://github.com/eulerto/wal2json)) turns WAL records into a structured change
stream a consumer replays over the replication protocol
([logical decoding docs](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html)).

Trade-offs against a hand-rolled `pg_notify` trigger:

- **Upside**: zero application-code discipline required to "remember to notify" — every
  `UPDATE dagw.nodes SET status=…` is automatically visible to a decoding consumer with no
  trigger to write or maintain, and the stream naturally includes old-row/new-row values (useful
  for computing exactly which field changed) without needing a bespoke `events` table schema.
- **Downside — the resource-retention trap.** A replication slot is a promise: PostgreSQL will
  retain every WAL segment newer than the slot's confirmed position, and — sharply relevant for
  a table with an internal ready→leased→success/error state machine — **it will also prevent
  VACUUM from removing dead rows/tuples that a lagging logical consumer might still need to see**,
  per the docs' explicit warning. A stalled or forgotten decoding consumer therefore does not
  just risk disk-filling WAL growth; it can independently reintroduce the exact bloat/vacuum
  pathology this dossier spends §8 warning about, on a table that already churns heavily. This
  makes logical decoding strictly higher-operational-risk than trigger-based `NOTIFY` for a
  library meant to be embedded by third parties with unknown operational maturity — a forgotten
  slot is invisible until disk pressure or transaction-ID-wraparound protection kicks in.
- **Fit for dag-worker-go**: logical decoding is the right tool for a host application that wants
  to mirror `dagw.events` into Kafka/Debezium for cross-service fan-out, but it is the wrong
  default inside the library itself — it requires `wal_level = logical`, a slot-lifecycle owner,
  and (for wal2json specifically) an extra native extension most managed Postgres offerings do
  support (RDS, Cloud SQL, Azure Flexible Server) but which is one more environmental dependency
  than a plain trigger. **Recommendation**: ship the trigger + `events` table + `pg_notify` design
  as the only backend-native mechanism; document logical decoding as an integration path for
  operators who want it, not as something the library manages.

## 5. Advisory locks: session vs. transaction scope, and when they beat row locks

Advisory locks are locks the application defines the meaning of; Postgres enforces the mutual
exclusion but attaches no semantics to the lock key beyond two 32-bit integers or one 64-bit
integer ([explicit-locking docs](https://www.postgresql.org/docs/current/explicit-locking.html)).

| | `pg_advisory_lock` / `pg_try_advisory_lock` | `pg_advisory_xact_lock` |
|---|---|---|
| Scope | Session — held until explicit `pg_advisory_unlock` or the session ends | Transaction — released automatically at commit/rollback |
| Survives rollback of the acquiring transaction? | Yes — session locks ignore transaction boundaries entirely | No |
| Needs explicit release? | Yes, or it leaks until the connection closes | No |
| Best fit | Locks that must outlive a single SQL statement/transaction — e.g., "only one worker process may run the timeout-sweeper for scope X right now," held across a whole sweep loop | Locks scoped to exactly one unit of work — e.g., "serialize DAG-completion checks for this scope" wrapped around one transaction |

Why reach for an advisory lock instead of `SELECT … FOR UPDATE` at all: **advisory locks don't
touch a row**, so they add zero MVCC garbage and are invisible to autovacuum — the docs list
"avoiding table bloat" as one of the stated advantages over flag-column-based locking schemes.
For dag-worker-go, three uses fit cleanly:

1. **Leader election for scope-scoped maintenance work** (the timeout sweeper, a periodic
   ready-set consistency check): each instance tries
   `pg_try_advisory_lock(hashtext('dagw.sweeper')::bigint, hashtext(scope)::bigint)` in a
   background goroutine; the one that gets it runs the sweep for that scope, others no-op. This
   is strictly cheaper than a `river_leader`-style row (River's own approach, per §2) when the
   only thing being coordinated is "run this loop somewhere," not "own a durable row."
2. **Serializing the write-skew-prone "is this whole DAG done" check** flagged in §14 —
   `pg_advisory_xact_lock(hashtext('dagw.dag-complete')::bigint, scope_hash)` around the read of
   the aggregate + the write of the completion event avoids paying `SERIALIZABLE`'s SSI overhead
   on the hot claim/complete path while still closing the race.
3. **NOT for per-node claiming.** `SKIP LOCKED` row locks already solve that with less overhead
   and, critically, with `pg_locks`-visible, deadlock-detector-visible semantics; advisory locks
   used per-row would sidestep the deadlock detector for locks the application takes in whatever
   order it pleases, reintroducing exactly the risk deterministic row-lock ordering (§14) is
   designed to eliminate. Que's own history — moving toward `SKIP LOCKED` for the claim path once
   Postgres 9.5 existed — is the field validating this choice (per its
   [job-locking documentation](https://github.com/que-rb/que)).

One correctness trap called out directly in the docs: `SELECT pg_advisory_lock(id) FROM foo
WHERE id > 12345 LIMIT 100` does not guarantee the `LIMIT` is applied before the lock function is
evaluated per row — the planner can lock far more rows than the `100` implies. Always wrap in a
subquery that materializes the row set first, then lock: `SELECT pg_advisory_lock(q.id) FROM
(SELECT id FROM foo WHERE id > 12345 LIMIT 100) q`.

## 6. Partial and covering indexes for the ready-set

At 1,000,000 nodes, the vast majority are either terminal (`success`/`error`) or not yet ready
(`new`, still blocked on dependencies). The claim query only ever needs `status = 'ready'` rows —
plausibly a few hundred to a few thousand at any instant, even at 1M total. A full index on
`(scope, status, priority, node_id)` would put every terminal row in the index forever, growing
without bound as the DAG runs; a **partial index** keeps only the live rows:

```sql
CREATE INDEX nodes_ready_idx
    ON dagw.nodes (scope, priority DESC, node_id)
    WHERE status = 'ready';
```

Per the [partial index docs](https://www.postgresql.org/docs/current/indexes-partial.html), this
is the textbook "index the interesting subset" pattern (their own worked example is nearly
identical: `orders_unbilled_index ON orders (order_nr) WHERE billed IS NOT TRUE`). The mechanism
that makes it viable here specifically is that the predicate matches the query's `WHERE status =
'ready'` clause *verbatim* — the docs stress that Postgres's ability to prove "query predicate
implies index predicate" is limited to simple, syntactically-recognizable implications, and a
parameterized predicate (`status = $1`) will **never** be recognized as implying `status =
'ready'` even when `$1` happens to be `'ready'` at execution time — the match happens at plan
time against the literal text of the query, not against bound parameter values. **Concretely:
the claim query must be issued with the literal `'ready'` in the SQL text (or use `PREPARE`
carefully / rely on the planner's generic-vs-custom-plan logic falling back to a custom plan),
not as a bind parameter, or this index silently stops being used.** This is a sharp, easy-to-miss
footgun for a Go library built on `pgx`'s prepared-statement-by-default query paths and deserves
an explicit code comment at the call site.

The docs also warn against the anti-pattern of fanning partial indexes out per status value
(`WHERE status=1`, `WHERE status=2`, …) — the planner cannot reason across multiple partial
indexes and must consider each independently, which is worse than one ordinary composite index
once you have more than a couple of statuses of interest; only the *live* half of the ready/dead
divide benefits from partial-indexing, precisely because the dead half is uninteresting to every
query the library issues.

**Covering (`INCLUDE`) variant** for read-heavy monitoring paths (a UI polling "what's in the
ready queue right now" without wanting to touch the wide `nodes` heap): [`CREATE
INDEX`](https://www.postgresql.org/docs/current/sql-createindex.html) supports `INCLUDE` columns
that ride along in the index leaf pages purely for index-only-scan purposes, not for the sort
key:

```sql
CREATE INDEX nodes_ready_peek_idx
    ON dagw.nodes (scope, priority DESC, node_id)
    INCLUDE (kind, created_at)
    WHERE status = 'ready';
```

Caveat straight from the docs: `INCLUDE`d columns bloat the index (every byte of `kind` and
`created_at` is duplicated per row) and are never deduplicated, and an index-only scan still
needs the visibility map to be current to skip the heap — which on a table with the update
churn described in §8 is exactly the map that tends to go stale fastest. Treat this as an
optional, monitoring-path optimization, not a claim-path one; the claim path needs the full
`payload` anyway and gains nothing from `INCLUDE`.

## 7. The B-tree index on `(scope, deadline)` for the timeout sweeper

The sweeper's query is structurally different from the claim query: it wants *all* leases whose
deadline has passed, across however many are currently outstanding, ordered by nothing in
particular (it processes all expired ones in a batch). A plain composite B-tree serves this
well **without needing to be partial**, for a reason worth stating explicitly: unlike the
`nodes` table (1M rows, only a sliver "ready"), the `leases` table's *total* row count is bounded
by the number of concurrently in-flight (leased-but-not-yet-acked) nodes — a function of worker
concurrency, not of total DAG size. Even at 1M nodes, if 500 workers each hold one lease, the
`leases` table has ~500 rows; rows are `DELETE`d on completion, so it never accumulates the way
`nodes` does. This makes it one of the cleanest tables in the whole schema from a bloat
standpoint — see §13's DDL — and a leading-column index of `(scope, deadline)` is sufficient:

```sql
CREATE INDEX leases_deadline_idx ON dagw.leases (scope, deadline);
```

If the sweeper runs as one global process per storage cluster (not per scope), drop the `scope`
prefix so a single ordered scan over all outstanding deadlines works cluster-wide:
`CREATE INDEX leases_deadline_idx ON dagw.leases (deadline);` — pick whichever matches the chosen
work-distribution model from the companion multi-instance-coordination research doc; both are
cheap because the table itself stays small by construction.

## 8. MVCC bloat and autovacuum: the classic failure mode, with receipts

This is the single most load-bearing operational risk in "Postgres as a queue," and it is not
theoretical — three independently-documented incident writeups converge on the identical
mechanism:

**The mechanism** (from the [routine vacuuming
docs](https://www.postgresql.org/docs/current/routine-vacuuming.html)): an `UPDATE` or `DELETE`
never overwrites a row in place; it leaves the old tuple version behind, marked dead, until no
open transaction's snapshot can still see it, at which point `VACUUM` (normally `autovacuum`)
can reclaim the space. A queue table's job lifecycle — insert, then one or more `UPDATE`s to flip
status, then either a terminal `UPDATE` or a `DELETE` — means **every row generates multiple dead
tuple versions over its life**, on both the heap and every index that touches an updated column.

**Incident 1 — PlanetScale, "Keeping a Postgres queue healthy"**
([planetscale.com/blog/keeping-a-postgres-queue-healthy](https://planetscale.com/blog/keeping-a-postgres-queue-healthy)):
reproducing a 2015-vintage benchmark, they hit "catastrophic failure" within 15 minutes of
sustained load. Concrete numbers from their stress run at 800 jobs/sec: **155,000-job backlog,
383,000 dead tuples, and row-lock acquisition times spiking past 300ms** (vs. a 2–3ms healthy
baseline) — a death spiral where slower locks mean longer-held snapshots mean autovacuum falls
further behind mean even slower locks. Their root cause: **B-tree index scans have to walk past
dead-tuple index entries to find live ones**, so index bloat directly inflates lock-acquisition
latency, not just storage. Their eventual fix was not a Postgres knob at all — it was
application-level admission control (limiting concurrent long-running analytical queries against
the same database) to open a window in which autovacuum could catch up, because "the MVCC
horizon" was being held open by unrelated long queries on the same connection pool, not by the
queue traffic itself.
**Lesson for dag-worker-go: an oldest-open-transaction anywhere in the same Postgres instance
(even against an unrelated table) can stall vacuum on the DAG tables.** The library cannot
control what else shares the database, but its own documentation should tell operators to
monitor `pg_stat_activity.xact_start` for long-idle-in-transaction sessions as a first-line
defense, and the library itself must never hold a transaction open across a network call to an
external worker.

**Incident 2 — richyen.com, "Potential Consequences of Using Postgres as a Job Queue"**
([richyen.com/postgres/…](https://richyen.com/postgres/2026/05/04/postgres_job_queue.html)):
identifies a second, distinct mechanism above and beyond plain tuple bloat —
**MultiXact contention**. When many concurrent sessions try to lock the *same* row (e.g., a hot
"claim" scan repeatedly probing recently-touched rows before `SKIP LOCKED` filters them out, or
row-level FK lock checks), Postgres must allocate MultiXactIds to represent multiple concurrent
lockers, and at high concurrency "those lookups serialize on LWLocks," pegging CPU with dozens to
hundreds of backends piled up waiting — a failure mode invisible in query plans because the
individual queries are cheap; the bottleneck is internal lock-manager contention, not I/O. The
same piece quantifies bloat independently: tables where "the actual live data was only a few
megabytes" ballooning to "tens of gigabytes." Its practical throughput guidance —
Postgres+`SKIP LOCKED` remains comfortably viable under roughly 100 concurrent worker
connections, advisory locks or a purpose-built extension (`pgq`) as a step up, dedicated
broker (Redis Streams) beyond that, Kafka at "massive" scale — is a reasonable, if informal, set
of thresholds to plan around; treat it as a rule of thumb, not a guarantee, since the actual
ceiling depends heavily on row width, index count, and how well autovacuum is tuned (below).

**Incident 3 — Hatchet, hitting the wall at real production scale**
([hatchet.run/blog/postgres-partitioning](https://hatchet.run/blog/postgres-partitioning)):
a single unpartitioned tasks table degraded badly around **~200 million rows**, independent of
raw disk size, purely from index/table bloat on a table sustaining "hundreds of millions of
tasks per day" in aggregate. Their fix was time-based partitioning (§10) rather than autovacuum
tuning alone — a strong signal that autovacuum tuning buys headroom but does not remove the
ceiling; partitioning changes the *shape* of the problem (bounded-size active partitions,
bulk-droppable old ones) rather than fighting bloat within one ever-growing relation.

### 8.1 Autovacuum tuning that actually matters for a queue table

Global `autovacuum_vacuum_scale_factor` (default 0.2 — vacuum after 20% of the table is dead
tuples) is tuned for large, slowly-changing tables; on a queue table churning through its own
row count every few minutes, 20% of 1,000,000 rows is 200,000 dead tuples before vacuum even
triggers — far too much slack. Set **per-table** storage parameters
([routine vacuuming docs](https://www.postgresql.org/docs/current/routine-vacuuming.html)):

```sql
ALTER TABLE dagw.nodes SET (
    autovacuum_vacuum_scale_factor  = 0.02,  -- vacuum after ~2% dead, not 20%
    autovacuum_vacuum_cost_delay    = 2,     -- ms sleep per cost-limit unit; keep vacuum aggressive
    autovacuum_vacuum_cost_limit    = 2000,  -- higher budget per pass = faster catch-up
    autovacuum_vacuum_insert_scale_factor = 0.02,  -- 1M-row insert bursts trigger vacuum too
    fillfactor = 80
);
ALTER TABLE dagw.leases SET (
    autovacuum_vacuum_scale_factor = 0.05    -- small table, but every row is insert+delete: churns 100%/cycle
);
```

For a genuinely high-churn table, some operators go further and set
`autovacuum_vacuum_scale_factor = 0` with a fixed absolute `autovacuum_vacuum_threshold`, so the
trigger is a constant row count regardless of table growth — appropriate once the table's
*steady-state* dead-row rate is well understood empirically, not as a starting default.

## 9. HOT updates and fillfactor

A **Heap-Only Tuple (HOT) update** avoids the most expensive part of an ordinary `UPDATE`:
writing a new entry into *every* index on the table. Per the [HOT storage
docs](https://www.postgresql.org/docs/current/storage-hot.html), a HOT update happens when both
hold: (1) none of the columns the update touches are referenced by *any* index (BRIN summarizing
indexes excepted), and (2) there is free space left on the same heap page to fit the new row
version. When both hold, Postgres can also opportunistically prune intermediate dead versions
during ordinary `SELECT` traffic, without waiting for `VACUUM` — HOT-pruning is a second,
continuous, low-cost cleanup mechanism layered on top of periodic vacuum.

This has a direct, sharp implication for the DDL in §13: **the claim query's `UPDATE … SET
status='leased'` is never a HOT update**, because `status` is indexed by the partial ready-set
index — every claim forces a fresh index entry. This is unavoidable (the whole point of the
index is to find rows by status), but it means the *other* columns updated in the same statement
should be minimized and, where possible, moved to columns that are *not* indexed, so at least
those don't independently disqualify HOT elsewhere. E.g., `updated_at` should not be indexed;
`pending_deps` (updated by the decrement query in §14, a different statement entirely from the
claim) should also stay unindexed so *that* update path — which touches potentially many
successor rows per completion — gets HOT treatment and skips index maintenance overhead on the
hottest fan-out operation in the whole system.

`fillfactor` is the lever that makes HOT possible for updates that don't touch an indexed column:
default fillfactor is 100 (pack pages full at insert time), which leaves *zero* room for an
in-place update, forcing the new version onto a different page and defeating HOT immediately.
Lowering it (documented default for B-tree indexes is 90; heap tables default to 100 with no
built-in "recommended" value, so this is an explicit operator choice) reserves slack on every
page at write time in exchange for updates being able to land beside their predecessor:

```sql
ALTER TABLE dagw.nodes SET (fillfactor = 80);   -- 20% slack per page for in-place updates
```

80 is a reasonable starting point for a table expected to receive 2–4 updates per row over its
life (new→ready→leased→success, plus retries); it trades roughly 20% extra table size for a
meaningfully higher HOT-update rate. Verify empirically per deployment via
`pg_stat_user_tables.n_tup_hot_upd / n_tup_upd` — the docs point at exactly this ratio as the
tuning signal.

## 10. Partitioning: by scope, or by time?

Two independent axes are on the table, and they solve different problems:

**Partition by time (`RANGE (created_at)`), Hatchet's approach.** Solves *retention* and *bloat
containment*: old partitions are dropped instantaneously (`DROP TABLE
nodes_2026_08_01` costs nothing proportional to row count, vs. a `DELETE` of a million old rows,
which is a slow, WAL-heavy, bloat-generating operation in its own right) and each partition stays
a bounded, vacuum-friendly size instead of one relation growing forever. This is the correct
answer to "the terminal (`success`/`error`) nodes from finished DAG runs need to go somewhere
that isn't the hot table forever."

**Partition by scope (`LIST` or `HASH (scope)`).** Solves *isolation and pruning by tenant*: a
query already filtered by `scope` (which every claim/lookup query in this design is) only ever
touches one partition, and one scope's write-heavy workload cannot inflate the query-planning
surface (or, with per-partition autovacuum settings, the bloat) of another scope's data. It does
**not** help with the terminal-row-accumulation problem — a long-lived scope just keeps growing
its own partition forever.

**Hatchet's own hard-won caveat, directly transferable**: they specifically flag that
**autovacuum does not run `ANALYZE` on a partitioned table's parent** — only on the leaf
partitions — so the *parent's* planner statistics silently go stale even while every child is
being vacuumed and analyzed correctly. In their numbers, this produced row-count estimation
errors "off by a factor of 6,100,000×" against the parent relation, and queries that should take
single-digit milliseconds took 20ms+ under load purely from bad plans chosen off stale parent
stats. **Mitigation, verbatim from their fix**: run `ANALYZE` on the parent table explicitly
(cron or hooked into the partition-creation routine), don't rely on autovacuum's default
per-relation triggering to cover it. They also used `DETACH PARTITION … CONCURRENTLY` (a
`SHARE UPDATE EXCLUSIVE`-only operation, vs. the `ACCESS EXCLUSIVE` a synchronous detach or
`DROP` briefly needs) specifically to avoid blocking live claim/complete traffic while retiring
old partitions, followed by `DETACH PARTITION … FINALIZE` to close out orphaned detach attempts —
both documented in [PostgreSQL's partitioning
chapter](https://www.postgresql.org/docs/current/ddl-partitioning.html). They deliberately
avoided `pg_partman`/TimescaleDB specifically to stay dependency-free for arbitrary Postgres
hosts (RDS, Cloud SQL, self-hosted) — the same constraint applies to dag-worker-go as a
library that cannot assume operators will install extensions.

**Recommendation for dag-worker-go**: partition `dagw.nodes` and `dagw.events` by time
(`RANGE (created_at)`, e.g. daily or weekly partitions sized so each stays comfortably under
autovacuum's effective working set), and treat `scope` as an ordinary indexed leading column
within each partition rather than a partition key — because unlike Hatchet's tenants, a
dag-worker-go "scope" is not guaranteed to be long-lived or high-cardinality-bounded (scopes are
"created implicitly on use," per the brief, so partition-per-scope risks an unbounded and
unpredictable partition count, which the docs separately warn inflates query-planning time and
per-session memory). Cap active (non-partitioned-off) nodes/events at a size the operator can
tune, and lean on the `(scope, status)` partial index (§6) to make cross-scope noise a non-issue
within a time partition rather than trying to solve it via a second partition dimension.

## 11. `UNLOGGED` tables

An `UNLOGGED` table skips WAL entirely for its own writes — faster, but the docs are unambiguous
about the cost: it is **automatically truncated on crash or unclean shutdown**, its contents are
**never replicated to standbys** (so a failover to a replica finds it empty), and it cannot be
declared for a partitioned table
([`CREATE TABLE` docs](https://www.postgresql.org/docs/current/sql-createtable.html)).

Both River (`river_leader`, used purely for transient leader-election bookkeeping) and Que
(`que_lockers`, tracking which backend PIDs are currently listening) use `UNLOGGED` for exactly
the right reason: the data is process-liveness metadata that is *correct to lose* on crash — a
crashed leader's row disappearing on restart is the desired outcome (someone else can now win
leader election), not data corruption. **The same reasoning applies to dag-worker-go's `leases`
table**: a lease is inherently a soft, revocable grant with a deadline; if Postgres crashes and
restarts, every outstanding lease *should* be treated as gone (the sweeper would have expired
them anyway once their deadlines passed, and a fresh Postgres process has no memory of who was
supposed to be sweeping). Declaring `dagw.leases` `UNLOGGED` removes WAL-write overhead from the
highest-churn insert+delete cycle in the schema (every single claim writes a lease row; every
single completion or timeout deletes one) at the cost of losing in-flight lease state across a
crash — an acceptable trade given leases are advisory grants with their own timeout, not the
system of record for node status (which lives in the WAL-logged `nodes` table and must survive
a crash intact). **Do not** apply `UNLOGGED` to `nodes`, `edges`, or `events` — those are the
actual durability contract the library makes to its host application.

## 12. "Don't use a database as a queue" — and the modern rebuttal

The anti-pattern warning is old and specific: a naive queue-on-a-table implementation (poll with
plain `SELECT … WHERE status='pending' LIMIT 1`, then `UPDATE`, with no locking discipline)
either double-processes rows under concurrency or serializes all consumers behind lock
contention, and — independent of locking correctness — accumulates exactly the MVCC bloat
documented in §8, which is what most "don't do this" folklore is actually reacting to. A widely
cited 2013 writeup and its retrospective 2019 Hacker News discussion
([news.ycombinator.com/item?id=21536698](https://news.ycombinator.com/item?id=21536698)) is
useful precisely because it shows the pattern evolving in public: the original approach (pre-9.5,
using bare advisory locks or plain `FOR UPDATE`, and — critically — **holding a database
connection open for the duration of externally-executed work**, which the discussion identifies
as fundamentally mismatched with Postgres's process-per-connection model, "roughly 1000x fewer
connections than a similarly-sized MySQL instance") gave way, by the time Que matured, to
`SKIP LOCKED` plus a strict rule that **the database connection is only held for the claim/ack
transactions, never across the external work itself** — exactly the design dag-worker-go must
enforce, since the whole point of the library is handing work to *external* workers outside the
process.

**The modern counter-argument**, well-articulated across
[leontrolski.github.io/postgres-as-queue.html](https://leontrolski.github.io/postgres-as-queue.html),
[dagster.io's Postgres-vs-Kafka piece](https://dagster.io/blog/skip-kafka-use-postgres-message-queue),
and reinforced by the field survey in §2 (River, Oban, Hatchet, graphile-worker, pgqueuer, Que
are all *production, currently-maintained* systems built exactly this way): the anti-pattern
warning describes a *naive* implementation, not the primitive itself. With `SKIP LOCKED`,
per-table autovacuum tuning, and partial indexes, teams report running "50–100 queues... with
millions of records" sustaining "millions of units of work an hour on a moderately spec'd
server," getting for free what a separate broker cannot give you: transactional co-commit of a
job's enqueue with the business-data write that created it (no dual-write problem, no outbox
pattern needed), foreign keys and joins against the rest of the application's data, and one
fewer moving part to operate. **The honest synthesis**: Postgres-as-queue is not an anti-pattern
in the abstract; it has a real ceiling (§8's incidents put it somewhere in the low-hundreds of
concurrent claimers and, independently, the low-hundreds-of-millions-of-rows-per-unpartitioned-table
range), and a design that respects that ceiling — `SKIP LOCKED`, partial indexes, tuned
autovacuum, time-based partitioning, no connections held across external work — is a legitimate,
widely-deployed choice, not a mistake waiting to happen. dag-worker-go's job is to build to that
ceiling deliberately, document it, and let the multi-backend design (Redis/Memcached/Postgres)
give operators an escape hatch once a given deployment's DAG size or worker concurrency actually
exceeds it.

## 13. Concrete DDL proposal

```sql
CREATE SCHEMA IF NOT EXISTS dagw;

-- Public status vocabulary is minimal (new/in_progress/success/error, per the brief).
-- Internally we need one more state (`ready`) to distinguish "blocked" from
-- "unblocked but not yet claimed" without a table scan — collapsed to
-- `in_progress`... no: collapsed to the public `new` bucket until claimed, then
-- to `in_progress` once claimed. ready and leased both map to a single internal
-- waypoint the host program never sees directly.
CREATE TYPE dagw.node_status AS ENUM (
    'new',      -- pending_deps > 0: blocked on at least one predecessor
    'ready',    -- pending_deps = 0, no lease outstanding: eligible for claim
    'leased',   -- claimed by a worker, lease outstanding, not yet acked
    'success',
    'error'
);

-- Wide, cold(er) columns: the actual node data.
CREATE TABLE dagw.nodes (
    scope               text        NOT NULL,
    node_id             bigint      GENERATED ALWAYS AS IDENTITY,
    external_key        text        NOT NULL,          -- caller idempotency key
    status              dagw.node_status NOT NULL DEFAULT 'new',
    pending_deps        integer     NOT NULL DEFAULT 0 CHECK (pending_deps >= 0),
    priority            smallint    NOT NULL DEFAULT 0,
    rank                double precision NOT NULL DEFAULT 0,  -- Pearce–Kelly topo priority, see §14.4
    payload             jsonb       NOT NULL DEFAULT '{}'::jsonb,
    default_timeout_ms  integer     NOT NULL DEFAULT 30000 CHECK (default_timeout_ms > 0),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, node_id),
    UNIQUE (scope, external_key)
)
WITH (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_cost_delay = 2)
PARTITION BY RANGE (created_at);

-- One partition per week (adjust to observed volume); parent has no rows of its own.
CREATE TABLE dagw.nodes_2026w34 PARTITION OF dagw.nodes
    FOR VALUES FROM ('2026-08-17') TO ('2026-08-24');

-- Ready-set: the ONLY index the claim query touches; stays tiny regardless of
-- total node count because dead/terminal rows are never entered here (§6).
CREATE INDEX nodes_ready_idx ON dagw.nodes (scope, priority DESC, node_id)
    WHERE status = 'ready';

-- Edges: PK gives O(log n) forward lookups ("what does node X depend on completing
-- before X"); the extra covering index gives O(log n) reverse lookups ("who
-- depends on X finishing") without a second table scan on the hot decrement path.
CREATE TABLE dagw.edges (
    scope       text   NOT NULL,
    from_node   bigint NOT NULL,   -- must complete first
    to_node     bigint NOT NULL,   -- becomes more-ready when from_node succeeds
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, from_node, to_node)
);
CREATE INDEX edges_to_idx ON dagw.edges (scope, to_node) INCLUDE (from_node);

-- Leases: small-by-construction (bounded by concurrency, not DAG size, §7),
-- UNLOGGED because losing in-flight lease state on crash is the correct
-- behavior (§11) — the sweeper's deadline check subsumes it.
CREATE UNLOGGED TABLE dagw.leases (
    scope       text        NOT NULL,
    node_id     bigint      NOT NULL,
    lease_id    uuid        NOT NULL DEFAULT gen_random_uuid(),  -- fencing token, see §14.2
    worker_id   text        NOT NULL,
    instance_id text        NOT NULL,   -- which library process holds it
    leased_at   timestamptz NOT NULL DEFAULT now(),
    deadline    timestamptz NOT NULL,
    PRIMARY KEY (scope, node_id)
);
CREATE INDEX leases_deadline_idx ON dagw.leases (scope, deadline);
CREATE UNIQUE INDEX leases_lease_id_idx ON dagw.leases (lease_id);

-- Events: durable, replayable log backing the reactive stream (§3); partitioned
-- by time for the same bloat/retention reasons as nodes.
CREATE TABLE dagw.events (
    scope       text        NOT NULL,
    event_id    bigint      GENERATED ALWAYS AS IDENTITY,
    node_id     bigint      NOT NULL,
    event_type  text        NOT NULL,   -- 'status_changed' | 'ready_for_pickup'
    from_status dagw.node_status,
    to_status   dagw.node_status,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, event_id)
) PARTITION BY RANGE (created_at);

CREATE TABLE dagw.events_2026w34 PARTITION OF dagw.events
    FOR VALUES FROM ('2026-08-17') TO ('2026-08-24');

-- One trigger does both jobs required by the brief: durable event row + NOTIFY hint.
CREATE OR REPLACE FUNCTION dagw.notify_node_change() RETURNS trigger AS $$
BEGIN
    INSERT INTO dagw.events (scope, node_id, event_type, from_status, to_status)
    VALUES (NEW.scope, NEW.node_id, 'status_changed', OLD.status, NEW.status);

    PERFORM pg_notify(
        'dagw_events',
        NEW.scope || ':' || NEW.node_id || ':' || NEW.status::text
    );  -- payload well under the 8000-byte ceiling (§3); listeners re-read by id
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER nodes_notify_change
    AFTER UPDATE OF status ON dagw.nodes
    FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION dagw.notify_node_change();
```

## 14. The four required queries

### 14.1 Atomically claim up to N ready nodes and issue leases

One round trip, `SKIP LOCKED` on the partial index, lease rows created in the same statement,
and the fencing `lease_id` (§14.2) returned to the caller alongside the payload:

```sql
WITH claimed AS (
    SELECT scope, node_id
    FROM dagw.nodes
    WHERE scope = $1 AND status = 'ready'     -- literal 'ready', see the §6 planner caveat
    ORDER BY priority DESC, node_id
    FOR UPDATE SKIP LOCKED
    LIMIT $2                                   -- N
),
updated AS (
    UPDATE dagw.nodes n
    SET status = 'leased', updated_at = now()
    FROM claimed c
    WHERE n.scope = c.scope AND n.node_id = c.node_id
    RETURNING n.scope, n.node_id, n.payload, n.default_timeout_ms
),
issued AS (
    INSERT INTO dagw.leases (scope, node_id, worker_id, instance_id, deadline)
    SELECT scope, node_id, $3, $4,
           now() + make_interval(
               secs => COALESCE($5, default_timeout_ms) / 1000.0   -- per-claim override, $5 nullable
           )
    FROM updated
    RETURNING scope, node_id, lease_id, deadline
)
SELECT u.node_id, u.payload, i.lease_id, i.deadline
FROM updated u JOIN issued i USING (scope, node_id);
```

### 14.2 Complete a node and decrement successor dependency counters, one round trip

Keyed by `lease_id`, not `node_id` — this is the fencing-token pattern
([Kleppmann, "How to do distributed locking"](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)):
a worker's ack for a lease the sweeper already reaped naturally becomes a zero-row no-op instead
of corrupting state, because the `DELETE … WHERE lease_id = $2` is the single arbitration point —
whichever of {this transaction, a concurrent sweep} deletes the row first wins, and this ordering
(delete the lease row, *then* touch `nodes`) is deliberately the same order the sweeper uses in
§14.3, which is what eliminates the cross-operation deadlock discussed in §15.2:

```sql
WITH acked AS (
    DELETE FROM dagw.leases
    WHERE scope = $1 AND node_id = $2 AND lease_id = $3    -- fencing: stale/reaped lease_id = no-op
    RETURNING node_id
),
completed AS (
    UPDATE dagw.nodes n
    SET status = 'success', updated_at = now()
    FROM acked
    WHERE n.scope = $1 AND n.node_id = acked.node_id AND n.status = 'leased'
    RETURNING n.node_id
),
-- Deterministic lock order (§15.2): lock every successor row, sorted ascending
-- by node_id, BEFORE the decrementing UPDATE touches any of them, so two
-- nodes completing concurrently whose successor sets overlap can never
-- deadlock against each other.
locked_successors AS (
    SELECT n.node_id
    FROM dagw.nodes n
    WHERE n.scope = $1
      AND n.node_id IN (SELECT to_node FROM dagw.edges WHERE scope = $1 AND from_node = $2)
      AND EXISTS (SELECT 1 FROM completed)
    ORDER BY n.node_id
    FOR UPDATE OF n
),
decremented AS (
    UPDATE dagw.nodes n
    SET pending_deps = n.pending_deps - 1, updated_at = now()
    FROM locked_successors ls
    WHERE n.scope = $1 AND n.node_id = ls.node_id
    RETURNING n.node_id, n.pending_deps
)
UPDATE dagw.nodes n
SET status = 'ready', updated_at = now()
FROM decremented d
WHERE n.scope = $1 AND n.node_id = d.node_id
  AND d.pending_deps <= 0 AND n.status = 'new'
RETURNING n.node_id;
```

Why `READ COMMITTED` is enough here (see §15.1 for the general argument): the decrement is a
blind `SET pending_deps = pending_deps - 1`. If two predecessors of the same successor complete
concurrently, the second transaction's `UPDATE` blocks on the row lock the first holds; once the
first commits, Postgres's `EvalPlanQual` mechanism re-evaluates the second transaction's target
list — `pending_deps - 1` — against the *just-committed* row version, not the stale snapshot the
second transaction originally read. The result is a correct, serialized decrement-by-two with no
lost update and no need for `SELECT … FOR UPDATE` immediately followed by an application-side
`pending_deps - 1` computed in Go — the arithmetic can live entirely in SQL and get the
re-evaluation for free. This is the documented behavior distinguishing Read Committed's
row-level re-check from a naive read-then-write race
([transaction isolation docs](https://www.postgresql.org/docs/current/transaction-iso.html)).

### 14.3 Sweep expired leases

Same lease-then-node lock order as §14.2, for the same deadlock-avoidance reason — and because
it deletes by `deadline`, it correctly reaps a lease exactly once even if two sweeper instances
race (the loser's `DELETE` affects zero rows and the loser's subsequent `UPDATE` join is
therefore also a no-op):

```sql
WITH expired AS (
    DELETE FROM dagw.leases
    WHERE scope = $1 AND deadline < now()
    RETURNING node_id, lease_id
)
UPDATE dagw.nodes n
SET status = 'error', updated_at = now()
FROM expired e
WHERE n.scope = $1 AND n.node_id = e.node_id AND n.status = 'leased'
RETURNING n.node_id, e.lease_id;
```

The public status this produces is `error`; the library layer distinguishes "error-with-timeout"
from a worker-reported error either via a separate `error_reason` column (omitted above for
brevity, trivially added) or via the corresponding `dagw.events` row's `event_type`, keeping the
*public* status vocabulary minimal per the brief while the internal event log retains the detail.

### 14.4 Insert a node plus its edges, with cycle rejection

Exact, whole-graph cycle detection on every edge insert is an O(V+E) reachability check — flatly
incompatible with the O(1)/O(log n) goal at 1M nodes if paid on every insert. The mitigation
transferable from the graph-algorithms literature is Pearce & Kelly's incremental topological-order
maintenance ([Pearce & Kelly, "A Dynamic Topological Sort Algorithm for Directed Acyclic
Graphs"](https://whileydave.com/publications/pk07_jea/)): maintain a numeric `rank` per node such
that every existing edge satisfies `rank(from) < rank(to)`; inserting an edge that already
satisfies this ordering is provably not a cycle and needs **no traversal at all** — an O(log n)
index lookup on both endpoints' ranks is sufficient proof. Only when the new edge *violates* the
existing order does a graph search become necessary, and even then Pearce & Kelly's algorithm
localizes the work to just the region of the graph between the two endpoints' current ranks
rather than the whole graph — their own benchmarking is the basis for the algorithm's adoption in
Abseil, TensorFlow, and JGraphT for exactly this reason.

```sql
CREATE OR REPLACE FUNCTION dagw.add_node_edges(
    p_scope text, p_from bigint, p_to bigint
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_from_rank double precision;
    v_to_rank   double precision;
    v_would_cycle boolean;
BEGIN
    SELECT rank INTO v_from_rank FROM dagw.nodes WHERE scope = p_scope AND node_id = p_from FOR UPDATE;
    SELECT rank INTO v_to_rank   FROM dagw.nodes WHERE scope = p_scope AND node_id = p_to   FOR UPDATE;

    IF v_from_rank < v_to_rank THEN
        -- Fast path: edge already respects topological order. O(log n), no traversal.
        INSERT INTO dagw.edges (scope, from_node, to_node) VALUES (p_scope, p_from, p_to);
        UPDATE dagw.nodes SET pending_deps = pending_deps + 1
            WHERE scope = p_scope AND node_id = p_to;
        RETURN;
    END IF;

    -- Slow path: order violated. Bounded, deduplicated reachability search —
    -- true cost is proportional to the affected region, not the whole graph,
    -- for the sparse, roughly-causally-ordered DAGs this library targets.
    -- SET LOCAL statement_timeout upstream of this call to fail safe (reject
    -- the edge) rather than risk an unbounded scan on a pathological graph.
    WITH RECURSIVE reach(node_id) AS (
        SELECT p_to
        UNION  -- UNION, not UNION ALL: dedupes visited nodes, guarantees termination
        SELECT e.to_node
        FROM dagw.edges e JOIN reach r ON e.scope = p_scope AND e.from_node = r.node_id
    )
    SELECT EXISTS (SELECT 1 FROM reach WHERE node_id = p_from) INTO v_would_cycle;

    IF v_would_cycle THEN
        RAISE EXCEPTION 'dagw: edge % -> % would close a cycle in scope %', p_from, p_to, p_scope
            USING ERRCODE = '23514';
    END IF;

    -- Legal edge, but out of rank order: bump p_to past p_from. A full
    -- implementation applies Pearce–Kelly's localized re-numbering of the
    -- affected region here rather than this single-node approximation.
    UPDATE dagw.nodes SET rank = v_from_rank + 1 WHERE scope = p_scope AND node_id = p_to;
    INSERT INTO dagw.edges (scope, from_node, to_node) VALUES (p_scope, p_from, p_to);
    UPDATE dagw.nodes SET pending_deps = pending_deps + 1
        WHERE scope = p_scope AND node_id = p_to;
END;
$$;
```

Honest limitation: the single-node rank bump in the fallback branch is a simplification: it is
sufficient to keep the fast path's invariant (`rank(from) < rank(to)` for direct edges) working
for the edge just inserted, but a real implementation should apply Pearce & Kelly's full
localized-region renumbering so that *transitively* affected nodes' ranks stay consistent too —
otherwise the fast path's hit rate degrades over many out-of-order insertions and more edges fall
through to the expensive traversal than necessary. This is flagged again in Open Questions.

## 15. Isolation level and deadlock avoidance

### 15.1 `READ COMMITTED` is enough — with the right locking

Per the [isolation docs](https://www.postgresql.org/docs/current/transaction-iso.html), Read
Committed's defining weakness is that a bare `UPDATE … WHERE <condition on other rows>` can act
on a stale view of those other rows. Every one of the four queries in §14 avoids this not by
raising the isolation level but by making every cross-row dependency an explicit row lock (`FOR
UPDATE`) or an in-place arithmetic update (`SET x = x - 1`) that Postgres's `EvalPlanQual`
re-evaluates correctly against concurrent commits, as detailed in §14.2. `SERIALIZABLE`'s
predicate-lock machinery (`SIReadLock`, visible in `pg_locks`) is real insurance against a
different class of bug — multi-statement, multi-row **read-then-decide-then-write** logic where
the "decide" step spans rows no explicit lock protects, i.e. **write skew**. The one place that
genuinely appears in this design is a **DAG-completion check**: "read the count of non-terminal
nodes in scope X; if zero, mark the DAG complete and emit a final event." Two workers finishing
the last two nodes concurrently can each observe a self-consistent-but-stale "one node still
pending" (the other worker's, not yet committed) and both correctly conclude "not done yet,"
after which neither ever runs the completion logic — a genuine lost update, and the canonical
write-skew shape the isolation docs' own worked example (`SUM` from two categories, insert
cross-category) illustrates.

**Recommendation: don't pay `SERIALIZABLE`'s SSI overhead on the entire hot claim/complete path
to fix one rare check.** Scope the fix narrowly: wrap only the completion-check transaction in
`pg_advisory_xact_lock(hashtext('dagw.dag-complete')::bigint, hashtext(scope)::bigint)` (§5) so
at most one worker per scope evaluates "are we done" at a time, serializing exactly the operation
that needs it and leaving the claim/complete/sweep hot path at Read Committed with row locks,
which is both simpler to reason about and avoids `SERIALIZABLE`'s documented cost: transactions
can abort at commit time with `40001` (serialization failure) purely from *read* conflicts that
never touch the same row, which is a worse tail latency profile for a tight claim loop than
`SKIP LOCKED`'s bounded, never-blocking behavior.

### 15.2 Deadlock avoidance via deterministic lock ordering

Per the [deadlock docs](https://www.postgresql.org/docs/current/explicit-locking.html), Postgres
detects deadlocks automatically and aborts one participant, but "exactly which transaction …
is difficult to predict" — the fix is architectural, not reactive: "the best defense … is
generally to avoid them by being certain that all applications … acquire locks on multiple
objects in a consistent order." Two concrete deadlock shapes exist in this schema, and both are
closed by ordering decisions already baked into §14, worth stating explicitly as design rules
rather than accidents:

1. **Fan-in on shared successors** (two predecessors of the same downstream node completing
   concurrently): closed by locking every affected successor row in ascending `node_id` order
   *before* any of them is written (§14.2's `locked_successors` CTE) — if transaction A's
   successor set is `{5, 9}` and transaction B's is `{9, 5}` in whatever order an unordered index
   scan happened to return them, sorting both to `(5, 9)` before locking means both transactions
   always attempt row 5 before row 9, so the second to arrive simply waits for the first to
   finish rather than each holding one and waiting on the other.
2. **Complete vs. sweep racing the same node** (a worker's ack for a node arriving at nearly the
   same instant the sweeper decides that node's lease expired): closed by making *both*
   operations touch `leases` before `nodes` (delete-then-update, in that order, in both §14.2 and
   §14.3) — reversing the order in only one of the two operations (a natural-looking but wrong
   first draft locks the node, checks its status, *then* deletes the lease) is precisely the
   textbook two-resources-opposite-order deadlock shape the docs' own bank-transfer example
   illustrates. The fencing-token `DELETE … WHERE lease_id = $x` additionally makes this race
   *correct*, not just deadlock-free: exactly one of {complete, sweep} succeeds in deleting the
   row, and the other's subsequent join naturally becomes a no-op.

As a backstop — not a substitute — for cases neither rule anticipates, the host application layer
should catch SQLSTATE `40P01` (deadlock_detected) around every write transaction and retry with
jittered backoff; the docs frame consistent ordering and retry-on-deadlock as complementary, not
either/or.

## 16. Quantification

### 16.1 Storage and index size at 1,000,000 nodes

Grounded in the documented physical layout constants
([page layout docs](https://www.postgresql.org/docs/current/storage-page-layout.html)): an 8192-byte
page, a 24-byte page header, a 4-byte line pointer per row, and a 23-byte `HeapTupleHeaderData`
per tuple version, rounded up to the platform's `MAXALIGN` (8 bytes on all common 64-bit
targets) — so **every row's fixed overhead is 24 + 4 + 24 = ~52 bytes before a single user
column is counted**, plus whatever a `jsonb` payload adds.

| Table | Row shape | Est. bytes/row (incl. overhead) | 1M-row heap size (est.) |
|---|---|---|---|
| `dagw.nodes` | fixed cols (~70B) + small `jsonb` payload (~150B typical) + ~52B overhead | ~270B | ~260–300 MB |
| `dagw.edges` (avg fan-out 3) | 3 bigints + overhead, 3M rows | ~90B/row × 3M | ~250–300 MB |
| `dagw.leases` | bounded by concurrency (§7), not node count | n/a | tens of KB–few MB at any instant |
| `dagw.events` (avg 3 transitions/node) | small fixed row, 3M rows, partitioned+droppable | ~80B/row × 3M | ~230–270 MB *before* old partitions are dropped |

Index sizes: the well-known empirical anchor is `pgbench`'s default-scale primary key index on
`pgbench_accounts` — a single `bigint` primary key over 1,000,000 rows lands around 21–22MB on
disk at the default 90% B-tree fillfactor. Extrapolating with the same per-entry overhead model:

| Index | Key width | Est. size @ 1M rows |
|---|---|---|
| `nodes` PK `(scope, node_id)` | ~16B key | ~25–30 MB |
| `nodes_ready_idx` (partial, `WHERE status='ready'`) | ~20B key, but **only live rows** | **hundreds of KB–low single-digit MB**, not proportional to 1M — this is the entire point of §6 |
| `edges` PK `(scope, from_node, to_node)`, 3M rows | ~24B key | ~90–110 MB |
| `edges_to_idx` (covering, 3M rows) | ~24B key + 8B included | ~110–140 MB |

**Total durable footprint at 1M nodes, steady state, before autovacuum/bloat slack**: roughly
1–1.3 GB — small by modern server standards. The operative risk was never raw size; it is the
**bloat multiplier** documented in §8, where the same nominal dataset was observed at 10–100×
its live size under sustained churn without adequate autovacuum tuning. Budget disk headroom
against the bloated case, not the steady-state case above.

### 16.2 Throughput

No single authoritative number exists — it is workload- and hardware-dependent — but the sources
above triangulate a usable envelope:

- **Sustainable, healthy zone**: the richyen.com piece's rule of thumb of "under ~100 concurrent
  worker connections" for `SKIP LOCKED` to remain comfortably viable is corroborated by the
  general shape of the PlanetScale incident, where trouble began under sustained *high*
  concurrency and long-running competing transactions, not under this library's expected
  claim/complete traffic pattern in isolation.
- **Stress/failure zone**: PlanetScale's reproduction pinpoints **800 jobs/sec sustained** as the
  load at which their setup entered the bloat death-spiral (155K backlog, 383K dead tuples,
  300ms+ lock latency) — a concrete, citable ceiling for *that* hardware/schema/autovacuum
  configuration, not a universal constant, but a realistic order-of-magnitude anchor: comfortably
  north of 100/sec is achievable; sustaining 800+/sec indefinitely requires the mitigations in
  §8.1 and §10 to already be in place, not bolted on after the fact.
- **What breaks first, in order of appearance under increasing load**: (1) HOT-update rate drops
  as fillfactor slack fills, forcing full index maintenance on every claim/complete — a
  *gradual* latency increase, not a cliff; (2) partial-index-scan latency on `nodes_ready_idx`
  rises as dead-tuple entries accumulate between vacuum passes — the PlanetScale mechanism,
  visible as rising `FOR UPDATE SKIP LOCKED` p99 latency; (3) MultiXact/LWLock contention at high
  concurrent-locker counts (richyen's mechanism) — visible as CPU pegged with cheap-looking
  queries and high `pg_stat_activity` wait counts on lock manager waits, not I/O waits; (4) at
  the table-size ceiling independent of churn rate, unpartitioned single-relation bloat
  (Hatchet's ~200M-row wall) — visible as a step-function planner behavior change once parent
  statistics go stale (§10) compounding whatever bloat already existed. The design in §13
  (partial ready-set index, per-table autovacuum tuning, time partitioning, thin lease table)
  directly targets stages 1–3 and defers stage 4 well past 1M rows by keeping partitions bounded.

## Recommendations for dag-worker-go

1. **Adopt the CTE-chained `SKIP LOCKED` claim pattern in §14.1 verbatim as the Postgres backend's
   claim implementation** — it is the pattern every surveyed production system converges on
   independently, and doing it in one round trip inside one transaction is what makes the claim
   atomic without an explicit application-level lock.
2. **Split the schema the way Hatchet does**: keep `nodes` narrow enough that the partial
   ready-set index (§6, §13) stays index-only-scan-friendly, and never let the claim query's hot
   `UPDATE` touch more indexed columns than `status` — every additional indexed column on that
   `UPDATE` is one more index entry rewritten per claim and one more thing standing between the
   update and a HOT update (§9).
3. **Key every ack by `lease_id`, never by `node_id` alone** (§14.2) — this is the single highest
   -leverage correctness fix in this whole dossier: it makes complete-vs-sweep races safe by
   construction instead of requiring careful timing analysis, and it is nearly free to implement.
4. **Make `pg_notify` a pure latency optimization over the durable `dagw.events` table, never the
   source of truth** (§3) — every subscriber resumes from a last-seen `event_id`, and a
   disconnected/reconnecting subscriber replays from the events table rather than trusting it
   received every notification. Treat `pg_notification_queue_usage() > 0.5` as a page-worthy
   alert in the library's operational docs.
5. **Ship per-table autovacuum tuning and `fillfactor=80` in the migration DDL itself**, not as
   an optional operator tuning guide — the PlanetScale and richyen incidents both stem from
   defaults tuned for slowly-changing tables being silently applied to a queue table; don't make
   every dag-worker-go operator rediscover this independently.
6. **Partition `nodes` and `events` by time from day one**, even before 1M rows, because
   retrofitting partitioning onto a live, already-bloated table is strictly harder than starting
   partitioned (Hatchet's own post is implicitly a story about not having done this from the
   start). Do not partition by `scope`; scope cardinality is unbounded and caller-controlled per
   the brief, which is exactly the shape the partitioning docs warn against.
7. **Declare `leases` `UNLOGGED`** (§11, §13) — it is a pure latency win on the highest-churn
   insert/delete cycle in the schema, with a loss-on-crash characteristic that is actually
   *correct* behavior for a revocable, deadline-bound grant.
8. **Do not implement exact whole-graph cycle detection on every edge insert.** Ship the
   rank-based fast path (§14.4) with a hard `statement_timeout` around the fallback traversal,
   fail closed (reject the edge) on timeout, and treat the localized Pearce-Kelly renumbering as
   a v2 refinement once the simpler single-node rank bump is observed (via the fast-path hit
   rate) to degrade in practice.
9. **Scope `SERIALIZABLE` (or its advisory-lock substitute) to exactly the DAG-completion check**,
   nowhere else (§15.1) — the claim/complete/sweep hot path should stay on Read Committed with
   explicit row locks, which this dossier has shown is sufficient and lower-latency-tail than SSI.
10. **Enforce the lease-then-node lock order in both `complete` and `sweep`**, and the
    ascending-`node_id` lock order in the successor-decrement fan-out, as code-reviewed invariants
    (comment the ordering requirement at both call sites) rather than tribal knowledge — this is
    the cheapest possible deadlock-avoidance mechanism and it only works if both operations are
    kept in sync as the code evolves.

## Open questions

- **Rank maintenance for the fast cycle-check path (§14.4) is a full research topic on its own**:
  the single-node rank bump shown is a correctness-preserving simplification, not the full
  Pearce-Kelly localized-region renumbering; whether the simplification's fast-path hit rate
  degrades unacceptably over a long-lived, heavily-mutated DAG (many out-of-causal-order edge
  insertions) needs empirical measurement against dag-worker-go's actual expected edge-insertion
  patterns before committing to it long-term.
- **Where exactly does the "hundred concurrent claimers" ceiling from §16.2 actually sit for this
  specific schema and query shape?** The cited numbers come from other systems' schemas and
  hardware; a dag-worker-go-specific benchmark at the 1M-node target (already mandated by the
  brief) is the only way to replace this with a number worth publishing.
- **Should the timeout sweeper be a single global process per storage cluster, or one
  advisory-lock-elected leader per scope (§7, §5)?** The DDL supports either; the choice
  interacts with the still-open multi-instance work-distribution question (partition-per-scope vs.
  consistent hashing vs. pure pull) covered in the companion research doc, and should be settled
  jointly with that decision rather than independently here.
- **Logical decoding (§4) is deliberately scoped out of the library's own responsibilities** —
  is that the right line, or should the library optionally manage a wal2json-based mirror of
  `dagw.events` for host applications that already run Debezium/Kafka Connect elsewhere? The
  slot-retention foot-gun argues against it as a default, but it may be worth a supported
  "advanced" mode with the library actively monitoring and auto-dropping its own slot on
  detected consumer staleness.
- **Does `events` partitioning by time conflict with a subscriber's need to resume from an
  arbitrary historical `event_id` after a long disconnect?** If old partitions get dropped for
  retention before a slow subscriber catches up, that subscriber needs an explicit "you're too
  far behind, resync from a full snapshot of current node state" path — this needs to be a
  first-class API, not an edge case discovered in production.
- **Fillfactor and per-table autovacuum numbers in §13 (80, 0.02, cost_delay=2) are reasoned
  defaults, not benchmarked ones** — they need validation against the mandated 1M-node benchmark
  suite before being presented to library users as tuned recommendations rather than starting
  points.
