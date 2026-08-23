# 10 — Event Bus and Delivery Semantics

Research dossier for **dag-worker-go**: the event/reactive layer that lets a host program
subscribe to (a) every node status transition and (b) "this node is ready, come take it,"
across a pluggable, multi-instance storage layer (in-memory, Redis, Memcached, PostgreSQL).

---

## 1. Vocabulary and scope

Two structurally different signals flow out of the engine, and the whole design falls out of
treating them differently instead of forcing one event bus to serve both:

| Signal | Cardinality | Loss tolerance | Consumer contract |
|---|---|---|---|
| **`NodeStatusChanged`** | One per transition, ever | Should not be silently lost (it's the audit trail) | "Tell me what happened" — read-only, fan-out to N subscribers |
| **`NodeReady`** ("take this node") | One hint per node-becomes-ready event, but is inherently re-derivable | Fine to drop, coalesce, or duplicate | "Wake me up so I go pull" — at most one worker actually gets the node |

The second column is the crux of this dossier: a status-change feed is an *observation* stream
(subscribers are readers of history), while a ready signal is a *doorbell* for a competing-consumers
pull (subscribers are racers for a lease). Conflating them — e.g., trying to deliver "take this
node" as a guaranteed, ordered, at-least-once push — reproduces the exact anti-pattern that the
Reactive Streams effort was created to eliminate (§9).

---

## 2. Delivery semantics: at-most-once, at-least-once, "effectively-once"

### 2.1 The three semantics, precisely

- **At-most-once**: a message is delivered zero or one times. Achieved by *not retrying* — the
  producer or transport gives up on failure rather than risk a duplicate. Cheapest, lossy.
- **At-least-once**: a message is delivered one or more times. Achieved by retrying until an ack
  is observed; duplicates are the accepted cost. AWS SQS is the textbook instance: a consumer's
  `ReceiveMessage` starts a *visibility timeout*, and "if the consumer fails to delete the message
  before the timeout expires, the message becomes visible again and can be received by another
  consumer" — and AWS is explicit that standard queues "[don't] guarantee that a message won't be
  delivered more than once within the visibility timeout period" even absent any failure —
  [AWS SQS visibility timeout docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html).
- **Exactly-once / "effectively-once"**: every message has exactly one observable effect. This is
  not a stronger version of at-least-once obtained by trying harder — it requires a *different
  architecture element*: an idempotency boundary.

### 2.2 Why true exactly-once delivery is impossible, and why that's the wrong question

The impossibility is old and specific to the **network**, not to competence: Akkoyunlu, Ekanadham,
and Huber proved in 1975 ("Some Constraints and Tradeoffs in the Design of Network
Communications") that two parties communicating over a channel that can lose messages can never
both become *certain* the other received a given message, no matter how many confirmation
messages they exchange — every ack needs an ack, forever. Jim Gray named this the **Two Generals
Problem** in 1978 (*Notes on Data Base Operating Systems*) — see the summary in
[Akkoyunlu et al. via Grokipedia](https://grokipedia.com/page/Two_Generals'_Problem). A companion
result at a different layer, Fischer–Lynch–Paterson (1985), shows no deterministic algorithm can
guarantee consensus (agreement + termination) in a fully asynchronous system with even one
crash-prone process — [FLP impossibility summary, INRIA](https://team.inria.fr/antique/the-impossibility-result-of-fischer-lynch-and-paterson-and-its-proof/).
Neither result says "you cannot build reliable systems"; both say "you cannot build a *sender*
that unilaterally *knows* delivery happened exactly once by watching the wire." The fix is to move
the guarantee off the wire and onto **idempotent state at the receiver**: let the network duplicate
freely, and make duplicate application a no-op.

### 2.3 The Kafka counterexample and why it doesn't overturn the impossibility result

Kafka is routinely cited as delivering "exactly-once semantics" (EOS), and it's worth being precise
about what that claim actually covers, because it's the strongest real system in this space.
Kafka layers two mechanisms:

1. **Idempotent producer** — the broker assigns each producer a `PID` and each record a sequence
   number; retried sends of the same batch are deduplicated by the broker "based on the tuple
   (PID, epoch, sequence number)" — [Conduktor: exactly-once semantics](https://www.conduktor.io/glossary/exactly-once-semantics-in-kafka).
   Since Kafka 3.0 this is on by default (`enable.idempotence=true`, `acks=all`).
2. **Transactions** — a producer can atomically write to multiple partitions *and* commit consumer
   offsets in the same transaction, so a consume-transform-produce loop either fully commits or
   fully rolls back, observable to downstream consumers only under `isolation.level=read_committed`
   — [Strimzi: exactly-once semantics with Kafka transactions](https://strimzi.io/blog/2023/05/03/kafka-transactions/).

This is real, but it is **exactly-once *inside the log*** — atomic, deduplicated writes to Kafka
itself. It does not, and cannot, extend to an arbitrary external side effect a consumer performs
after reading a record (calling a payment API, writing to a different database, dispatching to an
external worker process) unless that side effect is itself transactional against the same
offset-commit, which is precisely the **transactional outbox** trick turned inside-out (§5.3).
Tyler Treat's widely cited argument makes the general case: "you cannot have exactly-once delivery"
between two independent systems without idempotent receivers, full stop —
[Brave New Geek, "You Cannot Have Exactly-Once Delivery"](https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/).
Kafka's EOS is the strongest evidence *for* this framing, not against it: it works by shrinking the
problem to "exactly-once within one transactionally-capable system," which is exactly the escape
hatch the impossibility proofs leave open.

**Consequence for dag-worker-go**: the worker's ack (`success`/`error`) crossing the
library↔external-worker boundary is a message over an unreliable channel by construction (it's a
network call, a process boundary, or at minimum a goroutine handoff with the possibility of
retries after apparent timeout). It must be treated as **at-least-once**, and `Ack`/`Nack` **must
be idempotent** at the storage layer: acking an already-acked node lease is a no-op, keyed on the
lease's fencing token, not an error that corrupts state. The same is true in reverse — if the
library redelivers a `NodeReady` doorbell after a spurious disconnect, that's fine *precisely
because* the actual reservation happens through an idempotent, single-winner `Reserve` call
(§9), not through the doorbell itself carrying state.

---

## 3. Ordering: total, per-node, and causal

### 3.1 The theory

Lamport's 1978 "happened-before" relation (→) is a **partial** order: `a → b` when `a` and `b` are
on the same process and `a` precedes `b`, or `a` is a message send and `b` its matching receive
(transitively closed); events that are neither ordered are **concurrent**, and Lamport clocks alone
cannot tell you their real-time order — [Lamport clocks / happened-before, CMU 15-712 lecture notes](https://www.cs.cmu.edu/~dga/15-712/S11/lectures/04-clocks.pdf).
A **total** order is always obtainable from a partial one by adding a deterministic tie-break
(process ID is the classic choice), but the total order it produces is an *artifact of the
tie-break*, not a discovery of real causality between concurrent events — see the discussion in
[HackerNoon: How Logical Clocks Keep Distributed Systems in Sync](https://hackernoon.com/how-logical-clocks-keep-distributed-systems-in-sync).
**Causal** ordering is the useful middle ground: preserve order only where a real happens-before
edge exists; leave concurrent events unordered (cheaper, and doesn't lie about causality that
isn't there).

### 3.2 What Kafka actually promises, and why it's the right cost model to copy

Kafka's real guarantee is deliberately narrower than "ordered": *"Kafka guarantees message ordering
only within a single partition"* — cross-partition (and cross-topic) there is no ordering promise
at all, and getting a genuine total order across partitions requires collapsing to one partition
and eating the throughput loss —
[Kafka ordering guarantees](https://pulse.support/kb/kafka-ordering-guarantees),
[per-partition ordering mechanics](https://medium.com/@sohail_saifii/the-kafka-partition-strategy-that-guarantees-message-ordering-3abe46dc6837).
This is the standard, load-bearing move in every high-throughput log: **shard the ordering domain
down to the smallest unit that actually needs it**, and let everything outside that unit be
concurrent by definition.

### 3.3 What dag-worker-go actually needs ordered — and what it does not

Walk through what a subscriber can legitimately ask of node-status ordering:

- **Within one node's lifecycle** (`new → in-progress → success`), transitions must be strictly
  ordered — a subscriber must never observe `success` before `in-progress` for the same node.
  This is the *only* ordering domain that matters operationally.
- **Across two unrelated nodes**, there is no meaningful ordering question — they may transition
  in either order or concurrently, and no correct scheduler decision depends on knowing which came
  "first" in wall-clock time.
- **Across two nodes joined by a DAG edge** (a dependency), the *causal* relationship — child
  cannot become `ready` before all its parents are `success` — is enforced by the **DAG structure
  itself** at the storage layer (the engine only ever marks a node ready after checking parent
  states in the same read/transaction that flips the state), not by the ordering of the event
  stream. This is the key simplification: **the event bus does not need to encode graph causality
  at all**, because graph causality is a property of the persisted state, independently verifiable
  by any subscriber via a plain read. The event is a *notification that a read would now return
  something new*, not the *only* record of what happened (§4).

**Recommendation**: per-node monotonic sequence numbers, exactly the Kafka partition-offset /
Redis Stream-ID pattern applied at node granularity instead of topic/stream granularity. Each node
gets its own strictly increasing `Seq` (`uint64`), assigned by the storage backend at write time
under the same lock/transaction that performs the state transition. This is:

- **Cheap** — no cross-node coordination, no distributed clock, no vector clock bookkeeping.
- **Sufficient** — it gives every subscriber exactly the one ordering guarantee that's load-bearing
  (this node's own history), and lets a subscriber detect and drop stale/duplicate/reordered
  delivery of the *same* node's events trivially (`if e.Seq <= lastSeenSeq[e.NodeID] { drop }`).
- **A ready-made resume cursor** — `(NodeID, Seq)` (or, for a scope-wide feed, `(ScopeSeq)` — see
  §6) is exactly the shape of a Kafka offset or a Redis Stream ID, and composes cleanly with the
  cursor/resume-token design in §6.

A single global total order (one incrementing counter for the whole scope, à la a single-partition
Kafka topic) is offered as an *opt-in* mode for subscribers who want "one merged timeline to render
in a UI," but it is explicitly not the default because it forces all writers in the scope through
one serialization point — the same throughput/parallelism trade Kafka documents for its
single-partition case.

---

## 4. Event sourcing (log as truth) vs. state-plus-notification

### 4.1 Event Sourcing, per Fowler

Martin Fowler's canonical description: *"Capture all changes to an application state as a sequence
of events,"* such that *"all changes to the domain objects are initiated by the event objects,"*
and the acid test is that you can *"discard the application state completely and rebuild it by
re-running the events from the event log on an empty application"* —
[Fowler, Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html). Temporal's workflow
engine is the modern, hardened production instance of this idea: *"Event History is an append-only
log of Events that is durably persisted by the Temporal service,"* and a workflow is required to be
**deterministic** — *"every execution of its Workflow Definition produces the same Commands in the
same sequence given the same input"* — so that replaying the history after a crash reproduces
identical decisions —
[Temporal: Events and Event History](https://docs.temporal.io/workflow-execution/event). Temporal's
replay model is instructive for exactly the failure mode dag-worker-go must avoid: it works
*because* Temporal's own SDK intercepts and short-circuits every side-effecting call during replay
(activities are not re-invoked; their previously recorded results are fed back from history
instead).

### 4.2 The replay hazard, and why it rules out pure event sourcing for the *dispatch* side

Fowler flags the exact trap directly: *"if these events cause update messages to be sent to
external systems, then things will go wrong because those external systems don't know the
difference between real processing and replays,"* and recommends wrapping such interactions in a
**gateway** that can distinguish live dispatch from replay —
[Fowler, Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html). dag-worker-go's
entire purpose is to send "go do this" to external workers that live *outside the library* and
have no concept of "this is a replay." A naive event-sourced design — "rebuild state by replaying
the event log, and the replay mechanically re-emits `NodeReady`" — would re-dispatch every
in-flight or completed node to real workers on every recovery, restart, or backfill. Temporal
avoids this only because it built the gateway Fowler describes directly into its SDK (activity
result caching keyed by history position); building and maintaining the equivalent machinery is a
large, ongoing surface area for a library whose stated goal is a minimal public surface.

### 4.3 Recommendation: state-plus-notification, not event-sourced-log-as-truth

dag-worker-go should treat **persisted node state** (in the storage backend's node table/hash/row)
as the single source of truth, and the event stream as a **best-effort, derivative notification**
of transitions to that state — not the mechanism by which state is reconstructed. Concretely:

- The authoritative write path is: transition the node's row/hash/key (with its `Seq` bump) inside
  one atomic operation (a DB transaction, a Lua script, an in-memory mutex-guarded map write).
  This is the fact.
- The event is emitted as a side effect of that write succeeding, carrying the resulting `Seq` and
  new status, but its delivery to any given subscriber is decoupled from that fact's durability —
  if every subscriber that would have received `NodeReady` vanishes and reconnects a minute later,
  the correct recovery path is **a plain read of "which nodes are `new` with satisfied
  dependencies,"** not "replay the log from the beginning." This directly matches the pull side of
  §9: `Reserve` always re-derives eligibility from current state, never from accumulated event
  history, so a lost or duplicated `NodeReady` doorbell is never a correctness problem, only a
  latency one.
- Full event-log retention (keep every transition forever, replayable) is offered as an **optional
  add-on for audit/observability** (a subscriber can materialize its own history table by
  consuming the feed durably), but the library's own liveness and correctness never depend on that
  log surviving or being complete. This sidesteps the replay-safety burden Fowler and Temporal both
  have to solve, because the log is never the thing state gets rebuilt *from* for dispatch purposes.

---

## 5. The read-your-writes problem

### 5.1 The problem, precisely

Werner Vogels' definition of the property: *"process A, after it has updated a data item, always
accesses the updated value and will never see an older value"* — a special case of causal
consistency — [Vogels, "Eventually Consistent," ACM Queue 2008](https://queue.acm.org/detail.cfm?id=1466448).
Inverted onto an event bus, the failure mode is: a subscriber receives a `NodeStatusChanged` event
saying a node is now `success`, immediately issues a read against storage to fetch the node's
output, and — because the event was emitted from application memory *before* (or concurrently
with) the storage write actually committing/replicating — gets back stale data, a not-found, or
(worse, on a replica) a state that hasn't caught up yet. This is the general **dual-write problem**:
whenever "update the record" and "notify about the update" are two separate operations, there is a
window where they're observably out of sync, and no amount of retrying either operation alone
fixes it — [microservices.io: Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html).

### 5.2 The three fixes

1. **Emit after commit.** Never publish the notification until the underlying write is durably
   committed (and, for replicated backends, until it's visible on whatever the subscriber will
   read from). This is necessary but not sufficient on its own — it removes the "event before
   write" ordering bug, but does nothing about a subscriber reading from a stale replica.
2. **Carry the version in the event.** The event includes the `Seq` (§3.3) of the write it
   describes. A subscriber that reads storage afterward can compare `readSeq >= event.Seq` and,
   if the read comes back stale (`readSeq < event.Seq`), retry the read (with backoff) instead of
   silently acting on old data. This turns an invisible correctness bug into a detectable,
   retryable condition — cheap to implement, cheap to carry (8 bytes), and it composes with any
   storage backend including eventually-consistent replicas.
3. **The event carries the full new state.** The event itself embeds the new status (and, if the
   design extends to carrying node output/error payloads later) so the subscriber never needs to
   read storage at all to react correctly. This fully eliminates the read-after-event race but
   couples event size to state size and forces every subscriber to receive the full payload even
   if it only wanted the status bit.

**Recommendation**: (1) is mandatory and non-negotiable — never publish before commit. (2) is the
default and is nearly free (`Seq` is already carried for ordering, §3.3, so it does double duty as
a staleness token). (3) is offered as a **filter option at subscribe time** (`WithPayload(true)`)
for subscribers who explicitly want to avoid the read-back round trip and are willing to pay for
larger events — this matters more for the `NodeReady` doorbell than for the audit stream, since a
worker that gets `NodeReady` still has to call `Reserve` (§9) which re-reads current state as part
of the atomic reservation anyway, making fix (2) sufficient there by construction.

### 5.3 Per-backend mechanics for "emit after commit"

| Backend | How atomicity of write+notify is achieved | Failure mode if done wrong |
|---|---|---|
| **In-memory** | Single mutex-guarded critical section: mutate map, bump `Seq`, then push to subscriber channels, all before releasing the lock. Trivial — same process, same memory. | None if lock discipline is followed; a bug here is a straightforward data race, not a distributed one. |
| **PostgreSQL** | Single transaction does `UPDATE nodes SET status=..., seq=seq+1 ...` **and** either `SELECT pg_notify(...)` (fires only on commit — Postgres queues NOTIFY payloads for delivery *after* the issuing transaction commits, never before) or an `INSERT INTO node_events (...)` outbox row that a relay polls. See §5.4 for the SQL. | Publishing via a *separate* connection/transaction after the fact reopens the dual-write gap; a crash between the two writes loses the notification (state is right, event never fires) — acceptable only if pull-based `Reserve` (§9) is the source of truth and the event is purely a latency hint. |
| **Redis** | A single Lua script (atomic under Redis's single-threaded execution model) does the `HSET`/state mutation **and** `XADD` to the notification stream in one round trip — no other client can observe the state change without the stream entry also existing, or vice versa. See §5.4 for the script. | Doing the `HSET` and `XADD` as two separate commands (even back-to-back) admits a window where a second client's `XREAD`-triggered wakeup races a `HGET` that hasn't seen the write, or (worse) a crash between the two commands. |
| **Memcached** | No transactions, no scripting, no atomic multi-key ops beyond single-key CAS. Best achievable: CAS-write the state, then best-effort publish via whatever the bus uses for Memcached (§8.2 — polling, since Memcached has no pub/sub primitive at all). There is **no way to make these atomic** on this backend. | This is a genuine, documented gap for the Memcached backend: the notification is *always* a best-effort, eventually-consistent hint there, never a commit-coupled signal. Any subscriber relying on Memcached-backed events for correctness (rather than as a latency optimization on top of polling `Reserve`) is misusing the API — this must be called out loudly in the backend's doc comment, not quietly shipped as if it matched the Postgres/Redis guarantee. |

### 5.4 Concrete atomic write+notify per backend

**PostgreSQL — transactional outbox / direct NOTIFY:**

```sql
-- Option A: direct NOTIFY (fires only after COMMIT; payload capped at 8000 bytes —
-- https://www.postgresql.org/docs/current/sql-notify.html)
BEGIN;
UPDATE dag_nodes
   SET status = 'success', seq = seq + 1, updated_at = now()
 WHERE scope_id = $1 AND node_id = $2 AND status = 'in_progress'
 RETURNING seq;
SELECT pg_notify('dag_worker_' || $1, json_build_object(
  'node_id', $2, 'status', 'success', 'seq', seq)::text);
COMMIT;

-- Option B: outbox table + relay poll (unbounded payload, durable, survives a relay
-- crash between COMMIT and delivery — matches microservices.io's Transactional Outbox:
-- https://microservices.io/patterns/data/transactional-outbox.html)
BEGIN;
UPDATE dag_nodes
   SET status = 'success', seq = seq + 1, updated_at = now()
 WHERE scope_id = $1 AND node_id = $2 AND status = 'in_progress';
INSERT INTO dag_node_events (scope_id, node_id, seq, kind, payload, created_at)
VALUES ($1, $2, currval('dag_nodes_seq'), 'status_changed',
        '{"status":"success"}', now());
COMMIT;
-- Relay: SELECT ... FROM dag_node_events WHERE id > $last_relayed_id ORDER BY id
--        FOR UPDATE SKIP LOCKED LIMIT 100;  -- then pg_notify or push to local subs, mark relayed
```

Option A is the low-latency default (cross-instance fan-out for free via LISTEN/NOTIFY, §8.2);
Option B is the durable fallback subscribers can opt into when they need a persistent cursor that
survives their own downtime (equivalent to a Kafka consumer group's committed offset).

**Redis — atomic state + stream write via Lua** (Lua scripts run atomically relative to all other
commands because of Redis's single-threaded execution model, which is exactly why this is the
correct place to close the dual-write gap, not two round trips from the client):

```lua
-- KEYS[1] = node hash key, e.g. "scope:{s}:node:{n}"
-- KEYS[2] = scope's notification stream, e.g. "scope:{s}:events"
-- ARGV[1] = expected current status (CAS guard, prevents double-transition races)
-- ARGV[2] = new status
-- ARGV[3] = node id (for the event payload)
local current = redis.call('HGET', KEYS[1], 'status')
if current ~= ARGV[1] then
  return redis.error_reply('status_mismatch:' .. tostring(current))
end
local seq = redis.call('HINCRBY', KEYS[1], 'seq', 1)
redis.call('HSET', KEYS[1], 'status', ARGV[2])
redis.call('XADD', KEYS[2], '*',
  'node_id', ARGV[3], 'status', ARGV[2], 'seq', seq)
return seq
```

The Redis Stream ID returned by `XADD` is itself monotonic (`<ms-time>-<sequence>`, rejecting any
ID not greater than the stream's current maximum) — [Redis Streams docs](https://redis.io/docs/latest/develop/data-types/streams/)
— which is a second, independent monotonic counter beyond the per-node `Seq`; dag-worker-go uses
the per-node `Seq` for the resume-cursor contract exposed to library users (backend-independent),
and lets the Stream ID be an implementation detail of the Redis adapter's own resumability (§6).

---

## 6. Resume tokens and the bounded-retention tradeoff

Every durable pub/sub system that lets a client disconnect and reconnect faces the same shape of
problem: hand the client an opaque **cursor** marking "everything up to here has been seen," let it
reconnect later and say "resume from this cursor," and — because retaining infinite history is
never free — define what happens when the cursor points behind the oldest retained data.

| System | Cursor | Resume call | What happens when the cursor is too old |
|---|---|---|---|
| **etcd** | MVCC revision (int64, global to the keyspace) | `Watch(key, WithRev(rev))` | Compaction discards history before a revision; a watch requesting a compacted revision is **canceled**, and the response's `CompactRevision` field reports "the minimum historical revision available" so the client knows how far it can *not* go back — [etcd Watch API docs](https://etcd.io/docs/v3.5/learning/api/); the client's only recourse is a full re-list plus resync from the current revision. |
| **Kafka** | `(topic, partition, offset)` | `consumer.seek(tp, offset)` / `auto.offset.reset` | Retention (`retention.ms` / log compaction) deletes old segments; a consumer whose committed offset falls before the earliest retained offset gets `OffsetOutOfRangeException` and must decide (via `auto.offset.reset`) whether to jump to `earliest` or `latest`, silently skipping the gap either way. |
| **MongoDB Change Streams** | Resume token (opaque BSON, embeds an oplog/change-stream timestamp) | `resumeAfter` / `startAfter` | The oplog (or change-stream pre-image collection) has bounded retention; a token older than the retention window is invalid and the driver surfaces this explicitly as a **`ChangeStreamHistoryLost`** condition. `startAfter` additionally tolerates resuming after an *invalidate* event (collection drop/rename) where `resumeAfter` cannot — [MongoDB Change Streams manual](https://www.mongodb.com/docs/manual/changeStreams/). |
| **PostgreSQL logical replication** | LSN (log sequence number) | slot's `restart_lsn` | A replication slot pins WAL retention at its `restart_lsn`; `max_slot_wal_keep_size` caps how much WAL a slot may force PostgreSQL to retain, and a sufficiently lagging slot has its WAL reclaimed out from under it, **invalidating the slot** (surfaced via `pg_replication_slots.wal_status = 'lost'`) rather than silently skipping data — [Gunnar Morling, Postgres Replication Slots: confirmed_flush_lsn vs restart_lsn](https://www.morling.dev/blog/postgres-replication-slots-confirmed-flush-lsn-vs-restart-lsn/); unbounded retention (`max_slot_wal_keep_size = -1`, the default) trades "cursor never goes stale" for "an inactive slot can fill the disk" — [PostgreSQL replication config docs](https://www.postgresql.org/docs/current/runtime-config-replication.html). |
| **Redis Streams** | Stream entry ID (`ms-seq`) | `XREADGROUP ... STREAMS key <last-id-or->` | `XTRIM`/`MAXLEN` bound stream length; as of Redis 8.2, trimming is **consumer-group-aware** and will not delete entries still pending (unacknowledged) in a group's PEL, closing the gap where an old but unacked message could be silently discarded out from under a slow consumer — [Redis Streams docs](https://redis.io/docs/latest/develop/data-types/streams/). Older Redis versions (and any use of the plain `XADD MAXLEN` trim) do **not** have this protection and can drop pending work. |

**The shared shape**: bounded retention is not optional at scale (unbounded history is an
unbounded storage bill and, per §4.3, is not even the source of truth here), so every backend must
define, and dag-worker-go must surface uniformly, a canonical **cursor-too-old** condition. The
library's `Subscription` interface (§11) exposes this as a typed error
(`ErrCursorExpired`) rather than silently resuming from "latest" or "earliest" — silent behavior
here is exactly the kind of surprising per-backend divergence the "one API over both" goal (§8)
exists to prevent. A subscriber that receives `ErrCursorExpired` has one correct recovery path
regardless of backend: **fall back to a full storage scan** for current node state (the
state-plus-notification design of §4.3 makes this always possible — the event log is never the
only place truth lives) and re-subscribe from "now."

Retention policy is deliberately made a per-backend, per-deployment tuning knob, not a library
constant, because the right answer trades off directly against storage cost and is backend-native
anyway (`retention.ms`-equivalent for Redis is `MAXLEN`; for Postgres it's the outbox table's own
retention job; in-memory has no retention question because it has no cross-restart durability at
all — a process restart is definitionally a full cursor reset).

---

## 7. Backpressure: what happens when a subscriber is slow

### 7.1 The five strategies

1. **Unbounded buffer.** Queue everything for the slow subscriber, forever. Guarantees no message
   loss and no backpressure on the producer — until the process OOMs. Never acceptable as a
   default; only defensible as an explicit, capacity-planned opt-in.
2. **Bounded, drop-oldest.** Fixed-size ring buffer; a full buffer evicts its oldest entry to make
   room for the newest. Prioritizes freshness over completeness — correct instinct for a status
   feed a UI is rendering (nobody cares about a `NodeStatusChanged` from ten seconds ago once a
   newer one for the same node exists) but wrong for anything a consumer must not miss.
3. **Bounded, drop-newest.** Fixed-size buffer that rejects new entries once full, preserving what
   was already queued. Prioritizes the oldest information over the newest — rarely what you want
   for a live status feed, since it means a subscriber that fell behind keeps consuming
   increasingly stale data instead of catching up to "now."
4. **Bounded, block.** Producer blocks (or the storage-side fan-out goroutine blocks) until the
   subscriber drains. This is **disqualified for anything on the scheduling path**: the whole point
   of the DAG engine is to keep discovering and dispatching newly-ready nodes, and if emitting a
   `NodeReady` doorbell to one slow, forgotten subscriber can stall that discovery loop, one bad
   subscriber degrades the entire scope for every other worker — a **head-of-line blocking**
   failure mode. This is also the precise justification Reactive Streams gives for putting the
   *bound* on the subscriber's own declared buffer rather than letting a blocked queue propagate
   backpressure into the publisher's control flow: *"resource consumption needs to be carefully
   controlled such that a fast data source does not overwhelm the stream destination,"* achieved
   by bounding buffers to sizes the subscriber itself requests via `request(n)`, never by having
   the source block — [Reactive Streams JVM spec](https://github.com/reactive-streams/reactive-streams-jvm/blob/master/README.md).
5. **Disconnect the slow subscriber with an error.** Treat "too far behind" as a subscriber fault,
   sever the connection, and let it resync via a cursor (§6) rather than let it degrade the
   producer. Two production instances of this exact policy:
   - **NATS** tracks per-subscription *pending* limits (default 65,536 messages / 64 MiB) and, once
     exceeded, the server drops the subscriber and increments a `slow_consumers` counter visible on
     the monitoring endpoint — [NATS Slow Consumers docs](https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers).
   - **etcd** does the equivalent for watches: a watch that falls behind the compaction horizon
     doesn't get silently starved — it is explicitly **canceled** with a `CompactRevision` telling
     the client where it can resume from — [etcd Watch API docs](https://etcd.io/docs/v3.5/learning/api/).
6. **Per-subscriber persistent cursor (competing/durable consumers).** Don't buffer *in the
   transport* at all — persist a per-subscriber (or per-consumer-group) read position in the
   storage backend itself, and let a reconnecting subscriber pick up exactly where its own cursor
   left off, independent of every other subscriber's pace. Redis Streams consumer groups are the
   clean instance: `XREADGROUP` records delivery in a **Pending Entries List** per group, `XACK`
   removes an entry once processed, and `XPENDING` lets you inspect (and, via `XCLAIM`, reassign)
   anything a crashed consumer never acknowledged —
   [Redis XREADGROUP](https://redis-doc-test.readthedocs.io/en/latest/topics/streams-intro/),
   [Redis XACK](https://redis.io/docs/latest/commands/xack/),
   [Redis XPENDING](https://redis.io/docs/latest/commands/xpending/). This is structurally the
   *only* strategy on this list that gives at-least-once delivery **without** requiring the
   subscriber to stay connected and fast — the tradeoff is that the storage backend now carries
   per-subscriber bookkeeping instead of the transport.

### 7.2 Recommended policy per tier and per event kind

The two event kinds from §1 get *different* backpressure policies, and that split is itself the
main recommendation:

| Backend | `NodeStatusChanged` (observation feed) | `NodeReady` (doorbell) |
|---|---|---|
| **In-memory** | Bounded Go channel per subscriber (default size configurable, e.g. 1024), **drop-oldest** on overflow — implemented as a small ring buffer feeding the channel, since a plain Go channel send would itself block (strategy 4) if unbuffered and full. A dropped event is fine because a subsequent read of `Seq` will show the subscriber it missed something (§3.3), and the state is always independently re-readable (§4.3). | Same bounded channel, but **coalescing** instead of dropping: multiple `NodeReady` for the same `NodeID` collapse to one pending doorbell (a `map[NodeID]struct{}` gate rather than a queue) — since the doorbell carries no payload beyond "go call `Reserve`," coalescing is strictly free information-wise. |
| **Redis** | Redis Stream + consumer group per named subscriber (strategy 6): `XREADGROUP` with `XACK` gives an at-least-once, crash-recoverable feed; `MAXLEN`-with-approximate-trim bounds memory, and Redis 8.2's PEL-aware trimming (§6) keeps trimming from eating unacked entries. Slow/disconnected subscribers fall behind in their own PEL without affecting anyone else. | Plain Redis pub/sub (fire-and-forget, at-most-once) as the low-latency doorbell layer *on top of* the same Stream — a lost pub/sub notification is harmless because any subscriber can also just poll the Stream's tail or call `Reserve` directly; pub/sub is a latency optimization, never the only path. |
| **PostgreSQL** | Outbox table + relay (§5.4 Option B), each subscriber tracked as a row with its own `last_relayed_id` — the SQL-native version of strategy 6. Retention is a periodic `DELETE ... WHERE created_at < now() - retention_interval AND id < min(last_relayed_id) across subscribers`. | `LISTEN`/`NOTIFY` (§5.4 Option A) as the doorbell: at-most-once, payload-capped at 8000 bytes, and Postgres's own queue-full behavior degrades to failing the notifying transaction if a listening session never drains ("if this queue becomes full, transactions calling NOTIFY will fail at commit," and Postgres warns in the log once the queue is half-full pointing at the offending session) — [PostgreSQL NOTIFY docs](https://www.postgresql.org/docs/current/sql-notify.html). Because the doorbell is provably non-load-bearing here (`Reserve` always re-derives truth), the mitigation is simply: never let a library-internal listener session sit idle inside an open transaction, and treat NOTIFY delivery as a pure hint. |
| **Memcached** | No pub/sub, no scripting, no multi-key atomicity (§5.3). The only honest implementation is **polling**: a subscriber (or the library's Memcached adapter, on subscribers' behalf) periodically re-reads a small per-scope "last seq" counter key and diffs against its own cursor, backed by short-TTL keys for recent transitions. This is at-most-once relative to the poll interval (a transition that both happens and is superseded between two polls is invisible) and its "backpressure" story is simply "poll less often" — call this out as a documented, deliberate degradation of Memcached-backed reactivity, not a bug. | Same poll loop drives `Reserve` calls directly; the "doorbell" concept degrades entirely into "poll interval," which is the honest floor for this backend. |

The load-bearing principle across every row: **strategy 4 (block) never appears**, because it is
the one strategy that lets a subscriber's slowness become the producer's problem, and the producer
here is the scheduler discovering and advancing the DAG for everyone.

---

## 8. Local vs. cross-instance subscribers, one API

### 8.1 Why the two cases have fundamentally different cost profiles

A **local** (in-process) subscriber is a goroutine holding a Go channel that the engine writes to
directly under the same lock that performs the state transition (§5.3, in-memory row). There is no
serialization, no network hop, and ordering is free because the write and the fan-out happen in the
same critical section on the same machine.

A **cross-instance** subscriber — a different OS process, potentially on a different host, watching
the same shared storage — cannot be reached this way at all. The storage backend itself has to fan
the notification out, and every backend does this differently:

- **Redis pub/sub**: `PUBLISH`/`SUBSCRIBE`, fire-and-forget, zero persistence — a subscriber that's
  briefly disconnected loses everything published during the gap, which is why §7.2 uses it only as
  a doorbell layered over the durable Stream, never as the durable channel itself.
- **Redis Streams + consumer groups**: durable, persistent-cursor delivery (§7.1 strategy 6) —
  strictly the better choice for anything that must not be silently missed, at the cost of needing
  an explicit `XACK` protocol instead of a bare subscribe.
- **PostgreSQL `LISTEN`/`NOTIFY`**: session-scoped (a listening session must stay connected on the
  same backend connection the whole time), at-most-once, 8000-byte payload cap, queue-full failure
  mode as described in §7.2 — [PostgreSQL NOTIFY docs](https://www.postgresql.org/docs/current/sql-notify.html).
- **Polling**: works against *any* backend including ones with no native fan-out (Memcached), at
  the cost of trading "reactive" for "reactive up to the poll interval" — the universal fallback,
  never the preferred path where something better exists.

### 8.2 One API, degraded guarantees documented rather than hidden

dag-worker-go presents a single Go interface (§11) regardless of mode: `Subscribe` returns the same
`Subscription` type whether the engine is running with the in-memory backend inside one process or
against shared Redis/Postgres/Memcached storage from many. What differs, and what the documentation
must say plainly rather than paper over:

| Property | Local (in-memory backend) | Cross-instance (Redis/Postgres/Memcached) |
|---|---|---|
| Latency | Sub-microsecond (direct channel send) | Network round trip + backend fan-out latency (pub/sub: ~ms; NOTIFY: ~ms within one Postgres session; polling: up to the poll interval) |
| Ordering | Trivially exact — single writer, single memory space | Only the per-node `Seq` ordering guarantee from §3.3; no stronger global ordering is available cheaply across instances |
| Delivery guarantee | Can be made effectively lossless (bounded channel large enough that drop is a capacity-planning failure, not a design certainty) | Backend-native — durable (Streams, outbox) or best-effort (pub/sub, NOTIFY) per §7.2, and this must be a subscribe-time, explicit choice, not an accident of which backend happens to be configured |
| Failure mode on falling behind | Local drop-oldest (§7.2) | Backend-specific disconnect/expire (NATS-style slow-consumer disconnect analog, or a stale/expired cursor per §6) |

What a user of the library gives up by running cross-instance is exactly the list above, not
functionality — the same `Subscribe`/`Reserve` calls work either way — but they must set
expectations by choosing a durability tier (`Subscribe(..., WithDurable(true))` maps to Streams /
outbox; the default maps to pub/sub / NOTIFY / local channel, i.e. "fast and best-effort").
Silently promising local-process guarantees ("every event, in order, never dropped") to a
cross-instance subscriber over Redis pub/sub or Postgres NOTIFY would be a correctness lie the API
must not tell.

---

## 9. Push, pull, or both for "take this node"

### 9.1 Why pure push to an unknown-capacity consumer is a known anti-pattern

The Reactive Streams initiative exists specifically because early reactive libraries pushed data at
whatever rate the producer could manage, and any consumer slower than the producer had exactly the
choices in §7.1 (buffer without bound, drop, or block) forced on it *by the producer's design*, not
by its own capacity. The spec's fix is to invert control: a `Subscriber` calls
`Subscription.request(n)` to declare exactly how much it can currently handle, and *only* that much
may be pushed — "the maximum number of elements that may arrive... is `P - N`" (requested minus
already-delivered), which lets every buffer in the pipeline be sized to a number the subscriber
itself chose — [Reactive Streams JVM README](https://github.com/reactive-streams/reactive-streams-jvm/blob/master/README.md).
This is a **pull-with-signaling hybrid**, not pure pull: the producer still proactively announces
"data available" (`onNext`), but *how much* is bounded by consumer-declared demand.

Real work-distribution systems converge on the identical shape once they hit production scale under
varying consumer capacity:

- **NATS JetStream** started with push consumers and now explicitly steers new projects toward pull
  consumers because "pull consumers give more control over flow and back pressure handling" — the
  client asks for a batch, processes it, asks for the next batch, and load balancing/backpressure
  falls directly out of how often and how large a batch it requests — [NATS JetStream Consumers docs](https://docs.nats.io/nats-concepts/jetstream/consumers), [Pull consumers in depth](https://docs.nats.io/learn/jetstream/pull-consumers).
- **AWS SQS** never offered a push primitive at all for standard consumption: `ReceiveMessage` is
  pull, paired with a **visibility timeout** lease — the message is invisible to other consumers
  until the timeout expires or the consumer explicitly deletes it, and a consumer can extend its
  own lease via `ChangeMessageVisibility` while work is still in progress — [AWS SQS visibility timeout docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html).
  This is structurally identical to the per-node, per-take timeout dag-worker-go's brief specifies.
- **PostgreSQL `FOR UPDATE SKIP LOCKED`** is the SQL-native version of the same pattern: a puller
  atomically claims and locks the next available row(s), and every other puller's identical query
  transparently skips rows already claimed — "this reduces contention and raises throughput" and is
  the mechanism underneath production job-queue libraries like Que and Oban — [Netdata: Using FOR UPDATE SKIP LOCKED for queue workflows](https://www.netdata.cloud/academy/update-skip-locked/).

None of these systems ship a design where the broker force-pushes work at a consumer with no
consumer-declared bound. Where a push-like mode exists at all (NATS core pub/sub, SQS notifications
via SNS/EventBridge), it is explicitly the *notification* layer sitting on top of a pull-based
work-claim, never the work-claim mechanism itself — which is exactly the split this dossier has
been building toward since §1.

### 9.2 Verdict for dag-worker-go: pull-with-notification, argued and verified

**`NodeReady` is an event; taking the node is always a `Reserve`/`Fetch` pull call.** The event is
cheap, coalescable, safe to drop or duplicate (§7.2), and exists purely to avoid making a
zero-work poller the only option — a caller with no event subscription still gets all work by
calling `Reserve` on a timer, just at whatever latency its poll interval implies (the Memcached
floor from §7.2, generalized as the *fallback* for every backend, not the primary path for any of
them). The pull call is where every actual guarantee lives:

- **Capacity is caller-declared** (`n` in `Reserve(ctx, scope, n, timeout)`), matching Reactive
  Streams' `request(n)` and SQS's `MaxNumberOfMessages` — the library never hands a worker pool
  more concurrent work than it explicitly asked for.
- **The lease/timeout requirement from the brief** ("timeout settable per node at the moment the
  worker takes it, with a library default") maps directly onto the SQS visibility-timeout model
  and the `SKIP LOCKED` claim pattern: `Reserve` returns a `Lease` with an `ExpiresAt`, the
  storage backend is responsible for making an expired, unacked lease's node reappear as eligible
  (transitioning it to `error-with-timeout` per the brief, or back to `new` for retry — a policy
  choice covered in the state-machine dossier, not this one), and the worker's normal path is to
  `Ack`/`Nack` before expiry, with `Nack`/late-`Ack` handled idempotently per §2.3.
- **A duplicated or late doorbell is provably harmless**: because `Reserve` is the only path that
  actually changes a node's status to `in-progress`, and that transition is atomic and
  single-winner at the storage layer (the CAS guard in the Lua script of §5.4, the
  `SKIP LOCKED`/`FOR UPDATE` claim in Postgres, a simple mutex-guarded check in memory), racing two
  workers on the same `NodeReady` doorbell resolves exactly the way racing two `SKIP LOCKED`
  pollers resolves in Postgres: one wins, one gets nothing back, no corruption either way.

---

## 10. Event type taxonomy

```go
// EventKind discriminates the two structurally different signals (§1).
type EventKind uint8

const (
	// EventNodeStatusChanged is emitted on every transition of a node's public
	// status. Observation-only; any number of subscribers may receive it.
	EventNodeStatusChanged EventKind = iota

	// EventNodeReady is a doorbell meaning "at least one node in this scope may
	// now be eligible for Reserve." It carries no claim — receiving it confers
	// no right to the node; only a successful Reserve does (§9).
	EventNodeReady

	// EventCursorExpired is not delivered on the Events() channel; it is
	// reported via Subscription.Err() when the backend can no longer resume
	// the caller's cursor (§6) — e.g. a Redis Stream trimmed past it, a
	// Postgres outbox row garbage-collected, or (for backends with a native
	// analog) a compacted etcd-style revision. The only correct recovery is a
	// fresh full read of current state followed by Subscribe from "now."
	EventCursorExpired
)

// Status is the library's MINIMAL public vocabulary (kept deliberately small
// per the project brief; see the state-machine dossier for the full design).
type Status uint8

const (
	StatusNew Status = iota
	StatusInProgress
	StatusSuccess
	StatusError
)

// Event is the single wire type delivered on a Subscription's Events()
// channel, covering both EventKind values so that ordering between a status
// change and its associated readiness doorbell (when both fire for the same
// write) is preserved by delivering them as one stream, not two.
type Event struct {
	Kind EventKind

	ScopeID string
	NodeID  string

	// Seq is the per-node monotonic sequence number assigned atomically with
	// the underlying state write (§3.3, §5.4). It is both the ordering key
	// and (per §5.2) the staleness token a subscriber compares against a
	// subsequent storage read.
	Seq uint64

	// From/To are populated only for EventNodeStatusChanged; both are the
	// zero value (StatusNew) for EventNodeReady, which is why NodeReady
	// intentionally carries no status information — it is purely a hint to
	// go call Reserve, never a substitute for reading current state.
	From, To Status

	// OccurredAt is the backend's commit-time timestamp, not local wall-clock
	// time at delivery — set once, at write time, so that clock skew between
	// producer and subscriber processes never leaks into the event.
	OccurredAt time.Time

	// Payload is populated only when the subscriber opted into
	// WithPayload(true) at Subscribe time (§5.2, fix 3); nil otherwise.
	Payload []byte
}
```

---

## 11. Go interface

```go
package events

import (
	"context"
	"errors"
	"time"
)

// Cursor is an opaque, backend-specific resume token (§6). Callers persist it
// verbatim and pass it back to Subscribe; they must never parse or compare it.
type Cursor string

// ErrCursorExpired is returned by Subscribe, or delivered on Err(), when the
// backend can no longer resume from the given Cursor (§6). The only correct
// response is a full storage read for current state, then Subscribe(ctx,
// scope, filter, CursorNow) to resume live delivery from "now."
var ErrCursorExpired = errors.New("events: cursor expired, resync required")

// CursorNow is the sentinel Cursor meaning "start from the current position,"
// i.e. the caller only wants events from this point forward and explicitly
// does not need history.
const CursorNow Cursor = ""

// Filter narrows a subscription. An empty Filter matches every event in Scope.
type Filter struct {
	Kinds    []EventKind // nil = all kinds
	NodeID   string      // "" = all nodes in the scope
	Payload  bool        // request fix (3) from §5.2 for matching events
}

// Subscription is the single type returned regardless of backend or of
// whether the subscriber is local or cross-instance (§8.2). Callers must
// range over Events() and separately select on Err() to detect fatal
// backend-level conditions (cursor expiry, slow-consumer disconnect, etc.).
type Subscription interface {
	// Events delivers in-order (per Event.Seq, per NodeID — §3.3) events
	// matching the subscription's Filter. The channel is closed after Err()
	// has delivered its one value, or after Close() is called.
	Events() <-chan Event

	// Err delivers at most one value: the reason the subscription ended
	// abnormally (ErrCursorExpired, a wrapped backend-specific slow-consumer
	// error, or a transport failure). A clean Close() delivers nothing here.
	Err() <-chan error

	// Cursor returns the position of the last event delivered on Events(),
	// safe to persist and pass to a future Subscribe call to resume exactly
	// after it (at-least-once: a resumed subscription may redeliver the
	// event at Cursor() itself if the caller didn't separately record having
	// processed it — callers wanting exactly-once effects must dedupe on
	// (NodeID, Seq), per §2.3).
	Cursor() Cursor

	// Close releases the subscription's backend resources (consumer group
	// registration, LISTEN session, local channel). Idempotent.
	Close() error
}

// Bus is the subscribe-side of the event/reactive layer. One implementation
// per storage backend; the in-memory implementation additionally fans out
// to same-process subscribers without touching the storage backend at all.
type Bus interface {
	Subscribe(ctx context.Context, scope string, filter Filter, from Cursor) (Subscription, error)
}

// Lease represents a single successful reservation returned by Reserve. The
// worker holding it must call Ack or Nack before ExpiresAt, or the node is
// treated as timed out (§9.2) and becomes eligible for reservation again
// (policy — retry vs. error-with-timeout — is a state-machine decision, not
// an events-layer one).
type Lease struct {
	ScopeID   string
	NodeID    string
	Seq       uint64 // the Seq the node was at when this lease was granted
	Token     string // opaque fencing token; required on Ack/Nack
	ExpiresAt time.Time
}

// Reserver is the pull side of "take this node" (§9.2). It is the only path
// by which a node's status moves to in-progress, and is safe to call
// speculatively — on a timer, in response to a NodeReady event, or both —
// because an unsuccessful reservation attempt for an already-taken node is
// simply a no-op (zero leases returned), never an error.
type Reserver interface {
	// Reserve claims up to n eligible nodes in scope, each leased for
	// timeout (or the library default if timeout is zero — per-node
	// override per the project brief). Returns immediately with between 0
	// and n leases; it does not block waiting for work to appear (that is
	// what subscribing to EventNodeReady is for).
	Reserve(ctx context.Context, scope string, n int, timeout time.Duration) ([]Lease, error)

	// Ack reports successful completion of the node the token was issued
	// for. Idempotent: acking an already-acked, expired, or reassigned
	// token is a no-op, never an error (§2.3).
	Ack(ctx context.Context, token string) error

	// Nack reports failure. err is recorded as the node's public error
	// state. Idempotent on the same terms as Ack.
	Nack(ctx context.Context, token string, err error) error
}
```

---

## 12. Guarantees table

| Dimension | In-memory | Redis | PostgreSQL | Memcached |
|---|---|---|---|---|
| `NodeStatusChanged` delivery (local subscriber) | At-least-once*, drop-oldest under sustained overload (§7.2) | same, plus network hop if the process itself isn't the writer | same | same |
| `NodeStatusChanged` delivery (cross-instance) | n/a (no cross-instance mode) | At-least-once via Streams + consumer group (`XACK`); at-most-once if configured on plain pub/sub | At-least-once via outbox+relay; at-most-once if configured on direct NOTIFY | At-most-once, poll-interval granularity only (§7.2) — **no durable cross-instance guarantee available** |
| `NodeReady` delivery | At-most-once, coalesced, non-load-bearing (§9.2) | At-most-once via pub/sub, non-load-bearing | At-most-once via NOTIFY, non-load-bearing | Poll-interval only, non-load-bearing |
| Ordering | Exact, per-process | Per-node `Seq`, exact; no cross-node order | Per-node `Seq`, exact; no cross-node order | Per-node `Seq` if polled state includes it; coarser than poll interval otherwise |
| Read-your-writes | Guaranteed (same-process, same lock, §5.3) | Guaranteed for the writer via the Lua script (§5.4); other instances get it via `Seq` comparison (§5.2 fix 2) | Guaranteed for the writer (same txn, §5.4); other instances via `Seq` comparison | Not guaranteed — CAS write is atomic per-key, but there is no atomic write+notify (§5.3); callers must use `Seq`/version fields and tolerate staleness |
| Resume after disconnect | No — process restart = new subscription from `CursorNow` | Yes, via Stream ID cursor, bounded by `MAXLEN`/`XTRIM` retention (§6) | Yes, via outbox row id / relayed-id cursor, bounded by outbox retention job (§6) | No native resume — cursor is "last seq observed," bounded by whatever TTL the polled keys carry |
| Slow-subscriber isolation | Per-subscriber bounded channel; one slow subscriber never blocks others or the scheduler (§7.1, §7.2) | Per-consumer-group PEL isolates slow consumers from each other (§7.1 strategy 6) | Per-subscriber `last_relayed_id` isolates equivalently | Isolation is trivial — every subscriber polls independently — but so is the ceiling on freshness |
| `Reserve` correctness under duplicate/lost doorbells | Unaffected — `Reserve` re-derives eligibility from current state every call (§4.3, §9.2) | Unaffected, same reasoning | Unaffected, same reasoning | Unaffected, same reasoning |

\* "At-least-once" here means *within the bound of the drop-oldest ring buffer* — a subscriber that
never falls behind sees every event exactly once; one that falls behind sees a gap it can detect
via `Seq` (§3.3) and must close by reading current state (§4.3), which is why this table calls the
in-memory backend's practical guarantee "at-least-once, drop-oldest," not "exactly-once": no tier
in this design claims exactly-once delivery of the event itself, only idempotent, exactly-once
*effect* at the `Reserve`/`Ack` boundary (§2.3), which is the guarantee that actually matters.

---

## Recommendations for dag-worker-go

1. **Split the API in two, not one.** Ship `Bus.Subscribe` (observation, §11) and
   `Reserver.Reserve`/`Ack`/`Nack` (work-claim, §11) as separate interfaces from day one. Do not be
   tempted to unify them into a single "subscribe and get handed work" call — that is precisely the
   push-based anti-pattern §9.1 documents real systems moving away from, and unifying them now would
   make the eventual split a breaking API change instead of the original design.
2. **Make `NodeReady` provably non-load-bearing, and say so in the doc comment on the type.** Every
   guarantee in §12's bottom row depends on `Reserve` being able to re-derive eligibility from
   scratch. Do not let any future optimization cache "known-ready" node IDs from the event stream
   in a way that becomes the only record — that would silently reintroduce exactly the event-sourcing
   replay hazard §4.2 exists to avoid.
3. **Adopt per-node `Seq` as the one ordering and staleness primitive everywhere**, including inside
   the storage-layer schema (a `seq BIGINT` column / hash field / in-memory struct field bumped
   atomically with every status write). It is the cheapest mechanism found in this research that
   simultaneously solves ordering (§3.3), staleness detection (§5.2), and half of the resume-cursor
   design (§6) — do not invent a second mechanism for any of those three problems.
4. **Never ship a `Subscribe` mode that can block a state-transition write.** Enforce this with a
   design rule, not just a convention: the function that performs a node's storage-layer state
   transition must never itself wait on a subscriber channel send; all fan-out to slow or bounded
   consumers happens through a buffer whose overflow policy is drop/disconnect (§7), never block.
5. **Treat Memcached as a second-class citizen for the reactive layer and document it as such**,
   rather than quietly reimplementing pub/sub badly on top of it. It has no scripting, no
   transactions, no native fan-out; the honest adapter is poll-based, and every guarantee row for it
   in §12 should read worse than the other three backends because it genuinely is.
6. **Expose `ErrCursorExpired` as one typed error across all backends**, with one documented recovery
   procedure (full state read, then `Subscribe(..., CursorNow)`), so that application code written
   against the in-memory backend during development does not silently do the wrong thing the day it
   is pointed at Redis or Postgres in production and a cursor goes stale for the first time.
7. **Default to the durable tier for `NodeStatusChanged` and the best-effort tier for `NodeReady`**
   per backend (§7.2's table), and make the choice explicit and overridable per `Subscribe` call
   (`WithDurable(bool)`) rather than baking one blanket policy into each backend adapter — different
   callers legitimately want different points on this trade (a UI dashboard wants low-latency
   best-effort; an audit consumer wants the durable outbox/Stream path).
8. **Do not build a general event-sourced replay/rebuild feature for v1.** State-plus-notification
   (§4.3) is sufficient for correctness and dramatically smaller in surface area than a
   Temporal-style replay-safe event history; revisit only if a concrete use case needs full
   historical replay (e.g., a debugging/audit tool), and if so, build it as an opt-in consumer of the
   durable event feed (§7.2), never as the mechanism the engine itself depends on for recovery.

## Open questions

- **Global (scope-wide) sequence numbers**: is a secondary, opt-in scope-level monotonic counter
  (for subscribers who want one merged, totally-ordered timeline to render) worth the added
  write-path serialization it would impose on every backend, or should that be left entirely to
  client-side merge-by-timestamp with the understanding that it's approximate? §3.3 recommends
  making it opt-in but doesn't fully resolve whether the write-path cost is acceptable on Redis
  (a single incrementing key would serialize all writes in a scope through one `INCR`) or Postgres
  (a sequence is cheap, but coordinating it with per-node `seq` in the same statement needs care).
- **Cross-backend consumer-group semantics parity**: Redis Streams and a Postgres outbox both give
  "durable per-subscriber cursor," but their crash-recovery mechanics differ (`XCLAIM`-based
  reassignment of another consumer's abandoned PEL entries vs. a plain SQL re-read past
  `last_relayed_id`). Should the library's `WithDurable(true)` mode expose a uniform
  "reclaim abandoned deliveries" call, or leave that as a backend-specific escape hatch?
- **Payload-carrying events and size limits**: §5.2's fix (3) is offered as an opt-in, but backends
  have wildly different natural payload ceilings (Postgres NOTIFY: 8000 bytes hard cap; Redis
  Streams: effectively unbounded per entry but a de facto operational ceiling well below that;
  in-memory: no real ceiling). Should the library enforce one conservative cross-backend limit for
  `WithPayload(true)` so behavior doesn't silently change when the backend is swapped, and if so,
  what's the right number?
- **Memcached's fitness as a `Reserver` backend at all**: this dossier focused on the *event* side
  and treated Memcached's `Reserve` path as "poll plus CAS," but it did not evaluate whether
  Memcached's lack of any atomic multi-key primitive (no `SKIP LOCKED` equivalent, no Lua) makes
  race-free node reservation under concurrent claimants achievable at all without a secondary
  coordination key that starts to look like reimplementing a lock service on top of a cache — that
  question belongs to the storage-backend dossier but bears directly on whether Memcached can ever
  reach parity with the other three tiers in §12's bottom rows.
