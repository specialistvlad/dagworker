# Distributed Leasing, Visibility Timeouts, and Fencing

## Scope and why this is the correctness-critical dossier

Every other design question in this project (storage backend, DAG representation, scope
namespacing) is a performance or ergonomics question. This one is a **safety** question. The
entire value proposition of dag-worker-go is: *"an external worker takes a node, and if it dies
without answering, the library reliably notices and re-issues that node to someone else — without
ever letting two workers believe they alone hold it, and without ever accepting a stale answer
from a worker who has already been declared dead."* Every production system that hands work to a
detached executor over a network has solved (or partially solved, or shipped broken) exactly this
problem, and the solutions cluster around one mechanism — the **lease** — with the differences
concentrated in three axes: whose clock adjudicates expiry, whether stale re-admission is possible
(and if so, whether it is *cheap* — a race window — or *disastrous* — silent double execution and
corrupted shared state), and how the timeout sweep is distributed across independent readers
without becoming an O(n) or thundering-herd operation. This document surveys the production
systems that made this call, the two academic papers that frame *why* the call has to be made the
way it does, and the applied consequence for a library that must support four storage backends and
multiple concurrent library instances simultaneously.

---

## 1. The two failure modes that define the problem

A lease-based work-assignment system has exactly two ways to fail, and they are asymmetric in
severity:

| Failure | What happens | Severity |
|---|---|---|
| **False expiry** (lease reclaimed while the worker is still alive and working) | Two workers now believe they own the same node; if the storage write is not fenced, the slow worker's write can land *after* the new worker's write and silently corrupt state. | Correctness-critical — can violate the DAG's single-writer invariant. |
| **Missed expiry** (lease is not reclaimed even though the worker died) | The node sits stuck, the DAG stalls, downstream nodes never become ready. | Availability/liveness — annoying but self-evidently visible and safe to fix by widening the timeout. |

Every mechanism below is fundamentally a trade between these two. The unifying insight, argued
independently by Google's Chubby team, Martin Kleppmann, and the visibility-timeout designers at
AWS, is that **you cannot make false expiry impossible with timeouts alone** — clocks skew, GC
pauses happen, hypervisors freeze VMs, `SIGSTOP`/cgroup-freezer suspends processes for arbitrary
wall-clock time. The only way to make false expiry *safe rather than catastrophic* is to combine
the lease with a **fencing token**: a monotonically increasing number that every write must present,
so that even if two workers believe they hold the node, the storage layer itself rejects the
loser's write. This is the load-bearing idea of the entire document, and it recurs in Chubby's
sequencers, ZooKeeper's `zxid`, Kleppmann's critique of Redlock, and Spanner's TrueTime-driven
commit-wait. Section 6 turns it into a concrete Go/Lua/SQL design.

---

## 2. Survey: how production message/queue systems solve it

### 2.1 SQS visibility timeout

Amazon SQS is the closest existing system to what dag-worker-go's "hand a ready node to an
external worker" primitive needs to be, and its semantics — including its rough edges — are worth
copying deliberately, not accidentally.

**Mechanics.** When a consumer calls `ReceiveMessage`, the message is not deleted; it becomes
invisible to other consumers for the queue's **visibility timeout**, default **30 seconds**, range
**0 seconds to 12 hours** — the message returns to visibility for redelivery unless the consumer
calls `DeleteMessage` (ack) before the timer elapses ([AWS SQS visibility timeout
docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)).
A consumer that needs more time calls `ChangeMessageVisibility` to push the deadline out
programmatically — this is the lease-extension / heartbeat primitive, and its semantics are
subtle and worth internalizing before designing an equivalent API:

- The new timeout is computed **from the moment the `ChangeMessageVisibility` call is received**,
  not from the original receive time and not additively: "the 10 seconds begin to count from the
  time that you make the `ChangeMessageVisibility` call" — a call is an absolute reset of the
  deadline, not a delta ([AWS SQS visibility timeout
  docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)).
- **The extension is not persisted as "the new default" for that message** — if the message is
  redelivered later (e.g., after another failure), its visibility timeout reverts to the queue's
  original configured value, not to whatever a prior worker last set it to. This is a well-known
  gotcha: teams assume a heartbeat "sticks" and are surprised when a second delivery gets the
  short default timeout again.
- There is a **hard ceiling of 12 hours from first receipt**, and it is not extendable past that
  regardless of how many `ChangeMessageVisibility` calls are made — a design constraint explicitly
  meant to bound how long a message can be walled off from the rest of the system by one stuck
  worker.
- SQS is explicitly **at-least-once**: "because of the at-least-once delivery model, Amazon SQS
  doesn't guarantee that a message won't be delivered more than once within the visibility timeout
  period" — this is stated as a property of the system, not a bug, and the guide's remedy is
  application-level idempotency, not a stronger delivery guarantee
  ([docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)).
- SQS caps **in-flight messages** at roughly 120,000 per standard queue; exceeding it returns
  `OverLimit` on short polling or silently withholds new messages on long polling. This is the
  practical reason SQS-style visibility timeout does not scale past a certain outstanding-lease
  count without partitioning — relevant directly to dag-worker-go's O(1)/O(log n) mandate at 1M
  nodes, where "in-flight" (in-progress) nodes could plausibly be a large fraction of the graph.
- **No fencing token exists in the SQS API at all.** SQS purely offers a `ReceiptHandle`, an opaque
  string that identifies *this particular delivery* — `DeleteMessage`/`ChangeMessageVisibility`
  calls made with a stale receipt handle (past its visibility timeout) fail, but there is no
  mechanism to hand that receipt handle to a downstream storage system as a CAS token the way a
  fencing token in Chubby or ZooKeeper works. SQS punts fencing entirely to the application: if a
  worker's write to *your* database can race a second worker's write, SQS gives you zero help.

**The lesson for dag-worker-go**: reset-not-extend semantics for lease renewal is the right choice
(simpler mental model, no "sticky new value" surprise), a hard maximum total lease-extension
ceiling per node is worth having as a safety valve independent of the sweeper, and — critically —
dag-worker-go must not repeat SQS's fencing gap, because unlike SQS (which is usually the last hop
before a genuinely idempotent, dedup-capable consumer), dag-worker-go's storage *is* the
authoritative state of the DAG; a stale write here is not "redundant," it's a correctness
violation of the single-owner invariant.

### 2.2 Google Cloud Pub/Sub ack deadlines and lease management

Pub/Sub's model is structurally identical to SQS's (deadline-based invisibility) but makes
different defaults and offers a cleaner *client-library-managed* lease-extension abstraction that
is a good API-shape reference.

- Default ack deadline is **10 seconds**; configurable up to **600 seconds (10 minutes)** per
  `modifyAckDeadline` call and at the subscription level
  ([Pub/Sub lease management docs](https://docs.cloud.google.com/pubsub/docs/lease-management);
  [`modifyAckDeadline` reference](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/modifyAckDeadline)).
- Like SQS's `ChangeMessageVisibility`, `ModifyAckDeadline` is **absolute-from-call-time**: "the
  new ack deadline will expire N seconds after the `ModifyAckDeadline` call was made" — same
  reset-not-additive semantics as SQS.
- The high-level client libraries implement **automatic lease management**: rather than the
  application hand-rolling a heartbeat loop, the library itself tracks outstanding (unacked)
  messages and periodically calls `ModifyAckDeadline` on the caller's behalf, up to a configurable
  **maximum total extension period** (default up to ~1 hour) and using the **99th percentile of
  observed ack latency** to size each individual extension
  ([docs](https://docs.cloud.google.com/pubsub/docs/lease-management)). This is a meaningfully
  better design than "the application must remember to call extend": the library measures how long
  processing has historically taken and extends by an amount informed by that, rather than a fixed
  guess. This maps directly onto a "heartbeat" design in dag-worker-go's worker SDK: the client
  library, not hand-written host code, should own the heartbeat loop, sized from an
  application-tunable "expected processing time" percentile rather than a single knob.
- The most important caveat, stated flatly in Google's own docs: **"acknowledgment deadlines are
  not guaranteed to be respected unless you enable exactly-once delivery"** — i.e., in Pub/Sub's
  default (at-least-once) mode, a message can in rare cases be redelivered to a second subscriber
  *before* the ack deadline of the first subscriber has actually expired, because deadline
  enforcement itself is best-effort in the non-exactly-once path. This is a striking admission from
  a hyperscale vendor: **even the deadline check itself is not linearizable unless you pay for the
  stronger mode.** It is the strongest argument in this entire survey for building the fencing
  token into the storage layer rather than trusting "the deadline hasn't passed yet" as a safety
  property on its own.

### 2.3 Kafka consumer groups: `session.timeout.ms`, `max.poll.interval.ms`, and rebalance storms

Kafka's consumer-group liveness model is a useful *cautionary* case study because it conflates two
different kinds of liveness — "is the process's network heartbeat thread alive" and "is the
process actually making progress consuming" — and getting that conflation wrong is exactly the
class of bug this project must avoid in its own timeout design.

- **`session.timeout.ms`** (default **45 seconds** on modern brokers) governs the *group
  coordinator's* liveness check: "the client sends periodic heartbeats to indicate its liveness to
  the broker. If no heartbeats are received by the broker before the expiration of this session
  timeout, then the broker will remove this client from the group and initiate a rebalance."
- **`max.poll.interval.ms`** (default **5 minutes**) governs a *separate* liveness check on actual
  message-processing progress: "if the consumer does not call `poll()` within [this] interval... the
  client leaves the consumer group and a rebalance is triggered" — even if the heartbeat thread
  (which runs independently since client version 0.10.2) is still happily beating.
- The reason these are two separate knobs is historical and instructive: pre-0.10.2 clients had
  **no dedicated heartbeat thread** — heartbeats were piggybacked on `poll()` calls, so a slow
  message handler silently starved the heartbeat and triggered `session.timeout.ms`-based eviction
  even though the process was alive and (slowly) working. The fix was to split the two concerns:
  a background thread proves *the process is alive*; a separate, much longer timer proves *the
  process is still making progress through its work loop*
  ([Confluent's Kafka rebalancing explainer](https://www.confluent.io/learn/kafka-rebalancing/);
  [Kafka consumer configs](https://kafka.apache.org/41/configuration/consumer-configs/)).
- **Rebalance storms**: every time group membership changes (a consumer joins, leaves, or is
  evicted by either timeout), Kafka's default eager rebalance protocol **revokes every partition
  from every consumer in the group and reassigns from scratch** — "stop the world" — which under a
  flapping consumer (repeatedly hitting `session.timeout.ms` due to GC pauses, or repeatedly
  restarting) produces a storm of full-group rebalances, each one pausing consumption for the
  *entire* group while reassignment completes.
- **KIP-345 static membership** is the fix Kafka shipped for exactly this: a consumer supplies a
  stable `group.instance.id`; on a short-lived restart or disconnect within `session.timeout.ms`,
  it **rejoins with the same identity and gets the same partition assignment back without
  triggering a rebalance at all** — "every consumer restart or temporary disconnect causes a full
  group rebalance... static membership solves this by avoiding these unnecessary rebalances"
  ([KIP-345](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345:+Introduce+static+membership+protocol+to+reduce+consumer+rebalances)).
  Static membership additionally **fences duplicate identities**: two processes joining with the
  same `group.instance.id` is treated as a conflict and one is rejected, rather than silently
  splitting the assignment.

**Lessons for dag-worker-go:** (1) "is the worker process alive" and "is this specific node's
processing making progress" must be two different timers, exactly as Kafka eventually learned to
split them — a per-node lease deadline is the analog of `max.poll.interval.ms`, and it should not
be conflated with any process-wide worker liveness heartbeat if the library ever adds one; (2) the
sweeper/rebalance action on timeout must be **scoped to the one expired node**, never "recompute
assignment for everything" — the multi-instance work-distribution question in this project's brief
must not accidentally reinvent Kafka's eager-rebalance stop-the-world behavior; (3) a stable
worker/consumer identity that can reclaim its own in-flight leases after a brief reconnect (the
static-membership idea) is a cheap, valuable feature to offer in the worker SDK.

### 2.4 Redis Streams consumer groups: `XREADGROUP` / `XPENDING` / `XCLAIM` / `XAUTOCLAIM`

Redis Streams is the closest off-the-shelf primitive to "hand a ready item to exactly one worker,
track who has it, and let someone else reclaim it if they vanish," and its actual command
semantics (read from the Redis docs directly, not summarized secondhand) are worth using as a
template for dag-worker-go's Redis backend.

- `XREADGROUP GROUP <group> <consumer> ... STREAMS <key> >` delivers new entries to `<consumer>`
  and, as a side effect, creates an entry in the group's **Pending Entries List (PEL)** — this is
  the "the node is now leased to this worker" record. The PEL tracks, per pending message: the
  current owner consumer, the **idle time** in milliseconds since last delivery, and a **delivery
  counter**.
- **`XPENDING`** is the read/inspect side. The summary form (`XPENDING key group`) returns count,
  min/max IDs, and per-consumer pending counts in O(1)-ish time; the extended form
  (`XPENDING key group [IDLE ms] start end count [consumer]`) returns, per message, `(id, consumer,
  idle_ms, delivery_count)` and — critically for a sweeper — supports an `IDLE min-idle-time`
  filter added in Redis 6.2, so a scan for "everything idle for longer than my timeout" is a native,
  server-side-filtered range query, not a client-side filter over every pending entry
  ([XPENDING docs](https://redis.io/docs/latest/commands/xpending/)). The docs explicitly note
  that filtering by a specific consumer is *not* an O(n) scan of all consumers' entries — "we have a
  pending entries list data structure both globally, and for every consumer" — i.e., the PEL is
  itself indexed per-consumer as well as globally, which matters for a sweeper design that wants to
  avoid scanning irrelevant consumers.
- **`XCLAIM key group consumer min-idle-time id [id ...]`** transfers ownership of specific pending
  message IDs to `consumer`, but **only if their current idle time exceeds `min-idle-time`** — and,
  crucially, claiming an entry **resets its idle time to zero**, which is what makes two consumers
  racing to claim the same stale entry safe: "because as a side effect `XCLAIM` will also reset the
  idle time..., two consumers trying to claim a message at the same time will never both succeed:
  only one will successfully claim the message" ([XCLAIM
  docs](https://redis.io/docs/latest/commands/xclaim/)). This is the reclaim-race-safety property
  dag-worker-go needs, and it comes from Redis's single-threaded command execution serializing the
  two `XCLAIM` calls — the second one reads the *already-reset* idle time and fails the
  `min-idle-time` check. `XCLAIM` also increments the delivery counter (unless `JUSTID` is passed),
  which the docs recommend using as a poison-pill detector: "messages that cannot be processed for
  some reason... will start to have a larger counter and can be detected." Complexity is documented
  as **O(log N)** in the size of the consumer's PEL.
- **`XAUTOCLAIM key group consumer min-idle-time start [COUNT n]`** (Redis 6.2+) is the sweeper
  primitive proper: it is "equivalent to calling `XPENDING` and then `XCLAIM`, but provides a more
  straightforward way to deal with message delivery failures via `SCAN`-like semantics" — it scans
  the PEL starting from a cursor (`start`, use `0` initially), claims up to `COUNT` (default 100)
  entries whose idle time exceeds `min-idle-time`, and **returns a cursor for the next call**,
  exactly the `SCAN`-family pattern that makes it safe to run incrementally without holding a large
  result set or blocking. It also handles the "message was trimmed/deleted from the stream but is
  still in the PEL" case automatically, evicting orphaned PEL entries and reporting their IDs
  separately, and complexity is documented as "O(1) if COUNT is small" ([XAUTOCLAIM
  docs](https://redis.io/docs/latest/commands/xautoclaim/)). Two independent sweeper instances
  calling `XAUTOCLAIM` concurrently on the same group are safe for the same reason `XCLAIM` is: the
  claim operation is a single atomic server-side operation.
- **What Streams does *not* give you**: no fencing token. The PEL's delivery counter is a *hint*
  (useful for poison-pill detection) not a compare-and-swap token — nothing stops a worker holding
  a since-reclaimed entry from writing to an external system after its claim has moved on, unless
  the *application* threads a per-claim epoch through to that external write. Streams also has no
  built-in notion of "timeout policy per entry set at claim time" — `min-idle-time` is a parameter
  the *claimer* chooses at claim time, not a deadline the *original consumer* set when it took the
  message, which is a weaker model than this project's stated requirement ("timeout is settable per
  node at the moment the worker takes it"). A Streams-based Redis backend for dag-worker-go would
  therefore need to store the per-node deadline explicitly (e.g., in a companion hash or in the
  stream entry payload) and have the sweeper read *that* value rather than relying on Streams'
  generic idle-time concept, which measures time since last *delivery* attempt, not a
  library-chosen deadline.

### 2.5 The Redis "reliable queue" pattern: `RPOPLPUSH`/`LMOVE` + a ZSET of deadlines

Before Streams existed, and still common in simpler codebases, is the classic **reliable queue**
pattern built from plain lists: `BRPOPLPUSH source processing` (or its modern replacement,
`BLMOVE source processing RIGHT LEFT`, since `RPOPLPUSH` is deprecated as of Redis 6.2.0 in favor
of `LMOVE`/`BLMOVE`) atomically pops a ready item off `source` and pushes it onto a per-consumer (or
shared) `processing` list in one server-side operation, so a crash between "pop" and "push" is
impossible — there is no window where the item exists in neither list.

The reliability gap this pattern must additionally close is: *if the worker then crashes while the
item sits in `processing`, who notices?* The standard answer is exactly the ZSET-of-deadlines
design named in this project's brief: a companion `ZADD processing:deadlines <expiry_unix_ms>
<item_id>` alongside the `LMOVE`, and a periodic sweeper that does `ZRANGEBYSCORE
processing:deadlines -inf <now> LIMIT 0 <batch>` to find expired claims in `O(log n + k)` (`k` =
batch size) rather than scanning the processing list, then re-queues (`LMOVE processing source
LEFT RIGHT` for the specific matched item, which requires locating it by value — in practice
implementations instead store `item_id` as the list payload and re-`LPUSH` it directly onto
`source`, using the ZSET purely as the O(log n) index and the list only as an audit trail) and
`ZREM`s the deadline entry. This ZSET-as-secondary-index-by-deadline structure is precisely the
shape section 6d generalizes into dag-worker-go's cross-backend sweeper design — it is the same
idea Redis Streams' PEL implements internally (a per-group structure ordered for idle-time queries)
but made explicit and directly controllable, including the crucial ability to **set the deadline at
claim time to a caller-chosen value**, which native Streams idle-time does not offer.

---

## 3. Lease and lock theory: the papers and the debate

### 3.1 Gray & Cheriton, "Leases: An Efficient Fault-Tolerant Mechanism for Distributed File Cache
Consistency" (SOSP 1989)

This is the paper that named and formalized the concept everything above is a special case of. Its
setting is distributed file cache consistency (a server grants a client a time-bounded lease on a
cached copy of data so the client can serve reads locally without a round trip, and the *lease
term* — not an explicit invalidation round trip — is what makes the server willing to allow this),
but the structural argument transfers directly to worker leasing
([paper](https://dl.acm.org/doi/10.1145/74851.74870); summarized in [the morning
paper](https://blog.acolyer.org/2014/10/31/leases-an-efficient-fault-tolerant-mechanism-for-distributed-file-cache-consistency/),
mirrored at [Stanford's technical report
archive](http://i.stanford.edu/pub/cstr/reports/cs/tr/90/1298/CS-TR-90-1298.pdf), CS-TR-90-1298).
The core argument, in the form relevant here:

1. **A lease is a contract with an expiry, not a promise contingent on a liveness check.** The
   granting party (here: the library's storage) does not need to *detect* that the holder (the
   worker) has failed in order to safely act as though it has — it only needs the lease term to
   elapse. This sidesteps the fundamentally hard and, per FLP-style impossibility results,
   *unsolvable-in-general* problem of perfect failure detection in an asynchronous network: you
   cannot reliably distinguish "the worker is dead" from "the worker (or the network to it) is just
   slow," but you *can* reliably say "the time I promised is up."
2. **The term length is the whole design tension**, stated explicitly by Gray & Cheriton as a
   trade-off: a short lease bounds how long the server can be blocked by an unresponsive holder
   (fast reclaim after real failure) but increases the overhead of renewal traffic and increases
   the odds that ordinary slow-but-alive holders get fenced out; a long lease amortizes renewal
   overhead but leaves the server exposed to the holder's failure for longer, and to bogus reclaim
   pressure for shorter.
3. **Clock synchronization between granter and holder is assumed to be bounded but imperfect**, and
   the paper's protocol is explicitly designed to tolerate a bounded clock-drift error term between
   the two clocks rather than assuming they agree exactly — the granter's notion of "expired" and
   the holder's notion of "my lease is about to expire, I should renew" are allowed to disagree by
   up to that bound, and the protocol (renew comfortably before the deadline, not exactly at it) is
   built to absorb it rather than pretend it away. This is the direct ancestor of the "who's clock
   authoritative" question in section 6a: Gray & Cheriton's answer, translated to today's
   vocabulary, is *the granter's clock is authoritative for expiry; the holder must renew with
   margin, not precision.*

### 3.2 Chubby: session leases and the sequencer (fencing token) mechanism

Google's Chubby lock service ([Burrows, OSDI 2006, "The Chubby lock service for loosely-coupled
distributed systems"](https://static.googleusercontent.com/media/research.google.com/en//archive/chubby-osdi06.pdf))
is the paper that turned "leases need a companion fencing mechanism" into a shipped, load-bearing
production API, and it is the direct intellectual ancestor of the fencing-token design this
project needs.

**Session leases and `KeepAlive`.** Every Chubby client holds a session with the master, kept
alive by an outstanding `KeepAlive` RPC. The master deliberately **delays its `KeepAlive` reply
until the lease is nearly due to expire**, then replies with an extension — the default lease
length is **12 seconds**, adaptively lengthened under server load — and the client immediately
issues the next `KeepAlive`, so there is always exactly one outstanding renewal request in flight.
This "delay the reply, don't just accept a ping" design is notably different from a naive
heartbeat: the *reply itself* is the extension grant, arriving right when the client needs to know
the new deadline, rather than an independent unacknowledged ping ([summary with sourced quotes:
Chubby lock service
writeup](https://blog.hieunt.me/blog/chubby-lock-service-for-loosely-coupled-distributed-systems)).

**Jeopardy and grace period on master failover.** If a client's local timer runs out on its
current lease without a `KeepAlive` reply (e.g., because the master crashed and a new one is being
elected), the client does not immediately treat its session as dead — it enters a state called
**jeopardy**: local caches are disabled and the application is warned to quiesce, but the session
is given a further **grace period, default 45 seconds**, to reconnect to a (possibly new) master
and resume `KeepAlive`s. If a `KeepAlive` succeeds within the grace period, the session survives
uninterrupted (a "safe" event); if the grace period elapses with no reply, the session is
irrevocably expired. This two-stage design (lease timer, then a longer independent grace window
specifically to absorb *master* failover, distinct from *client* failure) is a pattern worth
lifting directly: it decouples "my lease with the specific storage node I was talking to expired"
from "my session with the *service* as a whole is dead," which matters enormously for
dag-worker-go once storage is a Postgres/Redis cluster with its own failover, not a single node.

**Sequencers — the fencing token, by name, in 2006.** This is the mechanism this whole document
is building toward. When a Chubby client successfully acquires a lock, it can call
`GetSequencer()` to obtain an opaque byte string — the **sequencer** — that encodes the lock's
name, its mode (exclusive/shared), and a **monotonically increasing lock generation number**. The
client attaches this sequencer to every request it subsequently sends to a *third-party* service
that the lock is meant to protect (Chubby's own worked example is a GFS-style file server
protected by a lock on "who is currently the primary"). That third-party service either calls back
into Chubby's `CheckSequencer()` to validate the token against the current holder, or — more
efficiently, and how it's actually used in practice — simply **remembers the highest generation
number it has seen for that lock and rejects any request bearing a lower one outright**, with no
round trip to Chubby needed on the hot path. This is precisely "compare-and-swap on a per-node
lease epoch," described in a paper from 2006, applied to files instead of DAG nodes. The direct
translation for dag-worker-go: every node acquisition bumps a per-node epoch; every worker
acknowledgment must present the epoch it was handed at claim time; storage rejects any ack whose
epoch does not match the node's *current* epoch, and rejection requires zero coordination with
whatever reclaimed the node — the check is purely local to the storage row/key.

### 3.3 ZooKeeper: session semantics and `zxid`-based fencing

ZooKeeper's session model is close kin to Chubby's (unsurprising — it was explicitly designed as
an open alternative), with the same jeopardy-like asymmetry between client-perceived and
server-perceived session state:

- **The server, not the client, owns expiry.** "Session expiration is managed by the ZooKeeper
  cluster itself, not by the client... Expirations happen when the cluster does not hear from the
  client within the specified session timeout period" ([ZooKeeper Programmer's
  Guide](https://zookeeper.apache.org/doc/r3.4.13/zookeeperProgrammers.html)). This is an explicit,
  deliberate design choice this project should copy: **the authority that declares a lease dead
  must be the storage/service side**, never the client, because a client that believes it is still
  within its lease has no way to know that its clock, GC pauses, or network partition have made
  that belief false.
- **Heartbeat cadence is a fraction of the timeout, with failover built in**: "if the session
  timeout is time t, and if the client has not interacted with the server within 1/3t of time, it
  sends a heartbeat... [if that server appears dead] the client tries to find another server... at
  time 2/3t" — i.e., the client-side heartbeat schedule is itself staged to leave two full retry
  windows (at 1/3 and 2/3 of the deadline) before the deadline is reached, rather than a single
  heartbeat sent right before expiry with no slack for a retry on failure.
- **Ephemeral nodes tie liveness state directly to the session**: "at session expiration the
  cluster will delete any/all ephemeral nodes owned by that session and immediately notify any/all
  connected clients of the change" — this is the mechanism ZooKeeper-based lock recipes use to make
  lock release automatic on failure, and it is architecturally the same idea as "when a node's
  lease expires, the sweeper transitions its status and fires an event," just implemented via
  watches instead of a polling sweeper.
- **`zxid` as a free fencing token**: every ZooKeeper write is assigned a globally, strictly
  increasing transaction ID (the `zxid`) by the leader as part of the atomic broadcast protocol
  that orders all mutations. Because the `Stat` structure returned when an ephemeral lock node is
  created carries the `zxid` of that creation as `czxid`, and `zxid`s are guaranteed monotonically
  increasing across the whole ensemble, a lock holder can hand out `czxid` as a fencing token
  exactly like a Chubby sequencer, with *no extra bookkeeping required* — the ordering primitive
  the consensus protocol already needs to maintain replicated state is reused, for free, as the
  fencing mechanism (documented in the [ZooKeeper recipes
  guide](https://zookeeper.apache.org/doc/r3.1.2/recipes.html); Apache Curator's `InterProcessMutex`
  is the standard client-side implementation). The generalizable lesson: **if your storage backend
  already has a monotonic, per-write sequence number as part of its replication protocol
  (Postgres's `xid`/LSN, a Raft log index, a `zxid`), that sequence number is a free fencing token
  — don't invent a second counter when the storage engine is already handing you one.**

### 3.4 The Kleppmann/antirez Redlock debate — what it actually settles

This exchange (Kleppmann's ["How to do distributed
locking"](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html), 2016, and
antirez's rebuttal ["Is Redlock safe?"](https://antirez.com/news/101)) is widely cited but
frequently mischaracterized as "Redlock is broken, don't use Redis for locks." What it actually
establishes is narrower and more useful:

**Kleppmann's core claim is about fencing, not about Redis specifically.** His central argument is
that *any* lock service — including one that is otherwise perfectly correct — is unsafe **if the
protected resource does not itself enforce a fencing token**: "you need to include a fencing token
with every write request to the storage service... a number that increases every time a client
acquires the lock." His worked failure scenario is a GC pause: client 1 acquires the lock (lease
generation 33), then stalls in a long GC pause past the lease's expiry; client 2 acquires the lock
in the meantime (generation 34) and writes; client 1 resumes, still believing it holds the lock,
and writes too — and *without a fencing check at the storage layer*, client 1's stale write can
land and corrupt state regardless of which lock algorithm granted the lease. His specific critique
of Redlock is that Redlock **has no facility for generating such a token at all** — its lock value
is a random string used only to prove *who requested the unlock*, not a monotonically increasing
number the protected storage can use for CAS. His secondary, more debated critique is that Redlock
(unlike, say, ZooKeeper/`zxid`-based locking or Chubby) additionally depends on **synchronous-system
timing assumptions** — bounded clock drift across the independent Redis instances, bounded network
delay relative to lock TTL — for its *mutual-exclusion* guarantee at all, not just for the
fencing-adjacent guarantee. His recommendation: for anything where correctness genuinely depends on
mutual exclusion, "use a proper consensus system such as ZooKeeper," which gives you `zxid` as a
fencing token essentially for free.

**Antirez's rebuttal does not deny the value of fencing tokens** — he explicitly agrees a
CAS-style token at the storage layer is good practice ("we set its state to `<token>`, then we
operate the read-modify-write only if the token is still the same when we write") — his
disagreement is narrower: (1) he argues many real uses of a distributed lock are "best-effort
efficiency" cases (e.g., avoid redundant work) where losing exclusivity occasionally is a
performance blemish, not a correctness bug, so demanding ZooKeeper-grade rigor for every lock use
case is overkill; (2) on the timing-assumption critique specifically, he points out Redlock
**checks elapsed time both before and after acquiring the majority** — "whatever delay happens in
the network or in the processes involved, after acquiring the majority we check again that we are
not out of time" — narrowing (but, notably, not eliminating, since the check-then-act window itself
can still be preempted by a pause) the exposure window; (3) on clock jumps specifically, he
concedes the point about needing monotonic time sources and argues it's an operational
solvable-in-practice concern (disable NTP step-adjustment on lock-service hosts, use monotonic
clocks), not a fundamental flaw in the algorithm's model.

**What both sides actually agree on, and what to take from this debate for dag-worker-go:**

1. **A lease alone — no matter how correctly the lease-granting protocol itself is implemented —
   does not make concurrent-write safety hold.** The write-time check (the fencing token /
   generation compare) is the thing that actually provides safety; the lease/lock is only what
   determines *who is expected to be able to pass that check right now*.
2. **The fencing check must live at the point of the actual mutation** (dag-worker-go's storage
   layer, on the ack write), not merely at lock-acquisition time, and it must be a single atomic
   compare-and-swap the storage backend can execute unilaterally, without a round trip back to
   whatever granted the lease.
3. **Process pauses (GC, `SIGSTOP`, VM freeze) are not edge cases to be argued away — they are the
   central failure mode fencing tokens exist to make safe**, and no amount of "check the clock
   again right before writing" fully closes the window, because the pause can occur *between* that
   check and the write actually landing. This is why section 6b's design does the epoch check
   **inside the same atomic operation as the mutation itself** (a single Lua script, a single SQL
   `UPDATE ... WHERE epoch = $n`), never as a separate preceding step.
4. dag-worker-go's use case is squarely on the "correctness matters" side of antirez's own
   distinction — a stale worker ack corrupting a node's status is exactly the kind of bug this
   library exists to prevent — so the project should adopt Kleppmann's stricter posture by default:
   fencing tokens are not optional hardening, they are the mechanism that makes the whole "node
   times out and is reassigned" feature safe to ship at all.

### 3.5 Spanner, TrueTime, and why timeouts computed from wall-clock reads are unsafe in general

Google Spanner's TrueTime API ([Corbett et al., OSDI 2012, "Spanner: Google's Globally-Distributed
Database"](https://static.googleusercontent.com/media/research.google.com/en//archive/spanner-osdi2012.pdf))
is the canonical demonstration that **"read the wall clock" is not actually a single well-defined
operation** in a distributed system — it is an operation with an *error bar*, and pretending the
error bar is zero is exactly the class of bug this document is about.

- `TT.now()` does not return a timestamp — it returns an **interval `[earliest, latest]`**
  guaranteed to contain the true absolute time of the call, with the interval's half-width, ε,
  bounded and continuously tracked. In Google's reported deployment, ε forms a sawtooth **between
  roughly 1ms and 7ms** — about 6ms attributable to worst-case clock drift accumulating between
  synchronization polls, about 1ms attributable to communication delay to the time-master
  infrastructure — rather than a fixed constant, because it grows monotonically between
  synchronizations with the time masters and resets on each sync ([summarized with sourced
  figures](https://sookocheff.com/post/time/truetime/); consistent with independent secondary
  analysis at [muratbuffalo's distributed-databases
  series](http://muratbuffalo.blogspot.com/2025/01/use-of-time-in-distributed-databases.html)).
  Time masters combine **GPS receivers** (fast, precise, but capable of correlated failure during
  antenna or receiver issues) with **atomic clocks** ("Armageddon masters," slower-drifting
  standalone backups precisely so that GPS and atomic-clock failures are uncorrelated) as two
  independently-failing time sources cross-checked against each other.
- **Commit-wait**: to assign a transaction a timestamp that is safe to treat as its true
  linearization point relative to every other transaction in the system, Spanner does not just read
  `TT.now()` once — it **waits out the uncertainty interval before allowing the transaction's
  effects to become externally visible**, i.e., it deliberately spends real wall-clock time (up to
  2ε, commonly averaging under 10ms) converting *timestamp uncertainty* into *added latency*, which
  is the only way to make "transaction A's assigned timestamp is truly before transaction B's" a
  fact rather than a probabilistic guess.
- **The generalizable lesson for any lease/timeout system, independent of Spanner's transactional
  context**: a clock reading is never a point value with zero error — it always carries an implicit
  uncertainty determined by how well-synchronized the reading clock is to whatever other clock it
  will be compared against. Google can afford to *measure* and *bound* that uncertainty to single
  digit milliseconds because it operates its own GPS/atomic-clock time-master fleet across its own
  datacenters. dag-worker-go, as a library embedded in arbitrary host processes running on
  arbitrary infrastructure (laptops, cheap cloud VMs, wildly different NTP hygiene across
  instances), has **no such guarantee** and must not assume it — which is precisely the argument for
  making the *storage backend's own clock* (Redis's `TIME`, Postgres's `clock_timestamp()`) the sole
  authority for lease deadlines, discussed concretely in section 6a, rather than trusting whatever
  each library instance's local wall clock says relative to some other instance's.

---

## 4. Clock pathologies: what actually goes wrong in production, concretely

| Pathology | Mechanism | Effect on a naive wall-clock lease | Mitigation |
|---|---|---|---|
| **NTP step correction** | A drifted local clock is corrected by *stepping* (jumping) rather than *slewing* (gradually adjusting), typically after being lagged by more than the ~128ms threshold for continuous slewing or after a long outage; NTP conventionally steps rather than slews once drift exceeds roughly 900 seconds of continuous lag | A backward step can make an already-expired lease look valid again to whichever side just experienced the step; a forward step can make a fresh lease look expired instantly | Never derive a lease deadline from `time.Now()` arithmetic that could itself be stepped; prefer a monotonic clock source (Go's `time.Now()` *does* carry a monotonic reading alongside wall-clock when both are read from the same call and neither side has serialized/deserialized it — see 6a) for *duration* measurement, and treat the storage server's own clock as authoritative for absolute deadlines |
| **VM live migration** | Migrating a running VM between hypervisor hosts pauses the guest's clock relative to real elapsed time; guest clock can measurably lag real time after resuming until corrected, and vendor documentation (VMware) explicitly warns "migrating virtual machine may cause guest operating system clock to fall behind real time" | A worker running inside a migrated VM can believe far less wall-clock time has passed than actually has, so it neither renews its lease on schedule nor perceives it as expired — meanwhile the storage side, unaffected by the migration, correctly expires the lease | Deadline enforcement must live entirely on the storage side (never trust a worker's self-reported "time remaining"); a fencing-token check on the write path makes this scenario safe regardless — the migrated worker's stale-but-earnest completion write is simply rejected |
| **Container pause (`docker pause` / cgroup freezer)** | Uses the Linux **freezer cgroup**, which — unlike `SIGSTOP` — the frozen process **cannot observe or intercept**: "sequences of SIGSTOP and SIGCONT are not always sufficient for stopping and resuming tasks in userspace. Both of these signals are observable from within the tasks we wish to freeze" is exactly why the freezer cgroup exists as a *transparent*, non-catchable suspend ([Linux kernel freezer-subsystem docs](https://docs.kernel.org/admin-guide/cgroup-v1/freezer-subsystem.txt)) | A worker frozen mid-processing resumes with no idea any time passed at all — it has no signal handler to run, no exception to catch, nothing; from its own perspective execution was contiguous | This is the purest form of "the client-side lease timer cannot be trusted, full stop" — there is categorically no way for application code running inside a frozen container to detect the freeze happened, so correctness cannot depend on the worker noticing anything; it must depend entirely on the storage-side deadline plus fencing |
| **GC / scheduler pause** | Kleppmann's own worked example (§3.4): a stop-the-world GC pause, or simply being descheduled by a busy host for an extended period, stalls a process for longer than its lease term | Identical shape to the container-freeze case: the process resumes unaware time has passed and may complete work and write a "success" result using a now-stale lease | Same remedy: fencing token compare-and-swap on the write, never a client-side "is my lease still valid" self-check as the sole gate |
| **Leap seconds** | POSIX time and most system clocks either repeat or smear a second around a leap-second insertion; Google's own public engineering practice is to **smear** leap seconds across a full day rather than stepping, specifically to avoid exactly this class of distributed-timing bug | A raw UTC-based deadline arithmetic scheme could in principle be affected by a repeated second; in practice, smeared implementations avoid discontinuities but non-smeared systems in the same fleet can disagree with smeared ones during the smear window | Reinforces the same conclusion: absolute wall-clock arithmetic performed independently by multiple parties is not safe to rely on for a hard boundary condition; let one clock (the storage server) be the sole decision-maker |

The unifying takeaway across every row of this table: **there is no pause, skew, or clock event a
worker can experience that a storage-side deadline check plus a fencing token cannot render safe,
and there is no amount of client-side cleverness that can make a client's own clock trustworthy
enough to be the sole safety mechanism.** This is not a controversial claim in the literature
surveyed above — Chubby, ZooKeeper, Kleppmann, and (with narrower scope) even antirez all converge
on it.

---

## 5. Design for dag-worker-go

This section answers, precisely and opinionated, the six questions posed in the brief.

### 5a. Monotonic vs. wall clock for deadlines; whose clock rules when they differ

**Server-side (storage) time is authoritative for every deadline decision, full stop.** No library
instance's local clock, monotonic or wall, is ever consulted to decide whether a lease has expired.
Concretely:

- **Redis backend**: every operation that reads "now" for the purpose of comparing against a
  deadline is computed *inside* a Lua script via `redis.call('TIME')`, which "returns the current
  server time as a two-item list: a Unix timestamp and microseconds elapsed in the current second"
  ([Redis `TIME` docs](https://redis.io/docs/latest/commands/time/)) — never passed in as an
  argument computed by the calling library instance's clock. This makes the deadline decision
  entirely a function of one clock (the Redis server's), eliminating cross-instance clock skew as a
  source of false-expiry/missed-expiry bugs by construction.
- **Postgres backend**: every deadline write and comparison uses **`clock_timestamp()`**, never
  `now()`/`CURRENT_TIMESTAMP`/`transaction_timestamp()`. This is not a stylistic preference — `now()`
  is frozen at transaction start ("`now()` is a traditional PostgreSQL equivalent to
  `transaction_timestamp()`... returns the actual current time" is explicitly *not* what `now()`
  does; `clock_timestamp()` is the one that "returns the actual current time, and therefore its
  value changes even within a single SQL statement" — [Postgres docs, Date/Time Functions and
  Operators](https://www.postgresql.org/docs/current/functions-datetime.html)). A long-running
  sweeper transaction that used `now()` to compute "current time" for an expiry comparison would
  compare against a timestamp that is stale by however long the transaction has been open — exactly
  the class of bug this section exists to prevent. `clock_timestamp()` is the Postgres equivalent of
  Redis's `TIME`: a genuine server-side read, immune to transaction duration.
- **In-memory backend**: there is exactly one process's clock in scope (the shared in-memory store
  is, by this project's own design, shared *within* one process across all workers in that
  process), so the authoritative clock is simply that process's `time.Now()` — but even here, use
  Go's **monotonic clock reading** (present by default alongside the wall-clock reading whenever a
  `time.Time` is produced by `time.Now()` and not round-tripped through serialization, per the Go
  runtime's documented time-package design) for computing elapsed-time-since-claim and deadline
  comparisons, since the wall-clock component of that same `time.Time` remains exposed to NTP
  step-correction on that single host, while the monotonic component is defined never to jump
  backward or forward due to such corrections.
- **What a worker's local clock is used for**: essentially nothing safety-relevant. A worker may use
  its own clock to decide *when to send its next heartbeat/extend call* (a scheduling decision,
  where being early or late by some margin is harmless), but never to decide *whether its lease is
  still valid* — that question is always answered by the storage backend's response to the next
  operation the worker attempts, via the fencing check in 5b.
- **Consequence for the public timeout API**: a per-node timeout is specified by the host program as
  a **duration** (e.g., `30 * time.Second`), and the library converts it to an absolute deadline by
  adding it to a server-side "now" read at the moment of the claim — never by asking the calling
  process what time it thinks it is and shipping that absolute value to storage.

### 5b. Rejecting a late worker ack: fencing token / lease epoch, compare-and-swap

Every node carries a **`lease_epoch`** — an unsigned integer, starting at 0, incremented exactly
once every time the node transitions into "claimed by a worker" (whether that claim is a fresh pull
from `ready`, or a sweeper-driven reclaim after a prior lease's timeout). The epoch is Chubby's
sequencer generation number and ZooKeeper's `czxid`, specialized to a single integer field on a
single node because dag-worker-go's storage backends are not themselves running a consensus
protocol that hands out a global sequence number for free — the epoch must be maintained explicitly,
per node, as ordinary application-level state.

**The claim response to the worker includes the epoch it was handed.** Every subsequent
acknowledgment (success or error) the worker sends back must present that exact epoch. The storage
backend's ack-processing operation is a single atomic read-modify-write:

> `UPDATE ... SET status = <new>, ... WHERE node_id = <id> AND lease_epoch = <the epoch presented>
> AND status = 'in_progress'` — if this affects zero rows, the ack is stale (either the lease was
> already reclaimed by the sweeper and re-issued at a higher epoch, or a duplicate/racing ack from
> the same worker already landed) and is **rejected outright**, logged, and dropped. The worker is
> never told to retry such a rejection — retrying cannot help, since the epoch it is holding is
> permanently obsolete.

Real Postgres implementation (schema sketch: `nodes(node_id text primary key, scope text, status
text, lease_epoch bigint, lease_owner text, lease_deadline timestamptz, ...)`):

```sql
-- Claim: pull one ready node in a given scope (see 5d for the batching/index shape).
-- Executed inside a single statement so the epoch bump and the ownership/deadline
-- write are atomic with respect to any concurrent claimer or sweeper.
WITH candidate AS (
    SELECT node_id
    FROM nodes
    WHERE scope = $1
      AND status = 'ready'
    ORDER BY node_id          -- any stable order; see 5d for real ordering by priority/insertion
    LIMIT 1
    FOR UPDATE SKIP LOCKED    -- other concurrent claimers skip a row already being claimed
)
UPDATE nodes n
SET status         = 'in_progress',
    lease_epoch     = n.lease_epoch + 1,
    lease_owner     = $2,                                  -- worker/consumer identity
    lease_deadline  = clock_timestamp() + make_interval(secs => $3)  -- $3 = per-claim timeout override or library default
FROM candidate c
WHERE n.node_id = c.node_id
RETURNING n.node_id, n.lease_epoch, n.lease_deadline;
```

```sql
-- Ack: worker reports success or error, presenting the epoch it was handed at claim time.
UPDATE nodes
SET status     = $3,                 -- 'success' or 'error'
    lease_owner = NULL,
    updated_at  = clock_timestamp()
WHERE node_id    = $1
  AND lease_epoch = $2               -- the fencing check: CAS on the epoch
  AND status      = 'in_progress'    -- also rejects an ack after the sweeper already
                                      -- transitioned this node to error-with-timeout
RETURNING node_id;
-- Zero rows returned  ==>  stale ack, reject. This is the entire safety mechanism:
-- it requires no coordination with whatever reclaimed the node, only a local CAS.
```

`FOR UPDATE SKIP LOCKED` is the standard, well-documented Postgres pattern (available since 9.5)
for exactly this "many workers competing for rows from a queue-shaped table without blocking each
other" access pattern — "SKIP LOCKED tells PostgreSQL to skip rows that are already locked by
another transaction and move on to the next one," which is what lets concurrent claimers avoid
contending on the same candidate row without an external mutex. It is explicitly *not* sufficient
on its own for invariants stronger than "don't hand the same row to two claimers" (e.g., "never more
than N in flight globally" needs a real counter or advisory lock), which does not apply to this
specific claim query but is worth remembering for any future global-concurrency-limit feature.

Real Redis implementation (Lua, single-key-per-node hash plus a per-scope ZSET of deadlines — see
5d):

```lua
-- claim.lua
-- KEYS[1] = "node:{scope}:{id}"          (hash: status, lease_epoch, lease_owner, lease_deadline)
-- KEYS[2] = "deadlines:{scope}"          (zset: score = lease_deadline ms, member = node id)
-- ARGV[1] = lease_ms (per-claim override, or library default resolved by the caller)
-- ARGV[2] = worker/consumer id
local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'ready' then
  return redis.error_reply('not_ready')
end
local t = redis.call('TIME')                       -- server-side clock, never client-supplied
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local epoch = tonumber(redis.call('HINCRBY', KEYS[1], 'lease_epoch', 1))
local deadline = now_ms + tonumber(ARGV[1])
redis.call('HSET', KEYS[1],
  'status', 'in_progress',
  'lease_owner', ARGV[2],
  'lease_deadline', deadline)
redis.call('ZADD', KEYS[2], deadline, ARGV[2] .. ':' .. KEYS[1])  -- member encodes node key for the sweeper
return {epoch, deadline}
```

```lua
-- ack.lua  (fencing CAS on the epoch — the safety-critical operation)
-- KEYS[1] = "node:{scope}:{id}"
-- KEYS[2] = "deadlines:{scope}"
-- ARGV[1] = expected lease_epoch (presented by the worker, from its claim response)
-- ARGV[2] = new status: "success" or "error"
-- ARGV[3] = member string used in the deadlines ZSET for this claim (to remove it)
local current_epoch = redis.call('HGET', KEYS[1], 'lease_epoch')
local current_status = redis.call('HGET', KEYS[1], 'status')
if (not current_epoch) or tonumber(current_epoch) ~= tonumber(ARGV[1])
   or current_status ~= 'in_progress' then
  return 0   -- stale ack: epoch mismatch, or already reclaimed/finalized by the sweeper
end
redis.call('HSET', KEYS[1], 'status', ARGV[2])
redis.call('HDEL', KEYS[1], 'lease_owner')
redis.call('ZREM', KEYS[2], ARGV[3])
return 1
```

Both scripts are single Redis commands from the client's point of view (`EVALSHA`), so both the
claim and the ack are atomic with respect to every other claimer, acker, and sweeper touching the
same node — there is no read-then-write race window in either direction, which is the Redis
equivalent of Postgres's single-`UPDATE`-statement atomicity above.

### 5c. Heartbeat vs. lease-extension API design

Two related but distinct capabilities, both needed, modeled directly on the best parts of Pub/Sub's
and Chubby's designs surveyed above:

1. **`Extend(nodeID, epoch, newDuration) (newDeadline, error)`** — an explicit, worker-initiated
   call, gated by the same epoch fencing check as the ack path (a worker cannot extend a lease it no
   longer holds; the storage-side CAS is identical in shape to the ack CAS in 5b, just setting a new
   `lease_deadline` instead of a terminal status, and it must **fail** rather than silently succeed
   if the epoch has moved on, so a worker's extend call after the sweeper has already reclaimed the
   node gets an unambiguous "you no longer own this" error rather than a false sense of continued
   ownership). Deadline math follows SQS's and Pub/Sub's convention exactly: **the new deadline is
   `server_now + newDuration`, an absolute reset, never additive to whatever deadline was previously
   set** — this avoids SQS's own documented gotcha where stacking multiple `ChangeMessageVisibility`
   calls produces confusing cumulative semantics.
2. **A managed heartbeat loop in the worker-facing SDK, modeled on Pub/Sub's automatic lease
   management, not left to hand-rolled host code.** The host program should not be expected to
   remember to call `Extend` on a timer; the client library that wraps a claimed node hands the host
   a handle whose `Done(status)` method the host calls when actual work finishes, and *the library
   itself* runs a background goroutine that calls `Extend` at some safe fraction of the remaining
   lease (analogous to ZooKeeper's 1/3-then-2/3 staged retry schedule from §3.3, and to Pub/Sub's
   percentile-informed automatic extension from §2.2) until `Done` is called or the process is
   shutting down. This removes an entire class of "the host forgot to heartbeat and its own honest,
   still-working node got timed out from under it" bug reports before they can be filed.
3. **A hard ceiling on total lease lifetime per node claim**, independent of how many times `Extend`
   succeeds, mirroring SQS's 12-hour absolute cap — a configurable `MaxLeaseLifetime` after which
   even a healthy, faithfully-heartbeating worker is forcibly timed out and the node re-issued. This
   protects the DAG from a single pathologically long-running (or silently wedged-but-still-calling-
   Extend) worker holding a node forever, and gives operators a hard, auditable upper bound on "how
   long can this node possibly sit in-progress" independent of any single timeout value.
4. **Heartbeat/extend is deliberately *not* the same signal as "the node's status changed."** Per
   §2.3's Kafka lesson, conflating "the worker process is alive" with "this node's work is
   progressing" is exactly the bug that produced Kafka's original heartbeat-vs-`max.poll.interval`
   split. dag-worker-go has only one timer per node (there is no separate "process liveness" concept
   at all — the library never tracks worker processes as first-class entities, only node leases), so
   this particular conflation risk does not reappear in the same shape, but the API must still keep
   "extend my lease" (a scheduling/liveness signal) and "report status" (a terminal, one-time,
   fenced event) as two distinct call shapes rather than overloading one endpoint for both, precisely
   so that extend-call retries (safe to be idempotent-ish, at-least-once, low-stakes) are never
   confused with ack-call retries (must be exactly-once-effective via the epoch CAS, high-stakes).

### 5d. The timeout sweeper in O(log n), and avoiding duplicate sweeping across instances

**Data shape**: every backend maintains a secondary index ordered by `lease_deadline`, scoped per
DAG scope (never global across all scopes in one process — see this project's scope-namespacing
requirement), so that "find nodes whose lease has expired" is a **range query bounded by the batch
size requested, never a scan of all in-progress nodes**:

- **Redis**: the `deadlines:{scope}` ZSET from 5b's Lua scripts, scored by deadline in epoch-ms.
  `ZRANGEBYSCORE deadlines:{scope} -inf <now_ms> LIMIT 0 <batch>` is the query, and it is `O(log n +
  batch)` by construction of the sorted-set skip-list structure. (If the Redis backend instead
  layers on top of native Streams consumer groups per §2.4, the equivalent is `XAUTOCLAIM` against
  the group, with the caveat noted there that Streams' notion of idle-time must be replaced with an
  explicit per-entry deadline field checked by the sweeper's own logic, since Streams' claim-time
  `min-idle-time` parameter cannot express "this specific node's timeout, chosen when it was
  claimed.")
- **Postgres**: a **partial index** on `lease_deadline` restricted to in-progress rows —
  `CREATE INDEX idx_nodes_lease_deadline ON nodes (scope, lease_deadline) WHERE status =
  'in_progress';` — keeps the index small (proportional to the in-flight count, not the total node
  count) and keeps the sweep query an index range scan:

```sql
-- Sweeper batch reclaim. SKIP LOCKED means concurrent sweeper instances (see below)
-- partition the expired set between themselves for free, with zero coordination,
-- rather than blocking on each other's row locks or double-processing the same rows.
WITH expired AS (
    SELECT node_id, lease_epoch
    FROM nodes
    WHERE scope = $1
      AND status = 'in_progress'
      AND lease_deadline < clock_timestamp()
    ORDER BY lease_deadline
    LIMIT $2                          -- batch size, e.g. 500
    FOR UPDATE SKIP LOCKED
)
UPDATE nodes n
SET status      = 'error',
    error_kind  = 'timeout',
    lease_epoch = n.lease_epoch + 1,   -- fence out the now-abandoned worker's late ack
    lease_owner = NULL
FROM expired e
WHERE n.node_id = e.node_id
  AND n.lease_epoch = e.lease_epoch   -- belt-and-suspenders: re-check epoch hasn't
                                       -- moved between the CTE read and this write
RETURNING n.node_id;
```

- **Memcached** (no native sorted structure): the deadline index has to be maintained as an
  application-level structure, since Memcached exposes only a flat key-value space with per-key
  TTLs and no range queries. The pragmatic design is to **not** try to build an O(log n) index
  inside Memcached at all — instead, use Memcached purely for the hot key-value node records (fast
  reads of current status/epoch) and require a Memcached-backed deployment to *also* run the
  deadline index in whatever the library's default lightweight structure is (an in-process or
  Redis-backed ZSET-equivalent used only for the sweep index), or accept that the Memcached backend
  sweeps via periodic enumeration with a bound on in-flight count as its scaling limit. This is
  worth flagging plainly rather than papering over: **Memcached is structurally the wrong tool for
  an ordered-by-deadline index**, and the honest options are (a) document this as a known limitation
  of the Memcached backend, sized for smaller in-flight counts, or (b) require Memcached deployments
  to pair with a small amount of Redis/Postgres purely for the deadline index while node payloads
  stay in Memcached. Recommendation: **(a)** — keep the Memcached backend honest about its ceiling
  rather than silently hybridizing backends, and document the practical in-flight-count limit it
  was benchmarked against.

**Avoiding duplicate sweeping across multiple library instances.** Two independent, complementary
mechanisms, matching the "does correctness need this, or just efficiency" split that recurred
throughout §3:

1. **Correctness does not require mutual exclusion between sweepers at all**, by construction: the
   sweep-and-reclaim operation is itself epoch-fenced (the Postgres query's `WHERE n.lease_epoch =
   e.lease_epoch` re-check; the Redis reclaim Lua script's re-check of current status/deadline before
   acting, mirroring the ack script in 5b). If two sweeper instances race to reclaim the same expired
   node, the loser's write simply affects zero rows (Postgres) or no-ops after observing the epoch
   already moved (Redis) — this is the exact same idempotent-CAS property that makes `XCLAIM` safe
   under concurrent claimers in §2.4, generalized to the sweep path. **Duplicate sweeping is wasted
   work, never a correctness bug.**
2. **Efficiency (avoiding wasted duplicate work at scale) is handled by lightweight, best-effort
   partitioning, not a distributed lock.** Each library instance derives a consistent-hash-based
   shard assignment over `(scope, deadline-bucket)` or simply over `scope` when the number of
   instances is smaller than the number of active scopes, and each instance's sweeper primarily
   polls only its assigned shards; on failure of a peer instance (detected via a simple heartbeat
   key each instance refreshes, e.g. a short-TTL Redis key or a Postgres row with its own
   `clock_timestamp()`-based staleness check — deliberately the *same* lease-with-server-side-clock
   pattern as the node leases themselves, applied one level up to sweeper ownership), the shard is
   picked up by a survivor. This is explicitly **not** required for correctness (point 1 already
   guarantees safety even with zero coordination) — it exists purely to avoid every instance
   redundantly scanning every scope's deadline index every sweep interval as instance count grows,
   which would turn the sweep cost from "proportional to expired-node count" back toward
   "proportional to instance count times expired-node count." A simple, good-enough starting point:
   hash `scope` mod `known_instance_count` (instance count itself tracked via the same short-TTL
   heartbeat keys), re-sharding only on membership change, accepting brief double-coverage during
   rebalance windows as harmless per point 1 — explicitly avoiding Kafka's stop-the-world eager
   rebalance mistake from §2.3 by making the "assignment" here advisory and race-tolerant rather than
   a hard partition workers must agree on before proceeding.

### 5e. At-least-once vs. at-most-once delivery to workers, and why exactly-once is a lie

**dag-worker-go delivers at-least-once, by design, matching every system surveyed in §2** (SQS,
Pub/Sub, Kafka, Redis Streams all make the identical choice, and none of them offer a true
exactly-once *delivery* guarantee to the consumer — Pub/Sub's "exactly-once delivery" feature, per
its own documentation, only strengthens ack-deadline *enforcement*, and even Kafka's
"exactly-once semantics" is scoped to producer-side idempotence and transactional stream processing
*within Kafka*, not to guaranteeing an external side effect triggered by a consumer happens exactly
once). The reasons this is the only honest choice, not a compromise:

1. **The fundamental problem is the Two Generals Problem**, restated for this context: for a worker
   to be delivered a node *exactly* once, the library would need certainty that the worker received
   and is about to act on the claim message, but that certainty itself would require an
   acknowledgment from the worker, which itself could be lost, requiring a further acknowledgment of
   the acknowledgment, ad infinitum — there is no message count that closes the loop with certainty
   over an unreliable channel. This is a proven impossibility result for message delivery over a
   channel that can drop or delay messages, not an engineering gap that better tooling could close.
2. **Concretely in dag-worker-go's design**: a claim can be handed to a worker, and the response
   carrying that claim can be lost on the way back to the worker (network partition, worker restart
   between receiving and processing) — the library cannot distinguish "the worker never received the
   claim" from "the worker received it and is now working." If it times out and re-issues the node
   to a second worker (favoring liveness), and the first worker was in fact alive and does eventually
   finish and try to ack, that ack must be **safely rejected** by the fencing check in 5b — this is
   at-least-once delivery of the *node*, with the epoch fence converting the resulting "delivered
   twice" outcome into "processed at most once with observable effect," which the literature calls
   **effectively-once**: at-least-once delivery combined with an idempotency/fencing mechanism at the
   point of effect, which is achievable, versus true exactly-once *delivery*, which provably is not.
3. **At-most-once is available as an opt-in, lossy alternative, not the default**: a host program
   that would rather risk a node silently never being retried than risk any double-delivery race
   window can configure zero retries after a claim is issued (treat any timeout as terminal failure,
   no re-issue) — but this must be an explicit choice the host makes per-scope or per-node-type,
   never the library's default behavior, because silently dropping DAG nodes on ambiguous failures
   is a far worse default for a workflow engine than occasionally handing the same node to two
   workers and relying on the fencing token to keep that safe.
4. **What the library must guarantee, stated precisely, is**: at-least-once *delivery* of the "take
   this node" event to *some* worker eventually (modulo the host's own retry/backoff policy), and
   **at-most-once *acceptance* of a terminal status transition** per node per epoch — i.e., exactly
   one of the (possibly multiple) workers who were ever handed a given claim epoch can successfully
   record success or error for it, enforced by the CAS in 5b. This precise, narrower guarantee
   ("effectively-once effect, at-least-once attempt") is what the library should document and
   promise, rather than the unqualified and untrue "exactly-once" phrase.

### 5f. Clock jumps, VM migration, and container pauses (the `SIGSTOP`/freezer problem)

Directly applying §4's survey to the mechanisms designed in 5a–5d:

- **Because deadline authority lives entirely in the storage backend's own clock (5a) and because
  the write-time fencing check (5b) — not a client-side "am I still within my lease" self-belief —
  is what gates every state transition, none of the pathologies in §4's table require special-case
  handling in the protocol.** A worker that resumes from a container freeze, a GC pause, or a VM
  migration with no idea time has passed will simply have its late ack rejected by the epoch CAS if
  the lease already expired and was reclaimed — the exact same code path as any other stale ack,
  with no separate "detect a clock anomaly" logic needed anywhere. This is the payoff of taking
  Kleppmann's and Chubby's fencing-token lesson seriously rather than trying to detect or compensate
  for pauses directly (which, per the freezer-cgroup docs cited in §4, is provably impossible for the
  frozen process itself to do).
- **What the library should still do, as defense in depth and better observability, not as the
  primary safety mechanism**: surface the rejected-stale-ack event distinctly in the status/event
  stream (a `LateAckRejected` event, separate from ordinary `Error`/`Timeout` events) so host
  operators can distinguish "a worker's task genuinely errored" from "a worker's environment was
  paused/migrated long enough to blow past its lease," which is valuable operational signal (e.g.,
  to tune lease durations upward for workloads running on infrastructure known to migrate VMs
  frequently) even though it changes no correctness behavior.
- **Do not build clock-skew compensation into the protocol** (e.g., accepting an ack "close to" the
  deadline with some fudge factor) — per §3.5's Spanner lesson, any such fudge factor is itself just
  an unmeasured, undocumented epsilon, and the entire point of routing all deadline logic through
  the storage backend's own clock is to avoid needing to reason about cross-clock epsilon at all in
  the common case. The one place a deliberate, documented margin *is* appropriate is the
  heartbeat/extend scheduling in 5c (extend well before the deadline, not at it) — a scheduling
  margin chosen for robustness, not a correctness-load-bearing tolerance window.
- **Recommended default lease/timeout floor**: given container-freeze and VM-migration pauses are
  commonly observed in the seconds-to-tens-of-seconds range in cloud environments, and per-node
  default timeouts in this kind of system typically need to be workload-appropriate rather than
  protocol-mandated, the library should ship a conservative **default of low tens of seconds** (in
  the same neighborhood as SQS's 30-second and ZooKeeper's typical tens-of-seconds session timeout
  defaults, both surveyed above) with the explicit, prominent, per-node override this project's
  brief already requires — a single global default is not a safety mechanism, it is an ergonomic
  starting point, and the fencing token is what actually makes any chosen value safe to get "wrong."

---

## Recommendations for dag-worker-go

1. **Ship the fencing token (`lease_epoch`) as a non-optional, foundational field on every node from
   day one**, not as a v2 hardening pass — per §3.4, a lease without a storage-side fencing CAS is
   not a weaker version of a safe design, it is a different, unsafe design, and retrofitting it later
   means every existing ack-handling code path in every backend must be revisited.
2. **Make every deadline comparison a server-side clock read** (`redis.call('TIME')` inside Lua;
   `clock_timestamp()` inside Postgres SQL, never `now()`), and treat the in-memory backend's
   monotonic clock reading the same way — codify this as a lint-checkable rule (e.g., grep-ban
   `now()` in `.sql` files under the Postgres backend package, and CI-fail on it) given how easy it
   is for a future contributor to reach for the wrong Postgres time function.
3. **Model the worker-facing API on Pub/Sub's automatic lease management (§2.2) and ZooKeeper's
   staged renewal schedule (§3.3), not on "the host must remember to call extend."** The SDK owns the
   heartbeat loop; the host only reports terminal completion.
4. **Design the ack and extend endpoints as two distinct RPC shapes from the start**, per §2.3's
   Kafka lesson about conflating liveness signals with progress signals — even though this library
   has only one timer per node today, keeping the call shapes separate avoids foreclosing a future
   "worker-process-level liveness" feature from accidentally reusing (and thereby weakening) the
   node-ack fencing path.
5. **Treat the sweeper's cross-instance coordination as a pure efficiency optimization, never a
   correctness dependency** — build and ship the epoch-fenced, coordination-free sweep first (safe
   with zero instances aware of each other), and add the consistent-hash/shard-heartbeat
   optimization from §5d as a follow-up once benchmarking at 1M nodes across multiple instances shows
   redundant-sweep overhead actually matters; do not build the coordination layer before establishing
   it is needed, since an incorrect coordination layer is strictly worse than a slightly wasteful
   correct one (this generalizes the Redlock debate's real lesson from §3.4: don't reach for a
   heavier consensus mechanism than the correctness requirement actually demands, but never let
   "efficiency layer" quietly become "the thing correctness now silently depends on").
6. **Document the delivery guarantee precisely as "at-least-once delivery, at-most-once accepted
   effect per lease epoch" and never claim exactly-once**, per §5e — get ahead of the inevitable
   GitHub issue asking "why did my worker get the same node twice" with documentation that explains
   the fencing token is the actual safety mechanism, delivery duplication is expected and safe.
7. **Flag the Memcached backend's lack of an ordered-by-deadline structure as a documented scaling
   limit, not a hidden gap** — decide explicitly (recommend: yes) to bound and publish the maximum
   practical in-flight-node count for the Memcached backend based on 1M-node benchmark results,
   rather than silently degrading to O(n) enumeration under load.
8. **Borrow Kafka's static-membership idea (§2.3, KIP-345) for the worker SDK**: give workers a
   stable, host-supplied identity so a worker that restarts within its own outstanding leases' remaining
   time can reattach to (or at least be clearly attributed in events for) leases it previously held,
   rather than every reconnect being indistinguishable from a brand-new anonymous worker — useful
   operationally even though it changes no correctness guarantee.
9. **Set the library-default per-node timeout in the low tens of seconds** (§5f) and make the
   per-claim override loud and obvious in the API (a required-looking parameter, not a buried option),
   since the brief's own requirement that timeout be "settable per node at the moment the worker
   takes it" signals the project owner already expects defaults to be routinely overridden per
   workload.

## Open questions

1. **Exactly how should the epoch be exposed to the worker, and how tamper-resistant does it need to
   be?** A plain integer returned alongside the claim is simplest, but if untrusted or
   third-party-hosted workers are ever a target deployment shape, an opaque signed/HMAC'd token
   (bundling node ID + epoch + scope) may be warranted so a worker cannot simply guess or forge a
   higher epoch — this trades simplicity for a security property the current brief does not clearly
   require; needs a decision on the library's trust model for workers.
2. **What is the actual measured overhead of `FOR UPDATE SKIP LOCKED` plus a partial index at 1M
   in-flight rows under high claim/ack churn on Postgres**, versus the Redis Lua-script approach —
   this document asserts both are O(log n)-shaped by construction, but the *constant factor* at the
   million-node benchmark target this project has committed to is an empirical question the
   benchmark suite (not this research pass) must answer, and it may argue for different default
   batch sizes per backend.
3. **Should the cross-instance sweeper-shard-heartbeat mechanism (§5d point 2) itself be built on the
   in-memory backend's own primitives when all instances share one process** (the "shared in-memory
   store" case this project's brief calls out specially), or does the single-process case simply skip
   sharding entirely since there is only ever one sweeper loop to schedule regardless of worker
   count — likely the latter, but worth confirming it doesn't quietly reintroduce a redundant-sweep
   cost inside a single process across many goroutines.
4. **How should `MaxLeaseLifetime` (the hard per-node ceiling from §5c point 3) interact with a node
   whose *host-configured* per-claim timeout is deliberately very long** (e.g., a node representing an
   hours-long external batch job) — is the ceiling a multiple of the requested timeout, a fixed
   library-wide constant, or configurable per scope? SQS's fixed 12-hour absolute cap is a clean
   precedent but may be wrong for this project's stated goal of supporting arbitrarily-defined,
   host-controlled node semantics.
5. **Does the Redis backend commit to Streams-native consumer groups (§2.4) or to the
   hand-rolled hash-plus-ZSET design used in this document's Lua examples (§2.5/§5b–d)?** Streams
   gets PEL bookkeeping, `XAUTOCLAIM`'s cursor-based batching, and delivery-count poison-pill
   detection for free, but its idle-time model doesn't natively express a caller-chosen deadline set
   at claim time (§2.4's stated gap) — a hybrid (Streams for the event/notification fan-out this
   project's brief separately requires, hash+ZSET for the authoritative lease state) is plausible but
   adds real complexity; this needs a dedicated design pass, likely in the storage-backend-specific
   research document rather than this one.
6. **What is the right behavior when the *sweeper itself* cannot reach storage** (network partition
   between a library instance and, say, its Postgres primary) — does that instance simply stop
   sweeping (safe, since other instances or the eventual primary's own recovery will reclaim once
   reachable) or does it need its own alerting path distinct from ordinary node-timeout events, given
   a fully partitioned sweeper is functionally equivalent to "no sweeper is running" for however long
   the partition lasts?
