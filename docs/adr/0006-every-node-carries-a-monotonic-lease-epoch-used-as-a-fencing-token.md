# ADR-0006: Every node carries a monotonic lease epoch used as a fencing token

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/03-leases-heartbeats-timeouts.md §3.2-3.4, §5b, Recommendation 1; docs/research/01-prior-art-workflow-engines.md §16, §18; docs/research/06-memcached-and-storage-abstraction.md (Temporal `RangeID`, §B.2); docs/research/10-event-bus-and-delivery-semantics.md §2.3 (Kafka `(PID, epoch, sequence)`)

## Context

A lease-based work-assignment system fails in exactly two asymmetric ways (03 §1): **false expiry**
(the storage layer reclaims a node while the worker holding it is still alive and working, so two
workers now believe they own it) and **missed expiry** (a dead worker's node is never reclaimed, so
the DAG stalls). Missed expiry is a liveness annoyance, visibly wrong and safe to fix by widening a
timeout. False expiry is correctness-critical: if the slow worker's eventual write lands after the
new worker's write, it can silently corrupt the node's final state. Every mechanism surveyed —
Gray & Cheriton's original lease paper, Chubby's session leases, ZooKeeper's session model, Kafka's
consumer-group liveness — converges on the same conclusion, stated most sharply by Kleppmann: **you
cannot make false expiry impossible with timeouts alone.** Clocks skew, GC pauses run to "several
minutes" (01 §16), VM live migration lags the guest clock, and the Linux freezer cgroup suspends a
process in a way it categorically cannot observe or catch (03 §4) — none of these are edge cases to
argue away; they are the exact failure modes fencing tokens exist to make safe.

The mechanism every mature system independently re-derives is a **monotonically increasing token**
that the protected write must present, checked at the point of the actual mutation, not at
lease-acquisition time. Chubby's `GetSequencer()` returns an opaque byte string encoding "a lock
generation number," and a protected resource "simply remembers the highest generation number it has
seen... and rejects any request bearing a lower one outright" — zero round trip back to the lock
service required (03 §3.2). ZooKeeper gets the same property for free from `zxid`, since every write
is already assigned a globally strictly increasing transaction ID by the consensus protocol (03
§3.3). Kafka's idempotent producer deduplicates on the tuple `(PID, epoch, sequence number)` — an
epoch bump is exactly how Kafka fences out a zombie producer instance (10 §2.3). Temporal's history
shards track an in-memory `RangeID`, "a monotonically increasing generation number used for
fencing... ensuring only one instance can write to a shard," checked on every persistence call, with
a stale one rejected as a shard-ownership-lost error rather than silently applied (06, Temporal
`RangeID`). dag-worker-go's storage backends run no shared consensus protocol that would hand out
such a number for free — none of in-memory, Redis, or Postgres gets a `zxid`-equivalent as a side
effect of something else it already does — so the token must be maintained explicitly, per node, as
ordinary application state, specialized to a single integer field rather than a whole-shard/whole-
lock generation number.

Kleppmann's Redlock critique (03 §3.4) supplies the strongest single argument for making this
mandatory rather than an optional hardening pass: his worked failure scenario is a client that
acquires a lock, stalls in a GC pause past the lease's expiry, and later resumes and writes as if it
still holds the lock — and "without a fencing check at the storage layer, [the] stale write can land
and corrupt state regardless of which lock algorithm granted the lease." Both sides of the
Kleppmann/antirez debate agree on this point even where they disagree elsewhere: a lease alone,
however correctly granted, never makes concurrent-write safety hold; only a write-time compare does
(03 §3.4, point 1). dag-worker-go's storage *is* the authoritative state of the DAG — unlike SQS,
which usually sits in front of an already-idempotent consumer, a stale write here is not redundant,
it is a violation of the single-writer invariant the whole library exists to guarantee.

## Decision

Every node carries an unsigned, monotonically increasing **`LeaseEpoch uint64`** field, present at
all times (zero means "never claimed"; the last-observed value persists even after the node leaves
`InProgress`). The epoch increments **exactly once**, as part of the same atomic write as the
mutation, on exactly the two transitions that hand a node to a worker:

- **T6** — a fresh `Claim` grant (ADR-0007) pulls a `Ready` node and bumps its epoch.
- **T10** — a `Sweep`-driven reclaim after the previous epoch's deadline elapses (ADR-0007, ADR-0008)
  bumps the epoch again before re-admitting the node to `Ready`.

No other write path may increment it. Every subsequent write against a claimed node —
`Complete`/`Ack`/`Nack`, `Extend` (ADR-0010) — must present the exact epoch it was handed, and the
storage backend performs the compare **inside the same atomic operation as the mutation**, never as
a preceding check that a pause could race:

```go
// Storage-port shape (ADR-0007). The epoch check and the mutation are one
// atomic operation on every backend — never read-then-write.
Complete(ctx context.Context, token ClaimToken, outcome Outcome) (readied []NodeRef, err error)
Extend(ctx context.Context, token ClaimToken, d time.Duration) (deadline time.Time, err error)
```

Zero rows affected (Postgres `UPDATE ... WHERE lease_epoch = $2`; Redis Lua re-check of the hash
field; in-memory compare-under-mutex) means the presented epoch is stale — reject outright, log it,
and never tell the caller to retry, since retrying with the same epoch cannot succeed (ADR-0009).

Backend realizations: **in-memory** — a `uint64` guarded by the node's shard `RWMutex` (ADR-0028);
**Redis** — `HINCRBY node:{scope}:{id} lease_epoch 1` inside the claim/reclaim Lua Function, alongside
the hash/ZSET mutation in the same script; **Postgres** — `lease_epoch = lease_epoch + 1` inside the
claiming `UPDATE ... RETURNING`. The epoch is exposed to the worker as `Claim.LeaseEpoch` (public
API) and is what every backend's `ClaimToken` opaquely carries (ADR-0035 explains why `ClaimToken`
stays an opaque type despite the epoch underneath being a plain integer). The same field also serves
as the retry-attempt counter — see ADR-0011 for why one field does both jobs.

`LeaseEpoch` is part of the storage port's **mandatory** core: a backend that cannot maintain and
fence on this field per node does not implement `dagstore.Store` at all, full stop — there is no
reduced-fencing capability tier, and no backend is usable without it (this is why memcached is
excluded from `Store` entirely, ADR-0017).

## Consequences

### Positive

- Closes the entire false-expiry correctness class with one cheap integer compare, co-located with
  the mutation it protects — no second round trip, no external lock service.
- Reused as the retry-attempt counter (ADR-0011): one field, two jobs, no separate lease-UUID scheme.
- Sweeper coordination becomes a pure efficiency question, never a correctness one: two sweepers
  racing to reclaim the same node have the loser's write affect zero rows, by construction (03 §5d).
- Identical mechanism across all three backends means the conformance suite (ADR-0018) can test one
  behavioral contract instead of three different safety stories.

### Negative

- Every node record permanently carries an extra `uint64`, and every ack/extend/sweep code path in
  every backend must thread it through the mutation's `WHERE`/CAS clause — a single omitted check in
  a new backend silently reopens the exact race this ADR closes.
- A plain, unsigned integer is forgeable by a worker that wants to (ADR-0035 names this trade-off
  explicitly and accepts it under the project's cooperative-worker trust model).
- Retrofitting this after shipping without it would touch every existing ack path in every backend —
  which is precisely why it ships mandatory from day one rather than as a v2 hardening pass.

### Neutral

- Durability of the epoch is a property of the backend it's stored in (Postgres WAL, Redis's own
  persistence configuration), not of this ADR — this ADR only fixes the field's semantics and CAS
  discipline, not the backend's crash-durability guarantee.
- `uint64` overflow is not a practical concern at any realistic per-node claim count; no wraparound
  handling is specified because none is needed.

## Alternatives considered

**A separate UUID-per-claim lease ID instead of a monotonic integer.** Rejected: it forfeits the
free "epoch also equals attempt count" property ADR-0011 relies on, needs an auxiliary index to
determine "is this the current one," and a monotonic-integer compare is cheaper for every backend to
execute than string/UUID equality on the hottest write path in the system.

**HMAC-signed or otherwise cryptographically protected token from day one.** Rejected for v1 per
ADR-0035: the project's cooperative-worker trust model does not require defending against a worker
that deliberately forges a token, and building the protection unused contradicts this project's
general bias against speculative complexity (mirrored in ADR-0012's rejection of an unrequested retry
budget). The opaque `ClaimToken` wrapper (ADR-0016) keeps this addable later without a wire break.

**Rely on a plain `status = 'in_progress'` check with no epoch**, as SQS's opaque `ReceiptHandle` and
Redis Streams' PEL delivery counter effectively do. Rejected: SQS's own documentation is explicit
that it offers zero fencing help to the application layer (03 §2.1), and Streams' delivery counter is
a poison-pill *hint*, not a CAS token (03 §2.4) — neither prevents the GC-pause double-write scenario
Kleppmann's critique targets, because neither field is checked atomically with the mutation it would
need to protect.

**A Redlock-style external distributed lock service.** Rejected: Kleppmann's critique (01 §16, 03
§3.4) shows a lock service without a fencing check at the *protected resource* does not help, and
this project has no consensus protocol (ZooKeeper's `zxid`, Raft's log index) to borrow a free
fencing counter from — the token has to be maintained explicitly per node regardless of what grants
the lock, so introducing an external lock service adds an operational dependency for no closed gap.

## References

- Martin Kleppmann, "How to do distributed locking" — https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
- antirez, "Is Redlock safe?" — https://antirez.com/news/101
- Burrows, "The Chubby lock service for loosely-coupled distributed systems" (OSDI 2006) — https://static.googleusercontent.com/media/research.google.com/en//archive/chubby-osdi06.pdf
- ZooKeeper Programmer's Guide (session semantics, `zxid`) — https://zookeeper.apache.org/doc/r3.4.13/zookeeperProgrammers.html
- ZooKeeper recipes (fencing via `czxid`) — https://zookeeper.apache.org/doc/r3.1.2/recipes.html
- Temporal history-service architecture (`RangeID`) — https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md
- Temporal shard-ownership assertion issue #3135 — https://github.com/temporalio/temporal/issues/3135
- Conduktor, "Exactly-once semantics in Kafka" (`(PID, epoch, sequence)`) — https://www.conduktor.io/glossary/exactly-once-semantics-in-kafka
- Linux kernel freezer-subsystem docs (non-catchable suspend) — https://docs.kernel.org/admin-guide/cgroup-v1/freezer-subsystem.txt
- docs/research/03-leases-heartbeats-timeouts.md §1, §3.2-3.4, §4, §5b
- docs/research/01-prior-art-workflow-engines.md §16, §18
- docs/research/06-memcached-and-storage-abstraction.md (Temporal `RangeID` section, §B.2)
- docs/research/00-synthesis.md ADR-06 seed, §2.2-2.3
