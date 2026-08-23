# ADR-0013: Cross-instance work distribution v1 is pull-based competition

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/07-work-distribution-across-instances.md §1, §1.2, §5.4, §6,
  §7.1, §7.3; docs/research/01-prior-art-workflow-engines.md §15; docs/research/05-redis-backend.md
  §9.1; docs/research/04-postgres-backend.md §14.1

## Context

The library must let N processes of dag-worker-go run against the same shared Redis or PostgreSQL
storage and divide up the pool of ready nodes without double-dispatching, without a membership
protocol, and without a rebalancing storm every time an instance joins or leaves. Every production
system surveyed — SQS's visibility timeout, Sidekiq's `BRPOP`, Asynq's Lua dequeue, River's
`SELECT … FOR UPDATE SKIP LOCKED` — solves this with the same primitive: one atomic storage
operation that both selects and marks a unit of work claimed, with zero external coordinator (07
§1.1). This is "pure pull / competing consumers," and it is the correct v1 baseline because it is
the only strategy every mandatory backend (in-memory, Redis, PostgreSQL) implements natively — see
AMD-1: `ReadyQueue` and `TimeoutSweeper` are mandatory core, not optional facets, precisely because
a backend that cannot express this atomic claim is not usable by this library at all.

Pull-based competition has a known, quantified ceiling (07 §6). Redis is single-threaded per key;
a claim that fuses pop-plus-lease-bookkeeping via Lua lands in the tens-of-thousands-of-claims/sec
range against one hot list/ZSET before the key's serialization becomes visible. PostgreSQL's
`SKIP LOCKED` turns contention into wasted scan CPU rather than blocking, but real numbers are far
lower: Graphile-worker tops out around 100-200 jobs/sec on typical hardware before lock contention
dominates, and a benchmark comparing naive polling to `SKIP LOCKED` measured only a ~28% throughput
gain from the primitive itself — the bulk of the cost is elsewhere (round trips, autovacuum lag).
Postgres is 2-3 orders of magnitude below Redis for raw claim throughput on one table (07 §6.2).
Pull-based competition also gives up locality: an instance claims node 3 of one DAG on one tick and
node 3,004,102 of an unrelated DAG the next, so no per-DAG in-memory index survives across claims
(07 §1.2). Both costs are accepted deliberately for v1 in exchange for zero membership dependency,
because the escalation path (ADR-0014) is designed in from day one specifically so this ceiling is
never a dead end.

AMD-2 replaces the old `Transition(from, to)` primitive with fenced operations whose shapes this
ADR's claim path is defined in terms of: `Claim` grants ownership and a lease deadline in one
write; `Complete` (the engine's internal name for the fenced Ack/Nack path) returns the successor
nodes that just became ready in the *same* atomic operation, so a competing instance's next `Claim`
sees them without a second round trip. AMD-4 specifies the wakeup path for a blocking `Claim` so
that pull-based competition does not degenerate into a busy-loop hammering the same hot key that
§6's throughput numbers already show is the scarce resource.

## Decision

**v1 cross-instance work distribution is pure pull-based competition on the storage backend's
mandatory atomic claim primitive. There is no partitioning, no membership table, and no leader
involved in dispatch.** Every instance, for every `(scope, kind)` it services, calls the same
`Store.Claim` primitive directly against one shared ready-set:

```go
// Claim is the ONLY primitive that ever hands a node to a caller. It is
// mandatory core (AMD-1) — every backend must implement it natively.
// It grants ownership, bumps the fencing epoch, and sets the lease
// deadline in one atomic write; it never requires a second call to become
// safe against a paused-not-dead caller (ADR-0006/ADR-0007).
func (s Store) Claim(ctx context.Context, scope string, req ClaimRequest) (Claimed, error)

// Complete performs the fenced Ack/Nack write AND returns every direct
// successor that just became ready in the SAME atomic operation, so the
// engine emits events (and the ready-set gains entries) without a second
// round trip against the backend (AMD-2).
func (s Store) Complete(ctx context.Context, token ClaimToken, outcome Outcome) (readied []NodeRef, err error)
```

Backend-native atomics back this directly and are never emulated as a lowest-common-denominator
loop: Postgres uses one `SKIP LOCKED` CTE chained into an `UPDATE … RETURNING` (04 §14.1); Redis
uses one Lua Function doing `ZPOPMIN` plus hash mutation plus `ZADD` into the deadline ZSET (05
§9.1, §15.2); in-memory uses one mutex-guarded pop plus slab write. `SKIP LOCKED` order is
best-effort under contention (04 §14.1's own caveat: a locked highest-priority row is skipped, so
strict ordering is soft, never a hard guarantee) — this is accepted as part of choosing pull
competition, and is unaffected by anything in this ADR.

**Blocking `Claim` follows the wakeup path fixed by AMD-4, not a bespoke design per backend:**
1. An immediate, non-blocking `TryClaim` against the shared ready-set.
2. On empty, wait on the `EventReady` doorbell for the requested `(scope, kind)`, bounded by the
   caller's context.
3. A jittered poll timer runs concurrently as the fallback for backends whose doorbell is
   best-effort (Redis pub/sub, Postgres `LISTEN`/`NOTIFY` — both documented as best-effort in the
   capability matrix), so a missed or coalesced doorbell signal is a latency problem, never a
   liveness bug.

Every instance's failure or departure needs **zero protocol**: because no work was ever statically
assigned to it, there is nothing to reassign. Only its in-flight leases need reclaiming, and that
is the fenced `Sweep` path (AMD-2), which is unconditionally safe against duplicate execution by
any number of instances because it is itself fenced — this is the same zero-coordination-by-
construction property ADR-0015 relies on for the maintenance sweep.

This is the permanent v1 baseline: `P = 1` is not a placeholder value plugged into a partitioning
scheme, it is the literal absence of one. ADR-0014 documents the interface boundary that lets a
future virtual-partition scheme replace this without changing anything a caller sees.

## Consequences

### Positive
- Zero membership dependency to run multiple instances correctly from the first release — no
  heartbeat table, no consensus cluster, no gossip protocol (07 §5.1-§5.3 all rejected as v1
  requirements).
- Self-balancing by construction: an instance with spare capacity naturally claims more; nothing
  needs rebalancing because nothing was statically assigned (07 §1.2).
- Uniform across every mandatory backend because `ReadyQueue`/`TimeoutSweeper` are mandatory core
  (AMD-1) — there is no backend on which this design silently degrades to a weaker emulation.
- A single-instance deployment is not a special case: it is this same design with a live-instance
  count of one.

### Negative
- Hard throughput ceiling per `(scope, kind)`: tens of thousands of claims/sec on Redis, roughly
  100-200/sec baseline on Postgres before `SKIP LOCKED` contention and autovacuum lag dominate (07
  §6.1-§6.2). Operators running Postgres at high claim rates must tune
  `autovacuum_vacuum_scale_factor` aggressively on the node table specifically, or accept the
  documented degradation (14× dead-tuple growth, ~35% throughput drop under vacuum lag).
- No locality: every claim is a cold round trip; no per-instance in-memory topological/priority
  index survives across claims the way ADR-0014's owned-partition design would provide.
- One oversized scope can pin the shared claim throughput regardless of how many instances are
  otherwise idle — this is the head-of-line problem ADR-0014 exists to fix, not something this ADR
  mitigates.

### Neutral
- The mandatory fencing epoch (present on every claim regardless of this ADR) is what makes this
  design's total absence of membership tracking safe: a dead instance's held lease simply times
  out and is reclaimed by whoever claims next, with no special-casing for "who used to own this."

## Alternatives considered

- **Static partitioning by scope** (07 §2): rejected as the default — scope-size skew is a
  certainty in a multi-tenant DAG library, and a hard scope-to-instance mapping cannot borrow idle
  capacity across scopes, reproducing Kafka consumer groups' own documented head-of-line problem
  at coarser grain (07 §2, §7.2).
- **Virtual partitioning with HRW routing, shipped in v1** (07 §3, §7.1): rejected as premature —
  it requires a membership substrate this ADR deliberately avoids for the first release, and it
  needs a correctness floor (this ADR) to sit underneath it before the swappable assignment
  interface (ADR-0014) has anything to validate against.
- **Leader-elected single dispatcher** (07 §5.5): rejected as the dispatch mechanism — Airflow's
  pre-2.0 single-scheduler architecture is the direct, documented case study of this cap being hit
  and re-architected away from as DAG count grew; reserved instead for maintenance only (ADR-0015).
- **Consensus-backed claim coordination (Raft/ZooKeeper)**: rejected — a second stateful cluster
  is a disproportionate operational cost for a decision (who claims next) whose failure mode, once
  fenced, is merely a wasted attempt, never corruption (07 §5.2).

## References

- [River — `SELECT … FOR UPDATE SKIP LOCKED`](https://riverqueue.com/blog/announcing-river)
- [Netdata — SKIP LOCKED for queues](https://www.netdata.cloud/academy/update-skip-locked/)
- [Asynq `rdb.go` — Lua dequeue script](https://github.com/hibiken/asynq/blob/master/internal/rdb/rdb.go)
- [AWS SQS — visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [Apache Airflow — scheduler HA docs](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html)
- docs/research/07-work-distribution-across-instances.md §1, §5.4, §6, §7
- ADR-0033 (the blocking `Claim` wakeup protocol, fully specified — this ADR states only the
  three-stage shape); ADR-0038 (the fenced `Claim`/`Complete`/`Extend`/`Sweep` primitive
  signatures this ADR's claim path is defined in terms of, amending ADR-0016)
