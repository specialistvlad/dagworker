# ADR-0015: Leader election is reserved for maintenance, never for dispatch

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/07-work-distribution-across-instances.md §5.1, §5.2, §5.5,
  §7.1 Layer 1, open questions; docs/research/01-prior-art-workflow-engines.md §3.2-§3.3;
  docs/research/00-synthesis.md §10.6

## Context

Apache Airflow's pre-2.0 architecture ran exactly one active scheduler; HA was bolted on later
(AIP-15) precisely because a single scheduler process parsing every DAG and issuing every task
instance became the throughput ceiling as DAG count grew (07 §5.5). This is a documented,
architectural case study of what happens when a system's *work-dispatch* path is gated by an
elected leader: throughput is capped at whatever one process, one core-bound loop, and one
connection pool can push, and the only way to scale it is to make that one instance faster, never
to add instances. ADR-0013 and ADR-0014 exist specifically so dag-worker-go never has this cap.

Leader election is nonetheless a real, useful tool for exactly one thing: a periodic, low-frequency
maintenance task whose correctness is easier to reason about with one actor than with N racing
actors deduplicating via fencing — the timeout sweep's cross-instance shard rebalancing (07 §5.5).
Two dossiers (03, 07) state plainly that correctness never requires sweeper coordination at all —
the fencing epoch alone makes a duplicate sweep merely wasteful, never wrong — while a third (04)
raises leader election for the Postgres sweeper as an open question. The synthesis resolves this
explicitly (§10.6): 04's own advisory-lock section frames election as *"strictly cheaper… when the
only thing being coordinated is 'run this loop somewhere,'"* i.e. a pure efficiency choice, matching
03 and 07 exactly. There is no contradiction to resolve at the decision level — only the discipline
of keeping the two purposes (correctness vs. efficiency) from blurring into each other in an
implementation.

The open question 07 itself flags — whether a leader-elected sweep's own writes need the same
fencing protection as ordinary claims, given any lease-based election has a window where two
instances can both believe they hold leadership — is answered directly by this ADR: yes,
unconditionally.

## Decision

**Leader election is scoped to exactly one background task: the periodic cross-instance
sweep-shard rebalancing / timeout-sweep-trigger loop. It is never used for claiming nodes, for
computing partition ownership (ADR-0014's `Assigner` is computed independently by every instance
with no election involved), or for admitting `AddNode`/`AddEdge` calls.**

1. **Correctness never depends on there being exactly one leader.** The inline reclaim path — every
   `Claim` opportunistically runs `Sweep` against its own partition as part of the same claim
   attempt (AMD-2's `Sweep(ctx, scope, limit) (timedOut []NodeRef, readied []NodeRef, err error)`)
   — is what actually guarantees a timed-out node gets reclaimed, with **zero** leader election
   running at all. Leader election, when enabled, is a pure efficiency layer added on top to avoid
   N instances redundantly scanning the same scope's deadline index on a background ticker.
2. **Mechanism, when enabled:** the same storage-backed heartbeat-row-with-TTL substrate ADR-0013
   deliberately avoids needing for dispatch — a row/key per scope (or per sweep-shard) recording
   the current holder and a heartbeat timestamp, refreshed at roughly a third of the TTL (07 §5.1).
   No Raft, no ZooKeeper, no gossip layer — this reuses the KCL lease-table pattern (07 §5.1) and
   the same primitive class the storage backend already provides for everything else.
3. **The leader's own sweep writes are fenced identically to every other write in the system.** A
   leader that GC-paused past its lease and resumes believing it is still the leader has its
   `Sweep`-driven reclaim writes rejected by the same fencing-epoch check every `Complete`/`Extend`
   call is subject to — there is no special case for "this write came from the elected leader."
   This closes 07's own open question explicitly: a lease-based election's transient two-leader
   window is tolerated as a wasted-scan blip, never as a correctness exception.
4. **v0.1-v0.4 ship with no leader election at all.** The inline reclaim path alone is correctness-
   sufficient for the entire phased plan through the 1M-node benchmark milestone. Only v0.5
   introduces optional leader-elected sweep-shard rebalancing, and only once redundant scanning is
   measured to matter, per the same "don't build coordination before establishing it's needed"
   discipline dossier 03 states directly.

## Consequences

### Positive
- The architecture never acquires a single-dispatcher throughput ceiling — Airflow's own
  documented failure mode is structurally impossible here, because dispatch never routes through
  an elected role at any phase of the plan.
- v0.1-v0.4 ship correct multi-instance timeout reclaim with zero election machinery, zero new
  operational dependency, and zero new failure mode to reason about.
- Adding election in v0.5 is strictly additive — nothing about its introduction changes the
  correctness argument for anything shipped before it.

### Negative
- Until v0.5, N instances may run redundant sweep scans against the same scope's deadline index —
  wasted round trips and CPU, but bounded by `(scope count) × (instance count)`, never by node
  count, since the sweep only ever touches the small in-flight/deadline-indexed subset of a scope.
- A leader-elected sweep adds one more storage-backed heartbeat mechanism to operate once enabled,
  distinct from ADR-0013's instance liveness (dispatch needs none) and from an individual claim's
  lease deadline — three different TTL-governed concepts an operator must be able to tell apart.

### Neutral
- "Leader" here is deliberately a weak, storage-backed advisory role, not a consensus-elected one.
  A network partition can produce two instances that both believe they are leader simultaneously;
  this is explicitly tolerated as an efficiency-only blip rather than treated as a bug, because
  fencing absorbs every consequence a wrong belief could otherwise cause.

## Alternatives considered

- **Real consensus (Raft/ZooKeeper) for leader election**: rejected — a second stateful cluster is
  a disproportionate operational cost for a decision whose failure mode is wasted CPU, not
  corruption. Chubby's own rationale for existing at all is for un-fenceable, catastrophic
  decisions (who is the single source of truth); "which instance runs the sweep loop this hour" is
  not that decision (07 §5.2).
- **A single elected dispatcher as the actual work-distribution mechanism**: rejected outright —
  this is precisely Airflow's pre-2.0 architecture, and Airflow's own history is the cautionary
  precedent cited directly in this ADR's Context; adopting it would recreate the exact throughput
  ceiling ADR-0013/ADR-0014 exist to avoid (07 §5.5).
- **No leader-election concept at all, ever, relying purely on inline reclaim indefinitely**:
  rejected as too restrictive for large deployments — the phased plan explicitly wants the option
  to cut redundant scanning once it is measured to matter (07 recommendation 6); the interface cost
  of supporting an optional leader is low, and the inline path remains the correctness fallback
  regardless of whether election is enabled.

## References

- [Apache Airflow — scheduler HA docs](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html)
- [AWS — Kinesis Client Library lease table](https://docs.aws.amazon.com/streams/latest/dev/kcl-concepts.html)
- [Burrows — The Chubby lock service, OSDI 2006](https://static.googleusercontent.com/media/research.google.com/en//archive/chubby-osdi06.pdf)
- docs/research/07-work-distribution-across-instances.md §5, §7.1 Layer 1, open questions
- docs/research/00-synthesis.md §10.6
- ADR-0013 (dispatch never uses this mechanism); ADR-0014 (partition ownership computed without
  election)
