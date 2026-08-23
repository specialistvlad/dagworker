# ADR-0032: Priority is a first-class ready-set ordering key

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/05-redis-backend.md §3, §15.2; docs/research/04-postgres-backend.md
  §6, §14.1; docs/research/07-work-distribution-across-instances.md §1.1; docs/research/01-prior-art-workflow-engines.md
  §12.1

## Context

The public API surface already declares `Node.Priority int16` with no ADR backing it — this must
be justified or dropped. It is kept: every backend surveyed for the ready-set structure natively
supports priority ordering at negligible extra cost over plain FIFO, and every real system in the
prior-art survey that ships a work queue at all ships a priority column next to it (River's
`ORDER BY priority DESC, id ASC`, 01 §12.1; Postgres's own `nodes_ready_idx` composite index, 04
§6). Dropping it would not simplify any backend — it would just move the feature into application
code on top of the library, reproducing the same "claim, inspect, release-and-retry" waste pattern
ADR-0032's sibling decision on Kind-partitioned claims (12 §5.4) already argues against for
worker-capability matching.

The concrete design question is the ordering key's exact shape, because each backend's atomic
claim primitive expresses order differently. Redis's `ZPOPMIN` needs one `float64` score per
member — there is no second sort column (05 §3). PostgreSQL's `SKIP LOCKED` claim reads a real
multi-column B-tree index and needs no packing at all (04 §6, §14.1). The in-memory backend's
sharded ready structure is native Go, where a comparator can read struct fields directly. A single
ADR needs to fix all three so a node claimed under identical priority-and-arrival conditions is
ordered the same way regardless of which backend is in use — "priority" must mean one thing across
the whole capability matrix, not three loosely-compatible approximations.

PostgreSQL's `SKIP LOCKED` claim query does **not** give a hard priority guarantee under
concurrent claimers: if the highest-priority ready row is locked by another in-flight transaction,
the query skips it and returns a lower-priority row to a different claimer first (04 §6). This is
not a bug to route around — it is the accepted cost of `SKIP LOCKED`'s whole non-blocking design,
and it must be documented as a soft guarantee rather than silently promised as strict.

Because priority strictly orders the ready-set on every backend, a scope whose highest tier is
kept perpetually non-empty can, in principle, starve everything below it indefinitely — no dossier
in this series designs an anti-starvation mechanism, and this ADR must take an explicit,
implementable position rather than leaving the question open.

## Decision

**`Node.Priority` stays as `int16` in the public API. Higher numeric value is served first; the
zero value (`0`) is "normal" priority, matching every other zero-value-is-safe default in this
design.** Ties within the same priority value are broken by arrival order (FIFO) — a node that
became ready earlier is claimed before one that became ready later at the same priority.

**Ordering key per backend:**

- **Redis (ZSET score, `float64`):** pack an inverted, offset priority into the high bits and a
  32-bit monotonic FIFO sequence (the low 32 bits of the node's `Seq`, ADR-0020) into the low bits,
  so ascending score order — the order `ZPOPMIN` pops in — is exactly "highest priority first, then
  earliest-ready first":

  ```
  rank  := uint32(0xFFFF) - uint32(int32(priority) + 32768)   // 0 = priority 32767 (highest)
  score := float64(rank)*4294967296.0 + float64(uint32(seq))  // rank*2^32 + fifoSeq
  ```

  `65535 * 2^32 ≈ 2.815e14`, well inside `float64`'s exact-integer range (`2^53 ≈ 9.007e15`), so no
  precision is lost across the full `int16` range. The FIFO component wraps at `2^32`
  (~4.3 billion ready events); wraparound degrades the tie-break to approximately-FIFO at the wrap
  boundary only, never incorrectly — this is a fairness nuance, not a correctness defect, and is
  scoped per `(scope, kind)` ready-set so the practical wrap horizon is per-partition, not global.
  `ZADD` on this score; `ZPOPMIN key N` claims the top N in one round trip fused with lease
  bookkeeping via the same Lua Function the claim path already uses (05 §15.2).

- **PostgreSQL (composite B-tree index, no packing needed):**

  ```sql
  CREATE INDEX nodes_ready_idx ON dagw.nodes (scope, priority DESC, node_id)
      WHERE status = 'ready';
  ```

  `node_id` is the table's `GENERATED ALWAYS AS IDENTITY` column, monotonic globally and therefore
  monotonic within any single scope's subsequence — ascending `node_id` is a valid FIFO order
  within a scope with no separate sequence column required. The claim query is
  `ORDER BY priority DESC, node_id FOR UPDATE SKIP LOCKED LIMIT $n` (04 §14.1), scanning this index
  only — it never touches a row outside the `ready` partial index regardless of scope size.
  **This ordering is soft under concurrent claimers** (04 §6): `SKIP LOCKED` skips a row already
  locked by a concurrent transaction and returns the next unlocked row in index order instead,
  which can hand out a lower-priority node before a higher-priority one that happened to be
  contended at that instant. Document this explicitly in the public API's `Priority` field comment.

- **In-memory (heap comparator, native Go, no packing needed):** one `container/heap`-compatible
  min-heap per `(scope, kind)` shard, comparator:

  ```go
  func (h *readyHeap) Less(i, j int) bool {
      if h.items[i].Priority != h.items[j].Priority {
          return h.items[i].Priority > h.items[j].Priority // higher priority pops first
      }
      return h.items[i].Seq < h.items[j].Seq // FIFO within the same priority
  }
  ```

  `heap.Pop` returns the highest-priority, earliest-ready item; this is the exact same total order
  as the Redis packing above and the Postgres index above, expressed natively instead of packed,
  and is a **hard** guarantee on this backend (no `SKIP LOCKED`-style contention skip exists for a
  single mutex-guarded heap).

**The starvation question: v1 ships no anti-starvation aging. Priority ordering is strict.** A
scope whose higher-priority tier stays continuously non-empty can starve lower tiers indefinitely
on every backend except to the (soft) extent Postgres's `SKIP LOCKED` contention accidentally lets
a lower-priority row through. This is a documented, caller-manageable risk, not a hidden footgun:
the mitigation the library actually ships is **`Kind`-partitioned ready-sets** (12 §5.4) — a host
that needs a latency-sensitive class of low-priority work protected from a busy high-priority tier
gives that class its own `Kind`, which has its own ready-set entirely untouched by another `Kind`'s
priority ordering. A future opt-in aging policy (`effective_priority = priority +
floor(waiting_duration / agingInterval)`) is a **named, reserved, unshipped** extension point —
it is not part of this ADR's decision and must not be implemented speculatively.

## Consequences

### Positive
- One ordering semantic, three faithful backend expressions — a caller sees the same relative
  claim order (priority, then FIFO) regardless of which backend the host chose, modulo Postgres's
  documented soft guarantee under contention.
- Claim-N stays a single round trip on every backend: `ZPOPMIN key N` on Redis, one indexed
  `SKIP LOCKED … LIMIT N` scan on Postgres, one heap-pop loop in-memory — priority ordering adds no
  extra round trip anywhere.
- `Kind` already existing as the worker-capability-matching partition key (12 §5.4) means the
  starvation mitigation requires no new mechanism — it reuses a structure the library ships anyway.

### Negative
- Redis's packed score is opaque bit-math that must be documented precisely and tested for
  boundary values (`priority = -32768`, `priority = 32767`, `seq` near a `2^32` wrap) — a subtle
  encoding bug here silently reorders the ready-set rather than crashing.
- Strict priority ordering with no aging is a real starvation risk for a workload the host
  misconfigures (e.g., every node submitted at the same high priority, defeating the field's
  purpose) — the library does not protect a caller from this by design.

### Neutral
- Postgres's soft guarantee under `SKIP LOCKED` contention means "priority" has a slightly
  different strength on that backend than on Redis or in-memory — this must be stated plainly in
  the public `Priority` field's doc comment, not treated as a portability bug to fix later.

## Alternatives considered

- **Drop `Priority` entirely, ship FIFO-only ready-sets**: rejected — every backend's natural
  ready-set structure (Redis ZSET, Postgres composite index, an in-memory heap) supports priority
  at effectively the cost of a comparator, and every mature system surveyed with a comparable queue
  ships one (River, 01 §12.1); dropping it pushes the need into application-level claim-inspect-
  requeue loops, which is strictly worse for both latency and code complexity than the field
  costing.
- **Plain LIST/FIFO-only Redis structure to save ~15 bytes/member**: rejected per 05 §3's own
  conclusion — losing priority now to save a small per-member memory cost is "a bad trade against a
  well-known limitation coming back later as a feature request"; removing priority later is easy,
  adding it back under load is not.
- **A separate priority queue per tier instead of one ordered structure**: rejected — this
  reproduces `Kind`-partitioning's mechanism for a purpose `Kind` is already better suited to
  (coarse worker-capability routing), while making claim-N across tiers require multiple round
  trips instead of the single `ZPOPMIN`/indexed-scan/heap-pop this ADR's design gives for free.
- **Built-in priority aging in v1**: rejected as premature — no surveyed system in this design's
  research corpus specifies a validated aging formula for this exact workload shape (bursty,
  dependency-driven readiness, not Poisson arrivals), and shipping an unvalidated anti-starvation
  heuristic risks masking real misconfiguration rather than fixing it; the `Kind`-partitioning
  mitigation is available immediately with zero new mechanism.

## References

- [River — priority claim query](https://github.com/riverqueue/river)
- docs/research/05-redis-backend.md §3 (ZSET vs. LIST vs. SET vs. Stream, priority packing)
- docs/research/04-postgres-backend.md §6, §14.1 (`nodes_ready_idx`, `SKIP LOCKED` soft ordering)
- docs/research/12-dag-semantics-and-state-machine.md §5.4 (`Kind`-partitioned ready-sets)
- ADR-0020 (`Seq`, reused as the FIFO tie-break source); ADR-0023 (per-scope, per-`Kind` ready-set
  isolation this ADR's starvation mitigation depends on)
