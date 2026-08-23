# ADR-0020: A per-node monotonic sequence is the ordering and resume primitive

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/10-event-bus-and-delivery-semantics.md §3, §5.2, §6

## Context

A subscriber to the observation feed (ADR-0019) needs exactly one ordering guarantee that is
load-bearing: it must never see `Success` before `InProgress` for the *same* node. It needs no
guarantee at all about the relative order of two unrelated nodes' transitions, because no correct
scheduling decision anywhere in the system depends on knowing which of two independent nodes
"came first" in wall-clock time — the causal relationship that actually matters (a child cannot
become ready before its parents succeed) is enforced by the DAG structure itself, in the same
atomic write that flips state, not by the ordering of the event stream (10 §3.3). This is the
direct generalization of Kafka's own load-bearing guarantee: ordering only within one partition,
none across partitions (10 §3.2) — here the "partition" is a single node's own lifecycle.

A single global total order — one counter incrementing across every write in a scope — is easy to
reach for (it's what a naive "just add an auto-increment column" design produces) but it serializes
every writer in the scope through one point of contention, exactly the throughput trade Kafka's own
documentation calls out for its single-partition case. At the mandated 1,000,000-node scale, with
many nodes completing concurrently across shards/connections, that serialization point would
become the system's ceiling, for a guarantee almost nothing needs.

The read-your-writes problem (10 §5) is the second forcing function: a subscriber that gets told
"node X is now `Success`" and immediately reads storage for X's result can observe stale data if
the event and the write are not provably associated with the same version. Carrying a version
number in the event that the subsequent read can be compared against turns an invisible
correctness bug into a cheap, detectable, retryable condition (`if readSeq < event.Seq { retry }`)
— and this version number is free to compute if it is the same counter already used for ordering.

Finally, every durable pub/sub system that allows disconnect-and-resume needs a resume cursor, and
every one of them (Kafka offsets, Redis Stream IDs, Postgres LSNs/outbox row IDs, etcd MVCC
revisions) defines what happens when a cursor points behind retained history: it is never silently
served from a wrong position, it is reported as expired (10 §6, ADR-0021).

## Decision

Every node carries one field, `Seq uint64`, bumped exactly once, atomically, in the same storage
write as any state-changing mutation to that node (`Claim`/`Complete`/`Extend`/`Sweep` per AMD-2,
or an `AddEdges` write that changes the node's `pending` state). `Seq` starts at `1` on
`CreateNode` and never decreases or resets for the life of the node.

```go
type Node struct {
    // ...
    Seq Seq // per-node monotonic; ADR-0020
}
type Seq uint64

type Event struct {
    Seq      Seq    // the Seq of the write that produced this event
    Scope    Scope
    NodeID   NodeID
    Kind     EventKind
    From, To Status
    At       time.Time
}
```

`Seq` does three jobs with one field:

1. **Ordering within a node.** A subscriber compares `Seq` values for the same `NodeID` and
   discards or reorders anything `<= lastSeen[NodeID]` — trivial duplicate/replay detection with
   zero coordination.
2. **Staleness token.** After receiving an event with `Seq = s` for node `n`, a subsequent read of
   `n` that returns `readSeq < s` is provably stale; the caller retries the read (with backoff)
   instead of acting on old data. This is mandatory practice for every subscriber that reads
   storage after an event and is documented as such on `Event`.
3. **Resume unit, single-node case.** `SubscribeOptions{Filter: NodeFilter(id), From: s}` resumes
   delivery of node `id`'s own events strictly after `s`. This case needs no additional machinery:
   `Seq` values for one node are already a total order.

**Scope-wide resume is opt-in, and uses a second, explicit counter, never the per-node `Seq`
reinterpreted.** `SubscribeOptions` gains a `GlobalOrder bool` field:

```go
type SubscribeOptions struct {
    Scope       Scope
    Filter      Filter
    From        Seq
    GlobalOrder bool // opt-in: From is a ScopeSeq, not a per-node Seq
    BufferSize  int
    Overflow    OverflowPolicy
    Durable     bool
}
```

- `GlobalOrder == false` (default): a scope-wide `Subscribe` (no `NodeID` filter) requires
  `From == 0` ("start from now"). There is no cross-node resume cursor in the default mode — the
  documented recovery procedure for a caller that needs to catch up after a disconnect is the one
  ADR-0021 specifies: read current state, then resubscribe from now. This keeps the default,
  common case free of any serialization cost.
- `GlobalOrder == true`: the backend additionally assigns a strictly increasing `ScopeSeq` at
  write-commit time, scoped to the whole `Scope`, and `From` is interpreted against that counter
  for that subscription. This is the one path that pays Kafka's single-partition serialization
  cost, and it is paid only by callers who explicitly ask for "one merged timeline to render," per
  the ADR-19 seed's own framing. A backend that cannot produce this counter without a global lock
  (none of the three required backends are in this position — Postgres has `outbox.id`, Redis has
  the Stream's own monotonic ID, in-memory has a scope-level atomic counter available for the
  asking) documents the throughput cost in its package doc rather than silently degrading.

`ScopeSeq` is a distinct exported type (`type ScopeSeq uint64`) from `Seq`; a `GlobalOrder`
subscription's events still carry the node-level `Seq` too (for staleness-token purposes), so
opting into global order never removes the per-node ordering/dedup guarantee, it only adds a
second, more expensive one on top.

## Consequences

### Positive

- Zero cross-node coordination in the default path: no distributed clock, no vector clock, no
  single serialization point, matching the actual ordering need (10 §3.3) exactly rather than
  over-delivering a guarantee nothing consumes.
- One field does staleness-detection, dedup, and single-node resume — no separate version column
  and no separate cursor type need to be designed, stored, or kept consistent with each other.
- The expensive mode (`GlobalOrder`) is opt-in and its cost is visible in the type system
  (`ScopeSeq`, a `bool` the caller must set) rather than hidden behind a `Subscribe` call that
  looks free but silently serializes every writer in the scope.

### Negative

- A scope-wide subscriber that wants delivery order across nodes but does not want to pay the
  `GlobalOrder` serialization cost has no third option — this is a deliberate binary choice, not a
  spectrum, and a future request for a cheaper partial order (e.g., per-`Kind` ordering) would need
  a new ADR, not a parameter tweak.
- Two counter types (`Seq`, `ScopeSeq`) in the public API is one more concept for an integrator to
  learn than a single unified sequence number would be; this is judged worth it because conflating
  them would either weaken the per-node guarantee's cheapness or silently impose the global-order
  cost on every caller.

### Neutral

- Each backend is free to have its own internal delivery-position bookkeeping (a Redis Stream ID,
  a Postgres outbox row ID) distinct from the `Seq`/`ScopeSeq` types exposed to callers — those
  remain implementation details of the adapter's own resumability, exactly as dossier 10 §5.4
  recommends, and are never leaked across the storage port boundary (ADR-0016).

## Alternatives considered

**One global auto-increment counter per scope, always on, no opt-in.** Rejected: this is Kafka's
own single-partition-topic trade made mandatory for every caller regardless of need — it forces
every write in a scope through one serialization point at exactly the scale (1,000,000 nodes) this
project is designed not to bottleneck on (10 §3.2, §3.3).

**Vector clocks or Lamport clocks for genuine causal ordering across nodes.** Rejected: the causal
relationship that matters here (parent-succeeds-before-child-ready) is already enforced by the DAG
structure and the storage layer's atomic read-check-write, which makes an event-stream-level causal
clock pure overhead solving a problem that does not exist at this layer (10 §3.1, §3.3).

**An opaque `Cursor string` type at the public API** (the shape dossier 10 §11 proposes at its own
interface layer) instead of a typed `Seq`/`ScopeSeq`. Rejected for the public surface: an opaque
string cursor hides the staleness-token use (job 2 above) behind a type callers cannot compare or
reason about, and this library's read-your-writes fix depends on `Event.Seq` being directly
comparable to a freshly-read `Node.Seq` — an opaque cursor cannot serve that second purpose without
becoming, in effect, `Seq` again with extra indirection. Backends remain free to use an opaque
cursor internally for their own delivery bookkeeping (see Neutral, above).

## References

- docs/research/10-event-bus-and-delivery-semantics.md §3.1–§3.3, §5.1–§5.2, §6
- docs/research/00-synthesis.md §3 (ADR-20 seed), §4 (`Seq`, `Event`), §5 (state-machine table)
- Kafka partition ordering — https://pulse.support/kb/kafka-ordering-guarantees
- Lamport, "Time, Clocks, and the Ordering of Events in a Distributed System" (happened-before) — https://www.cs.cmu.edu/~dga/15-712/S11/lectures/04-clocks.pdf
