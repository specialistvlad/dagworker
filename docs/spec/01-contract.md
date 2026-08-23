# dagworker — Normative Contract

**Status:** normative. **Version:** targets v1.0. **Date:** 2026-08-22.

This document is the implementation contract. The ADRs in `../adr/` record *why* each rule exists;
the research dossiers in `../research/` record the evidence. Where this document and an ADR
disagree, this document wins and the ADR is amended.

Key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, **MAY** are to be interpreted as in
[RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) / [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174).

---

## 1. Vocabulary

| Term | Definition |
|---|---|
| **Node** | One unit of work. Identified by `(Scope, NodeID)`. Carries an opaque payload. |
| **Edge** | A directed dependency `from → to`, meaning `to` MUST NOT run until `from` resolves. |
| **Scope** | A namespace. The unit of isolation, configuration, completion, retention, and key prefixing. |
| **Kind** | A ready-set partition key within a scope. A worker MAY claim by kind. |
| **Instance** | One `*Manager` in one process, attached to one `Store`. |
| **Worker** | Code *outside* this library that claims a node, does the work, and acks. |
| **Lease** | A time-bounded, fenced grant of exclusive right to complete a node. |
| **Lease epoch** | A monotonic `uint64` per node. The fencing token. Also the attempt number. |
| **Doorbell** | A best-effort "work is available" signal. Never load-bearing. |
| **Seal** | A caller assertion that no further nodes will be added to a scope. |

### 1.1 Identifier rules

- `Scope` **MUST** be non-empty, valid UTF-8, and **MUST NOT** exceed 255 bytes.
- `NodeID` **MUST** be non-empty, valid UTF-8, and **MUST NOT** exceed 255 bytes. Unique per scope.
- `Kind` **MAY** be empty (the default partition). **MUST NOT** exceed 64 bytes.
- Scopes are created implicitly on first write. There is no `CreateScope`.
- Violations return an `*InvalidArgumentError` naming the offending field, which unwraps to
  `ErrInvalidArgument`. (The design draft called this `ErrInvalidIdentifier`; a separate sentinel
  for identifiers earned nothing over one `invalid argument` class carrying the field name, and
  every adapter maps them to the same status anyway.)

---

## 2. State model

### 2.1 Public `Status` — exactly four values, frozen at v1.0

```
StatusNew         node exists and has not completed an attempt successfully
StatusInProgress  a worker holds a valid lease
StatusSuccess     terminal, succeeded
StatusError       terminal, did not succeed
```

Implementations **MUST NOT** add a fifth value. `Status.Terminal()` is `Success || Error`.

### 2.2 `Reason` — why, closed set

```
ReasonNone            no significant outcome yet
ReasonWorkerError     the worker acked failure (Nack)
ReasonTimeout         the lease deadline elapsed with no ack
ReasonUpstreamFailed  a predecessor failed and the trigger rule can no longer be satisfied
ReasonSkipped         the trigger rule is provably unsatisfiable for a non-failure reason
ReasonCancelled       Cancel or CancelScope
ReasonRemoved         a predecessor was removed under CascadeFail
```

`Reason` is populated as follows and this is normative:

| Status | Attempt | `Reason` means |
|---|---|---|
| `New` | 0 | `ReasonNone` |
| `New` | > 0 | why the **most recent attempt** failed (node is awaiting retry) |
| `InProgress` | ≥ 1 | why the **previous** attempt failed, or `ReasonNone` on the first attempt |
| `Success` | ≥ 1 | `ReasonNone` |
| `Error` | ≥ 0 | why the node is **terminally** failed |

`Node` carries `Reason` and `Message` as flat fields, never a pointer. At 1,000,000 nodes a pointer
per node is a pointer the GC must scan; see ADR-0028.

### 2.3 Internal `Phase` — five values, debug-only, no stability promise

```
PhaseBlocked    ≥1 unsatisfied predecessor           → Status New
PhaseScheduled  deps satisfied, retry backoff pending → Status New
PhaseReady      claimable now                         → Status New
PhaseClaimed    a worker holds a valid lease          → Status InProgress
PhaseDone       terminal                              → Status Success | Error
```

`Phase` **MUST NOT** appear on the event stream, in the gRPC/HTTP wire format, or in any type a
caller is expected to persist. It is reachable only via `Manager.Inspect`. It exists because
dynamic edge insertion flips a node `Ready ↔ Blocked` with no actor involved, and a subscriber must
never see that as a status regression (ADR-0002).

> **Deviation from the design synthesis (§4):** the synthesis proposed a nine-value `Phase`
> (`Succeeded`/`Failed`/`TimedOut`/`UpstreamFailed`/`Skipped`/`Cancelled`) that duplicates `Reason`
> one-for-one. Collapsed to `PhaseDone`; the terminal detail lives in `Reason` only, so there is
> exactly one representation of "why" and no way for the two to disagree.

### 2.4 Transition table

`Engine` = internal scheduler. `Caller` = host via the public API. `Worker` = external, via
`Ack`/`Nack`/`Extend`. `Sweeper` = the fenced reclaim path.

| # | From | To | Actor | Trigger | Fenced? |
|---|---|---|---|---|---|
| T1 | — | `New/Blocked` | Caller | `AddNode` with ≥1 unsatisfied dep | no |
| T2 | — | `New/Ready` | Caller | `AddNode` with no unsatisfied dep | no |
| T3 | — | `Error/UpstreamFailed` | Engine | `AddNode` whose dep is already `Error` and rule unsatisfiable | no |
| T4 | `New/Blocked` | `New/Ready` | Engine | last unsatisfied predecessor resolves | no (idempotent per-edge guard) |
| T5 | `New/Blocked` | `New/Blocked` | Caller | `AddEdge` into a blocked node | no |
| T6 | `New/Ready` | `New/Blocked` | Caller | `AddEdge` adding an unresolved dep to a ready node | **atomic with the edge insert** |
| T7 | `New/Ready` | `InProgress/Claimed` | Worker (via `Claim`) | claim granted | **yes — epoch++** |
| T8 | `InProgress/Claimed` | `Success/Done` | Worker | `Ack` | **yes — CAS on epoch** |
| T9 | `InProgress/Claimed` | `Error/Done` (`WorkerError`) | Worker | `Nack`, attempts exhausted | **yes — CAS on epoch** |
| T9b | `InProgress/Claimed` | `Error/Done` (`Skipped`) | Worker | `Nack` with `ReasonSkipped` — terminal on first report, never retried | **yes — CAS on epoch** |
| T10 | `InProgress/Claimed` | `New/Scheduled` | Engine | `Nack`, attempts remain | **yes — CAS on epoch** |
| T11 | `InProgress/Claimed` | `Error/Done` (`Timeout`) | Sweeper | deadline elapsed, attempts exhausted | **yes — CAS on epoch** |
| T12 | `InProgress/Claimed` | `New/Scheduled` | Sweeper | deadline elapsed, attempts remain | **yes — CAS on epoch** |
| T13 | `New/Scheduled` | `New/Ready` | Engine | backoff elapsed | no |
| T14 | `New/*` | `Error/Done` (`UpstreamFailed`) | Engine | predecessor failed, rule unsatisfiable | no |
| T15 | `New/*` | `Error/Done` (`Skipped`) | Engine | rule provably unsatisfiable, no failure | no |
| T16 | `New/*` or `InProgress/Claimed` | `Error/Done` (`Cancelled`) | Caller | `Cancel` / `CancelScope` | **yes if InProgress** |
| T17 | `New/*` | `Error/Done` (`Removed`) | Caller | predecessor removed under `CascadeFail` | no |
| T18 | terminal | — (deleted) | GC | retention TTL elapsed **and** subscriber low-water mark passed | no |

**Invariants.**

- **I1.** Nothing leaves `Success` except T18. `AddEdge` **MUST NOT** re-block a terminal node;
  it returns `ErrAlreadyTerminal`.
- **I2.** Every row that changes `Status` writes `Status`, `Reason`, `Message`, and `Seq` in the
  same storage operation. A reader **MUST NOT** be able to observe a torn transition.
- **I3.** T7, T8, T9, T10, T11, T12, and T16-when-`InProgress` are the fenced rows. Every one is a
  compare-and-swap on `lease_epoch`. A mismatch returns `ErrLeaseMismatch` and **MUST NOT** be
  reported as retryable.
- **I4.** T6 is atomic with the edge insert. Popping a node out of the ready-set and incrementing
  its unsatisfied-predecessor count **MUST** be indivisible from recording the edge, or a worker
  claims it in the gap.
- **I5.** `Attempt` increases by exactly one on each T7 and never decreases.

---

## 3. Guarantees

| ID | Guarantee | Enforced by |
|---|---|---|
| **G1** | At most one worker holds a valid lease on a node at any instant. | atomic claim (§4.2) |
| **G2** | A mutation presenting a stale lease epoch is rejected. | fencing CAS (I3) |
| **G3** | A node becomes claimable only when its trigger rule is satisfied. | §5 |
| **G4** | The graph is acyclic at every observable instant. | insert-time rejection (§6) |
| **G5** | Work delivery is **at-least-once**; the accepted effect is **at-most-once per epoch**. | G1 + G2 |
| **G6** | Events for one node are totally ordered by `Seq` and gap-free within a subscription. | §7 |
| **G7** | `EventReady` is advisory. Dropping every one changes latency, never outcome. | §7.3 |
| **G8** | Claim, Complete, Extend, and Sweep are each **one** atomic storage operation. | §4 |
| **G9** | An edge never crosses a scope boundary. | `ErrCrossScopeEdge` |
| **G10** | `Close` returns only after every goroutine the `Manager` started has exited. | §9 |

**Exactly-once delivery is not offered and MUST NOT be claimed** in documentation, README, or
marketing copy. The honest phrasing, which every public artifact **MUST** use, is: *at-least-once
delivery with at-most-once accepted effect per lease epoch.*

---

## 4. The lease protocol

### 4.1 Clock authority

Every deadline comparison **MUST** read the clock of the storage backend that owns the node:

| Backend | Clock source |
|---|---|
| in-memory | the injected `Clock` (a monotonic `time.Now()` in production) |
| Redis | `redis.call('TIME')` **inside** the Lua function |
| PostgreSQL | `clock_timestamp()` **inside** the statement |

A client-computed wall-clock deadline **MUST NOT** be sent to storage. Using `now()` in PostgreSQL
is a defect — it is transaction-start time, not statement time — and CI **MUST** fail on any
occurrence of `now()` in `storage/postgres/**/*.sql` (ADR-0008).

### 4.2 Claim

One atomic operation **MUST** do all of:

1. select an eligible node from the `(scope, kind)` ready-set, honouring priority then FIFO;
2. set `Status = InProgress`, `Phase = Claimed`;
3. increment `lease_epoch` and set `Attempt = lease_epoch`;
4. set `deadline = <storage clock now> + leaseTimeout`;
5. insert the node into the deadline index;
6. increment `Seq`.

`leaseTimeout` resolves as `min(max(requested, ScopeConfig.MinLeaseTimeout), ScopeConfig.MaxLeaseTimeout)`,
where `requested` defaults to `ScopeConfig.DefaultLeaseTimeout`. A caller **MAY** override it per
claim; the ceiling exists so a misconfigured caller cannot strand a node indefinitely.

### 4.3 Complete (Ack / Nack)

One atomic operation, gated on `lease_epoch = presented AND Status = InProgress`, that **MUST**:

1. write the terminal status (or `New/Scheduled` for a retry), `Reason`, `Message`, `Seq`;
2. remove the node from the deadline index;
3. for **each direct successor**, mark this edge satisfied and re-evaluate the successor's trigger
   rule; a successor that becomes satisfied is pushed to its ready-set **in the same operation**;
4. decrement the scope's non-terminal counter if the node became terminal;
5. return the list of successors that became ready, so the engine emits `EventReady` without a
   second round trip.

Zero rows / keys affected means the epoch was stale: return `ErrLeaseMismatch`. The library
**MUST NOT** retry a fenced mismatch and **MUST NOT** report it to the worker as transient.

### 4.4 Extend

`Extend(token, d)` is a distinct operation from `Ack`/`Nack` (ADR-0010). It CAS's on the epoch,
sets `deadline = <storage clock now> + d` subject to the same clamp as §4.2, moves the deadline
index entry, and returns the new deadline. It **MUST NOT** change `Status`, `Attempt`, or `Seq`.

### 4.5 Sweep

Reclaim **MUST** be correct with zero coordination between instances. Duplicate sweeping is
wasteful, never wrong, because every write the sweeper performs is fenced on the epoch it observed.

Reclaim runs in two places and both use the identical fenced primitive:

- **inline** on the claim path — whoever asks for work also reclaims expired leases it encounters;
- **background**, on a ticker, bounded by `ScopeConfig.SweepBatchSize`.

A node whose lease expired transitions to T11 (attempts exhausted) or T12 (retry). Sweeping
**MUST** be `O(m log n)` for a batch of `m`, driven by an index ordered on `deadline` — never a
scan of all in-progress nodes.

### 4.6 Trust model

Workers are **cooperative**: operated by the same team as the library instances (ADR-0035). The
fencing token is a plain `uint64`. A malicious worker can forge a higher epoch and steal a node it
does not hold, or replay an old ack. It cannot corrupt graph structure, cross a scope boundary, or
exceed the payload cap. This limitation **MUST** be documented in the README and in the `Claim`
doc comment. `ClaimToken` is an opaque type at the storage port so HMAC signing can be added inside
a backend later without a wire break.

---

## 5. Trigger rules

Five rules ship in v1 (ADR-0030). Each **MUST** be evaluable incrementally as predecessors resolve —
no rule may require a scan of all predecessors on every event.

| Rule | Becomes ready when | Unsatisfiable when |
|---|---|---|
| `AllSuccess` (default) | every predecessor succeeded | any predecessor failed, **or any was skipped** |
| `AllDone` | every predecessor is terminal, however it finished | never |
| `NoneFailed` | every predecessor is terminal and none **failed** | any predecessor **failed** |
| `NoneFailedMinOneSuccess` | every predecessor terminal, none failed, ≥1 `Success` | any predecessor failed, or every predecessor resolved without a single success |
| `Always` | **immediately — predecessors are never consulted** | never |

Two of those rows are easy to get wrong, and both were wrong here until they were checked against
`DepCounts.Ready` and `DepCounts.Unsatisfiable` in `node.go`:

- **`Always` is not `AllDone`.** It returns ready before the unsatisfied-predecessor check runs at
  all. ADR-0030 is explicit that conflating the two is the mistake to avoid: a host wanting "run
  after the predecessors finish, whatever happened" wants `AllDone`, and `Always` means what it
  says.
- **A skip is not a failure**, even though §2.2 makes a skipped node `StatusError` with
  `ReasonSkipped`. `NoneFailed` and `NoneFailedMinOneSuccess` key on the failure count alone, so a
  skipped predecessor does **not** make them unsatisfiable — which is precisely the cascading-skip
  footgun ADR-0030 exists to close. Only `AllSuccess` treats a skip as disqualifying.

Incremental evaluation state per node is **four** counters, not three: `unsatisfied` (predecessors
not yet terminal), `succeeded`, `skipped`, and `failed`. `skipped` is the one that separates the two
bullets above, and omitting it is what made this table wrong. All four are maintained by the same
atomic fan-out in §4.3. A node with zero predecessors is ready under
every rule.

The `one_success` / `one_failed` early-fire family is **deferred**: it requires a node to become
claimable while predecessors are still `InProgress`, which the transition table does not model.

---

## 6. Dynamic mutation

### 6.1 Insert

`AddNode` is idempotent (ADR-0025). Same `NodeID` with a byte-identical spec is a no-op returning
success. Same `NodeID` with a different spec returns `ErrIDConflict`. "Spec" is payload, kind,
labels, priority, trigger rule, and retry policy — not status, not attempt, not `Seq`.

For each declared predecessor at insert time:

- predecessor is `Success` → edge is born satisfied; no `unsatisfied` increment;
- predecessor is `Error` → the successor's rule is evaluated immediately and **MAY** yield T3;
- predecessor is non-terminal → `unsatisfied++`;
- predecessor does not exist → `ErrNotFound`. Forward references are **not** supported; use
  `AddNodes` to insert a batch atomically.

`AddNodes` is atomic per call within one scope: every node and edge lands, or none does.
Batch size is capped by `ScopeConfig.MaxBatchSize`.

### 6.2 Cycle rejection

Every node carries an integer topological rank `ord`. For a proposed edge `u → v`:

- `ord(u) < ord(v)` → **accept in O(1)**. This is the common, causally-ordered case.
- otherwise → run a bounded search over the affected region. If `v` reaches `u`, reject with
  `*CycleError` (which unwraps to `ErrCycle` and carries the path). Otherwise renumber the affected
  region and accept.

v1 ships **full Pearce–Kelly**: two bounded depth-first searches bounding the affected region,
then a merge that reassigns the union of that region's existing rank values in sorted order
(ADR-0004 as amended by ADR-0041).

**Not shipped:** the draft required a `topo_fastpath_hit_ratio` metric. The library exports no
metrics at all — it has no metrics interface, by the same argument that keeps a logger out of the
hot path — so this is an unmet requirement, not an implemented one. What is observable today is
`ScopeStats`, which a host can export itself. A metrics facet is a v0.5 question.

`AddEdge` into a terminal node returns `ErrAlreadyTerminal`. There is no `WithReopen` in v1.

### 6.3 Removal

`RemoveEdge(from, to)` drops the dependency. If the edge was unsatisfied, `unsatisfied--` and the
successor's rule is re-evaluated; it **MAY** become ready immediately.

`RemoveNode(id)`:

- node is `InProgress` → `ErrNodeInFlight`. Cancel it first.
- node has no successors → hard delete, plus its incident edges.
- node has successors and no cascade policy → `ErrHasSuccessors`.
- `CascadeDetach` → drop incident edges (as `RemoveEdge` each), then delete. Successors **MAY**
  become ready.
- `CascadeFail` → every successor transitions T17 to `Error/Removed`, recursively, then delete.

Silent cascading failure is not the default (ADR-0036).

### 6.4 Completion

A scope is complete iff `Sealed && nonTerminalCount == 0`. `nonTerminalCount` is maintained
atomically by every transition into and out of a terminal status; `IsComplete` is an **O(1)** read
and **MUST NOT** scan. `Seal` is caller-driven and irreversible; `AddNode` into a sealed scope
returns `ErrScopeSealed`.

---

## 7. Events

### 7.1 Kinds

```
EventCreated     a node came into existence. Always Seq 1. Carries its initial status.
EventTransition  a node's public Status changed. Durable tier where the backend supports it.
EventReady       a node became claimable. Advisory doorbell. Coalescing. Never load-bearing.
```

`EventCreated` was added during implementation. A subscriber maintaining a live view of the graph
otherwise had to infer "this node is new" from `Seq == 1` on its first transition, which is a trick
rather than a contract — and the inference is wrong for the one case that most needs to be right: a
node inserted behind a predecessor that has already failed is **born terminal**, so its first event
is not a transition from `StatusNew` at all.

### 7.2 Ordering and resume

Two counters, because one cannot do both jobs (ADR-0041):

- **`Seq`** is per node, bumped on every write to that node. Events for one node are totally
  ordered by `Seq`, and a snapshot's `Seq` detects a stale read. There is **no** cross-node
  ordering guarantee from `Seq`.
- **`Cursor`** is a position in a scope's event log. Events within a scope arrive in `Cursor`
  order, and `Cursor` is what a subscription resumes from. Cursors **MUST** be strictly increasing
  within a scope; they **MAY** be allocated from a store-wide sequence and so need not be
  contiguous, which lets a backend avoid a second hot row on its write path (ADR-0042 §4).

`Subscribe(From: cursor)` replays from just after `cursor`. If it predates retained history the
subscription fails with `ErrCursorExpired`, and the documented recovery — identical on every
backend — is: read current state, then resubscribe from now. Resuming requires a single scope;
`Scope == "" && From != 0` is rejected.

Events are emitted **after** the storage write commits, never before, and carry the `Seq` of the
write they describe, so a subscriber can detect that its own read is stale.

### 7.3 Backpressure

The fan-out point **MUST NOT** block on any subscriber. Each subscription has a bounded channel and
an `OverflowPolicy`:

- `DropOldest` (default) — drop the oldest buffered event and set `Event.Gap` on the next one
  delivered, so the subscriber is told, truthfully, that it missed something;
- `CloseSlow` — terminate the subscription with `ErrSubscriberLagged`.

There is deliberately **no** blocking policy. Blocking delivery needs somewhere to put the events
that arrive meanwhile: an unbounded buffer trades a dropped event for an OOM, and backpressure onto
the completing caller lets one slow observer stall the scheduler. A subscriber that must not miss a
transition uses `Durable` and resumes from a `Cursor`.

Subscriber code **MUST NOT** be invoked inline on the producer's goroutine.

---

## 8. Work distribution

v1 is **pure pull-based competition** (ADR-0013): every instance races on the storage's native
atomic claim. No membership table, no partition ownership, no leader.

**Not shipped:** ADR-0014 planned a `PartitionAssigner` interface present from the first commit
with a trivial `P=1` implementation, so that the v0.5 upgrade — jump consistent hash for
node→partition, HRW for partition→instance — would be an internal refactor. No such interface
exists. What ADR-0014 was really protecting is intact and is what matters: **no public signature
mentions a partition**, so introducing one later remains an internal change. The placeholder
interface would have been an unused abstraction with exactly one implementation, which is the
shape this project's own coding standards reject; the ADR's guarantee did not depend on it.

Leader election is permitted **only** for low-frequency maintenance (sweep-shard rebalancing),
never for dispatch (ADR-0015).

---

## 9. Lifecycle

- Every blocking method takes `ctx context.Context` as its first parameter.
- No exported type stores a `context.Context`.
- `New(store, opts...)` starts no goroutine that outlives `Close`.
- `Close(ctx)` stops accepting new work, drains, and returns only when every goroutine the
  `Manager` started has exited (G10). A `goleak` assertion **MUST** cover this on every backend.
- `Close` is idempotent. Calls after `Close` return `ErrClosed`.

---

## 10. Complexity contract

`n` = nodes in scope, `R` = ready-set size, `d` = out-degree, `k` = declared deps, `m` = batch size.
**No operation may be O(n).** CI enforces these as *ratio* guards across N ∈ {1e3, 1e4, 1e5, 1e6}
in the same run, never absolute nanosecond thresholds (ADR-0040).

| Operation | Bound |
|---|---|
| `AddNode` (k deps) | O(k) expected, O(1) amortized rank maintenance in causal order |
| `AddNodes` (m nodes) | O(Σk) |
| `AddEdge` | O(1) expected accept; O(affected region) worst case |
| `RemoveEdge` | O(1) expected |
| `GetNode` | O(1) expected |
| `Claim` | O(log R) |
| `Ack` / `Nack` | O(d) |
| `Extend` | O(log n) |
| `Sweep` (batch m) | O(m log n) |
| `IsComplete` | O(1) |
| `ListNodes` (page p) | O(log n + p), keyset only — `OFFSET` is forbidden |

---

## 11. Configuration

There are **no opinionated global defaults** for retention, concurrency, or partitioning
(ADR-0034). `ScopeConfig` is first-class, stored per scope in the backend so every instance agrees,
and every field has a conservative fallback so the zero value is usable.

| Field | Fallback | Why this fallback |
|---|---|---|
| `DefaultLeaseTimeout` | 30s | long enough for ordinary work, short enough that a dead worker is noticed |
| `MinLeaseTimeout` | 1s | below this, reclaim storms dominate |
| `MaxLeaseTimeout` | 24h | a hard ceiling so a bad caller cannot strand a node forever |
| `MaxAttempts` | 3 | one retry for transient faults, two for bad luck; not a retry loop |
| `RetryBaseDelay` / `RetryMaxDelay` | 1s / 5m | full-jitter bounds |
| `TerminalRetention` | 0 (never GC) | conservative: never delete data the caller did not ask to delete |
| `MaxSubscriberLag` | 0 (unbounded) | conservative: never advance past a subscriber silently |
| `MaxInFlight` | 0 (unlimited) | the host owns its own concurrency |
| `PayloadCap` | 256 KiB | comfortably under every backend's practical row/value ceiling |
| `MaxBatchSize` | 1000 | fits one Postgres transaction and one bounded Lua script |
| `SweepBatchSize` | 100 | bounds the Lua script's stall and the SQL statement's lock footprint |
| `PartitionCount` | 1 | v1 is pull-based; >1 is the v0.5 upgrade |

`TerminalRetention = 0` and `MaxSubscriberLag = 0` mean *disabled*, not *immediate*. A library that
deletes a caller's data by default is a defect.

---

## 12. Storage port obligations

Mandatory (ADR-0016 as amended): node CRUD, atomic graph mutation, and the four fenced primitives
`Claim`, `Complete`, `Extend`, `Sweep`. A backend that cannot provide an atomic claim is not a
backend; there is no fallback path and no capability escape for these. The full mandatory set is
the `Store` interface in `store.go`.

Optional facets, discovered by type assertion and reported by `CapabilityReporter`:

| Facet | Capability bit | What declining it costs |
|---|---|---|
| `Lister` | `CapList` | `Manager.List` returns `ErrUnsupported`. |
| `DurableEventStream` | `CapDurableEvents` | Events are in-process only; no resume across a restart. |
| `Doorbell` | `CapDoorbell` | A blocking claim falls back to a timed poll. |
| `Collector` | `CapCollect` | Terminal nodes are never garbage-collected. |

The draft named `ConditionalDeleter` and `BatchClaim` as facets. Neither exists: conditional
deletion turned out to be expressible through `RemoveNode`'s cascade policy, and batch claiming is
mandatory rather than optional — `Claim` takes a `Max` and every backend implements it, so a facet
for it would have had no non-implementer. `Doorbell` and `Collector` were discovered during
implementation and took their place.

A backend **MUST NOT** emulate a primitive it lacks. It declines with `ErrUnsupported`, which the
caller can anticipate by asking `Capabilities()` rather than by attempting the call.

**Memcached is rejected as a backend** (ADR-0017): no multi-key atomicity at any protocol version
including the 1.6+ meta commands, no ordered structure for deadline sweeping, no durable CAS across
restart or eviction, and an LRU that makes eviction indistinguishable from "never existed". The
three backends are in-memory, Redis, and PostgreSQL.

### 12.1 Conformance

Every backend **MUST** pass `dagstoretest.RunConformance(t, harness)` (ADR-0018). Capability
sub-suites `Skip` when the facet is absent; they **MUST NOT** silently pass. Test IDs are stable
identifiers referenced from backend documentation, e.g. `T-CLAIM-ATOMIC`, `T-FENCE-STALE-ACK`,
`T-EDGE-BATCH-ATOMIC`, `T-SWEEP-ORDERED`, `T-CLOCK-SERVER-SIDE`.

---

## 13. Durability disclosure

Each backend **MUST** publish what it actually guarantees. No backend may imply more.

| Backend | Durability |
|---|---|
| in-memory | none — process lifetime only. Suitable when all workers share one process. |
| Redis | async replication by default: a primary failover can lose ~1s of writes. `WAIT`/`WAITAOF` is available as an opt-in per-call cost. |
| PostgreSQL | full WAL durability for nodes, edges, and events. |
| file | no loss on an unclean exit: every mutation is `fsync`ed before its call returns, at a cost of one `fsync` per mutation. Single process — the log is a durability mechanism, not a coordination protocol, so `CapCrossProcess` is not set. |
