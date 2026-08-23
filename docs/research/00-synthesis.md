# dag-worker-go — Design Synthesis

Status: **design brief**, synthesized from twelve (plus two ancillary) research dossiers in this
directory. This document is the single source of truth for the v0.1→v1.0 implementation. Where the
dossiers disagreed, a position is taken and the losing options are named. This document is meant to
be implemented close to verbatim; deviations should be recorded as ADR amendments, not silent drift.

Dossiers referenced by number: 01 prior-art, 02 incremental-topo, 03 leases, 04 postgres,
05 redis, 06 memcached/storage-abstraction, 07 work-distribution, 08 go-api, 09 perf,
10 events, 11 testing, 12 dag-semantics, 14 http-json-protocol, 15 daemon-packaging.

---

## 1. Executive summary

dag-worker-go is a Go library, embedded in a host process, that turns a **dynamic DAG of work
items** into a stream of "take this now" claims for **external workers**, and a stream of
**status-transition events** for anyone who wants to watch. The host owns the workers; the library
owns the graph, the readiness computation, the lease/timeout protocol, and the pluggable storage.

The public surface is deliberately small. Node status is a **four-value enum** — `New`,
`InProgress`, `Success`, `Error` — with everything else (why it failed, which attempt, whose lease,
what internal scheduling phase it's in) riding on a closed, structured `Outcome.Reason` field or
staying entirely internal. Every mature prior-art system surveyed (Airflow, Temporal, Argo,
Kubernetes, River, Step Functions, Cloud Tasks) converges on exactly this shape once it has
production scars: a small closed top-level status, a richer reason underneath, and timeout modeled
as a *reason for failure*, never a sibling of success (01 §17, 12 §1.3-1.4).

Readiness is computed the way Kahn's algorithm already computes it — an atomic per-node
"unsatisfied predecessor" decrement on every completion, never a graph rescan (02 §1). Dynamic edge
insertion is made safe against cycles by maintaining an incremental topological order (Pearce–Kelly)
so the common, causally-ordered case is an O(1) accept and only genuinely out-of-order edges pay for
a bounded local search (02 §2.4, 12 §2.3). Every claim of a node is a **single atomic primitive per
backend** that grants ownership and records a lease deadline in the same write, and every subsequent
write against that node — ack, extend, or a sweeper's reclaim — is gated by a **monotonically
increasing fencing token** (`lease_epoch`, doubling as the retry-attempt counter) so a worker that
was merely paused, not dead, can never corrupt state after being superseded (03 §3.4, §5b; 01 §16;
12 §3.4). This one mechanism — atomic claim-with-deadline plus fencing-gated write — is the single
idea that recurs, independently re-derived, across every research file in this series, and it is
non-negotiable on every backend from day one.

Storage is pluggable behind a **narrow mandatory core** (`GetNode`/`PutNode`/`CreateNode`/
`Transition`/`AddEdges`/`DeleteNode`/`Close`) plus **optional capability facets**
(`Lister`, `ReadyQueue`, `TimeoutSweeper`, `EventStream`, `ConditionalDeleter`) discovered by type
assertion — the `database/sql/driver`, containerd, and Terraform pattern, not one fat interface (06
§B.3). In-memory, Redis, and PostgreSQL implement the full contract with their own native atomics
(a sharded map with mutexes; Lua/Functions; `SELECT … FOR UPDATE SKIP LOCKED` plus transactions).
Memcached implements **none** of `Store` — it has no multi-key atomicity, no ordered index, no
durability worth trusting — and instead ships as a `NodeCache` read-through decorator in front of a
real backend (06 Part A). Work distribution across multiple library instances defaults to **pure
pull-based competition** on the shared storage's native atomic claim, because it is the only
strategy every required backend implements natively with zero external coordinator, and it upgrades
non-breakingly to virtual-partition-plus-HRW routing later without changing the public claim API
(07 §7.3, 01 §15).

The event/reactive layer is two separate interfaces, not one: `Bus.Subscribe` for the durable-ish
"every status transition" observation feed, and `Reserver.Reserve`/`Ack`/`Nack` for the
loss-tolerant, single-winner "take this node" doorbell. `Reserve` always re-derives eligibility from
current storage state and never trusts accumulated event history — this is what makes a missed or
duplicated `NodeReady` signal a latency problem, never a correctness one, and is the direct Go
translation of Kubernetes' level-triggered reconciliation discipline (10 §9, 01 §13.1).

Performance at the mandated 1,000,000-node scale is a data-layout discipline, not a tuning
afterthought: struct-of-arrays keyed by dense `int32` handles, zero string keys off the hot path,
sharded (never single-mutex, never bare `sync.Map`) concurrent state, and CI gates expressed as
dimensionless ratios (pipelined ≥ 20× unpipelined, in-memory ≥ 100× Redis ≥ 300× Postgres, same run)
rather than brittle absolute-latency thresholds (09 Part 1-2, 4).

---

## 2. Layered architecture and hot-path data flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PUBLIC API (dagworker)                          │
│   Manager{New, AddNode, AddEdges, Claim, Ack, Nack, Extend, Subscribe,       │
│            Scope.Seal, Scope.Health}                — never blocks on I/O   │
│            it doesn't own; ctx first-arg everywhere; no goroutine spawned    │
│            on the caller's behalf beyond documented, awaited internals.     │
└───────────────┬───────────────────────────────────┬─────────────────────────┘
                │                                     │
                ▼                                     ▼
┌───────────────────────────────┐     ┌───────────────────────────────────────┐
│      CORE SCHEDULER            │     │           EVENT BUS                  │
│  - Kahn ready-set maintenance   │     │  Bus.Subscribe (observation, durable  │
│    (pending[] decrement, §3)    │◄───►│    tier when backend allows)          │
│  - Pearce-Kelly order/cycle     │     │  Reserver.Reserve/Ack/Nack (doorbell, │
│    maintenance (§4)             │     │    best-effort, never load-bearing)   │
│  - trigger-rule evaluation      │     │  per-subscriber bounded chan +        │
│    (all_success/all_done/...)   │     │    OverflowPolicy (drop-oldest/block/ │
│  - Sealed+notTerminalCount      │     │    close-slow), never blocks producer │
│    completion detection (O(1))  │     └───────────────────────────────────────┘
└───────────────┬─────────────────┘                     ▲
                │                                        │ emits Event{Seq,...}
                ▼                                        │ after commit, never before
┌───────────────────────────────────────────────────────┴─────────────────────┐
│                          LEASE MANAGER                                       │
│  claim = atomic(grant ownership + lease_epoch++ + deadline) in ONE write     │
│  ack/nack/extend = CAS keyed on lease_epoch (fencing token == attempt #)     │
│  sweep = inline-on-claim-path reclaim (correctness) + optional cross-        │
│          instance shard/heartbeat sharding (efficiency only, never required) │
└───────────────┬───────────────────────────────────────────────────────────────┘
                │
                ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                          STORAGE PORT  (dagstore.Store)                       │
│   mandatory core: GetNode/PutNode/CreateNode/Transition/AddEdges/DeleteNode    │
│   optional facets: Lister · ReadyQueue · TimeoutSweeper · EventStream ·        │
│                     ConditionalDeleter   (type-asserted, never emulated)       │
└───────┬───────────────┬───────────────┬───────────────┬──────────────────────┘
        ▼               ▼               ▼               ▼
   ┌─────────┐    ┌───────────┐   ┌───────────┐   ┌────────────────────┐
   │ IN-MEM  │    │  REDIS    │   │ POSTGRES  │   │ MEMCACHED           │
   │ sharded │    │ HASH+ZSET │   │ SKIP      │   │ NodeCache only —    │
   │ map+RW  │    │ +Lua/Fns  │   │ LOCKED +  │   │ never implements    │
   │ mutex,  │    │ +Streams  │   │ LISTEN/   │   │ Store; read-through │
   │ CSR adj │    │ (events)  │   │ NOTIFY    │   │ decorator in front  │
   └─────────┘    └───────────┘   └───────────┘   └────────────────────┘
```

### 2.1 Hot path — `AddNode` / `AddEdges` (insert)

1. Caller supplies a string `NodeID` + payload + zero or more predecessor `NodeID`s (same scope).
2. **Idempotency check**: same ID + byte-identical spec ⇒ no-op returning the existing node
   (Stripe-style; 12 §5.1). Same ID + different spec ⇒ typed `ErrIDConflict`.
3. For each predecessor edge: if the predecessor is already `Success`, the edge is born
   pre-satisfied (no `pending` increment); if the predecessor is `Error`, the new node is
   immediately `Error/UpstreamFailed`, never `New` (12 §2.5). Otherwise `pending++` and the edge is
   recorded as unsatisfied.
4. **Cycle/order check** happens only against non-satisfied edges: `ord(pred) < ord(target)` is an
   O(1) accept (the common, causally-ordered case); otherwise a bounded local search decides
   cycle-vs-reorder (02 §2.4, §3.2; 12 §2.3; 04 §14.4).
5. If the node's edge was inserted into an already-`Ready` node, popping it back out of the
   ready-set and incrementing `pending` must be **one atomic operation** with the edge insert —
   otherwise a worker can claim it in the gap (12 §2.2).
6. `pending == 0` ⇒ push to the per-`(scope, kind)` ready-set; this is the only path that ever adds
   to the ready-set, and it is `O(1)` amortized per edge over the DAG's whole life (02 §1.2).
7. Emit `EventTransition` (first appearance) after the write commits, carrying `Seq` (10 §5.2).

### 2.2 Hot path — `Claim` (dispatch to a worker)

1. Worker calls `Claim(ctx, scope, ClaimOptions{Kind, LeaseTimeout, MaxNodes})`.
2. Storage backend's `ReadyQueue.ClaimReady` executes the single atomic claim-with-lease primitive:
   Postgres — one `SKIP LOCKED` CTE chained into an `UPDATE … RETURNING`; Redis — one Lua
   Function doing `ZPOPMIN` + hash mutation + `ZADD` into the deadline ZSET; in-memory — one
   mutex-guarded pop + slab write (03 §5b, 04 §14.1, 05 §15.2).
3. The write bumps `lease_epoch` (= new `attempt`) and sets `lease_deadline = server_clock_now() +
   timeout`, **never** a client-computed wall-clock value (03 §5a — Redis `TIME`, Postgres
   `clock_timestamp()`, never `now()`).
4. The claim response carries `{NodeID, Payload, LeaseEpoch, LeaseDeadline}`. The SDK's managed
   heartbeat loop (08 §9.3, `context.AfterFunc`) starts a `context.WithDeadlineCause` timer; if it
   fires with no ack, the node is marked `Error/Timeout` by the *same* fenced write path a sweeper
   would use (§2.3 below), never by a separate "worker declares itself dead" signal.
5. Emit `EventTransition(New→InProgress)`.

### 2.3 Hot path — `Ack`/`Nack` (complete)

1. Worker calls `Ack(ctx, lease, result)` or `Nack(ctx, lease, err)`, presenting the `LeaseEpoch` it
   was handed at claim time.
2. Storage executes a **single fenced CAS**: `WHERE lease_epoch = $presented AND status =
   'in_progress'`. Zero rows affected ⇒ stale ack (already reclaimed by a sweeper, or a duplicate)
   — reject, never silently accept, never tell the worker to retry (03 §5b; 12 §3.4).
3. On success: `status = Success` (or `Error` with `Outcome.Reason`), and — in the **same atomic
   operation** — every direct successor's `pending` is decremented (Postgres: `UPDATE … RETURNING`
   chained CTE with a deterministic ascending-`node_id` lock order to avoid the fan-in deadlock
   class; Redis: one Lua Function; in-memory: one mutex-guarded walk of the CSR out-edges) (02 §1.2,
   §6.2; 04 §14.2, §15.2).
4. Any successor whose `pending` hits zero is pushed to the ready-set **inside the same write**.
5. The node's own consumer-refcount (for payload GC, 02 §6) is decremented in the same atomic op.
6. Emit `EventTransition(InProgress→Success|Error)` after commit, then `EventReady` for every
   successor that just became ready (10 §5, §3.3) — two different event kinds, two different
   backpressure/durability policies (10 §7.2).
7. If `Sealed && notTerminalCount == 0` after this transition, the scope is complete — an O(1)
   counter check, never a scan (12 §2.7).

---

## 3. Numbered decisions (ADR seeds)

| ID | Decision | Choice | Rationale (one line) | Dossier |
|---|---|---|---|---|
| ADR-01 | Public status vocabulary | Exactly 4 values: `New, InProgress, Success, Error`; never grow it | Every system that survived production scars converged here; timeout/cancel/skip ride a closed `Outcome.Reason`, not a 5th status | 01 §17, 12 §1.4 |
| ADR-02 | Internal scheduling detail | `Phase` enum (`Blocked/Ready/Claimed/...`) collapses to `StatusNew`/`InProgress` publicly; exposed only via a debug accessor | Blocked↔Ready churn from dynamic edges must never look like a status regression to a subscriber | 12 §1.5.1 |
| ADR-03 | Ready-set maintenance | Kahn in-degree/`pending[]` decrement on completion only; never rescan the graph | O(1) amortized per edge over the DAG's life; the entire "sublinear ready-set" requirement | 02 §1 |
| ADR-04 | Cycle rejection at insert time | Maintain an incremental topological order (Pearce–Kelly); `ord(u)<ord(v)` fast-accept, bounded DFS otherwise, cycle-detection folded into the same walk | Only algorithm surveyed with real production adoption (Abseil/TensorFlow/JGraphT) and cost proportional to actual disruption | 02 §2.4, §3; 12 §2.3 |
| ADR-05 | Predecessor-satisfied tracking | Per-node SET of not-yet-satisfied predecessor IDs (Redis SET / Postgres `edges.satisfied` boolean / in-memory adjacency+pending set) as the default; packed bitmap deferred | Dynamic edge *removal* needs addressable, idempotent per-predecessor cancellation a bare counter/bitmap-by-hash cannot give safely — see §10.1 contradiction | 05 §5; 02 §8 (resolved) |
| ADR-06 | Fencing token | Every node carries a monotonic `lease_epoch`, bumped on every claim/reclaim; every ack/extend/sweep CAS's on it in the same write as the mutation; mandatory on every backend from day one | Kleppmann's fencing argument — a lease alone never makes a write safe; retrofitting later touches every ack path | 03 §3.4, §5b; 01 §16; 06 (Temporal RangeID) |
| ADR-07 | Claim atomicity | One atomic primitive per backend grants ownership **and** records the lease deadline in the same write; no backend ever implements reclaim as a separate sweeper-only path | Faktory/Asynq/K8s-finalizer pattern — the only systems surveyed needing zero external sweeper for correctness | 01 §11.2, §18; 03 §5b-d |
| ADR-08 | Clock authority | Every deadline comparison reads the storage backend's own clock (Redis `TIME`, Postgres `clock_timestamp()`, in-memory monotonic `time.Now()`); never a client-computed wall-clock value; lint-banned `now()` in Postgres SQL | Spanner/TrueTime lesson generalizes: independent clock reads are never safe for a hard boundary; only one clock can be authoritative | 03 §3.5, §5a |
| ADR-09 | Delivery guarantee | Document precisely as "at-least-once delivery, at-most-once accepted effect per lease epoch" ("effectively-once"); never claim exactly-once | Two Generals / FLP — exactly-once *delivery* to an external process is provably impossible | 03 §5e; 10 §2 |
| ADR-10 | Heartbeat vs. ack are separate RPC shapes | `Extend(nodeID, epoch, dur)` (liveness, low-stakes, idempotent-ish) is a distinct call from `Ack`/`Nack` (terminal, fenced, high-stakes); SDK owns the heartbeat loop, not hand-written host code | Kafka's own `session.timeout` vs `max.poll.interval` split shows conflating liveness with progress causes false evictions | 03 §2.3, §5c |
| ADR-11 | Retry = new attempt, same NodeID | `attempt`/`lease_epoch` is one field doing double duty as retry-count and fencing token | River's own documented "stuck running, needs a rescuer" failure mode is exactly the race this closes | 12 §3.4 |
| ADR-12 | Default backoff | Full Jitter: `random(0, min(cap, base·2^attempt))`, settable per node at claim time alongside the ack timeout | AWS's own head-to-head testing; Step Functions later shipped it as `JitterStrategy: FULL` | 12 §3.3 |
| ADR-13 | Cross-instance work distribution, v1 | Pure pull-based competition on the shared storage's native atomic claim; no partitioning, no membership table | Only strategy all four required backends implement natively with zero external coordinator; degrades gracefully on instance death | 01 §15; 07 §7.3; 05 §9.1 |
| ADR-14 | Work distribution upgrade path | Internal partition-assignment function is a swappable interface from line one (even while v0.1 is the trivial P=1 case); v2 = jump-consistent-hash node→partition + HRW partition→instance + bounded-load capping; never a public API change | Node→partition is append-only/instance-agnostic (jump hash's sweet spot); partition→instance churns at arbitrary points (HRW's sweet spot) — conflating them reintroduces the exact failure jump hash's own paper warns against | 07 §3, §7 |
| ADR-15 | Leader election scope | Reserved strictly for a periodic, low-frequency maintenance task (cross-instance sweep-shard rebalancing); never the work-dispatch mechanism | Airflow's own architectural history is a documented case study of a single-dispatcher cap being hit and re-architected away from | 07 §5.5; 01 §3.2-3.3 |
| ADR-16 | Storage port shape | Narrow mandatory `Store` core + optional capability facets (`Lister`, `ReadyQueue`, `TimeoutSweeper`, `EventStream`, `ConditionalDeleter`) discovered by type assertion; `Version`/`ClaimToken` are opaque `interface{ String() string }` | containerd/Terraform/`database/sql/driver` pattern; lets Redis/Postgres use native atomics instead of a lowest-common-denominator emulation | 06 §B.2-B.3 |
| ADR-17 | Memcached's role | Implements only a 3-method `NodeCache` (get/set-if-absent/invalidate) read-through decorator; never `Store` | No multi-key atomicity at any protocol version, no durable CAS across restart/eviction — durability trap if it silently backed the real contract | 06 Part A |
| ADR-18 | Conformance suite | One `RunConformance(t, harnessMaker)` (modeled on `blob/drivertest`/`testing/fstest.TestFS`) written **before** the second backend; per-capability sub-suites `Skip`, never silently pass | Keeps the capability table a live, tested contract instead of documentation that drifts | 06 §B.5; 11 |
| ADR-19 | Event bus shape | Two separate interfaces: `Bus.Subscribe` (observation, durable when backend allows) and `Reserver.Reserve/Ack/Nack` (doorbell, best-effort); `Reserve` always re-derives eligibility from storage, never from accumulated event history | Reproducing the "subscribe = get handed work" fusion is the Reactive-Streams push anti-pattern and an un-doable breaking change later | 10 §1, §9; 08 (open questions) |
| ADR-20 | Ordering & resume primitive | One per-node monotonic `Seq`, assigned atomically with the state write; doubles as staleness token and half the resume-cursor design; a scope-wide total order is opt-in only | Kafka's per-partition-only ordering guarantee generalizes to per-node; a global counter would serialize every writer in the scope | 10 §3.3, §5.2 |
| ADR-21 | Recovery model | State-plus-notification, not event-sourced-log-as-truth; `ErrCursorExpired` is one typed error with one recovery procedure (full state read, then resubscribe from now) across every backend | Fowler's own caveat: external systems can't tell real dispatch from replay; a general event-sourced rebuild is unneeded surface area | 10 §4.3, §6 |
| ADR-22 | Subscriber backpressure | Bounded per-subscriber channel with an explicit `OverflowPolicy` (default `DropOldestAndMarkGap`); the fan-out point never blocks on any one subscriber | NATS/etcd/client-go convergence: a slow subscriber must never become the producer's problem (head-of-line blocking) | 08 §8.1-8.5; 10 §7.1 |
| ADR-23 | Scopes | Opaque caller-chosen string, created implicitly on first use; key-prefixed physical isolation in every backend; cross-scope `AddEdge` rejected outright (`ErrCrossScopeEdge`) | Keeps cycle-check, completion-detection, retention, and GC all provably scope-local — the precondition for the O(1)/O(log n) goal per operation | 12 §4 |
| ADR-24 | Scope completion | `Sealed && notTerminalCount == 0`, an atomically-maintained counter; caller-driven `Seal()`, never inferred | Collapses the general Dijkstra-Scholten/Safra termination-detection problem to a cheap local check because the graph is one owned, transactionally-inspectable store | 12 §2.7 |
| ADR-25 | Node identity & idempotency | Caller-supplied string `NodeID`, required; idempotent insert = same-ID-same-payload no-op, same-ID-different-payload typed conflict (Stripe pattern) | Library-generated IDs make correct retry-after-timeout impossible for the caller to express | 12 §5.1 |
| ADR-26 | Payload & sizing | `[]byte`/`json.RawMessage` at the storage/wire boundary (never generic); 256 KiB default cap = `min(library default, caller override, backend real limit)`; generics only as an optional in-process `Typed[T]` convenience wrapper | Cross-process shared storage means the payload boundary is necessarily byte-oriented; 256 KiB sits safely under Memcached's 1 MiB item ceiling | 08 §7; 12 §5.2-5.3 |
| ADR-27 | Go API shape | `New(store Store, opts ...Option)` — Uber's opaque-`Option`-interface functional options, required `store` positional; no `context.Context` ever stored on a struct; full sentinel+typed error taxonomy pre-v0.1; `Close` blocks until every owned goroutine has exited | Matches the Google/Uber style guides' own decision tables; a library embedded for years needs the compatibility discipline from commit one | 08 §2-6, §9 |
| ADR-28 | In-memory backend internals | Struct-of-arrays keyed by dense, generation-counted `int32` handles; sharded (striped) `RWMutex` map, never a single lock and never bare `sync.Map`; interned string IDs at the boundary only | GC cost is linear in *pointer-bearing* live heap, not object count (Discord/BigCache evidence); 256-shard knee measured at 8.3-9x over one mutex | 09 Part 1-2 |
| ADR-29 | Minimum Go version | Go ≥ 1.25 | Gets `sync.WaitGroup.Go`, GA `testing/synctest`, and per-iteration loop vars (already true since 1.22) guaranteed with no `GOEXPERIMENT` flag; greenfield library has no legacy floor to protect | 08 §2.5 note, "Recommend Go >=1.25"; 11 (resolved — see §10.4) |
| ADR-30 | Trigger rules, v1 | Ship exactly five: `all_success` (default), `all_done`, `none_failed`, `none_failed_min_one_success`, `always`; defer the `one_*` early-fire family | All five are evaluable incrementally as predecessors terminate; `one_*` needs a second "ready" concept the core transition table doesn't model yet | 12 §3.2 |
| ADR-31 | Repository/module topology | Multi-module monorepo: core has zero network/DB deps; each storage backend and each network adapter is its own Go module with its own `go.mod`/semver; `dagworkerd` daemon is the only module allowed to import everything | Go's own doc: split exactly where an optional feature drags in a dependency tree the median user doesn't want (gocloud.dev is the negative example) | 15 §1 |

---

## 4. Public Go API surface

```go
// Package dagworker is the public entry point. It never imports net/http,
// database/sql, redis, or grpc — those live in optional adapter/storage
// modules under github.com/<org>/dagworker/{storage,adapters}/*.
package dagworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ---------------------------------------------------------------- identity

type Scope string
type NodeID string

// ---------------------------------------------------------------- status (ADR-01, ADR-02)

// Status is the entire public vocabulary. Four values, forever.
type Status uint8

const (
	StatusNew        Status = iota // exists, not yet acked by a worker (blocked or ready — see Phase)
	StatusInProgress               // claimed by a worker, ack pending
	StatusSuccess                  // terminal, succeeded
	StatusError                    // terminal, did not succeed — see Outcome.Reason for why
)

func (s Status) Terminal() bool { return s == StatusSuccess || s == StatusError }

// Reason is closed and small; free text lives only in Outcome.Message.
type Reason uint8

const (
	ReasonNone Reason = iota
	ReasonWorkerError
	ReasonTimeout
	ReasonUpstreamFailed
	ReasonSkipped
	ReasonCancelled
	ReasonRemoved
)

type Outcome struct {
	Reason    Reason
	Message   string // host/engine text, size-capped
	Attempt   uint32 // 1-based; also the fencing epoch, see ADR-06/ADR-11
	Timestamp time.Time
}

// Phase is INTERNAL scheduling detail — never on the wire, never on the
// subscription stream. Exposed read-only for debug/admin tooling only;
// no stability promise across minor versions (ADR-02).
type Phase uint8

const (
	PhaseBlocked Phase = iota
	PhaseReady
	PhaseClaimed
	PhaseSucceeded
	PhaseFailed
	PhaseTimedOut
	PhaseUpstreamFailed
	PhaseSkipped
	PhaseCancelled
)

// ---------------------------------------------------------------- node shape

type Node struct {
	Scope     Scope
	ID        NodeID
	Kind      string // ready-set partition key, see ADR-26 note on labels
	Status    Status
	Outcome   Outcome
	Payload   []byte // json.RawMessage or opaque bytes; ADR-26
	Labels    map[string]string
	Priority  int16
	Seq       Seq // ADR-20
	UpdatedAt time.Time
}

type Seq uint64 // per-node monotonic; also the resume-cursor unit (ADR-20)

// ---------------------------------------------------------------- errors (ADR-27)

var (
	ErrNotFound         = errors.New("dagworker: not found")
	ErrConflict         = errors.New("dagworker: conflict")
	ErrIDConflict       = errors.New("dagworker: id exists with a different spec")
	ErrCycle            = errors.New("dagworker: dependency cycle")
	ErrCrossScopeEdge   = errors.New("dagworker: edge crosses scopes")   // ADR-23
	ErrAlreadyTerminal  = errors.New("dagworker: node already terminal") // ADR-23/12 §2.2, no WithReopen
	ErrNodeInFlight     = errors.New("dagworker: node is in-progress")
	ErrLeaseExpired     = errors.New("dagworker: lease expired")
	ErrLeaseMismatch    = errors.New("dagworker: lease epoch mismatch") // ADR-06 stale ack
	ErrScopeEmpty       = errors.New("dagworker: no ready node in scope")
	ErrScopeSealed      = errors.New("dagworker: scope is sealed")
	ErrClosed           = errors.New("dagworker: manager closed")
	ErrPayloadTooLarge  = errors.New("dagworker: payload exceeds cap")
	ErrSubscriberLagged = errors.New("dagworker: subscriber lagged and was disconnected")
	ErrCursorExpired    = errors.New("dagworker: resume cursor older than retained history") // ADR-21
	ErrNilStore         = errors.New("dagworker: store must not be nil")
	ErrInvalidConfig    = errors.New("dagworker: invalid option value")
)

type CycleError struct {
	Scope Scope
	Path  []NodeID
}

func (e *CycleError) Error() string { return fmt.Sprintf("dagworker: cycle in scope %q", e.Scope) }
func (e *CycleError) Unwrap() error { return ErrCycle }

// ---------------------------------------------------------------- options (ADR-27)

type Option interface{ apply(*config) }

type config struct {
	defaultLeaseTimeout time.Duration
	clock               Clock
	logger              *slog.Logger
	subscriberBuffer    int
	overflow            OverflowPolicy
	shardCount          int
	payloadCap          int
	retention           RetentionPolicy
}

func WithDefaultLeaseTimeout(d time.Duration) Option
func WithClock(c Clock) Option // test seam, ADR-27 / 08 §11
func WithLogger(l *slog.Logger) Option
func WithSubscriberBufferSize(n int) Option
func WithOverflowPolicy(p OverflowPolicy) Option
func WithPayloadCap(n int) Option
func WithRetention(r RetentionPolicy) Option

func New(store Store, opts ...Option) (*Manager, error)

// ---------------------------------------------------------------- Manager

type Manager struct{ /* unexported */ }

// AddNode is idempotent (ADR-25). deps must be in the same Scope (ADR-23).
func (m *Manager) AddNode(ctx context.Context, scope Scope, id NodeID, payload []byte, opts ...NodeOption) error

// AddNodes is atomic per call within one scope — either every node/edge in
// the batch lands or none does (12 §2.6).
func (m *Manager) AddNodes(ctx context.Context, scope Scope, specs []NodeSpec) error

func (m *Manager) AddEdge(ctx context.Context, scope Scope, from, to NodeID) error
func (m *Manager) RemoveEdge(ctx context.Context, scope Scope, from, to NodeID) error
func (m *Manager) RemoveNode(ctx context.Context, scope Scope, id NodeID) error // rejects in-flight, 12 §2.4

func (m *Manager) GetNode(ctx context.Context, scope Scope, id NodeID) (Node, error)

// Claim blocks (subject to ctx) until at least one ready node is available
// or ctx is done. Never returns ErrScopeEmpty to a blocking caller.
func (m *Manager) Claim(ctx context.Context, scope Scope, opts ...ClaimOption) (*Claim, error)

// TryClaim is the non-blocking variant; returns ErrScopeEmpty immediately.
func (m *Manager) TryClaim(ctx context.Context, scope Scope, opts ...ClaimOption) (*Claim, error)

type Claim struct {
	Node       Node
	LeaseEpoch uint64 // ADR-06 / ADR-11 fencing token == attempt number
	Deadline   time.Time
}

// Ack/Nack/Extend are fenced on LeaseEpoch (ADR-06, ADR-10). A stale epoch
// returns ErrLeaseMismatch, never a retryable error.
func (m *Manager) Ack(ctx context.Context, c Claim, result []byte) error
func (m *Manager) Nack(ctx context.Context, c Claim, cause error) error
func (m *Manager) Extend(ctx context.Context, c Claim, newTimeout time.Duration) (time.Time, error)

// Scope-level operations (ADR-23, ADR-24).
func (m *Manager) Seal(ctx context.Context, scope Scope) error
func (m *Manager) IsComplete(ctx context.Context, scope Scope) (bool, error)
func (m *Manager) Health(ctx context.Context, scope Scope) (ScopeHealth, error) // reason-aware, 12 §1.5.4
func (m *Manager) CancelScope(ctx context.Context, scope Scope) error
func (m *Manager) Cancel(ctx context.Context, scope Scope, id NodeID) error

// Subscribe is the observation feed (ADR-19, ADR-20, ADR-22).
func (m *Manager) Subscribe(ctx context.Context, opts SubscribeOptions) (*Subscription, error)

// Handle is sugar over Subscribe for callers who prefer a callback (08 §8.6).
func (m *Manager) Handle(ctx context.Context, opts SubscribeOptions, fn func(Event)) (stop func(), err error)

// Close blocks until every goroutine the Manager started has exited (ADR-27, 08 §9.5).
func (m *Manager) Close(ctx context.Context) error

// ---------------------------------------------------------------- events

type EventKind uint8

const (
	EventTransition EventKind = iota // ADR-19: observation, durable when backend allows
	EventReady                       // ADR-19: doorbell, always best-effort/coalescing
)

type Event struct {
	Seq      Seq
	Scope    Scope
	NodeID   NodeID
	Kind     EventKind
	From, To Status
	At       time.Time
}

type OverflowPolicy int

const (
	OverflowDropOldestAndMarkGap OverflowPolicy = iota // default, ADR-22
	OverflowBlock                                      // per-subscriber slot only, never the producer
	OverflowCloseSlow
)

type SubscribeOptions struct {
	Scope      Scope // "" subscribes to every scope
	From       Seq   // resume token; 0 = start from now
	BufferSize int
	Overflow   OverflowPolicy
	Durable    bool // WithDurable, 10 §7.2 — opt into the durable tier per-subscribe
}

type Subscription struct{ /* unexported */ }

func (s *Subscription) Events() <-chan Event
func (s *Subscription) Err() error
func (s *Subscription) Close() error

// ---------------------------------------------------------------- generics escape hatch (ADR-26)

// Typed is an optional, in-process-only convenience layer — never the
// primitive the Store interface or event stream are defined in terms of.
type Typed[T any] struct{ /* unexported */ }

func NewTyped[T any](m *Manager, scope Scope) Typed[T]
func (t Typed[T]) AddNode(ctx context.Context, id NodeID, payload T, deps ...NodeID) error
func (t Typed[T]) Claim(ctx context.Context, opts ...ClaimOption) (TypedClaim[T], error)

type TypedClaim[T any] struct {
	Claim
	Payload T
}

// ---------------------------------------------------------------- Clock (08 §11)

type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) (<-chan time.Time, func() bool)
	AfterFunc(d time.Duration, f func()) (stop func() bool)
}
```

---

## 5. Node state machine — public status × internal phase × actor

Legend: `Engine` = internal scheduler logic (no external call); `Caller` = host program via public
API; `Worker` = external worker via `Ack`/`Nack`/`Extend`; `Sweeper` = fenced reclaim path (may run
inline on the claim path or as a background loop — same fencing rule either way).

| # | From (Status/Phase) | To (Status/Phase) | Actor | Trigger | Storage op (fencing?) |
|---|---|---|---|---|---|
| T1 | — | New/Blocked | Caller | `AddNode` with unmet deps | INSERT node; init pending counter |
| T2 | — | New/Ready | Caller | `AddNode` with zero unmet deps | INSERT node; push ready-set |
| T3 | New/Blocked | New/Ready | Engine | last unmet predecessor edge resolves | atomic decrement→0; push ready-set (no fence needed — idempotent per-edge guard, ADR-05) |
| T4 | New/Blocked | New/Blocked | Caller | `AddEdge` into an already-blocked node | increment pending; INSERT edge |
| T5 | New/Ready | New/Blocked | Caller | `AddEdge` inserting an unresolved predecessor into a Ready node | atomic: pop ready-set + increment pending + INSERT edge (must be one op, §2.1 step 5) |
| T6 | New/Ready | InProgress/Claimed | Engine (on Worker's `Claim`) | `Claim(scope, kind)` | atomic pop + **fence bump** (`lease_epoch++`) + set deadline — **fenced** |
| T7 | InProgress/Claimed | Success | Worker | `Ack` | CAS on `lease_epoch` — **fenced**; fan-out successor decrements in same op |
| T8 | InProgress/Claimed | Error/Failed | Worker | `Nack` | CAS on `lease_epoch` — **fenced**; `Outcome.Reason=WorkerError` |
| T9 | InProgress/Claimed | New/Ready (retry) | Engine | `Nack` and `attempt < maxAttempts` | CAS; re-arm with Full Jitter delay; push ready-set (delayed) |
| T10 | InProgress/Claimed | Error/TimedOut | Sweeper | lease deadline elapsed, no ack | CAS `WHERE lease_epoch = X AND status = InProgress` — **fenced** |
| T11 | New/Blocked | Error/UpstreamFailed | Engine | required predecessor reached Error, trigger rule unsatisfiable | SET status=Error, Outcome; recurse to successors |
| T12 | New/\* | Error/Skipped | Engine | trigger-rule evaluation determines rule can never satisfy | SET status=Error, Outcome=Skipped; recurse |
| T13 | New/\* or InProgress/Claimed | Error/Cancelled | Caller | `Cancel`/`CancelScope` | CAS status ∈ {New,InProgress}→Error; recurse if fail-fast |
| T14 | any node with live successors | Error/Removed (successors only) | Caller | `RemoveNode`/`RemoveEdge` | successors' dep can no longer resolve ⇒ SET Error/Removed; the removed node is hard-deleted |
| T15 | Success or Error (terminal) | — (deleted) | Caller/GC | retention sweep past TTL and subscriber low-water mark | DELETE node + tombstone bookkeeping |

Invariants this table preserves (12 §1.6): every row that changes `Status` writes exactly one
`Outcome` in the same op; **T6 and T10 are the only rows requiring the fencing CAS at the point of
the mutation** — every other actor either only ever originates a transition (Caller/Engine) or is
itself gated transitively through T6/T7/T8/T10; nothing transitions out of `Success` except T15
(no `AddEdge` may re-block a terminal node by default — `WithReopen()` is an explicit, separately
named, unshipped-in-v1 opt-in, ADR-23-adjacent, flagged in Open Questions).

---

## 6. Storage port (`dagstore`)

```go
package dagstore

import (
	"context"
	"errors"
	"time"
)

// Version is an opaque, backend-defined optimistic-concurrency token
// (Vitess topo.Version shape). Never construct or compare one directly.
type Version interface{ String() string }

// ClaimToken is opaque and backend-defined, carries the fencing epoch.
type ClaimToken interface{ String() string }

type NodeStatus uint8

const (
	StatusNew NodeStatus = iota
	StatusInProgress
	StatusSuccess
	StatusError
)

type Node struct {
	Scope      string
	ID         string
	Status     NodeStatus
	Payload    []byte
	LeaseEpoch uint64    // ADR-06 — present even when Status != InProgress (last-seen value)
	Deadline   time.Time // zero if no worker currently holds it
	UpdatedAt  time.Time
	Seq        uint64 // ADR-20
}

type Edge struct{ Scope, From, To string }

var (
	ErrNotFound        = errors.New("dagstore: node not found")
	ErrVersionMismatch = errors.New("dagstore: version mismatch")
	ErrAlreadyExists   = errors.New("dagstore: node already exists")
	ErrCapability      = errors.New("dagstore: capability not supported by this backend")
)

// Store is the mandatory core (ADR-16). Every backend implements exactly
// this and nothing more to be usable at all — memcached does NOT implement
// this interface (ADR-17); it implements NodeCache instead (§6.1 below).
type Store interface {
	GetNode(ctx context.Context, scope, id string) (Node, Version, error)
	PutNode(ctx context.Context, n Node, expected Version) (Version, error)
	CreateNode(ctx context.Context, n Node) (Version, error)

	// Transition is atomic w.r.t. any other Transition/PutNode racing on the
	// same node. Backends use native single-record atomics; never emulated
	// as a lowest-common-denominator loop when the backend has something
	// better. See conformance test T-ATOMIC-TRANSITION.
	Transition(ctx context.Context, scope, id string, from, to NodeStatus) error

	// AddEdges is atomic per call: either every edge is visible or none is
	// (T-EDGE-BATCH-ATOMICITY). A backend lacking multi-key transactions
	// refuses via ErrCapability rather than offering partial semantics.
	AddEdges(ctx context.Context, edges ...Edge) error

	DeleteNode(ctx context.Context, scope, id string) error
	Close(ctx context.Context) error
}

// --------------------------------------------------------- optional facets (ADR-16)

type Lister interface {
	ListNodes(ctx context.Context, scope, cursor string, limit int) (nodes []Node, next string, err error)
}

// ReadyQueue grants exactly-once-in-flight claims fleet-wide and starts the
// ack-timeout clock in the same atomic operation as the grant (ADR-07).
type ReadyQueue interface {
	ClaimReady(ctx context.Context, scope string, ackTimeout time.Duration) (Node, ClaimToken, error)
	Ack(ctx context.Context, token ClaimToken) error
	Nack(ctx context.Context, token ClaimToken) error
}

// TimeoutSweeper discovers claims whose deadline has elapsed. May be called
// inline by the claim path (preferred, ADR-07) or on a ticker; either way
// every write it performs is fenced on the token it observed (ADR-06).
type TimeoutSweeper interface {
	SweepTimedOut(ctx context.Context, scope string, now time.Time) (int, error)
}

// EventStream fans transitions/doorbells to subscribers. Truthfully report
// durability tier via CapabilityReporter — do not silently reimplement
// pub/sub badly on a backend that can't do better (ADR-19, resolves the
// open question in 06 via 10's per-event-kind durability split).
type EventStream interface {
	Subscribe(ctx context.Context, scope string) (<-chan Event, error)
}

type ConditionalDeleter interface {
	DeleteNodeIf(ctx context.Context, scope, id string, expected Version) error
}

type Event struct {
	Scope, NodeID string
	Kind          EventKind
	From, To      NodeStatus
	Seq           uint64
	At            time.Time
}

type EventKind uint8

const (
	TransitionEvent EventKind = iota
	MustClaimEvent
)

// --------------------------------------------------------- capability discovery

type CapabilityReporter interface{ Capabilities() CapabilitySet }

type CapabilitySet uint32

const (
	CapList CapabilitySet = 1 << iota
	CapReadyQueue
	CapTimeoutSweep
	CapEventStream
	CapEventStreamDurable // NodeStatusChanged tier is at-least-once, not just best-effort (ADR-19/10 §7.2)
	CapConditionalDelete
	CapMultiKeyAtomicEdges
)

func (cs CapabilitySet) Has(c CapabilitySet) bool { return cs&c != 0 }

// --------------------------------------------------------- memcached's actual role (ADR-17)

// NodeCache is the ONLY interface a memcached adapter implements. It is a
// read-through/invalidate-on-write decorator in front of a real Store, not
// a Store itself — see §7 capability matrix.
type NodeCache interface {
	Get(ctx context.Context, scope, id string) (Node, bool, error)
	SetIfAbsent(ctx context.Context, n Node) error
	Invalidate(ctx context.Context, scope, id string) error
}
```

---

## 7. Per-backend capability matrix

| Guarantee | In-memory | Redis | PostgreSQL | Memcached |
|---|---|---|---|---|
| `Store` core (CRUD + fenced `Transition`) | Yes — sharded map + per-shard `RWMutex`, `Version` = incrementing `uint64` | Yes — HASH per node + Lua/Function CAS on a `rev` field | Yes — one row/scope-partition, `Version` = explicit `revision` column | **No — never implements `Store`** |
| `AddEdges` batch atomicity (`CapMultiKeyAtomicEdges`) | Yes — global scope lock for the call's duration | Yes — one Lua/Function touching all keys under the `{scope}` hash tag | Yes — one SQL transaction | N/A |
| `Lister` | Yes — in-process index | Yes — per-scope ZSET of member IDs | Yes — indexed `WHERE scope=$1` keyset pagination | No — no scan primitive at any protocol version |
| `ReadyQueue` (atomic claim+lease, fenced) | Yes — buffered channel + pending-set + `AfterFunc` per claim | Yes — ZSET (`ZPOPMIN`) fused with lease HASH write, one Function call | Yes — `SKIP LOCKED` CTE chained into `UPDATE…RETURNING` | No — no multi-key atomic, no ordered structure |
| `TimeoutSweeper` | Yes — min-heap on a ticker | Yes — deadline ZSET, `ZRANGEBYSCORE …LIMIT` | Yes — partial index `WHERE status='in_progress'` on `lease_deadline` | No — must bound and document a max in-flight ceiling if ever attempted (not shipped) |
| `EventStream` — `NodeStatusChanged` tier | Durable (in-process fan-out under the write lock) | Durable — Streams consumer group + PEL, `XAUTOCLAIM` | Durable — outbox table + relay, `pg_notify` as latency hint only | Best-effort polling only, documented as degraded |
| `EventStream` — `NodeReady` tier | Best-effort, coalescing | Best-effort — plain pub/sub over the same Stream | Best-effort — `LISTEN`/`NOTIFY`, 8000-byte payload cap | Best-effort — poll interval is the floor |
| `ConditionalDeleter` | Yes | Yes — Lua script | Yes — `DELETE…WHERE revision=$2` | **Yes** — `md key C<token>` on the meta protocol is a genuine single-key conditional delete (the one facet memcached honestly earns) |
| Clock authority for deadlines | monotonic `time.Now()` | `TIME` inside Lua | `clock_timestamp()`, never `now()` | N/A (no deadlines owned) |
| Fencing token (`lease_epoch`) | mutex-guarded `uint64` | Lua-scripted `HINCRBY` | `lease_epoch = lease_epoch + 1` in the claim `UPDATE` | N/A |
| Cross-instance work distribution | trivially single-owner (shared in-process) | native (`ZPOPMIN` atomicity) | native (`SKIP LOCKED`) | N/A — not a `ReadyQueue` |
| Durability across crash/failover | none (process-lifetime only) | documented gap: async replication by default, ~1s loss window unless `WAIT`/`WAITAOF` opted in per call | full — WAL-logged `nodes`/`edges`/`events`; `leases` table intentionally `UNLOGGED` | none — extstore explicitly non-durable, eviction indistinguishable from "never existed" |
| Recommended max scale (this design's target) | bounded by one process's RAM; benchmark target 1M nodes | 1M nodes/scope comfortably; one scope = one Cluster hash-slot ceiling | 1M nodes/scope with partitioned `nodes`/`events` tables and tuned autovacuum | cache-only; no independent node-count ceiling since it never owns the graph |

---

## 8. Repository layout

Multi-module monorepo (ADR-31, 15 §1.5). Only `cmd/dagworkerd` is allowed to import everything;
every other module resolves the minimum dependency set its job requires.

```
dag-worker-go/
├── go.mod                      module .../dagworker            — core only, near-zero deps
├── go.work                     dev-only convenience; not consumed by external `go get`
├── docs/
│   ├── adr/                    one file per ADR-NN from §3, immutable once accepted
│   └── research/                this synthesis + the 12+2 source dossiers
│
├── dagworker.go, options.go     public Manager/Option surface (§4)
├── errors.go                    sentinel + typed error taxonomy
├── status.go, phase.go          Status/Phase/Outcome/Reason (§5)
├── event.go, subscribe.go        Bus/Subscription types (ADR-19/20/22)
│
├── internal/
│   ├── engine/                  scheduler core: Kahn ready-set, trigger-rule eval, Seal/Health
│   ├── topo/                    Pearce–Kelly incremental order + cycle rejection (ADR-04)
│   ├── lease/                   fenced claim/ack/extend/sweep protocol (ADR-06/07/08/10)
│   ├── clock/                   Clock interface + real/fake implementations (08 §11)
│   ├── ready/                   per-(scope,kind) ready-set, sharded (09 §2.3)
│   ├── interner/                string→int32 handle interning (09 §1.4)
│   └── slab/                    generation-counted handle allocator (09 §1.6)
│
├── dagstore/                     storage port — Store core + optional facets (§6); no backend code
│   └── dagstoretest/             RunConformance suite (ADR-18); Harness interface
│
├── storage/
│   ├── memory/                   default backend: sharded map, CSR-with-slack adjacency (ADR-28)
│   │   └── go.mod                same module as core, OR folded into root — zero extra deps either way
│   ├── redis/
│   │   └── go.mod                module .../storage/redis — only this module resolves go-redis
│   │       (hash+ZSET+Lua/Functions for lease path; Streams for EventStream, §7)
│   ├── postgres/
│   │   └── go.mod                module .../storage/postgres — only this module resolves pgx
│   │       (SKIP LOCKED, partitioned nodes/events, UNLOGGED leases, outbox+NOTIFY)
│   └── memcached/
│       └── go.mod                module .../storage/memcached — NodeCache decorator only (ADR-17)
│
├── adapters/                       optional network adapters — dagworker core never imports these
│   ├── grpc/
│   │   └── go.mod                  module .../adapters/grpc
│   └── http/
│       └── go.mod                  module .../adapters/http  — resource+RPC hybrid (14 §1-5)
│
├── cmd/
│   └── dagworkerd/
│       └── go.mod                  module .../cmd/dagworkerd — composition root, imports everything
│                                    (config layering, health/ready, graceful shutdown — 15 Part 2)
│
├── examples/
│   └── go.mod                      own module, kept out of every real module's graph
│
└── test/
    ├── feature/                    behavioral tests, run in parallel, backend-agnostic via dagstoretest
    ├── perf/                       TestComplexity_* ratio-guard tests + 1M-node benchmark suite (09 §4)
    ├── storage/                    per-backend conformance + concurrency stress (11)
    ├── chaos/                      seeded chaos-wrapper around the in-memory store (11)
    └── e2e/
        └── docker-compose.test.yml  Redis/Postgres/Memcached on custom non-default ports, gated by
                                     DAGWORKER_INTEGRATION=1, health-checked via depends_on
```

---

## 9. Phased delivery plan

Each phase is independently shippable and tagged; none requires a public API break in a later
phase (the load-bearing constraint from ADR-13/14).

**v0.1 — Correctness kernel, single backend.**
In-memory `Store` only. Full public API surface (§4). Kahn ready-set (ADR-03), Pearce–Kelly cycle
rejection (ADR-04), fenced claim/ack/extend/sweep (ADR-06/07/08/10) with `P=1` pure pull
competition (ADR-13). Four-value `Status` + `Outcome.Reason` (ADR-01). `Bus.Subscribe` +
`Reserver` as two interfaces (ADR-19), in-process only. `dagstoretest.RunConformance` written
against this first backend. Property tests (rapid) for no-double-lease/eventual-readiness/
antichain/acyclicity. Ships as `v0.1.0`, usable standalone by any host that doesn't need shared
storage across processes.

**v0.2 — PostgreSQL backend.**
`storage/postgres` module. `SKIP LOCKED` claim (04 §14.1), fenced ack keyed by `lease_epoch`
(04 §14.2), partitioned `nodes`/`events` tables from day one (04 §10), `UNLOGGED leases`
(04 §11), outbox+`NOTIFY` event delivery (04 §3, 10 §5.4). Runs the same conformance suite.
`docker-compose.test.yml` and `DAGWORKER_INTEGRATION=1` gate introduced here.

**v0.3 — Redis backend.**
`storage/redis` module. HASH+ZSET+Lua/Function lease path (05 §9, §15), Streams-based
`EventStream` (05 §10), `{scope}` hash-tag discipline for Cluster-readiness (05 §9). Same
conformance suite passes. Cross-backend `TestComplexity_*` ratio guards (09 §4) become a required
CI job across all three real backends.

**v0.4 — Memcached NodeCache + 1M-node benchmark suite.**
`storage/memcached` module, `NodeCache` only (ADR-17), wrapping any of the three real backends.
The mandated 1,000,000-node benchmark suite is built out here per backend (in-memory, Redis,
Postgres) with `benchstat`-tracked nightly regression jobs and same-run dimensionless CI ratio
gates (09 §4). This is the first release the project can honestly call "benchmarked at 1M nodes
on every backend."

**v0.5 — Multi-instance work distribution, v0.2→v0.3 (07 §7.3).**
Internal virtual-partition layer (`P=32-64` default) with the naive modulo assignment first,
then swapped for HRW + bounded-load capping — an internal refactor behind the swappable interface
from ADR-14, no public API change. Storage-backed heartbeat membership (03 §5d, 07 §5.1).
Leader-elected sweep-shard rebalancing only (ADR-15).

**v0.6 — Dynamic-DAG hardening.**
Batch `AddNodes` atomicity and caps (12 §2.6), five trigger rules (ADR-30), `Full Jitter` retry
policy wired into `ClaimOption` (ADR-12), `Scope.Health()` reason-aware aggregate (12 §1.5.4),
retention/GC with subscriber low-water mark (12 §6). `WithReopen()` remains explicitly unshipped
(Open Questions §11).

**v0.7 — Optional network surface.**
`adapters/grpc`, `adapters/http` (14), `cmd/dagworkerd` composition root with layered config,
`/healthz`/`/readyz` split, graceful shutdown ordering (15 Part 2). These modules are entirely
optional; core's dependency graph is unaffected by their existence.

**v0.8 — Verification hardening.**
TLA+ spec of the lease/ack/timeout state machine (11), same-process Porcupine linearizability
check as a per-PR gate, seeded chaos harness exercising the four core properties under adversarial
timing, security review pass.

**v1.0 — API freeze.**
`Store` interface, event envelope, and on-storage schema (`schema_version`) frozen. 95%+ aggregate
coverage gate enforced. Full cross-backend benchmark report published with methodology and
hardware disclosed (River's own discipline, 01 §12.1). Independent semver per module from here on
(ADR-31); core crossing to a breaking `Store` change requires `/v2` per Go's import-compatibility
rule.

---

## 10. Contradictions between dossiers, resolved

### 10.1 Idempotent-decrement structure: packed bitmap vs. per-node SET

**02** (incremental-topo) recommends a **packed per-edge satisfied-bitmap** as the default
idempotency mechanism, citing a ~600-1000× storage advantage over a per-node SET at 1M-node/
out-degree-3 scale (§8.5-8.6). **05** (Redis backend, and by extension the reasoning independently
supports Postgres's `edges.satisfied` boolean design in **04**) recommends a **per-node SET of
not-yet-satisfied predecessor IDs**, explicitly *not* primarily for idempotency (the fencing-token
+ status guard on `complete_node` already makes that path idempotent) but because **dynamic edge
removal needs addressable, per-predecessor cancellation** that a bitmap addressed by a
dense-slot-assigned-at-insert-time cannot express without a slot-versioning/generation scheme that
neither dossier fully designs (02's own Open Questions flags this explicitly: "How does the
packed-bitmap edge-slot scheme survive node/edge removal?").

**Resolution:** ship the **per-node SET / per-edge-boolean design** (ADR-05) as the default across
all three real backends (Redis SET, Postgres `edges.satisfied` column, in-memory adjacency+pending
set), because (a) edge removal is a named requirement in the brief ("possibly" removed, and §2.4 of
dossier 12 treats it as a first-class transition, T14), (b) it gives free per-node observability
("what is this stuck node still waiting on" via `SMEMBERS`/a `WHERE satisfied=false` query) that
operators will need in production, and (c) the storage multiplier 02 computes (~225-270 MB vs
~366 KiB at 1M nodes/3M edges) is real but not disqualifying at the scale this project targets — it
is a single-digit percentage of the ~1-1.3 GB steady-state Postgres footprint 04 §16.1 computes for
the whole schema. Revisit the packed bitmap only as a documented v2 memory optimization, and only
once a generation-counter/slot-versioning scheme for safe removal is designed — do not ship it as
the default while that gap remains open.

### 10.2 Redis lease state: hand-rolled hash+ZSET vs. native Streams consumer groups

**03** (leases) and **05** (Redis) both flag this as unresolved: Streams' `XCLAIM`/`XAUTOCLAIM`
give PEL bookkeeping, poison-pill delivery counts, and cursor-based batch reclaim for free, but
Streams' idle-time model cannot express "this specific node's deadline, set by the caller at claim
time" — only "idle longer than the *claimer's* chosen threshold." A hand-rolled HASH (node record)
+ ZSET (deadline index) design *can* express a per-node deadline natively but reimplements what
Streams gives for free elsewhere.

**Resolution: use both, for different facets, which is not actually a contradiction once
decomposed.** The **lease/claim path** (`ReadyQueue`, `TimeoutSweeper`) uses the HASH+ZSET+Lua/
Function design from 05 §15, because it is the only one of the two that can express a
caller-chosen per-claim deadline. The **event bus** (`EventStream`) uses Redis Streams, because it
is unambiguously the right primitive for durable, replayable, consumer-group fan-out (05 §10, 10
§7.2) and neither dossier disputes that once the two facets are treated separately. This resolves
06's own open question ("is EventStream required to guarantee at-least-once… or is a truthfully
capability-flagged weaker tier acceptable") by construction: Redis's `EventStream` facet reports
`CapEventStreamDurable` because Streams-with-PEL genuinely earns it, which Redis's plain pub/sub
alone would not.

### 10.3 Cycle-check cost model: full Pearce–Kelly renumbering vs. single-node rank bump

**02** recommends implementing full Pearce–Kelly (bounded bidirectional DFS, affected-region
renumbering) as the primary mechanism. **04**'s concrete Postgres DDL (§14.4) ships a **simplified
single-node rank bump** in the slow-path fallback and explicitly flags it as an approximation whose
fast-path hit rate "should be validated... before committing to it long-term," i.e. a deliberately
lighter v1 than 02's own recommendation.

**Resolution:** ship 04's single-node rank-bump approximation for v0.1/v0.2 across all backends
(it preserves the load-bearing invariant — `ord(u) < ord(v)` for every direct edge — and is
materially simpler to get right under a deadline), instrumented with a fast-path-hit-rate metric
from day one. Escalate to full Pearce–Kelly affected-region renumbering only if that metric shows
degradation on real DAG shapes (the exact empirical trigger both 02's and 12's Open Questions call
for). This is a sequencing resolution, not a disagreement: both dossiers agree PK is the right
family; they disagree only on how much of it to build before measuring, and the measurement-first
position wins per this project's own "benchmark before publishing tuned numbers" discipline (04 §16).

### 10.4 Minimum Go version: 1.22 floor (11) vs. 1.25 floor (08)

**11** (testing) argues for pinning "go 1.22+ (prefer 1.25)" and separately raises as an open
question whether 1.25 is too aggressive a floor for enterprise adoption, suggesting a 1.23+
fallback with hand-rolled `errgroup`/excluded synctest tests as a hedge. **08** (Go API) argues
flatly for **≥1.25** to get `sync.WaitGroup.Go` and GA `testing/synctest` with no
`GOEXPERIMENT` flag, on the grounds that a greenfield library has no legacy user base to protect.

**Resolution: 1.25, per 08 (ADR-29).** This is a genuinely time-sensitive decision and the
resolution leans on the calendar: Go 1.25 shipped `sync.WaitGroup.Go` and GA `testing/synctest`
well over a year before this document's writing date, so "too aggressive for enterprise adoption"
— the concern 11 raises — has had a full release cycle to resolve itself in practice. A library
with zero existing users pays no compatibility cost for requiring a modern toolchain, and the
`context.AfterFunc`-based lease-timeout design (ADR-07/08 §9.3) and the `synctest`-based
deterministic timeout tests (11) both depend on this floor being real, not conditionally
compiled. If a future release genuinely needs to widen the floor for adoption reasons, that is a
`go.mod` `go` directive bump decision to make with real user data, not a speculative hedge to build
now.

### 10.5 Public status count: 5 values (01's own early framing) vs. 4 values (12's considered design)

**01**'s executive recommendations (written from the prior-art survey alone, before the dedicated
state-machine dossier existed) propose a **5-value** public vocabulary: `new / in-progress /
success / error / error-timeout`, explicitly keeping timeout as a peer of error. **12** (the
dossier whose entire scope is the state machine, and which cross-references 01's own comparison
table) argues decisively for **4 values**, with timeout folded into `Outcome.Reason`, and observes
that 01's own §17 comparison table is the evidence *for* the 4-value design once read carefully:
"no production system reviewed makes timeout its own peer of success/failure."

**Resolution: 4 values (ADR-01), per 12.** This is not really two dossiers disagreeing so much as
01's early framing being superseded by 12's later, more careful reading of the same evidence — 01's
own survey data (Step Functions' `States.Timeout` as an error *name*, Cloud Tasks'
`DEADLINE_EXCEEDED` as a *response status* not a task state, Temporal's four timeout *kinds*
all resolving to workflow-level `Failed`/`TimedOut`) supports 12's conclusion more than 01's own
initial 5-value recommendation. Treat 12 as authoritative on state-machine questions; 01 remains
authoritative on everything else it covers (leasing precedent, work-claiming taxonomy, prior-art
failure modes).

### 10.6 Sweeper coordination: zero-coordination-by-construction vs. an implied leader-elected sweeper

**03** and **07** both state plainly that correctness never requires sweeper coordination — the
fencing epoch alone makes duplicate sweeping merely wasteful, never wrong — and that cross-instance
sharding of the sweep is a pure efficiency optimization. **04**, discussing Postgres specifically,
raises as an open question whether the sweeper "should be one global process per storage cluster or
one advisory-lock-elected leader per scope," which reads as if leader election might be load-bearing
for that backend.

**Resolution:** no contradiction once read precisely — 04's own advisory-lock section (§5) frames
leader election for the sweeper as *"strictly cheaper... when the only thing being coordinated is
'run this loop somewhere,'"* i.e. an efficiency choice, matching 03/07 exactly. Adopt the
zero-coordination-by-construction design as the correctness baseline on every backend (inline
reclaim via the same fenced primitive the claim path uses is even simpler than a background
sweeper at all, per ADR-07), and offer an **optional** advisory-lock/heartbeat-elected sweeper loop
per scope purely to avoid redundant scanning once benchmarking shows it matters (ADR-15's leader-
election carve-out already covers this).

---

## 11. Open questions for the project owner

1. **`WithReopen()` — ship it at all?** Re-blocking a completed `Success`/`Error` node is explicitly
   designed as an opt-in escape hatch (12 §2.2) but is **not** in the v0.1-v0.6 phased plan above.
   Shipping it means adding an 8th `Reason` value (`ReasonReopened`) and designing an audit-log
   shape distinct from current `Outcome` (12's own Open Questions flags this as unresolved). Decide
   whether this ships in v1.0 or is deferred indefinitely as a documented non-goal.
   *Recommendation: defer past v1.0; nothing in the phased plan blocks adding it later as a
   backward-compatible opt-in.*

2. **Node-level heartbeat during long-running work.** The brief specifies a single flat per-node
   timeout (closer to Faktory's `reserve_for` model than Temporal's four-timeout model). Should a
   worker be able to report mid-flight progress (a Temporal-Activity-Heartbeat/Conductor-
   `callbackAfterSeconds` analogue) to reset its own deadline without a full `Extend` round trip, or
   is `Extend` (already in §4) sufficient? This is a product-shape decision, not an engineering one
   — it depends on whether the target host workloads include long-running external jobs (minutes to
   hours) where a single timeout value is awkward to size.
   *Recommendation: `Extend` as designed already covers this; do not add a separate heartbeat RPC
   unless a concrete workload demonstrates `Extend`'s call overhead matters.*

3. **Go ≥1.25 floor — confirmed acceptable?** ADR-29/§10.4 resolves this decisively for the design,
   but it is worth the owner explicitly signing off given it forecloses adoption by Go shops
   pinned to older toolchains. If the target embedder base skews conservative, this should be
   revisited before v0.1 tags, not after.

4. **Does `dagworkerd` (the optional daemon + gRPC/HTTP adapters, dossiers 14-15) belong in this
   project's v1.0 scope at all, or is it a separate, later initiative?** The phased plan places it
   at v0.7 as an optional, non-blocking module, but the brief's original scope ("library embedded
   into a host program") does not obviously require a standalone daemon. Confirm whether shipping a
   network-facing daemon is in scope before the multi-module repository layout (§8) is finalized,
   since removing it later is cheap (delete a module) but adding it as an afterthought reshapes the
   module boundary decisions made in ADR-31.

5. **Retention/GC defaults.** 24h terminal-node TTL and 72h max-subscriber-lag (12 §6.3, matching
   River's own defaults) are reasonable for job-queue-shaped workloads but may be wrong for a host
   running very long-lived batch DAGs where "the run" spans days. Confirm these defaults or specify
   per-workload overrides the library should expose more prominently.

6. **Is Memcached support, reinterpreted as "cache-only, never a `Store`," an acceptable reading of
   the original brief's "must also support Redis, Memcached, and PostgreSQL backends"?** Every
   dossier that touches Memcached (03, 05, 06) converges independently on the same conclusion: it
   cannot honestly be a durable backend for this workload. Confirm the owner is fine with Memcached
   appearing in the public capability matrix as a cache decorator rather than a peer backend,
   since this is a meaningful narrowing of the original requirement as literally stated.

7. **Cross-scope partition pooling for the v0.5 work-distribution upgrade** (07's own open
   question): should virtual partitions be namespaced strictly per-scope, or pooled across scopes on
   one shared instance-assignment ring? This is deferred past v1.0 in the phased plan but the
   decision shapes how much rework v0.5 vs. a hypothetical v1.1 requires — worth a preliminary
   steer now if the target deployment shape is "many small scopes" vs. "few huge scopes."

8. **Untrusted/third-party workers as a deployment shape.** Several dossiers (03 Open Questions,
   06 Open Questions) flag that the fencing-token design assumes cooperative, trusted workers (a
   plain integer epoch, not a signed/HMAC token). If the project's roadmap ever includes workers not
   controlled by the same operator as the library instances, the trust model needs revisiting before
   v1.0 freezes the wire shape of `LeaseEpoch`/`ClaimToken` — this is cheap to decide now and
   expensive to retrofit once `/v2` is required to change it.
