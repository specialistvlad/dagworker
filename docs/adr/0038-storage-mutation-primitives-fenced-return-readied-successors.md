# ADR-0038: Storage mutation primitives are fenced and return newly-ready successors

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** ADR-0016
- **Backing research:** docs/research/03-leases-heartbeats-timeouts.md §5b, §5c, §5d; docs/research/04-postgres-backend.md §14.1, §14.2, §15.2; docs/research/05-redis-backend.md §15.2, §15.3, §15.4; docs/research/02-incremental-topological-scheduling.md §1.2, §6.2

## Context

The design-synthesis draft's mandatory `Store` core (docs/research/06 §B.3, folded into
ADR-0016's first pass) included a single generic method:

```go
Transition(ctx context.Context, scope, id string, from, to NodeStatus) error
```

This is adequate for "flip my own status, guarded by a CAS on the current value," and nothing
else. It fails on every dimension the two hottest write paths in the system actually need:

1. **No fencing token parameter.** `Transition` carries no `lease_epoch`/`ClaimToken` at all, so
   nothing in its signature stops a worker whose lease already expired and was reclaimed from
   calling it and corrupting state. ADR-0006 already declares the fencing check non-negotiable
   from day one; a primitive that structurally omits the fencing parameter cannot honor that
   declaration no matter how carefully an engine author calls it.
2. **No `Outcome`.** A real completion writes `Reason`, `Message`, and `Attempt` in the same
   operation as the status flip (docs/research/00-synthesis.md §5, invariant: "every row that
   changes `Status` writes exactly one `Outcome` in the same op"). `Transition` has nowhere to put
   this, forcing a second write and reopening exactly the race the atomicity requirement exists to
   close.
3. **Cannot express successor fan-out atomically.** Completing a node must, in the *same* write,
   decrement every direct successor's pending-predecessor count and push any successor that just
   became ready onto the ready-set (synthesis §2.3 steps 3–4; 04 §14.2's chained CTE; 05 §15.3's
   Lua script both do exactly this in one round trip). A caller composing `Transition(to=Success)`
   followed by a separate query for "which successors just became ready" reopens the insert-into-
   the-gap race from synthesis §2.1 step 5, and — independent of correctness — doubles the number
   of round trips on the single highest-frequency write path in the whole system, the one every
   node in a 1,000,000-node DAG passes through exactly once.

AMD-2 replaces `Transition` outright with four fenced primitives that map onto the four distinct
mutation shapes the engine actually performs: grant a lease (`Claim`), record a terminal outcome
and fan out (`Complete`), extend a lease without changing status (`Extend`, kept structurally
separate from `Complete` per ADR-0010's heartbeat/ack split), and reclaim expired leases in bulk
(`Sweep`). Both `Complete` and `Sweep` must be able to report readied successors: a terminal
transition is a terminal transition regardless of whether it was a worker's `Ack`, a worker's
`Nack`, or a sweeper's timeout reclaim, and under a permissive trigger rule (`all_done`,
`none_failed_min_one_success` — ADR-0030) a sibling's *failure* or *timeout* can be exactly the
event that satisfies a successor's readiness condition, not only a sibling's success. A `Sweep`
that only reported `timedOut` and silently dropped any resulting `readied` set would leave those
successors stuck until the next unrelated write happened to re-evaluate them — an invisible
liveness bug, not merely an inefficiency.

## Decision

`dagstore.Store`'s mandatory core (ADR-0016) drops `Transition` and instead exposes:

```go
// ClaimRequest describes what a caller will accept from Claim. Field names
// and exact types below are illustrative; finalizing them is the
// implementation spec's job, not this ADR's — the shape and the fencing/
// atomicity contract are what this ADR fixes.
type ClaimRequest struct {
	Kind         string            // ready-set partition key (12 §5.4)
	Labels       map[string]string
	LeaseTimeout time.Duration
}

type Claimed struct {
	Node     Node
	Token    ClaimToken // opaque (ADR-0016); carries the fencing lease_epoch
	Deadline time.Time  // backend clock (ADR-0008), never a client-computed value
}

type Store interface {
	// ... CRUD/AddEdges/Close per ADR-0016 ...

	// Claim atomically pops one ready node, bumps its fencing epoch, and
	// records the lease deadline — all in the SAME write (ADR-0007). Never
	// emulated as a separate pop-then-write pair.
	Claim(ctx context.Context, scope string, req ClaimRequest) (Claimed, error)

	// Complete performs the fenced terminal transition (status + Outcome)
	// AND, in the same atomic write, decrements every direct successor's
	// pending-predecessor count, evaluates each successor's trigger rule,
	// and returns every successor that just became ready. Fenced on token:
	// zero effect (ErrLeaseMismatch) if the presented epoch no longer
	// matches the node's current epoch — never a silent accept, never a
	// "please retry."
	Complete(ctx context.Context, token ClaimToken, outcome Outcome) (readied []NodeRef, err error)

	// Extend resets a lease's deadline to server_now + d — an absolute
	// reset, never additive to whatever deadline existed (03 §5c) — fenced
	// on token exactly like Complete. Distinct call shape from Complete so
	// a liveness signal can never be confused with a terminal one (ADR-0010).
	Extend(ctx context.Context, token ClaimToken, d time.Duration) (deadline time.Time, err error)

	// Sweep finds every claim in scope whose deadline has elapsed,
	// transitions each to Error/Timeout via the SAME fenced write path
	// Complete uses internally, and returns both the timed-out nodes and
	// any successors that became ready as a result — a timeout is a
	// terminal transition too, and a permissive trigger rule can be
	// satisfied by it exactly as it can by a Complete-driven failure.
	// Bounded by limit; callers loop while len(timedOut) == limit (05 §15.4).
	Sweep(ctx context.Context, scope string, limit int) (timedOut, readied []NodeRef, err error)
}
```

**Fencing rule.** `Complete`, `Extend`, and `Sweep`'s internal reclaim all gate their write on the
node's current epoch matching the epoch embedded in the presented (or, for `Sweep`, the observed)
`ClaimToken`. A mismatch or an already-terminal node affects zero rows and returns a typed error
(`ErrLeaseMismatch`/`ErrNotFound`) — this is the entire safety mechanism (03 §5b) and it requires
no coordination with whatever else touched the node; a Postgres implementation expresses it as
`WHERE lease_epoch = $presented AND status = 'in_progress'` (04 §14.2), a Redis implementation as
a Lua script's own epoch read-and-compare before any `HSET` (05 §15.3).

**Deterministic lock ordering for fan-out.** Any backend whose `Complete`/`Sweep` touches multiple
successor rows in one write MUST lock or update them in a single deterministic order — ascending
node ID — before mutating any of them. This is not visible at the Go interface level, but it is a
conformance-relevant implementation constraint (ADR-0018): two nodes completing concurrently whose
successor sets overlap (`{5,9}` vs `{9,5}`) deadlock without it, and 04 §15.2 documents exactly this
fan-in deadlock class and its fix.

**Clock authority.** Every deadline `Claim`/`Extend`/`Sweep` produces is read from the backend's own
clock — Postgres `clock_timestamp()`, Redis `TIME` inside the Lua script, in-memory monotonic
`time.Now()` — never a client-supplied wall-clock value, per ADR-0008.

**Scope boundary of this ADR.** These four primitives are the *worker-facing* hot-path fenced
operations (state-machine rows T6–T10). Engine-originated terminal transitions that do not
originate from a worker holding a token — upstream-failure propagation (T11), skip (T12),
administrative cancel (T13), or a removal's cascade policy (T14) — need the same atomic
"terminal status + fenced successor fan-out" shape but are authorized by the engine itself, not by
a caller-presented `ClaimToken`. This ADR does not fix that internal mechanism; it is flagged here
as a real, related design surface the implementation spec must still resolve (most likely an
internal-only variant of `Complete` the engine can invoke without an externally-issued token), not
solved by inventing a signature for it.

## Consequences

### Positive
- Claim and completion are each a single round trip; the engine never re-queries storage for "who
  just became ready" after a completion, closing the race AMD-2 exists to close.
- The conformance suite (ADR-0018) gets four crisp atomic units to test instead of one generic
  method whose atomicity claims were only as strong as whatever the caller composed around it.
- `Sweep` returning `readied` closes an invisible-liveness-bug class: a permissive-trigger-rule
  successor of a timed-out node becomes ready the moment the timeout is discovered, not on the next
  unrelated write that happens to re-evaluate it.

### Negative
- Four methods with real per-backend logic replace one generic method — materially more code to get
  right per backend, particularly the successor lock-ordering rule in Postgres and Lua-script
  complexity in Redis.
- `Complete`/`Sweep` must be trigger-rule-aware inside the backend's atomic write (they must
  evaluate whether a successor's *rule*, not just its raw pending-count, is satisfied), pushing a
  slice of engine logic down into every backend implementation. This is mitigated by ADR-0030
  keeping the v1 trigger-rule set to five simple, incrementally-evaluable predicates, but it is a
  real coupling worth naming rather than hiding.

### Neutral
- `ClaimToken` stays the opaque type ADR-0016 already defines; nothing about this decision changes
  its representation or leaks the epoch's storage shape across backends.

## Alternatives considered

**Keep generic `Transition`, add a second `GetReadySuccessors(nodeID)` call for the engine to
invoke right after.** Rejected: reopens the exact insert-into-the-gap race (an edge added between
the two calls can be lost or double-counted) and doubles round-trip latency on the highest-
frequency write path in the system.

**Decompose `Complete` into a sequence of single-key CAS steps in the engine**, per 06 §A.8's
ABA-safe decomposition recipe for KV stores that lack multi-key atomicity. Rejected as the default:
that recipe itself says to use this technique *only* when the backend lacks native multi-key
atomicity (06 §B.3: "where a store offers genuine multi-key atomicity … skip the decomposition
entirely"). Redis (Lua) and Postgres (transaction/CTE) both have it; forcing every backend through
the harder, retry-driven path throws away exactly the native atomics ADR-0016 exists to let them
use.

**Carry the fencing token in a generic options bag or context value instead of a required
parameter.** Rejected: makes the fencing check optional-by-omission at call sites, the precise
mistake ADR-0006 exists to foreclose — Kleppmann's fencing argument requires the check to be
structurally impossible to skip, not merely available if an engine author remembers it.

## References

- Kleppmann, "How to do distributed locking" — https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
- PostgreSQL `FOR UPDATE SKIP LOCKED` — https://www.postgresql.org/docs/current/sql-select.html
- PostgreSQL isolation / deadlock docs — https://www.postgresql.org/docs/current/transaction-iso.html, https://www.postgresql.org/docs/current/explicit-locking.html
- Redis Streams `XAUTOCLAIM` (idle-time reclaim precedent) — https://redis.io/docs/latest/commands/xautoclaim/
- Sibling ADRs: ADR-0006 (fencing token), ADR-0007 (claim atomicity), ADR-0008 (clock authority),
  ADR-0010 (heartbeat/ack split), ADR-0016 (storage port shape), ADR-0018 (conformance suite),
  ADR-0030 (trigger rules)
