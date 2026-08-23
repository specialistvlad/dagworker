# ADR-0016: Storage port shape: a mandatory core plus optional capability facets

- **Status:** Accepted, amended by ADR-0044
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Amended by:** ADR-0044 §5 — the optional facets are `Lister`, `DurableEventStream`, `Doorbell` and `Collector`, not the `ConditionalDeleter`/`BatchClaim` pair named below, and declining one is `ErrUnsupported` rather than an `ErrCapability` sentinel.
- **Backing research:** docs/research/06-memcached-and-storage-abstraction.md §B.2, §B.3, §B.4, §B.5

## Context

`dagstore.Store` is the one interface every backend author writes against, and the brief's own
success criterion for it is that "a new backend is a few hundred lines." That is only achievable
if the mandatory surface is genuinely small; everything backend-specific has to be optional. At
the same time the port must be capability-rich when a backend allows it — Redis earns Lua-scripted
atomic transitions and Streams; PostgreSQL earns `SELECT … FOR UPDATE SKIP LOCKED` and
`LISTEN`/`NOTIFY` — and neither should be forced through a lowest-common-denominator emulation of
the other's native primitive.

The design-synthesis draft of this decision (docs/research/00-synthesis.md §3, ADR-16 seed) put
`ReadyQueue` and `TimeoutSweeper` behind the same type-assertion discovery as `Lister`,
`EventStream`, and `ConditionalDeleter` — five equally optional facets. The project owner overrode
that (AMD-1): a `Store` that cannot atomically claim a ready node and grant a lease in one write is
not a storage backend this library can run on at all, full stop, and there is no honest fallback
for its absence. Contrast this with `database/sql/driver`'s `driver.Pinger`, which is genuinely
optional because `DB.Ping` has a well-defined no-op behavior when a driver lacks it — nothing about
`sql.DB` stops functioning if `Ping` is unavailable. `ReadyQueue` has no such no-op: the library's
entire purpose is handing ready nodes to external workers exactly-once-in-flight, and dossier 06
itself already documents in the `ReadyQueue` doc comment that a backend lacking it "must not attempt
to fake it with a polling loop over individually-fetched keys, which would violate the AckTimeout
guarantee … under concurrent competing consumers." An interface that type-asserts `ReadyQueue` away
at runtime compiles fine and then either hangs (nothing ever claims) or corrupts exclusivity (a
naive polling emulation double-hands work) — a defect that should be a compile error, not a 2 a.m.
page. `TimeoutSweeper`'s absence is the same shape of failure: leases that are never reclaimed turn
every worker crash into a permanently stuck node, silently violating the fencing model ADR-0006
already declares non-negotiable from day one.

Memcached is the concrete backend that forced this question and it is now moot for a different
reason: AMD-5 removes memcached from the `Store` picture entirely (see ADR-0017) rather than
demoting it to a degraded facet set, so this ADR does not need to reason about a backend that
implements zero of the mandatory core.

## Decision

**Mandatory core** — every backend must implement all of it to be a `dagstore.Store` at all;
partial implementations are not a supported configuration:

```go
package dagstore

// Store is the mandatory core. There is no reduced-capability tier: a
// backend that cannot satisfy every method here is not a supported storage
// backend for dagworker (AMD-1). This narrows the containerd/Terraform/
// database-sql-driver pattern (06 §B.2) relative to the original synthesis
// draft: ReadyQueue and TimeoutSweeper move from "optional facet, type-
// asserted" into Store itself, because — unlike driver.Pinger — they have
// no defined fallback behavior for a claim-based scheduler.
type Store interface {
	// --- record CRUD ---------------------------------------------------
	GetNode(ctx context.Context, scope, id string) (Node, Version, error)
	PutNode(ctx context.Context, n Node, expected Version) (Version, error)
	CreateNode(ctx context.Context, n Node) (Version, error)
	DeleteNode(ctx context.Context, scope, id string) error

	// --- graph mutation --------------------------------------------------
	// Atomic per call (T-EDGE-BATCH-ATOMICITY, ADR-0018); a backend without
	// native multi-key transactions refuses via ErrCapability rather than
	// offer partial-batch visibility.
	AddEdges(ctx context.Context, edges ...Edge) error

	// --- fenced mutation primitives --------------------------------------
	// Replace the synthesis draft's single Transition method. Exact
	// signatures, the fencing rule, and the successor-fan-out contract are
	// ADR-0038's job; they are part of this mandatory core, not a facet.
	Claim(ctx context.Context, scope string, req ClaimRequest) (Claimed, error)
	Complete(ctx context.Context, token ClaimToken, outcome Outcome) (readied []NodeRef, err error)
	Extend(ctx context.Context, token ClaimToken, d time.Duration) (deadline time.Time, err error)
	Sweep(ctx context.Context, scope string, limit int) (timedOut, readied []NodeRef, err error)

	Close(ctx context.Context) error
}
```

**Optional facets** — a backend implements zero or more; callers discover them by type assertion,
exactly as `database/sql` checks for `driver.Pinger`/`driver.ExecerContext`:

```go
type Lister interface {
	ListNodes(ctx context.Context, scope, cursor string, limit int) (nodes []Node, next string, err error)
}

// EventStream's durability tier is reported truthfully via CapEventStreamDurable,
// never silently downgraded or silently upgraded (ADR-0019).
type EventStream interface {
	Subscribe(ctx context.Context, scope string) (<-chan Event, error)
}

type ConditionalDeleter interface {
	DeleteNodeIf(ctx context.Context, scope, id string, expected Version) error
}

// BatchClaim lets a backend hand out N claims in one round trip instead of
// the engine looping Claim N times. Genuinely optional: v0.1's in-memory
// backend and v0.2's Postgres backend may ship without it.
type BatchClaim interface {
	ClaimBatch(ctx context.Context, scope string, req ClaimRequest, max int) ([]Claimed, error)
}

type CapabilityReporter interface{ Capabilities() CapabilitySet }

type CapabilitySet uint32

const (
	CapList CapabilitySet = 1 << iota
	CapEventStream
	CapEventStreamDurable // at-least-once tier genuinely earned, not plain pub/sub (10 §7.2)
	CapConditionalDelete
	CapBatchClaim
)

func (cs CapabilitySet) Has(c CapabilitySet) bool { return cs&c != 0 }
```

Note what is gone relative to the synthesis draft: `CapReadyQueue` and `CapTimeoutSweep` no longer
exist as capability bits. Querying "can this backend claim?" is meaningless once `Store` guarantees
it by construction — the compiler is the capability check.

**The non-emulation rule, stated exactly, per facet:**

- `Lister`, `ConditionalDeleter`, `BatchClaim`: **no library-level fallback.** A missing type
  assertion means that feature is unavailable on this backend, full stop; a caller that needs it
  gets `ErrCapability` at the call site (or, for `ConditionalDeleter`, simply uses the mandatory
  unconditional `DeleteNode` — an explicit, caller-made choice, never an implicit one).
- `EventStream`: the **one** facet with a library-provided degraded path — when a backend lacks it,
  the library may run a polling adapter built *only* on the mandatory `Store` core, clearly reported
  as `CapEventStream=false` so a caller can detect and reason about the reduced timeliness. This is
  the sole sanctioned "degraded path the caller can detect"; it exists because observation-only
  polling cannot corrupt correctness the way a faked `ReadyQueue` or `ConditionalDeleter` could.
- This rule governs what a *backend* may fake at the storage-port boundary. It does not forbid the
  *engine* from composing mandatory-core calls to approximate a nice-to-have — e.g., `Manager.Claim`
  with `MaxNodes > 1` against a backend lacking `BatchClaim` simply calls `Claim` in a loop. That is
  the engine using a primitive it has N times, not a backend pretending to have one it lacks.

## Consequences

### Positive
- Any `Store` a host program configures is, by the type system alone, guaranteed to support the
  full claim/complete/extend/sweep lifecycle — "compiles but hangs at 2 a.m." is not a reachable
  state for this interface.
- Removing `CapReadyQueue`/`CapTimeoutSweep` as queryable bits is a net simplification: nobody has
  to write `if !caps.Has(CapReadyQueue) { ... }` dead code, because that branch can never be true.
- The remaining four optional facets map cleanly onto the two allowed responses (`ErrCapability` or
  a labeled degraded path), so a backend author never has to invent a fifth behavior.

### Negative
- Raises the bar for a hypothetical fourth community backend: a read-only or admin-only storage
  facade can no longer exist as a `dagstore.Store` at all — it must be modeled as a `Lister`-only
  reporting tool outside the `Store` interface, not a "partial" `Store`.
- `BatchClaim` is new relative to the synthesis draft and is not required for v0.1/v0.2; documenting
  it now without a shipping implementation is a small amount of speculative surface, mitigated by it
  being purely additive (no breaking change to add it to a backend later).

### Neutral
- The mandatory core's exact `Claim`/`Complete`/`Extend`/`Sweep` signatures are specified in
  ADR-0038, not here; this ADR fixes only that they are mandatory and why.

## Alternatives considered

**Keep `ReadyQueue`/`TimeoutSweeper` optional, type-asserted next to `Lister`** — the synthesis
draft's original position and Terraform's own `statemgr.Locker` precedent (locking is a separate,
optionally-satisfied interface, "not all backends support locking"). Rejected because Terraform's
case is honestly optional — a local, single-user backend with no locking is a real, supportable
deployment. There is no analogous legitimate deployment of dagworker on a `Store` that cannot claim.

**One fat interface, everything mandatory including `Lister`/`EventStream`/`ConditionalDeleter`/
`BatchClaim`.** Rejected: forces every backend through the exact "lowest-common-denominator
emulation" this port exists to avoid (06 §B.1 design goal #2), and bloats new-backend authoring past
"a few hundred lines" for facets that generally do have honest fallbacks.

**Thanos `objstore`-style capability query as the *only* discovery mechanism** (an enum method, no
type assertions) — rejected as primary: Thanos's own case is "does this one verb accept this one
option flag," a many-small-booleans shape; here the varying capability is "does this backend
implement this entire facet," which fits Go's idiomatic type-assertion pattern better.
`CapabilityReporter` is kept as a secondary, logging/preflight convenience only.

**Generic `Get(key) []byte` KV core instead of node/edge-shaped methods.** Rejected per 06 §B.3's
own design decision: it would push all DAG-shaped semantics into every backend implementation,
defeating the "few hundred lines" goal outright.

## References

- containerd `content`/`Snapshotter` packages — https://pkg.go.dev/github.com/containerd/containerd/content, https://github.com/containerd/containerd/blob/main/core/snapshots/snapshotter.go
- Terraform `backend.Backend` / `statemgr.Locker` — https://github.com/hashicorp/terraform/blob/main/internal/backend/backend.go, https://github.com/hashicorp/terraform/blob/main/internal/states/statemgr/locker.go, https://developer.hashicorp.com/terraform/language/state/locking
- gocloud.dev `docstore/driver` — https://pkg.go.dev/gocloud.dev/docstore/driver
- Thanos `objstore` capability query — https://github.com/thanos-io/objstore/blob/main/objstore.go, https://github.com/thanos-io/objstore/blob/main/README.md
- Vitess `topo.Conn` opaque `Version` — https://github.com/vitessio/vitess/blob/main/go/vt/topo/conn.go
- `database/sql/driver` optional-interface pattern (`driver.Pinger`, `driver.ExecerContext`)
- Sibling ADRs: ADR-0006 (fencing token), ADR-0017 (memcached rejected), ADR-0018 (conformance
  suite), ADR-0019 (event bus shape), ADR-0038 (fenced mutation primitives)
