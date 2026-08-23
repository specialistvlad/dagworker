package dagworker

import (
	"context"
	"time"
)

// Edge is a directed dependency: To must not run until From resolves. Both
// endpoints are in the same scope; an edge never crosses a scope boundary.
type Edge struct{ From, To NodeID }

// Effect is one observable state change a store made. Every mutating Store
// method returns the effects it produced so the Manager can emit events without
// re-reading storage — which also removes the window in which an event could
// describe a state a reader cannot yet see.
type Effect struct {
	NodeID   NodeID
	Kind     EventKind
	From, To Status
	Reason   Reason
	Message  string
	Attempt  uint32
	NodeKind string
	Seq      Seq
	Cursor   Cursor
	At       time.Time
}

// Lease is a time-bounded, fenced grant of the exclusive right to complete a
// node. It is also the token presented back to [Store.Complete] and
// [Store.Extend].
//
// Epoch is the fencing token. Under the cooperative trust model it is a plain
// integer and the backend checks it directly. Token is an opaque, backend-issued
// blob that a future backend may populate with a signed capability; a backend
// that does not need it leaves it nil. Callers must treat Token as opaque and
// round-trip it unchanged, which is what lets signing be added later without a
// wire break.
type Lease struct {
	Scope    Scope
	NodeID   NodeID
	Epoch    uint64
	Deadline time.Time
	Node     Node
	Token    []byte
}

// Valid reports whether l looks like a lease this library issued. It does not
// and cannot report whether the lease is still current; only the backend knows
// that, and it finds out by attempting the fenced write.
func (l Lease) Valid() bool { return l.Scope != "" && l.NodeID != "" && l.Epoch > 0 }

// ClaimRequest asks a store for work.
type ClaimRequest struct {
	Scope Scope

	// Kinds restricts the claim to these ready-set partitions. Empty claims
	// from any kind.
	Kinds []string

	// Max is the most nodes to return. Values below one are treated as one.
	// Batching amortises the round trip, which is the difference between a few
	// thousand and a few hundred thousand claims per second on a networked
	// backend.
	Max int

	// Timeout is the requested lease duration. Zero uses the scope's default.
	// The store clamps it to the scope's bounds; the clamp happens in the store
	// rather than the caller so that every instance sharing the backend agrees.
	Timeout time.Duration

	// WorkerID identifies the claimant for observability. It has no bearing on
	// correctness — a worker's right to complete a node comes from the lease
	// epoch, never from its identity.
	WorkerID string
}

// ClaimResult is what a store hands back from a claim.
type ClaimResult struct {
	Leases []Lease

	// Effects includes the New to InProgress transitions for the granted
	// leases, plus any transitions caused by leases this call reclaimed inline.
	Effects []Effect
}

// CompleteRequest reports the outcome of an attempt.
type CompleteRequest struct {
	Lease Lease

	// Success distinguishes an acknowledgement from a failure report.
	Success bool

	// Reason and Message describe a failure. Ignored when Success is true.
	Reason  Reason
	Message string

	// Result is the worker's output, stored on the node. Subject to the payload
	// cap like any other payload.
	Result []byte
}

// CompleteResult reports what completing a node caused.
type CompleteResult struct {
	// Effects carries the node's own transition plus one entry per successor
	// that became claimable or was terminated because its trigger rule became
	// unsatisfiable.
	Effects []Effect

	// Retrying is true when the failure was recorded as a retryable attempt and
	// the node returned to StatusNew rather than becoming terminal.
	Retrying bool

	// NextAttemptAt is when a retrying node becomes claimable again.
	NextAttemptAt time.Time
}

// ExtendRequest asks for more time on a lease.
type ExtendRequest struct {
	Lease Lease
	// Timeout is the new duration, measured from the store's own clock at the
	// moment it processes the request, not from the original grant.
	Timeout time.Duration
}

// SweepResult reports what a reclaim pass did.
type SweepResult struct {
	// Reclaimed is the number of expired leases revoked.
	Reclaimed int
	// Effects carries every transition the reclaim caused, including successors
	// terminated because a timed-out node exhausted its attempts.
	Effects []Effect
	// More is true when the batch limit was reached and work remains.
	More bool
}

// ScopeStats is an O(1) summary of a scope. Every counter is maintained
// incrementally by the transitions that change it; none is computed by scanning.
type ScopeStats struct {
	Total      uint64
	Blocked    uint64
	Scheduled  uint64
	Ready      uint64
	InProgress uint64
	Succeeded  uint64
	Failed     uint64

	// Sealed reports whether the caller has declared the scope closed to new
	// nodes. Complete is Sealed with no non-terminal nodes remaining.
	Sealed   bool
	Complete bool
}

// NonTerminal returns the number of nodes that have not reached a final status.
func (s ScopeStats) NonTerminal() uint64 {
	return s.Blocked + s.Scheduled + s.Ready + s.InProgress
}

// Store is the storage port. Every backend implements all of it: there is no
// partial implementation, because a store that cannot atomically claim is not
// a backend this library can drive and there would be no defined fallback.
//
// # Atomicity
//
// Each mutating method is one atomic operation with respect to every other
// operation on the same scope, on every instance sharing the backend. This is
// the whole contract. In particular:
//
//   - Claim must select a node, bump its epoch, set its deadline, and index that
//     deadline indivisibly. Two instances must never both be granted the same node.
//   - Complete must compare-and-swap on the epoch, write the terminal state, mark
//     each out-edge satisfied, re-evaluate each successor's trigger rule, and push
//     newly claimable successors onto the ready set — all indivisibly. A reader
//     must never observe a node succeeded while a successor is still blocked on it.
//   - AddEdges must, when it adds an unresolved dependency to a node that was
//     already claimable, remove that node from the ready set in the same operation
//     that records the edge. Otherwise a worker claims it through the gap.
//
// A backend implements these with its native primitives — a Lua function, a
// single SQL statement, a held mutex — and never by emulating a transaction with
// several round trips.
//
// # Clocks
//
// The store owns time. Every deadline is computed and every expiry compared
// against the store's own clock: Redis TIME inside the script, PostgreSQL
// clock_timestamp() inside the statement, the injected Clock for the in-memory
// backend. A caller-computed deadline is never accepted, because two parties
// reading two clocks cannot agree on a boundary.
//
// # Fencing
//
// Complete and Extend take a Lease and must reject it when its Epoch is not the
// node's current epoch, returning ErrLeaseMismatch. This is what makes a paused
// worker safe: when its lease is reclaimed and reissued, its late write is
// refused rather than overwriting the successor's.
//
// # Conformance
//
// Correctness here is defined by dagstoretest.RunConformance, not by prose. A
// backend is finished when that suite passes.
type Store interface {
	// ScopeConfig returns the scope's stored policy. An unknown scope returns
	// the zero ScopeConfig and no error: scopes are created implicitly, so
	// asking about one that does not exist yet is not an error.
	ScopeConfig(ctx context.Context, scope Scope) (ScopeConfig, error)

	// SetScopeConfig stores the scope's policy, creating the scope if needed.
	SetScopeConfig(ctx context.Context, scope Scope, cfg ScopeConfig) error

	// Seal marks the scope closed to new nodes. It is irreversible. Sealing an
	// already-sealed scope is a no-op, not an error.
	Seal(ctx context.Context, scope Scope) error

	// ScopeStats returns the O(1) counters. It must not scan.
	ScopeStats(ctx context.Context, scope Scope) (ScopeStats, error)

	// AddNodes creates nodes and their declared edges atomically: every spec
	// lands or none does. A spec whose ID exists with a byte-identical
	// definition is a no-op; one whose definition differs returns ErrIDConflict.
	// Dependencies must already exist, or appear earlier in specs.
	AddNodes(ctx context.Context, scope Scope, specs []NodeSpec) ([]Effect, error)

	// AddEdges adds dependencies atomically. An edge that would close a cycle
	// returns *CycleError. An edge into a terminal node returns
	// ErrAlreadyTerminal.
	AddEdges(ctx context.Context, scope Scope, edges []Edge) ([]Effect, error)

	// RemoveEdges drops dependencies atomically. A successor that loses its
	// last unsatisfied dependency may become claimable, which the effects
	// report.
	RemoveEdges(ctx context.Context, scope Scope, edges []Edge) ([]Effect, error)

	// RemoveNode deletes a node. A claimed node returns ErrNodeInFlight. A node
	// with successors returns ErrHasSuccessors unless policy says otherwise.
	RemoveNode(ctx context.Context, scope Scope, id NodeID, policy CascadePolicy) ([]Effect, error)

	// Cancel terminates nodes with ReasonCancelled and propagates to their
	// descendants. Terminal nodes are skipped, not errors. Cancelling a claimed
	// node revokes the lease, so the worker's later Complete fails the fencing
	// check.
	Cancel(ctx context.Context, scope Scope, ids []NodeID) ([]Effect, error)

	// CancelScope cancels every non-terminal node in the scope.
	CancelScope(ctx context.Context, scope Scope) ([]Effect, error)

	// GetNode returns a snapshot. Unknown nodes return ErrNotFound.
	GetNode(ctx context.Context, scope Scope, id NodeID) (Node, error)

	// Inspect returns internal scheduling state for debugging. It may be more
	// expensive than GetNode and carries no complexity guarantee beyond being
	// proportional to the node's own degree.
	Inspect(ctx context.Context, scope Scope, id NodeID) (Inspection, error)

	// Claim grants leases on ready nodes. It returns an empty result, not an
	// error, when nothing is ready: having no work is ordinary. Implementations
	// should reclaim any expired lease they encounter while looking, so a dead
	// worker's node is recovered by the next claimant without waiting for the
	// background sweeper.
	Claim(ctx context.Context, req ClaimRequest) (ClaimResult, error)

	// Complete records an attempt's outcome, fenced on the lease epoch.
	Complete(ctx context.Context, req CompleteRequest) (CompleteResult, error)

	// Extend moves a lease's deadline, fenced on the epoch. It must not change
	// status, attempt, or sequence.
	Extend(ctx context.Context, req ExtendRequest) (time.Time, error)

	// Sweep reclaims expired leases, at most limit of them. It must be driven
	// by an index ordered on deadline, never by a scan of in-progress nodes.
	// Correctness must not depend on only one instance sweeping: duplicate
	// sweeping is wasteful, never wrong, because every write it makes is fenced.
	Sweep(ctx context.Context, scope Scope, limit int) (SweepResult, error)

	// Scopes returns the scopes the store knows about. It exists so the
	// background sweeper and the retention collector can find work without the
	// caller enumerating scopes for them.
	Scopes(ctx context.Context) ([]Scope, error)

	// Close releases the store's resources. It must be idempotent.
	Close(ctx context.Context) error
}

// ---------------------------------------------------------------- optional facets

// Capability is a bit in the set a backend reports. Capabilities describe
// optional facets only; everything in [Store] is mandatory and never advertised.
type Capability uint32

const (
	// CapList means the backend implements [Lister].
	CapList Capability = 1 << iota
	// CapDurableEvents means the backend implements [DurableEventStream] with a
	// genuine at-least-once, resumable guarantee.
	CapDurableEvents
	// CapDoorbell means the backend implements [Doorbell], so a blocking claim
	// can wait on a signal instead of polling.
	CapDoorbell
	// CapCollect means the backend implements [Collector] for retention GC.
	CapCollect
	// CapDurableStorage means data survives process restart. The in-memory
	// backend does not set it.
	CapDurableStorage
	// CapCrossProcess means several processes may share the backend
	// concurrently. The in-memory backend does not set it.
	CapCrossProcess
)

// Capabilities is a set of [Capability] bits.
type Capabilities uint32

// Has reports whether every bit in c is present.
func (cs Capabilities) Has(c Capability) bool { return uint32(cs)&uint32(c) == uint32(c) }

// CapabilityReporter is implemented by backends that advertise optional facets.
// A backend that does not implement it is treated as having no optional facets,
// which is always safe.
type CapabilityReporter interface {
	Capabilities() Capabilities
}

// ListOptions is a keyset page request. There is no offset, ever: offset
// pagination is linear in the pages skipped, which would put an O(n) operation
// into a library whose whole premise is that none exists.
type ListOptions struct {
	// Statuses filters by status. Empty means all.
	Statuses []Status
	// Kinds filters by node kind. Empty means all.
	Kinds []string
	// Cursor continues a previous page. Empty starts at the beginning.
	Cursor string
	// Limit caps the page size.
	Limit int
}

// ListResult is one page.
type ListResult struct {
	Nodes []Node
	// Next is the cursor for the following page, empty when exhausted. It is
	// opaque; callers must not parse it.
	Next string
}

// Lister is the optional listing facet.
type Lister interface {
	ListNodes(ctx context.Context, scope Scope, opts ListOptions) (ListResult, error)
}

// DurableEventStream is the optional at-least-once, resumable event feed. A
// backend implements it only when it genuinely provides that guarantee; one
// that can offer only fire-and-forget delivery must not implement it, so that
// SubscribeOptions.Durable fails loudly rather than degrading in silence.
type DurableEventStream interface {
	// Watch streams a scope's events from just after the given log position. A
	// zero cursor starts from now. A cursor older than retained history returns
	// ErrCursorExpired. An empty scope watches every scope, in which case from
	// must be zero because cursors are per scope.
	Watch(ctx context.Context, scope Scope, from Cursor) (<-chan Event, error)
}

// Doorbell is the optional "work may be available" signal that lets a blocking
// claim wait instead of poll. It is advisory in the strongest sense: a spurious
// wakeup costs one wasted claim attempt, and a missed wakeup costs one poll
// interval of latency. Neither can cause incorrect behaviour, which is why the
// polling fallback is always sound.
type Doorbell interface {
	// WaitForWork blocks until work may be available in the scope for one of
	// the given kinds, or ctx is done. Empty kinds means any kind. Returning
	// nil means "try again", not "work is definitely there".
	WaitForWork(ctx context.Context, scope Scope, kinds []string) error
}

// Collector is the optional retention facet: deleting terminal nodes older than
// a cutoff. A backend without it simply never collects, which is the safe
// default given that the fallback retention policy is to keep everything.
type Collector interface {
	// CollectTerminal deletes at most limit terminal nodes whose last update
	// predates cutoff, and reports how many it deleted and whether more remain.
	CollectTerminal(ctx context.Context, scope Scope, cutoff time.Time, limit int) (deleted int, more bool, err error)
}
