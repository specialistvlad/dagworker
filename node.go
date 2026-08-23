package dagworker

import (
	"fmt"
	"time"
)

// TriggerRule decides when a node becomes claimable, given the outcomes of its
// predecessors. Every rule is evaluable incrementally as predecessors resolve:
// no rule requires re-examining all predecessors on each event, which is what
// keeps completion fan-out proportional to out-degree rather than to fan-in.
//
// A predecessor is classified exactly once it is terminal:
//
//	succeeded  Status is StatusSuccess
//	skipped    Status is StatusError with ReasonSkipped
//	failed     Status is StatusError with any other reason
//
// The distinction between skipped and failed is load-bearing, and collapsing it
// is a recurring complaint against engines that do. A branch that was not taken
// is not an error the downstream node should inherit.
type TriggerRule uint8

const (
	// TriggerAllSuccess fires when every predecessor succeeded. This is the
	// default and the only rule most graphs need.
	TriggerAllSuccess TriggerRule = iota

	// TriggerAllDone fires when every predecessor is terminal, whatever the
	// outcome. Use it for cleanup and teardown nodes that must run either way.
	TriggerAllDone

	// TriggerNoneFailed fires when every predecessor is terminal and none
	// failed. Skipped predecessors are acceptable.
	TriggerNoneFailed

	// TriggerNoneFailedMinOneSuccess fires when every predecessor is terminal,
	// none failed, and at least one succeeded. Use it to join optional
	// branches where at least one must have produced something.
	TriggerNoneFailedMinOneSuccess

	// TriggerAlways makes the node claimable immediately, ignoring predecessors
	// entirely. Edges into such a node still exist for documentation and for
	// cycle checking, but never gate it.
	TriggerAlways
)

// String implements [fmt.Stringer].
func (t TriggerRule) String() string {
	switch t {
	case TriggerAllSuccess:
		return "all_success"
	case TriggerAllDone:
		return "all_done"
	case TriggerNoneFailed:
		return "none_failed"
	case TriggerNoneFailedMinOneSuccess:
		return "none_failed_min_one_success"
	case TriggerAlways:
		return "always"
	default:
		return fmt.Sprintf("trigger(%d)", uint8(t))
	}
}

func (t TriggerRule) validate() error {
	if t > TriggerAlways {
		return invalidArg("trigger rule", "unknown value %d", uint8(t))
	}
	return nil
}

// DepCounts is the incremental predecessor tally a trigger rule is evaluated
// against. The four counters are maintained by the same atomic operation that
// completes a predecessor, so evaluating a rule is arithmetic on values already
// in hand rather than a query over the fan-in.
type DepCounts struct {
	// Unsatisfied is the number of predecessors that are not yet terminal.
	Unsatisfied uint32
	// Succeeded, Skipped and Failed tally the terminal predecessors.
	Succeeded uint32
	Skipped   uint32
	Failed    uint32
}

// Total returns the node's in-degree.
func (d DepCounts) Total() uint32 { return d.Unsatisfied + d.Succeeded + d.Skipped + d.Failed }

// Ready reports whether rule t is satisfied by these counts.
func (d DepCounts) Ready(t TriggerRule) bool {
	if t == TriggerAlways {
		return true
	}
	if d.Unsatisfied > 0 {
		return false
	}
	switch t {
	case TriggerAlways:
		// Already handled above; named so that adding a rule to the enum is a
		// compile-time-adjacent failure here rather than a silent false.
		return true
	case TriggerAllSuccess:
		return d.Failed == 0 && d.Skipped == 0
	case TriggerAllDone:
		return true
	case TriggerNoneFailed:
		return d.Failed == 0
	case TriggerNoneFailedMinOneSuccess:
		return d.Failed == 0 && d.Succeeded > 0
	default:
		return false
	}
}

// Unsatisfiable reports whether rule t can no longer be satisfied no matter how
// the remaining predecessors resolve. A node whose rule is unsatisfiable is
// terminated immediately rather than left blocked forever, which is what keeps
// a scope's completion decidable.
func (d DepCounts) Unsatisfiable(t TriggerRule) bool {
	switch t {
	case TriggerAlways, TriggerAllDone:
		return false
	case TriggerAllSuccess:
		return d.Failed > 0 || d.Skipped > 0
	case TriggerNoneFailed:
		return d.Failed > 0
	case TriggerNoneFailedMinOneSuccess:
		// Failing is decisive; so is every predecessor resolving without a
		// single success.
		return d.Failed > 0 || (d.Unsatisfied == 0 && d.Succeeded == 0)
	default:
		return false
	}
}

// TerminalReason returns the reason a node should carry when its rule became
// unsatisfiable: an upstream failure if a predecessor genuinely failed, and a
// skip if the branch simply was not taken.
func (d DepCounts) TerminalReason() Reason {
	if d.Failed > 0 {
		return ReasonUpstreamFailed
	}
	return ReasonSkipped
}

// RetryPolicy governs what happens when an attempt fails or times out. A retry
// is a new attempt on the same node, never a new node: the attempt counter is
// the same integer as the lease epoch, so the fencing check and the retry count
// can never disagree.
//
// Backoff is full jitter — a delay drawn uniformly from [0, min(MaxDelay,
// BaseDelay*2^attempt)). Full jitter beats equal jitter and plain exponential
// backoff on both completion time and contention.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts including the first. Zero
	// means "inherit the scope's setting". One means no retry.
	MaxAttempts uint32
	// BaseDelay and MaxDelay bound the full-jitter backoff. Zero means inherit.
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func (r RetryPolicy) validate() error {
	if r.BaseDelay < 0 {
		return invalidArg("retry base delay", "must not be negative")
	}
	if r.MaxDelay < 0 {
		return invalidArg("retry max delay", "must not be negative")
	}
	if r.BaseDelay > 0 && r.MaxDelay > 0 && r.BaseDelay > r.MaxDelay {
		return invalidArg("retry policy", "base delay %s exceeds max delay %s", r.BaseDelay, r.MaxDelay)
	}
	return nil
}

// CascadePolicy decides what [Manager.RemoveNode] does to a node's successors.
// The default rejects rather than cascading, because a cleanup call that
// silently fails a subgraph is too surprising to be the default.
type CascadePolicy uint8

const (
	// CascadeReject refuses to remove a node that has successors, returning
	// [ErrHasSuccessors]. This is the default.
	CascadeReject CascadePolicy = iota

	// CascadeDetach drops the incident edges and then removes the node.
	// Successors lose the dependency and may become claimable immediately.
	CascadeDetach

	// CascadeFail transitions every successor, recursively, to StatusError with
	// [ReasonRemoved], and then removes the node.
	CascadeFail
)

// String implements [fmt.Stringer].
func (c CascadePolicy) String() string {
	switch c {
	case CascadeReject:
		return "reject"
	case CascadeDetach:
		return "detach"
	case CascadeFail:
		return "fail"
	default:
		return fmt.Sprintf("cascade(%d)", uint8(c))
	}
}

// Node is a snapshot of one work item. Every field is a value: at a million
// nodes, a pointer per node is a pointer the garbage collector must scan on
// every cycle, so the type deliberately avoids optional-pointer fields in
// favour of flat values with documented zero meanings.
type Node struct {
	Scope Scope
	ID    NodeID

	// Kind partitions the ready set within a scope. A worker may claim by kind,
	// which is how heterogeneous worker pools are served without either
	// scanning or a secondary index.
	Kind string

	Status Status

	// Reason and Message describe the most recent significant outcome. See
	// [Reason] for exactly what they mean at each status.
	Reason  Reason
	Message string

	// Attempt is the number of times the node has been claimed, and the number
	// the retry policy's MaxAttempts is compared against. It counts this
	// node's own attempts, so it restarts at zero for an identifier that was
	// deleted and added again.
	//
	// It is deliberately not the fencing epoch, which must never go backwards
	// for a recycled identifier and so cannot restart with it (ADR-0011, as
	// amended by ADR-0043). A holder's epoch is in its [Lease]; the epoch a
	// node's current lease was granted at is [Inspection.LeaseEpoch].
	Attempt uint32

	// Priority orders the ready set; higher is claimed first, ties broken FIFO
	// by insertion sequence.
	Priority int16

	Trigger TriggerRule
	Retry   RetryPolicy

	// Payload is opaque to the library. It is bytes rather than a type
	// parameter because the storage and the event stream must stay meaningful
	// to a process — possibly in another language — that was not compiled
	// against the producer's types.
	Payload []byte

	// Labels carry caller metadata for subscription filtering. They are not an
	// indexed claim predicate; use Kind for that.
	Labels map[string]string

	// Seq is the per-node monotonic sequence of the write that produced this
	// snapshot. Compare it to decide whether a read is stale.
	Seq Seq

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Terminal reports whether the node has reached a final status.
func (n Node) Terminal() bool { return n.Status.Terminal() }

// NodeSpec describes a node to create. It is the unit of the batch insert and
// the unit of idempotency: re-adding a node whose spec is byte-identical is a
// no-op, while re-adding one whose spec differs is [ErrIDConflict].
type NodeSpec struct {
	ID       NodeID
	Kind     string
	Payload  []byte
	Labels   map[string]string
	Priority int16
	Trigger  TriggerRule
	Retry    RetryPolicy

	// Deps are predecessors, which must already exist in the same scope unless
	// they appear earlier in the same [Manager.AddNodes] batch. Forward
	// references across separate calls are not supported: an edge to a node
	// that does not exist yet cannot be cycle-checked.
	Deps []NodeID
}

// Validate reports whether the spec is well formed. Backends call it so that a
// malformed spec is rejected identically everywhere rather than surfacing as a
// different, less intelligible error from each backend's own storage layer.
func (s NodeSpec) Validate() error {
	if err := s.ID.validate(); err != nil {
		return err
	}
	if err := validateKind(s.Kind); err != nil {
		return err
	}
	if err := validateLabels(s.Labels); err != nil {
		return err
	}
	if err := s.Trigger.validate(); err != nil {
		return err
	}
	if err := s.Retry.validate(); err != nil {
		return err
	}
	seen := make(map[NodeID]struct{}, len(s.Deps))
	for _, d := range s.Deps {
		if err := d.validate(); err != nil {
			return err
		}
		if d == s.ID {
			return invalidArg("deps", "node %q depends on itself", s.ID)
		}
		if _, dup := seen[d]; dup {
			return invalidArg("deps", "duplicate dependency %q", d)
		}
		seen[d] = struct{}{}
	}
	return nil
}

// Inspection exposes internal scheduling state for debugging and admin tooling.
// It carries no compatibility promise across minor versions, never appears on
// the event stream, and never appears in a wire format.
type Inspection struct {
	Node Node

	// Phase distinguishes blocked from claimable, which Status deliberately
	// does not: adding an edge can flip a node between them with no actor
	// involved, and a subscriber must not see that as a status regression.
	Phase Phase

	// Deps is the incremental predecessor tally the trigger rule is evaluated
	// against.
	Deps DepCounts

	// Waiting lists the predecessors that are not yet terminal. This answers
	// "why is this node stuck", which is the first question every operator asks.
	Waiting []NodeID

	// Successors lists the node's direct out-edges.
	Successors []NodeID

	// Rank is the node's topological order number, used for O(1) cycle
	// rejection on edge insert.
	Rank int64

	// LeaseDeadline is when the current lease expires. Zero if unclaimed.
	LeaseDeadline time.Time

	// LeaseEpoch is the fencing epoch the node's current lease was granted at,
	// which is what a holder must present to complete or extend it. Zero
	// before the node's first claim. It is not [Node.Attempt]: see that
	// field's doc for why the two diverge.
	LeaseEpoch uint64

	// ReadyAt is when a scheduled retry becomes claimable. Zero if not waiting
	// on a backoff.
	ReadyAt time.Time
}
