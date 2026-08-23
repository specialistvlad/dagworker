package dagworker

import (
	"fmt"
	"time"
)

// EventKind distinguishes the two things a subscriber can be told. They have
// deliberately different guarantees, and conflating them is the mistake this
// split exists to prevent.
type EventKind uint8

const (
	// EventCreated reports that a node came into existence. It is the first
	// event any node produces, always with Seq 1, and it carries the node's
	// initial status — which is not always [StatusNew]: a node inserted behind
	// a predecessor that has already failed is born terminal.
	//
	// It exists as its own kind rather than being inferred from Seq so that a
	// subscriber maintaining a live view of the graph does not have to know a
	// trick to spot a new node.
	EventCreated EventKind = iota

	// EventTransition reports that a node's public [Status] changed. This is
	// the observation feed: on a backend that supports a durable stream it is
	// at-least-once and resumable from a [Cursor].
	EventTransition

	// EventReady reports that a node became claimable. It is a doorbell, not a
	// delivery: it is coalescing, best-effort, and may be dropped entirely
	// without affecting correctness, because a claim always re-derives
	// eligibility from storage rather than trusting accumulated events.
	//
	// If you find yourself keeping a set of node IDs learned from EventReady
	// and treating it as authoritative, stop: that reintroduces exactly the
	// failure this design avoids.
	EventReady
)

// String implements [fmt.Stringer].
func (k EventKind) String() string {
	switch k {
	case EventCreated:
		return "created"
	case EventTransition:
		return "transition"
	case EventReady:
		return "ready"
	default:
		return fmt.Sprintf("event(%d)", uint8(k))
	}
}

// Event is one notification. It is emitted only after the storage write it
// describes has committed, and it carries the [Seq] of that write, so a
// subscriber can always tell whether a subsequent read reflects this event or
// something older.
type Event struct {
	Kind   EventKind
	Scope  Scope
	NodeID NodeID

	// Seq is the per-node sequence of the write. Events for one node arrive in
	// Seq order. There is no ordering guarantee between nodes from Seq alone.
	Seq Seq

	// Cursor is this event's position in the scope's log. Events within a scope
	// arrive in Cursor order, and Cursor is what a reconnecting subscriber
	// resumes from.
	Cursor Cursor

	// From and To are the status transition. For [EventReady] both are
	// [StatusNew] and the fields carry no information.
	From, To Status

	// Reason and Message accompany a transition into a terminal status.
	Reason  Reason
	Message string

	// Attempt is the node's attempt counter at the time of the write.
	Attempt uint32

	// Kind of the node, mirrored here so a subscriber can filter without a read.
	NodeKind string

	At time.Time

	// Gap is set when the delivery mechanism dropped at least one event before
	// this one under [OverflowDropOldest]. A subscriber that must not miss
	// transitions should re-read the affected scope's state when it sees this.
	Gap bool
}

// OverflowPolicy decides what happens to a subscription whose consumer is not
// keeping up. Whatever the policy, the producer is never blocked: a slow
// subscriber must not become the scheduler's problem.
type OverflowPolicy uint8

const (
	// OverflowDropOldest discards the oldest buffered event and sets [Event.Gap]
	// on the next one delivered. This is the default: it keeps the subscription
	// alive and tells the subscriber, truthfully, that it missed something.
	OverflowDropOldest OverflowPolicy = iota

	// OverflowCloseSlow terminates the subscription with [ErrSubscriberLagged].
	// Choose it when silently missing a transition is worse than losing the
	// subscription, since it forces the consumer to resynchronise explicitly.
	OverflowCloseSlow
)

// There is deliberately no blocking policy. Blocking a subscriber's delivery
// requires somewhere to put the events that arrive meanwhile: either an
// unbounded buffer, which trades a dropped event for an out-of-memory crash, or
// backpressure onto the caller who just completed a node — which makes one slow
// observer able to stall the scheduler for everyone. A caller that must not
// miss a transition should subscribe with Durable set and resume from a cursor,
// which is the mechanism that actually provides that guarantee.

// String implements [fmt.Stringer].
func (p OverflowPolicy) String() string {
	switch p {
	case OverflowDropOldest:
		return "drop_oldest"
	case OverflowCloseSlow:
		return "close_slow"
	default:
		return fmt.Sprintf("overflow(%d)", uint8(p))
	}
}

func (p OverflowPolicy) validate() error {
	if p > OverflowCloseSlow {
		return invalidArg("overflow policy", "unknown value %d", uint8(p))
	}
	return nil
}

// SubscribeOptions configures a subscription.
type SubscribeOptions struct {
	// Scope restricts the subscription to one scope. Empty subscribes to every
	// scope the backend serves.
	Scope Scope

	// Kinds restricts delivery to these event kinds. Empty means both.
	Kinds []EventKind

	// NodeKinds restricts delivery to nodes with one of these kinds. Empty
	// means all.
	NodeKinds []string

	// From resumes just after this log position. Zero means "not resuming"; use
	// Replay to choose which end to start at. If the cursor predates retained
	// history the subscription fails with [ErrCursorExpired]; recover by
	// reading current state and resubscribing from now.
	//
	// Resuming is only meaningful for a single scope: cursors are per scope, so
	// a subscription with an empty Scope and a non-zero From is rejected.
	From Cursor

	// Replay starts from the oldest retained event instead of from now.
	// Ignored when From is set.
	Replay bool

	// BufferSize overrides the Manager's default channel depth. Zero uses it.
	BufferSize int

	// Overflow overrides the Manager's default policy for a lagging consumer.
	Overflow *OverflowPolicy

	// Durable requests the backend's at-least-once, resumable tier rather than
	// its best-effort one. A backend that cannot provide it returns
	// [ErrUnsupported] rather than silently delivering a weaker guarantee.
	Durable bool
}

func (o SubscribeOptions) validate() error {
	if o.Scope != "" {
		if err := o.Scope.validate(); err != nil {
			return err
		}
	}
	for _, k := range o.Kinds {
		if k > EventReady {
			return invalidArg("event kind", "unknown value %d", uint8(k))
		}
	}
	for _, nk := range o.NodeKinds {
		if err := validateKind(nk); err != nil {
			return err
		}
	}
	if o.Scope == "" && o.From != 0 {
		return invalidArg("from", "resuming requires a single scope, because cursors are per scope")
	}
	if o.BufferSize < 0 {
		return invalidArg("buffer size", "must not be negative")
	}
	if o.Overflow != nil {
		if err := o.Overflow.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (o SubscribeOptions) wants(e Event) bool {
	if o.Scope != "" && e.Scope != o.Scope {
		return false
	}
	if len(o.Kinds) > 0 && !containsKind(o.Kinds, e.Kind) {
		return false
	}
	if len(o.NodeKinds) > 0 && !containsString(o.NodeKinds, e.NodeKind) {
		return false
	}
	return true
}

func containsKind(ks []EventKind, k EventKind) bool {
	for _, x := range ks {
		if x == k {
			return true
		}
	}
	return false
}

func containsString(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
