package dagworker

import (
	"errors"
	"fmt"
)

// Sentinel errors form the public error taxonomy. Callers branch on these with
// [errors.Is]; every error this package returns wraps exactly one of them.
//
// Wrapping an error makes it part of the API, so this set is deliberately small
// and closed. Adding a sentinel is a minor-version change; removing one or
// changing what wraps what is a breaking change.
var (
	// ErrNotFound means the named scope or node does not exist.
	ErrNotFound = errors.New("dagworker: not found")

	// ErrIDConflict means a node with this ID already exists with a different
	// spec. Re-adding a node with a byte-identical spec is a no-op, not an error.
	ErrIDConflict = errors.New("dagworker: node exists with a different spec")

	// ErrCycle means the edge would create a cycle. The concrete error is a
	// [*CycleError] carrying the path.
	ErrCycle = errors.New("dagworker: dependency cycle")

	// ErrCrossScopeEdge means an edge's endpoints are in different scopes.
	// Scope-local edges are what keep every operation's cost independent of
	// total graph size.
	ErrCrossScopeEdge = errors.New("dagworker: edge crosses scopes")

	// ErrAlreadyTerminal means the operation would modify a node that has
	// already succeeded or failed. Nothing leaves a terminal status except
	// deletion under a retention policy.
	ErrAlreadyTerminal = errors.New("dagworker: node is already terminal")

	// ErrNodeInFlight means the node is claimed by a worker and cannot be
	// removed. Cancel it first.
	ErrNodeInFlight = errors.New("dagworker: node is in progress")

	// ErrHasSuccessors means [Manager.RemoveNode] was called on a node with
	// successors and no cascade policy. Pass [CascadeDetach] or [CascadeFail].
	ErrHasSuccessors = errors.New("dagworker: node has successors")

	// ErrLeaseMismatch means the presented lease epoch is not the node's
	// current epoch: the lease was superseded, most likely reclaimed after its
	// deadline elapsed. This is never retryable. The work may already have been
	// redone by another worker.
	ErrLeaseMismatch = errors.New("dagworker: lease epoch mismatch")

	// ErrLeaseExpired means the lease deadline has passed. Distinct from
	// [ErrLeaseMismatch]: the epoch still matches, but the grant is stale.
	ErrLeaseExpired = errors.New("dagworker: lease expired")

	// ErrNoWork means a non-blocking claim found nothing ready. It is an
	// expected, unexceptional outcome, not a failure.
	ErrNoWork = errors.New("dagworker: no ready node")

	// ErrScopeSealed means the scope was sealed and will accept no new nodes.
	ErrScopeSealed = errors.New("dagworker: scope is sealed")

	// ErrClosed means the Manager has been closed.
	ErrClosed = errors.New("dagworker: manager is closed")

	// ErrPayloadTooLarge means the payload exceeds the effective cap, which is
	// the smallest of the scope config, the library default, and the backend's
	// own limit.
	ErrPayloadTooLarge = errors.New("dagworker: payload exceeds cap")

	// ErrSubscriberLagged means a subscription using [OverflowCloseSlow] fell
	// behind and was terminated.
	ErrSubscriberLagged = errors.New("dagworker: subscriber lagged")

	// ErrCursorExpired means a resume cursor predates retained history. Recover
	// by reading current state, then resubscribing from now. The recovery
	// procedure is identical on every backend.
	ErrCursorExpired = errors.New("dagworker: resume cursor is older than retained history")

	// ErrInvalidArgument means a caller-supplied value is malformed: an empty
	// or oversized identifier, an unknown enum value, a negative duration.
	ErrInvalidArgument = errors.New("dagworker: invalid argument")

	// ErrInvalidConfig means an [Option] or [ScopeConfig] field is out of range
	// or internally inconsistent.
	ErrInvalidConfig = errors.New("dagworker: invalid configuration")

	// ErrNilStore means [New] was called without a store.
	ErrNilStore = errors.New("dagworker: store must not be nil")

	// ErrUnsupported means the backend does not implement an optional
	// capability the call requires. Check Capabilities before calling.
	ErrUnsupported = errors.New("dagworker: capability not supported by this backend")
)

// CycleError reports the dependency cycle an edge insertion would have created.
// It unwraps to [ErrCycle].
type CycleError struct {
	Scope Scope
	// From and To are the endpoints of the rejected edge.
	From, To NodeID
	// Path is the existing route from To back to From that closes the cycle,
	// starting at To and ending at From. It is empty only if the backend cannot
	// reconstruct it cheaply.
	Path []NodeID
}

func (e *CycleError) Error() string {
	if len(e.Path) == 0 {
		return fmt.Sprintf("dagworker: edge %s -> %s would create a cycle in scope %q",
			e.From, e.To, e.Scope)
	}
	return fmt.Sprintf("dagworker: edge %s -> %s would create a cycle in scope %q (%s already reaches %s in %d hops)",
		e.From, e.To, e.Scope, e.To, e.From, len(e.Path)-1)
}

// Unwrap makes errors.Is(err, ErrCycle) true.
func (e *CycleError) Unwrap() error { return ErrCycle }

// InvalidArgumentError names the field a caller got wrong. It unwraps to
// [ErrInvalidArgument].
type InvalidArgumentError struct {
	Field  string
	Detail string
}

func (e *InvalidArgumentError) Error() string {
	return fmt.Sprintf("dagworker: invalid %s: %s", e.Field, e.Detail)
}

// Unwrap makes errors.Is(err, ErrInvalidArgument) true.
func (e *InvalidArgumentError) Unwrap() error { return ErrInvalidArgument }

func invalidArg(field, format string, args ...any) error {
	return &InvalidArgumentError{Field: field, Detail: fmt.Sprintf(format, args...)}
}

// PayloadTooLargeError reports the payload size that was rejected and the cap
// that rejected it. It unwraps to [ErrPayloadTooLarge].
type PayloadTooLargeError struct {
	Size int
	Cap  int
}

func (e *PayloadTooLargeError) Error() string {
	return fmt.Sprintf("dagworker: payload is %d bytes, cap is %d", e.Size, e.Cap)
}

// Unwrap makes errors.Is(err, ErrPayloadTooLarge) true.
func (e *PayloadTooLargeError) Unwrap() error { return ErrPayloadTooLarge }
