package dagworker

import (
	"context"
	"encoding/json"
	"fmt"
)

// Typed is an optional convenience layer that marshals node payloads to and
// from a Go type, so a caller working in one process does not have to encode by
// hand.
//
// It is deliberately a wrapper and not the primitive. The storage port and the
// event stream are defined in terms of bytes because the process that reads a
// node is often not the process that wrote it — and, with the network adapters,
// often not even written in Go. A generic Store[T] would be a type that cannot
// survive its own wire format.
//
// The zero value is not usable; call [NewTyped].
type Typed[T any] struct {
	m     *Manager
	scope Scope
}

// NewTyped returns a typed view of one scope.
func NewTyped[T any](m *Manager, scope Scope) Typed[T] {
	return Typed[T]{m: m, scope: scope}
}

// Scope returns the scope this view is bound to.
func (t Typed[T]) Scope() Scope { return t.scope }

// Manager returns the underlying Manager, for the operations that do not
// involve a payload.
func (t Typed[T]) Manager() *Manager { return t.m }

// AddNode encodes payload as JSON and creates the node.
func (t Typed[T]) AddNode(ctx context.Context, id NodeID, payload T, opts ...NodeOption) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return &InvalidArgumentError{
			Field:  "payload",
			Detail: fmt.Sprintf("encoding payload for node %q: %v", id, err),
		}
	}
	return t.m.AddNode(ctx, t.scope, id, raw, opts...)
}

// TypedLease is a lease whose payload has been decoded.
type TypedLease[T any] struct {
	Lease
	Payload T
}

// Claim waits for a ready node, takes it, and decodes its payload.
//
// A payload that fails to decode is a poison node: it would fail identically on
// every attempt, so it is made terminal rather than retried, and the error is
// returned to the caller wrapping [ErrInvalidArgument]. Silently retrying a
// node that can never succeed is how a queue fills up with work nobody looks
// at, and it costs one worker's time per attempt to reach the same conclusion.
func (t Typed[T]) Claim(ctx context.Context, opts ...ClaimOption) (TypedLease[T], error) {
	lease, err := t.m.Claim(ctx, t.scope, opts...)
	if err != nil {
		return TypedLease[T]{}, err
	}
	return t.decode(ctx, lease)
}

// TryClaim is the non-waiting variant. It returns [ErrNoWork] when nothing is
// ready.
func (t Typed[T]) TryClaim(ctx context.Context, opts ...ClaimOption) (TypedLease[T], error) {
	lease, err := t.m.TryClaim(ctx, t.scope, opts...)
	if err != nil {
		return TypedLease[T]{}, err
	}
	return t.decode(ctx, lease)
}

func (t Typed[T]) decode(ctx context.Context, lease Lease) (TypedLease[T], error) {
	var payload T
	if len(lease.Node.Payload) > 0 {
		if err := json.Unmarshal(lease.Node.Payload, &payload); err != nil {
			return TypedLease[T]{}, t.poison(ctx, lease, err)
		}
	}
	return TypedLease[T]{Lease: lease, Payload: payload}, nil
}

// poison reports an undecodable payload and takes the node out of circulation.
//
// It is two operations rather than one because the storage port has no "fail
// this terminally on the first report" primitive for a genuine error: Nack
// honours the retry policy and ReasonSkipped means "there was nothing to do",
// which is a different thing that successors under [TriggerNoneFailed] are
// entitled to run after. So: Nack, which records the real cause and the real
// reason, and then Cancel if the policy scheduled a retry.
//
// The two are not atomic. A crash between them leaves the node scheduled for a
// retry that will fail the same way — which is exactly the behaviour without
// this method at all, so the window costs nothing that was not already being
// paid, and closing it properly means a new Reason across three backends and
// both wire protocols. That is an ADR, not a bug fix.
func (t Typed[T]) poison(ctx context.Context, lease Lease, cause error) error {
	decodeErr := &InvalidArgumentError{
		Field:  "payload",
		Detail: fmt.Sprintf("decoding payload of node %q: %v", lease.NodeID, cause),
	}
	res, nackErr := t.m.Nack(ctx, lease, decodeErr)
	if nackErr != nil {
		return fmt.Errorf("%w (also failed to report it: %v)", decodeErr, nackErr)
	}
	if !res.Retrying {
		return decodeErr
	}
	if cancelErr := t.m.Cancel(ctx, lease.Scope, lease.NodeID); cancelErr != nil {
		return fmt.Errorf("%w (retry scheduled and could not be cancelled: %v)", decodeErr, cancelErr)
	}
	return decodeErr
}

// Ack encodes result as JSON and completes the node successfully.
func (t Typed[T]) Ack(ctx context.Context, lease TypedLease[T], result any) error {
	var raw []byte
	if result != nil {
		var err error
		if raw, err = json.Marshal(result); err != nil {
			return &InvalidArgumentError{
				Field:  "result",
				Detail: fmt.Sprintf("encoding result for node %q: %v", lease.NodeID, err),
			}
		}
	}
	return t.m.Ack(ctx, lease.Lease, raw)
}

// Nack reports that the attempt failed, and says whether the scope's retry
// policy turned it into another attempt.
func (t Typed[T]) Nack(ctx context.Context, lease TypedLease[T], cause error) (AttemptResult, error) {
	return t.m.Nack(ctx, lease.Lease, cause)
}

// GetNode reads a node and decodes its payload.
func (t Typed[T]) GetNode(ctx context.Context, id NodeID) (Node, T, error) {
	var payload T
	n, err := t.m.GetNode(ctx, t.scope, id)
	if err != nil {
		return n, payload, err
	}
	if len(n.Payload) > 0 {
		if err := json.Unmarshal(n.Payload, &payload); err != nil {
			return n, payload, &InvalidArgumentError{
				Field:  "payload",
				Detail: fmt.Sprintf("decoding payload of node %q: %v", id, err),
			}
		}
	}
	return n, payload, nil
}
