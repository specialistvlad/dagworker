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
		return fmt.Errorf("dagworker: encoding payload for node %q: %w", id, err)
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
// every attempt, so it is failed immediately rather than retried, and the error
// is returned to the caller. Silently retrying a node that can never succeed is
// how a queue fills up with work nobody looks at.
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
			decodeErr := fmt.Errorf("dagworker: decoding payload of node %q: %w", lease.NodeID, err)
			if nackErr := t.m.Nack(ctx, lease, decodeErr); nackErr != nil {
				return TypedLease[T]{}, fmt.Errorf("%w (also failed to report it: %v)", decodeErr, nackErr)
			}
			return TypedLease[T]{}, decodeErr
		}
	}
	return TypedLease[T]{Lease: lease, Payload: payload}, nil
}

// Ack encodes result as JSON and completes the node successfully.
func (t Typed[T]) Ack(ctx context.Context, lease TypedLease[T], result any) error {
	var raw []byte
	if result != nil {
		var err error
		if raw, err = json.Marshal(result); err != nil {
			return fmt.Errorf("dagworker: encoding result for node %q: %w", lease.NodeID, err)
		}
	}
	return t.m.Ack(ctx, lease.Lease, raw)
}

// Nack reports that the attempt failed.
func (t Typed[T]) Nack(ctx context.Context, lease TypedLease[T], cause error) error {
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
			return n, payload, fmt.Errorf("dagworker: decoding payload of node %q: %w", id, err)
		}
	}
	return n, payload, nil
}
