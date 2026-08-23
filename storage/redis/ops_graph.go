package redis

import (
	"context"
	"fmt"

	dw "github.com/specialistvlad/dagworker"
)

// AddNodes implements [dagworker.Store]. Everything the Lua script cannot
// validate without first reading live state (idempotent re-insertion,
// dependency resolution, cycle rejection, the actual creation and linking)
// happens inside scriptAddNodes, atomically; everything that is a pure
// function of the specs and the resolved config — per-spec structural
// validity, cross-batch duplicate IDs, batch size, payload size — is checked
// here first, exactly mirroring the in-memory reference's own two-stage
// shape, so a malformed batch never reaches the server at all.
func (s *Store) AddNodes(ctx context.Context, scope dw.Scope, specs []dw.NodeSpec) ([]dw.Effect, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	if s.isClosed() {
		return nil, dw.ErrClosed
	}
	for _, spec := range specs {
		if err := spec.Validate(); err != nil {
			return nil, err
		}
	}
	seen := make(map[dw.NodeID]struct{}, len(specs))
	for _, spec := range specs {
		if _, dup := seen[spec.ID]; dup {
			return nil, fmt.Errorf("%w: node %q appears twice in the batch", dw.ErrInvalidArgument, spec.ID)
		}
		seen[spec.ID] = struct{}{}
	}

	cfg, err := s.resolvedConfig(ctx, scope)
	if err != nil {
		return nil, err
	}
	if len(specs) > cfg.MaxBatchSize {
		return nil, fmt.Errorf("%w: batch of %d exceeds the scope's limit of %d",
			dw.ErrInvalidArgument, len(specs), cfg.MaxBatchSize)
	}
	for _, spec := range specs {
		if len(spec.Payload) > cfg.PayloadCap {
			return nil, &dw.PayloadTooLargeError{Size: len(spec.Payload), Cap: cfg.PayloadCap}
		}
	}

	argv := make([]any, 0, 3+len(specs)*10)
	argv = append(argv, cfg.MaxBatchSize, len(specs))
	for _, spec := range specs {
		argv = append(argv,
			string(spec.ID), spec.Kind, int64(spec.Priority), int64(spec.Trigger),
			int64(spec.Retry.MaxAttempts), durToMs(spec.Retry.BaseDelay), durToMs(spec.Retry.MaxDelay),
			spec.Payload, encodeLabels(spec.Labels),
			len(spec.Deps),
		)
		for _, d := range spec.Deps {
			argv = append(argv, string(d))
		}
	}

	_, effects, err := s.runScript(ctx, s.scripts.addNodes, scope, argv...)
	if err != nil {
		return nil, err
	}
	s.registerScope(ctx, scope)
	return effects, nil
}

// AddEdges implements [dagworker.Store].
func (s *Store) AddEdges(ctx context.Context, scope dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	if len(edges) == 0 {
		return nil, nil
	}
	if s.isClosed() {
		return nil, dw.ErrClosed
	}
	argv := make([]any, 0, 2+len(edges)*2)
	argv = append(argv, len(edges))
	for _, e := range edges {
		argv = append(argv, string(e.From), string(e.To))
	}
	_, effects, err := s.runScript(ctx, s.scripts.addEdges, scope, argv...)
	return effects, err
}

// RemoveEdges implements [dagworker.Store].
func (s *Store) RemoveEdges(ctx context.Context, scope dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	if len(edges) == 0 {
		return nil, nil
	}
	if s.isClosed() {
		return nil, dw.ErrClosed
	}
	argv := make([]any, 0, 2+len(edges)*2)
	argv = append(argv, len(edges))
	for _, e := range edges {
		argv = append(argv, string(e.From), string(e.To))
	}
	_, effects, err := s.runScript(ctx, s.scripts.removeEdges, scope, argv...)
	return effects, err
}

// RemoveNode implements [dagworker.Store].
func (s *Store) RemoveNode(ctx context.Context, scope dw.Scope, id dw.NodeID, policy dw.CascadePolicy) ([]dw.Effect, error) {
	if s.isClosed() {
		return nil, dw.ErrClosed
	}
	_, effects, err := s.runScript(ctx, s.scripts.removeNode, scope, string(id), int64(policy))
	return effects, err
}

// Cancel implements [dagworker.Store]. Unlike the in-memory reference (which
// applies each id in order and can return effects accumulated before a later
// id's NotFound error), this backend validates every id exists before
// terminating any of them: a Redis error reply cannot carry a partial-success
// payload alongside it, so "all present or none processed" is the honest,
// strictly safer shape a single atomic script can offer. No conformance test
// depends on the reference's partial-effect behaviour on this path — see
// deviations.
func (s *Store) Cancel(ctx context.Context, scope dw.Scope, ids []dw.NodeID) ([]dw.Effect, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if s.isClosed() {
		return nil, dw.ErrClosed
	}
	argv := make([]any, 0, 1+len(ids))
	argv = append(argv, len(ids))
	for _, id := range ids {
		argv = append(argv, string(id))
	}
	_, effects, err := s.runScript(ctx, s.scripts.cancelNodes, scope, argv...)
	return effects, err
}

// CancelScope implements [dagworker.Store].
func (s *Store) CancelScope(ctx context.Context, scope dw.Scope) ([]dw.Effect, error) {
	if s.isClosed() {
		return nil, dw.ErrClosed
	}
	_, effects, err := s.runScript(ctx, s.scripts.cancelScope, scope)
	return effects, err
}
