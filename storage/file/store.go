package file

import (
	"context"
	"fmt"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// Capabilities reports what this backend actually delivers.
//
// CapDurableStorage is set and earned: every mutation is fsynced before its
// call returns, so an unclean exit loses nothing. CapCrossProcess is NOT set,
// and that is the honest answer rather than a limitation waiting to be lifted
// -- two processes over one directory would replay each other's history and
// then diverge in silence, which is exactly what ADR-0016's capability rule
// exists to stop a backend from claiming.
//
// The optional facets come from the in-memory backend underneath, minus
// CapCollect: retention deletes terminal nodes, and a delete that the log
// cannot express would be undone by the next replay.
func (s *Store) Capabilities() dw.Capabilities {
	return dw.Capabilities(dw.CapList | dw.CapDurableEvents | dw.CapDoorbell |
		dw.CapDurableStorage)
}

// mutate runs one logged mutation: record what the command reads, apply it,
// then append and fsync before returning.
//
// The order is deliberate. Appending after the mutation rather than before is
// what lets the record carry the readings the mutation actually consumed, and
// it costs nothing in durability: the call has not returned, so no caller has
// been told the write landed. A crash between the two loses the mutation
// entirely, which is indistinguishable from a crash just before it.
func mutate[T any](s *Store, op opKind, scope dw.Scope, fill func(*record), run func() (T, error)) (T, error) {
	var zero T
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return zero, dw.ErrClosed
	}

	end := s.gate.begin()
	out, err := run()
	rd := end()
	if err != nil {
		// A rejected command changed nothing, so there is nothing to replay.
		return zero, err
	}

	r := record{Op: op, Scope: string(scope), Readings: rd}
	fill(&r)
	if logErr := s.log.append(r); logErr != nil {
		// The mutation happened in memory and could not be made durable. Saying
		// so is the only honest option: reporting success would promise a
		// restart-survivable write that will not survive one.
		return zero, fmt.Errorf("file: %w", logErr)
	}
	return out, nil
}

// ---------------------------------------------------------------- mutations

// SetScopeConfig implements [dagworker.Store].
func (s *Store) SetScopeConfig(ctx context.Context, scope dw.Scope, cfg dw.ScopeConfig) error {
	_, err := mutate(s, opSetScopeConfig, scope,
		func(r *record) { c := cfg; r.Config = &c },
		func() (struct{}, error) { return struct{}{}, s.mem.SetScopeConfig(ctx, scope, cfg) })
	return err
}

// Seal implements [dagworker.Store].
func (s *Store) Seal(ctx context.Context, scope dw.Scope) error {
	_, err := mutate(s, opSeal, scope, func(*record) {},
		func() (struct{}, error) { return struct{}{}, s.mem.Seal(ctx, scope) })
	return err
}

// AddNodes implements [dagworker.Store].
func (s *Store) AddNodes(ctx context.Context, scope dw.Scope, specs []dw.NodeSpec) ([]dw.Effect, error) {
	return mutate(s, opAddNodes, scope,
		func(r *record) { r.Specs = specs },
		func() ([]dw.Effect, error) { return s.mem.AddNodes(ctx, scope, specs) })
}

// AddEdges implements [dagworker.Store].
func (s *Store) AddEdges(ctx context.Context, scope dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	return mutate(s, opAddEdges, scope,
		func(r *record) { r.Edges = edges },
		func() ([]dw.Effect, error) { return s.mem.AddEdges(ctx, scope, edges) })
}

// RemoveEdges implements [dagworker.Store].
func (s *Store) RemoveEdges(ctx context.Context, scope dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	return mutate(s, opRemoveEdges, scope,
		func(r *record) { r.Edges = edges },
		func() ([]dw.Effect, error) { return s.mem.RemoveEdges(ctx, scope, edges) })
}

// RemoveNode implements [dagworker.Store].
func (s *Store) RemoveNode(ctx context.Context, scope dw.Scope, id dw.NodeID, policy dw.CascadePolicy) ([]dw.Effect, error) {
	return mutate(s, opRemoveNode, scope,
		func(r *record) { r.NodeID, r.Policy = string(id), policy },
		func() ([]dw.Effect, error) { return s.mem.RemoveNode(ctx, scope, id, policy) })
}

// Cancel implements [dagworker.Store].
func (s *Store) Cancel(ctx context.Context, scope dw.Scope, ids []dw.NodeID) ([]dw.Effect, error) {
	return mutate(s, opCancel, scope,
		func(r *record) {
			r.IDs = make([]string, len(ids))
			for i, id := range ids {
				r.IDs[i] = string(id)
			}
		},
		func() ([]dw.Effect, error) { return s.mem.Cancel(ctx, scope, ids) })
}

// CancelScope implements [dagworker.Store].
func (s *Store) CancelScope(ctx context.Context, scope dw.Scope) ([]dw.Effect, error) {
	return mutate(s, opCancelScope, scope, func(*record) {},
		func() ([]dw.Effect, error) { return s.mem.CancelScope(ctx, scope) })
}

// Claim implements [dagworker.Store].
func (s *Store) Claim(ctx context.Context, req dw.ClaimRequest) (dw.ClaimResult, error) {
	return mutate(s, opClaim, req.Scope,
		func(r *record) { q := req; r.Claim = &q },
		func() (dw.ClaimResult, error) { return s.mem.Claim(ctx, req) })
}

// Complete implements [dagworker.Store].
func (s *Store) Complete(ctx context.Context, req dw.CompleteRequest) (dw.CompleteResult, error) {
	return mutate(s, opComplete, req.Lease.Scope,
		func(r *record) { q := req; r.Done = &q },
		func() (dw.CompleteResult, error) { return s.mem.Complete(ctx, req) })
}

// Extend implements [dagworker.Store].
func (s *Store) Extend(ctx context.Context, req dw.ExtendRequest) (time.Time, error) {
	return mutate(s, opExtend, req.Lease.Scope,
		func(r *record) { q := req; r.Extend = &q },
		func() (time.Time, error) { return s.mem.Extend(ctx, req) })
}

// Sweep implements [dagworker.Store].
func (s *Store) Sweep(ctx context.Context, scope dw.Scope, limit int) (dw.SweepResult, error) {
	return mutate(s, opSweep, scope,
		func(r *record) { r.Limit = limit },
		func() (dw.SweepResult, error) { return s.mem.Sweep(ctx, scope, limit) })
}

// ---------------------------------------------------------------- reads
//
// Reads are not logged and not serialised against each other: they change
// nothing, so there is nothing to replay and nothing to order.

// ScopeConfig implements [dagworker.Store].
func (s *Store) ScopeConfig(ctx context.Context, scope dw.Scope) (dw.ScopeConfig, error) {
	return s.mem.ScopeConfig(ctx, scope)
}

// ScopeStats implements [dagworker.Store].
func (s *Store) ScopeStats(ctx context.Context, scope dw.Scope) (dw.ScopeStats, error) {
	return s.mem.ScopeStats(ctx, scope)
}

// GetNode implements [dagworker.Store].
func (s *Store) GetNode(ctx context.Context, scope dw.Scope, id dw.NodeID) (dw.Node, error) {
	return s.mem.GetNode(ctx, scope, id)
}

// Inspect implements [dagworker.Store].
func (s *Store) Inspect(ctx context.Context, scope dw.Scope, id dw.NodeID) (dw.Inspection, error) {
	return s.mem.Inspect(ctx, scope, id)
}

// Scopes implements [dagworker.Store].
func (s *Store) Scopes(ctx context.Context) ([]dw.Scope, error) { return s.mem.Scopes(ctx) }

// ListNodes implements [dagworker.Lister], delegating to the in-memory
// backend underneath: the facet is a read, so there is nothing to log.
func (s *Store) ListNodes(ctx context.Context, scope dw.Scope, opts dw.ListOptions) (dw.ListResult, error) {
	return s.mem.ListNodes(ctx, scope, opts)
}

// Watch implements [dagworker.DurableEventStream], delegating to the in-memory
// backend underneath: the facet is a read, so there is nothing to log.
func (s *Store) Watch(ctx context.Context, req dw.WatchRequest) (<-chan dw.Event, error) {
	return s.mem.Watch(ctx, req)
}

// WaitForWork implements [dagworker.Doorbell], delegating to the in-memory
// backend underneath: the facet is a read, so there is nothing to log.
func (s *Store) WaitForWork(ctx context.Context, scope dw.Scope, kinds []string) error {
	return s.mem.WaitForWork(ctx, scope, kinds)
}

// Close flushes nothing, because there is nothing buffered: every mutation was
// already fsynced before its call returned. It closes the log and the store
// underneath it.
func (s *Store) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	memErr := s.mem.Close(ctx)
	logErr := s.log.close()
	if memErr != nil {
		return memErr
	}
	return logErr
}
