package postgres

import (
	"context"
	"fmt"

	dw "github.com/specialistvlad/dagworker"
)

// settleTouched re-evaluates every node named by ids, freshly re-read and
// locked one at a time — never a snapshot cached from earlier in the same
// call, so a cascade triggered by settling an earlier id is always visible
// before a later id's own turn. Shared by every mutation that can leave a
// node's trigger rule newly satisfiable or newly unsatisfiable: AddNodes and
// AddEdges (a dependency was linked), RemoveEdges and RemoveNode's
// CascadeDetach (a dependency was dropped).
func settleTouched(ctx context.Context, eng *engine, ids []int64) ([]dw.Effect, error) {
	var effects []dw.Effect
	for _, id := range ids {
		n, err := eng.loadForUpdate(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("postgres: settle: load: %w", err)
		}
		more, err := eng.settle(ctx, &n)
		if err != nil {
			return nil, err
		}
		effects = append(effects, more...)
	}
	return effects, nil
}

// validateAddNodesBatch rejects a malformed batch before anything is
// written, so the common failure mode never touches a row — the same split
// memory's own validateBatch makes.
func validateAddNodesBatch(cfg dw.ScopeConfig, specs []dw.NodeSpec) error {
	if len(specs) > cfg.MaxBatchSize {
		return fmt.Errorf("%w: batch of %d exceeds the scope's limit of %d",
			dw.ErrInvalidArgument, len(specs), cfg.MaxBatchSize)
	}
	seen := make(map[dw.NodeID]struct{}, len(specs))
	for _, spec := range specs {
		if err := spec.Validate(); err != nil {
			return err
		}
		if len(spec.Payload) > cfg.PayloadCap {
			return &dw.PayloadTooLargeError{Size: len(spec.Payload), Cap: cfg.PayloadCap}
		}
		if _, dup := seen[spec.ID]; dup {
			return fmt.Errorf("%w: node %q appears twice in the batch", dw.ErrInvalidArgument, spec.ID)
		}
		seen[spec.ID] = struct{}{}
	}
	return nil
}

// materialiseAddNodesBatch creates every spec's node, or verifies an
// existing one is byte-identical (AddNodes' idempotency rule). A batch that
// fails identity here has made no writes yet beyond whichever earlier specs
// in the same call already created rows — which the caller's deferred
// Rollback discards along with everything else.
func materialiseAddNodesBatch(ctx context.Context, eng *engine, specs []dw.NodeSpec) (nodes map[dw.NodeID]nodeRow, fresh []dw.NodeID, err error) {
	nodes = make(map[dw.NodeID]nodeRow, len(specs))
	for _, spec := range specs {
		existing, err := eng.loadForUpdateByExternal(ctx, string(spec.ID))
		switch {
		case err == nil:
			if !specMatches(existing, spec) {
				return nil, nil, fmt.Errorf("%w: node %q", dw.ErrIDConflict, spec.ID)
			}
			nodes[spec.ID] = existing
		case isNoRows(err):
			n, err := eng.createNode(ctx, spec)
			if err != nil {
				return nil, nil, err
			}
			nodes[spec.ID] = n
			fresh = append(fresh, spec.ID)
		default:
			return nil, nil, fmt.Errorf("postgres: AddNodes: load %q: %w", spec.ID, err)
		}
	}
	return nodes, fresh, nil
}

// linkAddNodesBatch records every spec's declared dependencies, resolving a
// forward reference against an earlier spec in the same batch before
// falling back to a row that must already exist.
func linkAddNodesBatch(ctx context.Context, eng *engine, specs []dw.NodeSpec, nodes map[dw.NodeID]nodeRow) (*orderedSet[int64], error) {
	touched := newOrderedSet[int64]()
	for _, spec := range specs {
		to := nodes[spec.ID]
		touched.add(to.ID)
		for _, dep := range spec.Deps {
			from, ok := nodes[dep]
			if !ok {
				loaded, err := eng.loadForUpdateByExternal(ctx, string(dep))
				if err != nil {
					if isNoRows(err) {
						return nil, fmt.Errorf("%w: %q depends on %q, which does not exist",
							dw.ErrNotFound, spec.ID, dep)
					}
					return nil, fmt.Errorf("postgres: AddNodes: load dep %q: %w", dep, err)
				}
				from = loaded
			}
			if err := eng.linkDependency(ctx, &from, &to); err != nil {
				return nil, err
			}
			nodes[dep] = from
			nodes[spec.ID] = to
		}
	}
	return touched, nil
}

// announceFresh emits the EventCreated effect for every newly materialised
// node, in the order the caller declared them — creation is reported before
// readiness so a subscriber never sees a node become claimable before it has
// heard the node exists.
func announceFresh(ctx context.Context, eng *engine, nodes map[dw.NodeID]nodeRow, fresh []dw.NodeID) ([]dw.Effect, error) {
	var effects []dw.Effect
	for _, extID := range fresh {
		n := nodes[extID]
		seq, err := eng.bumpSeq(ctx, n.ID)
		if err != nil {
			return nil, err
		}
		n.Seq = seq
		ef, err := eng.insertEvent(ctx, n.NodeID, n.Kind, dw.EventCreated, dw.StatusNew, n.Status, n.Reason, n.Message, n.Attempt, n.Seq)
		if err != nil {
			return nil, err
		}
		effects = append(effects, ef)
	}
	return effects, nil
}

// AddNodes implements [dagworker.Store]. It is one Postgres transaction:
// every node and edge lands, or the whole batch rolls back, which is what
// makes the manual journal-and-rollback bookkeeping memory's implementation
// needs entirely unnecessary here — a real ACID transaction already gives
// AddNodes its all-or-nothing guarantee for free.
func (s *Store) AddNodes(ctx context.Context, scope dw.Scope, specs []dw.NodeSpec) ([]dw.Effect, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	if err := s.ensureMigrated(ctx); err != nil {
		return nil, err
	}

	tx, err := beginTx(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("postgres: AddNodes: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := ensureScope(ctx, tx, string(scope), s.defaults); err != nil {
		return nil, err
	}
	if err := lockScopeGraph(ctx, tx, string(scope)); err != nil {
		return nil, fmt.Errorf("postgres: AddNodes: lock: %w", err)
	}

	sc, _, err := loadScope(ctx, tx, string(scope))
	if err != nil {
		return nil, err
	}
	if sc.Sealed {
		return nil, dw.ErrScopeSealed
	}
	cfg := sc.Cfg.Resolved()
	if err := validateAddNodesBatch(cfg, specs); err != nil {
		return nil, err
	}

	eng := newEngine(tx, string(scope), cfg, s.jitter)

	nodes, fresh, err := materialiseAddNodesBatch(ctx, eng, specs)
	if err != nil {
		return nil, err
	}
	touched, err := linkAddNodesBatch(ctx, eng, specs, nodes)
	if err != nil {
		return nil, err
	}

	// Settle every touched node against a freshly re-read row — never the
	// pass-two snapshot, so there is no question of a cascade from one
	// touched node's settle having already moved another before its turn.
	effects, err := announceFresh(ctx, eng, nodes, fresh)
	if err != nil {
		return nil, err
	}
	more, err := settleTouched(ctx, eng, touched.items)
	if err != nil {
		return nil, err
	}
	effects = append(effects, more...)

	if err := eng.notifyIfDirty(ctx); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: AddNodes: commit: %w", err)
	}
	return effects, nil
}

// AddEdges implements [dagworker.Store].
func (s *Store) AddEdges(ctx context.Context, scope dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	if len(edges) == 0 {
		return nil, nil
	}
	if err := s.ensureMigrated(ctx); err != nil {
		return nil, err
	}

	tx, err := beginTx(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("postgres: AddEdges: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, ok, err := loadScope(ctx, tx, string(scope)); err != nil {
		return nil, err
	} else if !ok {
		return nil, dw.ErrNotFound
	}
	if err := lockScopeGraph(ctx, tx, string(scope)); err != nil {
		return nil, fmt.Errorf("postgres: AddEdges: lock: %w", err)
	}

	eng := newEngine(tx, string(scope), dw.ScopeConfig{}, s.jitter)

	nodes := make(map[dw.NodeID]nodeRow, len(edges)*2)
	load := func(id dw.NodeID) (nodeRow, error) {
		if n, ok := nodes[id]; ok {
			return n, nil
		}
		n, err := eng.loadForUpdateByExternal(ctx, string(id))
		if err != nil {
			return nodeRow{}, err
		}
		nodes[id] = n
		return n, nil
	}

	touched := newOrderedSet[int64]()
	for _, e := range edges {
		from, err := load(e.From)
		if err != nil {
			if isNoRows(err) {
				return nil, fmt.Errorf("%w: edge source %q", dw.ErrNotFound, e.From)
			}
			return nil, fmt.Errorf("postgres: AddEdges: load %q: %w", e.From, err)
		}
		to, err := load(e.To)
		if err != nil {
			if isNoRows(err) {
				return nil, fmt.Errorf("%w: edge target %q", dw.ErrNotFound, e.To)
			}
			return nil, fmt.Errorf("postgres: AddEdges: load %q: %w", e.To, err)
		}
		if e.From == e.To {
			return nil, fmt.Errorf("%w: %q depends on itself", dw.ErrCycle, e.From)
		}
		if err := eng.linkDependency(ctx, &from, &to); err != nil {
			return nil, err
		}
		nodes[e.From], nodes[e.To] = from, to
		touched.add(to.ID)
	}

	effects, err := settleTouched(ctx, eng, touched.items)
	if err != nil {
		return nil, err
	}

	if err := eng.notifyIfDirty(ctx); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: AddEdges: commit: %w", err)
	}
	return effects, nil
}

// RemoveEdges implements [dagworker.Store].
func (s *Store) RemoveEdges(ctx context.Context, scope dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	if len(edges) == 0 {
		return nil, nil
	}
	if err := s.ensureMigrated(ctx); err != nil {
		return nil, err
	}

	tx, err := beginTx(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("postgres: RemoveEdges: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, ok, err := loadScope(ctx, tx, string(scope)); err != nil {
		return nil, err
	} else if !ok {
		return nil, dw.ErrNotFound
	}
	if err := lockScopeGraph(ctx, tx, string(scope)); err != nil {
		return nil, fmt.Errorf("postgres: RemoveEdges: lock: %w", err)
	}

	eng := newEngine(tx, string(scope), dw.ScopeConfig{}, s.jitter)

	idOf := make(map[dw.NodeID]int64, len(edges)*2)
	lookup := func(id dw.NodeID) (int64, error) {
		if v, ok := idOf[id]; ok {
			return v, nil
		}
		var v int64
		err := tx.QueryRow(ctx, `SELECT id FROM dagw.nodes WHERE scope = $1 AND node_id = $2`, string(scope), string(id)).Scan(&v)
		if err != nil {
			return 0, err
		}
		idOf[id] = v
		return v, nil
	}

	touched := newOrderedSet[int64]()
	for _, e := range edges {
		fromID, err := lookup(e.From)
		if err != nil {
			if isNoRows(err) {
				return nil, fmt.Errorf("%w: edge source %q", dw.ErrNotFound, e.From)
			}
			return nil, fmt.Errorf("postgres: RemoveEdges: %w", err)
		}
		toID, err := lookup(e.To)
		if err != nil {
			if isNoRows(err) {
				return nil, fmt.Errorf("%w: edge target %q", dw.ErrNotFound, e.To)
			}
			return nil, fmt.Errorf("postgres: RemoveEdges: %w", err)
		}
		changed, err := eng.unlinkDependency(ctx, fromID, toID)
		if err != nil {
			return nil, err
		}
		if changed {
			touched.add(toID)
		}
	}

	effects, err := settleTouched(ctx, eng, touched.items)
	if err != nil {
		return nil, err
	}

	if err := eng.notifyIfDirty(ctx); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: RemoveEdges: commit: %w", err)
	}
	return effects, nil
}

// applyCascade carries out policy's consequence for id's successors ahead of
// deletion: CascadeReject refuses outright, CascadeFail terminates every
// successor recursively, and CascadeDetach does nothing here (settling after
// the edges are dropped is its whole effect, handled by the caller).
func applyCascade(ctx context.Context, eng *engine, id dw.NodeID, policy dw.CascadePolicy, succs []nodeRow) ([]dw.Effect, error) {
	if len(succs) == 0 {
		return nil, nil
	}
	switch policy {
	case dw.CascadeReject:
		return nil, fmt.Errorf("%w: %q has %d successors", dw.ErrHasSuccessors, id, len(succs))
	case dw.CascadeFail:
		var effects []dw.Effect
		// Fail the successors first, while the edges still exist to walk;
		// terminate re-reads each one, so the locks successorsForUpdate just
		// took are released as soon as this reload takes its own.
		for _, w := range succs {
			more, err := eng.terminate(ctx, w.ID, dw.StatusError, dw.ReasonRemoved, terminalMessage(dw.ReasonRemoved))
			if err != nil {
				return nil, err
			}
			effects = append(effects, more...)
		}
		return effects, nil
	case dw.CascadeDetach:
		return nil, nil
	default:
		return nil, nil
	}
}

// RemoveNode implements [dagworker.Store].
func (s *Store) RemoveNode(ctx context.Context, scope dw.Scope, id dw.NodeID, policy dw.CascadePolicy) ([]dw.Effect, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return nil, err
	}

	tx, err := beginTx(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("postgres: RemoveNode: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, ok, err := loadScope(ctx, tx, string(scope)); err != nil {
		return nil, err
	} else if !ok {
		return nil, dw.ErrNotFound
	}
	if err := lockScopeGraph(ctx, tx, string(scope)); err != nil {
		return nil, fmt.Errorf("postgres: RemoveNode: lock: %w", err)
	}

	eng := newEngine(tx, string(scope), dw.ScopeConfig{}, s.jitter)

	n, err := eng.loadForUpdateByExternal(ctx, string(id))
	if err != nil {
		if isNoRows(err) {
			return nil, dw.ErrNotFound
		}
		return nil, fmt.Errorf("postgres: RemoveNode: load: %w", err)
	}
	if n.Phase == dw.PhaseClaimed {
		return nil, fmt.Errorf("%w: %q", dw.ErrNodeInFlight, id)
	}

	succs, err := eng.successorsForUpdate(ctx, n.ID)
	if err != nil {
		return nil, fmt.Errorf("postgres: RemoveNode: successors: %w", err)
	}
	effects, err := applyCascade(ctx, eng, id, policy, succs)
	if err != nil {
		return nil, err
	}

	successorIDs := make([]int64, len(succs))
	for i, w := range succs {
		successorIDs[i] = w.ID
		if _, err := eng.unlinkDependency(ctx, n.ID, w.ID); err != nil {
			return nil, fmt.Errorf("postgres: RemoveNode: detach successor: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM dagw.edges WHERE scope = $1 AND to_id = $2`, string(scope), n.ID); err != nil {
		return nil, fmt.Errorf("postgres: RemoveNode: detach predecessors: %w", err)
	}

	if err := eng.leaveBucket(ctx, n.Phase, n.Status); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE dagw.scopes SET stat_total = stat_total - 1 WHERE scope = $1`, string(scope)); err != nil {
		return nil, fmt.Errorf("postgres: RemoveNode: stat_total: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM dagw.nodes WHERE id = $1`, n.ID); err != nil {
		return nil, fmt.Errorf("postgres: RemoveNode: delete: %w", err)
	}

	if policy == dw.CascadeDetach {
		more, err := settleTouched(ctx, eng, successorIDs)
		if err != nil {
			return nil, err
		}
		effects = append(effects, more...)
	}

	if err := eng.notifyIfDirty(ctx); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: RemoveNode: commit: %w", err)
	}
	return effects, nil
}

// Cancel implements [dagworker.Store].
func (s *Store) Cancel(ctx context.Context, scope dw.Scope, ids []dw.NodeID) ([]dw.Effect, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if err := s.ensureMigrated(ctx); err != nil {
		return nil, err
	}

	tx, err := beginTx(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("postgres: Cancel: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, ok, err := loadScope(ctx, tx, string(scope)); err != nil {
		return nil, err
	} else if !ok {
		return nil, dw.ErrNotFound
	}
	if err := lockScopeGraph(ctx, tx, string(scope)); err != nil {
		return nil, fmt.Errorf("postgres: Cancel: lock: %w", err)
	}

	eng := newEngine(tx, string(scope), dw.ScopeConfig{}, s.jitter)

	var effects []dw.Effect
	for _, id := range ids {
		var internalID int64
		var phase int16
		err := tx.QueryRow(ctx, `SELECT id, phase FROM dagw.nodes WHERE scope = $1 AND node_id = $2`,
			string(scope), string(id)).Scan(&internalID, &phase)
		if err != nil {
			if isNoRows(err) {
				return nil, fmt.Errorf("%w: %q", dw.ErrNotFound, id)
			}
			return nil, fmt.Errorf("postgres: Cancel: %w", err)
		}
		if dw.Phase(phase) == dw.PhaseDone {
			continue // cancelling something already finished is a no-op, not an error
		}
		more, err := eng.terminate(ctx, internalID, dw.StatusError, dw.ReasonCancelled, terminalMessage(dw.ReasonCancelled))
		if err != nil {
			return nil, err
		}
		effects = append(effects, more...)
	}

	if err := eng.notifyIfDirty(ctx); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: Cancel: commit: %w", err)
	}
	return effects, nil
}

// CancelScope implements [dagworker.Store].
func (s *Store) CancelScope(ctx context.Context, scope dw.Scope) ([]dw.Effect, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return nil, err
	}

	tx, err := beginTx(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("postgres: CancelScope: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, ok, err := loadScope(ctx, tx, string(scope)); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	if err := lockScopeGraph(ctx, tx, string(scope)); err != nil {
		return nil, fmt.Errorf("postgres: CancelScope: lock: %w", err)
	}

	// Snapshot the live set first: terminate mutates phases as it cascades,
	// and a live query would see its own writes.
	liveRows, err := tx.Query(ctx, `SELECT id FROM dagw.nodes WHERE scope = $1 AND phase <> $2`,
		string(scope), int16(dw.PhaseDone))
	if err != nil {
		return nil, fmt.Errorf("postgres: CancelScope: list: %w", err)
	}
	ids, err := int64s(liveRows)
	if err != nil {
		return nil, fmt.Errorf("postgres: CancelScope: list: %w", err)
	}

	eng := newEngine(tx, string(scope), dw.ScopeConfig{}, s.jitter)
	var effects []dw.Effect
	for _, id := range ids {
		more, err := eng.terminate(ctx, id, dw.StatusError, dw.ReasonCancelled, terminalMessage(dw.ReasonCancelled))
		if err != nil {
			return nil, err
		}
		effects = append(effects, more...)
	}

	if err := eng.notifyIfDirty(ctx); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: CancelScope: commit: %w", err)
	}
	return effects, nil
}
