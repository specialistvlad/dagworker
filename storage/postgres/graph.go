package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	dw "github.com/specialistvlad/dagworker"
)

// settleTouched re-evaluates every node named by ids, freshly re-read and
// locked one at a time — never a snapshot cached from earlier in the same
// call, so a cascade triggered by settling an earlier id is always visible
// before a later id's own turn. Shared by every mutation that can leave a
// node's trigger rule newly satisfiable or newly unsatisfiable: AddNodes and
// AddEdges (a dependency was linked), RemoveEdges and RemoveNode's
// CascadeDetach (a dependency was dropped).
// settleTouched re-evaluates every node named by ids against its current
// dependency tally.
//
// The loads are pipelined into one round trip, which is safe only because of
// the rule below. Settling one node can cascade into another, so a snapshot of
// every node taken up front can be stale by the time a later id's turn comes.
// The original implementation avoided that by re-reading each node
// individually — correct, and two round trips per node.
//
// Instead: take all the snapshots at once, and notice when a settle touches any
// node other than the one being settled. That is the only way a later
// snapshot can go stale, so from that point on the remaining nodes are re-read
// individually. In the common case — nodes becoming ready, touching nobody —
// nothing is re-read and the whole set costs one round trip.
func settleTouched(ctx context.Context, eng *engine, ids []int64) ([]dw.Effect, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := eng.loadManyForUpdate(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("postgres: settle: load: %w", err)
	}
	snapshots := make(map[int64]nodeRow, len(rows))
	for i := range rows {
		snapshots[rows[i].ID] = rows[i]
	}

	var effects []dw.Effect
	stale := false
	for _, id := range ids {
		n, ok := snapshots[id]
		if !ok {
			// Deleted between being named and being read. Nothing to settle.
			continue
		}
		more, cascaded, err := settleOne(ctx, eng, id, n, stale)
		if err != nil {
			return nil, err
		}
		stale = stale || cascaded
		effects = append(effects, more...)
	}
	return effects, nil
}

// settleOne settles a single node, re-reading it first if an earlier settle in
// the same call may have moved it. It reports whether this settle itself
// reached beyond its own node, which is what makes every later snapshot
// suspect.
func settleOne(
	ctx context.Context, eng *engine, id int64, snapshot nodeRow, reload bool,
) (effects []dw.Effect, cascaded bool, err error) {
	n := snapshot
	if reload {
		fresh, err := eng.loadForUpdate(ctx, id)
		if err != nil {
			if isNoRows(err) {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("postgres: settle: reload: %w", err)
		}
		n = fresh
	}

	effects, err = eng.settle(ctx, &n)
	if err != nil {
		return nil, false, err
	}
	for _, ef := range effects {
		if string(ef.NodeID) != n.NodeID {
			cascaded = true
		}
	}
	return effects, cascaded, nil
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
	ids := make([]string, len(specs))
	for i := range specs {
		ids[i] = string(specs[i].ID)
	}
	existing, err := eng.loadManyForUpdateByExternal(ctx, ids)
	if err != nil {
		return nil, nil, err
	}

	nodes = make(map[dw.NodeID]nodeRow, len(specs))
	toCreate := make([]dw.NodeSpec, 0, len(specs))
	for _, spec := range specs {
		n, ok := existing[string(spec.ID)]
		if !ok {
			toCreate = append(toCreate, spec)
			continue
		}
		// Re-adding a node with an identical definition is a no-op; with a
		// different one it is a conflict.
		if !specMatches(n, spec) {
			return nil, nil, fmt.Errorf("%w: node %q", dw.ErrIDConflict, spec.ID)
		}
		nodes[spec.ID] = n
	}

	created, err := eng.createNodes(ctx, toCreate)
	if err != nil {
		return nil, nil, err
	}
	for i := range created {
		nodes[toCreate[i].ID] = created[i]
		fresh = append(fresh, toCreate[i].ID)
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
	if len(fresh) == 0 {
		return nil, nil
	}
	ids := make([]int64, len(fresh))
	for i, extID := range fresh {
		ids[i] = nodes[extID].ID
	}
	seqs, err := eng.bumpSeqMany(ctx, ids)
	if err != nil {
		return nil, err
	}

	evs := make([]pendingEvent, len(fresh))
	for i, extID := range fresh {
		n := nodes[extID]
		evs[i] = pendingEvent{
			nodeID:   n.NodeID,
			nodeKind: n.Kind,
			kind:     dw.EventCreated,
			from:     dw.StatusNew,
			to:       n.Status,
			reason:   n.Reason,
			message:  n.Message,
			attempt:  n.Attempt,
			seq:      seqs[i],
		}
	}
	return eng.insertEvents(ctx, evs)
}

// AddNodes implements [dagworker.Store]. It is one Postgres transaction:
// every node and edge lands, or the whole batch rolls back, which is what
// makes the manual journal-and-rollback bookkeeping memory's implementation
// needs entirely unnecessary here — a real ACID transaction already gives
// AddNodes its all-or-nothing guarantee for free.
func (s *Store) AddNodes(ctx context.Context, scope dw.Scope, specs []dw.NodeSpec) ([]dw.Effect, error) {
	return retryTransient(ctx, s, "AddNodes", func(ctx context.Context) ([]dw.Effect, error) {
		return s.addNodesOnce(ctx, scope, specs)
	})
}

func (s *Store) addNodesOnce(ctx context.Context, scope dw.Scope, specs []dw.NodeSpec) ([]dw.Effect, error) {
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

	if err := eng.finalize(ctx); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: AddNodes: commit: %w", err)
	}
	return effects, nil
}

// beginGraphTx opens a transaction over an existing scope with its graph lock
// already held, and returns the engine bound to it.
//
// The lock is taken before any node is read. Mutations that can reorder
// topological ranks must not interleave, and holding one scope-wide lock is
// both simpler and cheaper than proving that some per-node acquisition order is
// deadlock-free -- which is the bug class that recurs in every scheduler that
// tries the clever version.
//
//nolint:ireturn // pgx.Tx is the only shape Pool.Begin returns.
func beginGraphTx(ctx context.Context, s *Store, scope dw.Scope, op string) (pgx.Tx, *engine, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return nil, nil, err
	}
	tx, err := beginTx(ctx, s)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: %s: begin: %w", op, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if _, ok, err := loadScope(ctx, tx, string(scope)); err != nil {
		return nil, nil, err
	} else if !ok {
		return nil, nil, dw.ErrNotFound
	}
	if err := lockScopeGraph(ctx, tx, string(scope)); err != nil {
		return nil, nil, fmt.Errorf("postgres: %s: lock: %w", op, err)
	}
	committed = true
	return tx, newEngine(tx, string(scope), dw.ScopeConfig{}, s.jitter), nil
}

// finishGraphTx flushes notifications and commits: the identical tail of every
// graph mutation.
func finishGraphTx(ctx context.Context, tx pgx.Tx, eng *engine, op string, effects []dw.Effect) ([]dw.Effect, error) {
	if err := eng.finalize(ctx); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: %s: commit: %w", op, err)
	}
	return effects, nil
}

// nodeLoader reads and locks each node at most once per transaction, so a batch
// naming the same node twice sees its own earlier write rather than a stale
// copy, and cannot contend with itself for the same row lock.
type nodeLoader struct {
	eng   *engine
	op    string
	nodes map[dw.NodeID]nodeRow
}

func newNodeLoader(eng *engine, op string, hint int) *nodeLoader {
	return &nodeLoader{eng: eng, op: op, nodes: make(map[dw.NodeID]nodeRow, hint)}
}

// endpoint loads one end of an edge, reporting a missing row as ErrNotFound
// that names which end was missing -- "edge source" and "edge target" are very
// different mistakes to have made.
func (l *nodeLoader) endpoint(ctx context.Context, id dw.NodeID, which string) (nodeRow, error) {
	if n, ok := l.nodes[id]; ok {
		return n, nil
	}
	n, err := l.eng.loadForUpdateByExternal(ctx, string(id))
	if err != nil {
		if isNoRows(err) {
			return nodeRow{}, fmt.Errorf("%w: edge %s %q", dw.ErrNotFound, which, id)
		}
		return nodeRow{}, fmt.Errorf("postgres: %s: load %q: %w", l.op, id, err)
	}
	l.nodes[id] = n
	return n, nil
}

func (l *nodeLoader) put(id dw.NodeID, n nodeRow) { l.nodes[id] = n }

// AddEdges implements [dagworker.Store].
func (s *Store) AddEdges(ctx context.Context, scope dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	return retryTransient(ctx, s, "AddEdges", func(ctx context.Context) ([]dw.Effect, error) {
		return s.addEdgesOnce(ctx, scope, edges)
	})
}

func (s *Store) addEdgesOnce(ctx context.Context, scope dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	if len(edges) == 0 {
		return nil, nil
	}
	tx, eng, err := beginGraphTx(ctx, s, scope, "AddEdges")
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	loader := newNodeLoader(eng, "AddEdges", len(edges)*2)
	touched := newOrderedSet[int64]()
	for _, e := range edges {
		if e.From == e.To {
			return nil, fmt.Errorf("%w: %q depends on itself", dw.ErrCycle, e.From)
		}
		from, err := loader.endpoint(ctx, e.From, "source")
		if err != nil {
			return nil, err
		}
		to, err := loader.endpoint(ctx, e.To, "target")
		if err != nil {
			return nil, err
		}
		if err := eng.linkDependency(ctx, &from, &to); err != nil {
			return nil, err
		}
		loader.put(e.From, from)
		loader.put(e.To, to)
		touched.add(to.ID)
	}

	effects, err := settleTouched(ctx, eng, touched.items)
	if err != nil {
		return nil, err
	}
	return finishGraphTx(ctx, tx, eng, "AddEdges", effects)
}

// RemoveEdges implements [dagworker.Store].
func (s *Store) RemoveEdges(ctx context.Context, scope dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	return retryTransient(ctx, s, "RemoveEdges", func(ctx context.Context) ([]dw.Effect, error) {
		return s.removeEdgesOnce(ctx, scope, edges)
	})
}

func (s *Store) removeEdgesOnce(ctx context.Context, scope dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	if len(edges) == 0 {
		return nil, nil
	}
	tx, eng, err := beginGraphTx(ctx, s, scope, "RemoveEdges")
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	loader := newNodeLoader(eng, "RemoveEdges", len(edges)*2)
	touched := newOrderedSet[int64]()
	for _, e := range edges {
		from, err := loader.endpoint(ctx, e.From, "source")
		if err != nil {
			return nil, err
		}
		to, err := loader.endpoint(ctx, e.To, "target")
		if err != nil {
			return nil, err
		}
		changed, err := eng.unlinkDependency(ctx, from.ID, to.ID)
		if err != nil {
			return nil, err
		}
		if changed {
			touched.add(to.ID)
		}
	}

	effects, err := settleTouched(ctx, eng, touched.items)
	if err != nil {
		return nil, err
	}
	return finishGraphTx(ctx, tx, eng, "RemoveEdges", effects)
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
	return retryTransient(ctx, s, "RemoveNode", func(ctx context.Context) ([]dw.Effect, error) {
		return s.removeNodeOnce(ctx, scope, id, policy)
	})
}

func (s *Store) removeNodeOnce(ctx context.Context, scope dw.Scope, id dw.NodeID, policy dw.CascadePolicy) ([]dw.Effect, error) {
	tx, eng, err := beginGraphTx(ctx, s, scope, "RemoveNode")
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

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

	if err := deleteNodeRow(ctx, tx, eng, n); err != nil {
		return nil, err
	}

	if policy == dw.CascadeDetach {
		more, err := settleTouched(ctx, eng, successorIDs)
		if err != nil {
			return nil, err
		}
		effects = append(effects, more...)
	}

	return finishGraphTx(ctx, tx, eng, "RemoveNode", effects)
}

// Cancel implements [dagworker.Store].
func (s *Store) Cancel(ctx context.Context, scope dw.Scope, ids []dw.NodeID) ([]dw.Effect, error) {
	return retryTransient(ctx, s, "Cancel", func(ctx context.Context) ([]dw.Effect, error) {
		return s.cancelOnce(ctx, scope, ids)
	})
}

func (s *Store) cancelOnce(ctx context.Context, scope dw.Scope, ids []dw.NodeID) ([]dw.Effect, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	tx, eng, err := beginGraphTx(ctx, s, scope, "Cancel")
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

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
		if dw.Phase(narrowU8(phase)) == dw.PhaseDone {
			continue // cancelling something already finished is a no-op, not an error
		}
		more, err := eng.terminate(ctx, internalID, dw.StatusError, dw.ReasonCancelled, terminalMessage(dw.ReasonCancelled))
		if err != nil {
			return nil, err
		}
		effects = append(effects, more...)
	}

	return finishGraphTx(ctx, tx, eng, "Cancel", effects)
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

	if err := eng.finalize(ctx); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: CancelScope: commit: %w", err)
	}
	return effects, nil
}

// deleteNodeRow removes a node's own row and corrects the scope's counters.
//
// Shared by RemoveNode and CollectTerminal so the two cannot disagree about
// what deleting a node does to the statistics. A divergence there does not
// surface as a failed delete -- it surfaces much later as an IsComplete that
// never becomes true, with nothing left in the graph to explain why.
//
// It does not touch edges: the caller detaches those first, because what should
// happen to a successor depends on why the node is going away. The scope comes
// from the engine rather than a parameter, so the two cannot disagree about
// whose counters are being adjusted.
func deleteNodeRow(ctx context.Context, tx pgx.Tx, eng *engine, n nodeRow) error {
	eng.leaveBucket(n.Phase, n.Status)
	eng.addTotal(-1)
	// Retire this generation's epoch before the identifier can be reused.
	if _, err := tx.Exec(ctx,
		`UPDATE dagw.scopes SET epoch_floor = GREATEST(epoch_floor, $2) WHERE scope = $1`,
		eng.scope, widenI64(n.Epoch)); err != nil {
		return fmt.Errorf("postgres: delete node: epoch floor: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM dagw.nodes WHERE id = $1`, n.ID); err != nil {
		return fmt.Errorf("postgres: delete node: %w", err)
	}
	return nil
}
