package postgres

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/jackc/pgx/v5"

	dw "github.com/specialistvlad/dagworker"
)

// engine carries one mutating call's transaction and scope, and implements
// the trigger-rule fan-out every mutation that touches the graph's shape
// needs: settle (re-evaluate one node against its current dependency tally)
// and terminate (drive a node to a terminal status and cascade the
// consequence through its successors). Both are direct ports of
// storage/memory/scope.go's functions of the same name — same queue-not-
// recursion shape, same idempotency guards — with slice/array access
// replaced by SQL reads and writes against locked rows.
//
// It is deliberately request-scoped: constructed at the top of one Store
// method, discarded at the bottom, never retained across a call boundary. It
// does not itself hold a context.Context — every method takes one as its
// first parameter, same as the Store methods that construct an engine — so
// that cancellation always flows from the one call in progress rather than
// from whatever context happened to be current when the engine was built.
type engine struct {
	tx     pgx.Tx
	scope  string
	cfg    dw.ScopeConfig // resolved
	jitter func(n int64) int64

	// notify latches true the first time this call inserts an event, so the
	// caller issues exactly one pg_notify per transaction rather than one per
	// effect.
	notify bool
}

func newEngine(tx pgx.Tx, scope string, cfg dw.ScopeConfig, jitter func(int64) int64) *engine {
	return &engine{tx: tx, scope: scope, cfg: cfg, jitter: jitter}
}

// terminalMessage is the human-readable message a node carries when its
// trigger rule became unsatisfiable — verbatim from memory's function of the
// same name, since a subscriber reading either backend's Message field
// should not be able to tell them apart. Every Reason is named explicitly,
// not funnelled through a bare default, so a fifth reason added later fails
// this switch's exhaustiveness check instead of silently returning "".
func terminalMessage(r dw.Reason) string {
	switch r {
	case dw.ReasonUpstreamFailed:
		return "a predecessor failed and the trigger rule can no longer be satisfied"
	case dw.ReasonSkipped:
		return "the trigger rule can no longer be satisfied"
	case dw.ReasonRemoved:
		return "a predecessor was removed"
	case dw.ReasonCancelled:
		return "cancelled"
	case dw.ReasonNone, dw.ReasonWorkerError, dw.ReasonTimeout:
		return ""
	default:
		return ""
	}
}

// bucketColumn names the dagw.scopes counter a (phase, status) pair belongs
// to — the same switch memory's bucket method makes over phase, with the
// PhaseDone tie broken by status exactly as memory breaks it. PhaseDone is
// named explicitly (falling through to the same status check) rather than
// left to a bare default, for the same exhaustiveness reason terminalMessage
// names every Reason.
func bucketColumn(phase dw.Phase, status dw.Status) string {
	switch phase {
	case dw.PhaseBlocked:
		return "stat_blocked"
	case dw.PhaseScheduled:
		return "stat_scheduled"
	case dw.PhaseReady:
		return "stat_ready"
	case dw.PhaseClaimed:
		return "stat_in_progress"
	case dw.PhaseDone:
		fallthrough
	default:
		if status == dw.StatusSuccess {
			return "stat_succeeded"
		}
		return "stat_failed"
	}
}

// moveBucket adjusts the scope's incremental counters for one node's phase
// transition. A same-column move (e.g. PhaseBlocked -> PhaseBlocked, which
// never happens, or two PhaseDone/error transitions in a row, which cannot
// happen either since PhaseDone is terminal) is skipped rather than issuing a
// net-zero write.
func (e *engine) moveBucket(ctx context.Context, oldPhase dw.Phase, oldStatus dw.Status, newPhase dw.Phase, newStatus dw.Status) error {
	oldCol, newCol := bucketColumn(oldPhase, oldStatus), bucketColumn(newPhase, newStatus)
	if oldCol == newCol {
		return nil
	}
	sql := fmt.Sprintf(`UPDATE dagw.scopes SET %s = %s - 1, %s = %s + 1 WHERE scope = $1`, oldCol, oldCol, newCol, newCol)
	if _, err := e.tx.Exec(ctx, sql, e.scope); err != nil {
		return fmt.Errorf("postgres: move bucket: %w", err)
	}
	return nil
}

// enterBucket accounts for a node created directly into phase/status, with
// no prior bucket to leave.
func (e *engine) enterBucket(ctx context.Context, phase dw.Phase, status dw.Status) error {
	col := bucketColumn(phase, status)
	sql := fmt.Sprintf(`UPDATE dagw.scopes SET %s = %s + 1 WHERE scope = $1`, col, col)
	if _, err := e.tx.Exec(ctx, sql, e.scope); err != nil {
		return fmt.Errorf("postgres: enter bucket: %w", err)
	}
	return nil
}

// leaveBucket accounts for a node removed from the graph entirely (RemoveNode,
// CollectTerminal), with no destination bucket to enter.
func (e *engine) leaveBucket(ctx context.Context, phase dw.Phase, status dw.Status) error {
	col := bucketColumn(phase, status)
	sql := fmt.Sprintf(`UPDATE dagw.scopes SET %s = %s - 1 WHERE scope = $1`, col, col)
	if _, err := e.tx.Exec(ctx, sql, e.scope); err != nil {
		return fmt.Errorf("postgres: leave bucket: %w", err)
	}
	return nil
}

// insertEvent appends one durable log row and returns the [dagworker.Effect]
// the caller reports back to the Manager. Setting e.notify marks this
// transaction as having something worth a pg_notify wakeup once it commits.
func (e *engine) insertEvent(ctx context.Context, nodeID, nodeKind string, kind dw.EventKind, from, to dw.Status, reason dw.Reason, message string, attempt uint32, seq dw.Seq) (dw.Effect, error) {
	var cursor int64
	var at time.Time
	err := e.tx.QueryRow(ctx, `
INSERT INTO dagw.events (scope, node_id, kind, seq, from_status, to_status, reason, message, attempt, node_kind, at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, clock_timestamp())
RETURNING cursor, at`,
		e.scope, nodeID, int16(kind), int64(seq), int16(from), int16(to), int16(reason), message, int32(attempt), nodeKind,
	).Scan(&cursor, &at)
	if err != nil {
		return dw.Effect{}, fmt.Errorf("postgres: insert event: %w", err)
	}
	e.notify = true
	return dw.Effect{
		NodeID:   dw.NodeID(nodeID),
		Kind:     kind,
		From:     from,
		To:       to,
		Reason:   reason,
		Message:  message,
		Attempt:  attempt,
		NodeKind: nodeKind,
		Seq:      seq,
		Cursor:   dw.Cursor(cursor),
		At:       at,
	}, nil
}

// loadForUpdate locks and returns one node by its internal id.
func (e *engine) loadForUpdate(ctx context.Context, id int64) (nodeRow, error) {
	row := e.tx.QueryRow(ctx, `SELECT `+nodeColumns+` FROM dagw.nodes WHERE id = $1 FOR UPDATE`, id)
	return scanNode(row)
}

// successorsForUpdate locks and returns every direct successor of fromID, in
// ascending id order — the query terminate uses for its fan-out.
func (e *engine) successorsForUpdate(ctx context.Context, fromID int64) ([]nodeRow, error) {
	rows, err := e.tx.Query(ctx, `
SELECT `+nodeColumnsAliased("c")+`
FROM dagw.edges edg JOIN dagw.nodes c ON c.scope = edg.scope AND c.id = edg.to_id
WHERE edg.scope = $1 AND edg.from_id = $2
ORDER BY c.id
FOR UPDATE OF c`, e.scope, fromID)
	if err != nil {
		return nil, fmt.Errorf("postgres: load successors: %w", err)
	}
	defer rows.Close()
	var out []nodeRow
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: load successors: scan: %w", err)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// markEdgeSatisfied flips one edge's satisfied flag and reports whether it
// actually changed anything, so a repeated fan-out over the same edge costs a
// no-op UPDATE rather than double-counting a dependency — the same
// idempotency markSatisfied's per-edge guard gives memory.
func (e *engine) markEdgeSatisfied(ctx context.Context, fromID, toID int64) (bool, error) {
	tag, err := e.tx.Exec(ctx,
		`UPDATE dagw.edges SET satisfied = true WHERE scope = $1 AND from_id = $2 AND to_id = $3 AND satisfied = false`,
		e.scope, fromID, toID)
	if err != nil {
		return false, fmt.Errorf("postgres: mark edge satisfied: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// applyDepDelta records that one predecessor resolved against a successor's
// tally and returns the updated counts.
func (e *engine) applyDepDelta(ctx context.Context, succID int64, predStatus dw.Status, predReason dw.Reason) (dw.DepCounts, error) {
	col := "deps_failed"
	switch {
	case predStatus == dw.StatusSuccess:
		col = "deps_succeeded"
	case predReason == dw.ReasonSkipped:
		col = "deps_skipped"
	}
	sql := fmt.Sprintf(`
UPDATE dagw.nodes SET deps_unsatisfied = GREATEST(deps_unsatisfied - 1, 0), %s = %s + 1
WHERE id = $1
RETURNING deps_unsatisfied, deps_succeeded, deps_skipped, deps_failed`, col, col)
	var u, s, sk, f int32
	if err := e.tx.QueryRow(ctx, sql, succID).Scan(&u, &s, &sk, &f); err != nil {
		return dw.DepCounts{}, fmt.Errorf("postgres: apply dep delta: %w", err)
	}
	return dw.DepCounts{Unsatisfied: uint32(u), Succeeded: uint32(s), Skipped: uint32(sk), Failed: uint32(f)}, nil
}

// makeReady moves a node into the ready set: the only path by which a node
// becomes claimable, mirroring memory's function of the same name field for
// field (status resets to New, fifo is freshly assigned, ready_at clears).
func (e *engine) makeReady(ctx context.Context, n *nodeRow) (dw.Effect, error) {
	oldPhase, oldStatus := n.Phase, n.Status
	var seq, fifo int64
	err := e.tx.QueryRow(ctx, `
UPDATE dagw.nodes
SET phase = $2, status = $3, ready_at = NULL, fifo = nextval('dagw.fifo_seq'), seq = seq + 1
WHERE id = $1
RETURNING seq, fifo`, n.ID, int16(dw.PhaseReady), int16(dw.StatusNew)).Scan(&seq, &fifo)
	if err != nil {
		return dw.Effect{}, fmt.Errorf("postgres: makeReady: %w", err)
	}
	n.Phase, n.Status, n.ReadyAt, n.Fifo, n.Seq = dw.PhaseReady, dw.StatusNew, nil, fifo, dw.Seq(seq)
	if err := e.moveBucket(ctx, oldPhase, oldStatus, n.Phase, n.Status); err != nil {
		return dw.Effect{}, err
	}
	return e.insertEvent(ctx, n.NodeID, n.Kind, dw.EventReady, dw.StatusNew, dw.StatusNew, dw.ReasonNone, "", n.Attempt, n.Seq)
}

// makeBlocked pulls a node out of the ready set because a dependency was
// added. No event is emitted and no sequence bump happens — becoming blocked
// is scheduling detail, not a public transition (memory's makeBlocked does
// neither either).
func (e *engine) makeBlocked(ctx context.Context, n *nodeRow) error {
	oldPhase, oldStatus := n.Phase, n.Status
	_, err := e.tx.Exec(ctx, `UPDATE dagw.nodes SET phase = $2, status = $3 WHERE id = $1`,
		n.ID, int16(dw.PhaseBlocked), int16(dw.StatusNew))
	if err != nil {
		return fmt.Errorf("postgres: makeBlocked: %w", err)
	}
	n.Phase, n.Status = dw.PhaseBlocked, dw.StatusNew
	return e.moveBucket(ctx, oldPhase, oldStatus, n.Phase, n.Status)
}

// schedule parks a node until a retry backoff elapses, computed by
// PostgreSQL's own clock (ADR-0008): delaySeconds is a duration, never an
// absolute deadline, and the addition happens inside the UPDATE.
func (e *engine) schedule(ctx context.Context, n *nodeRow, delaySeconds float64) error {
	oldPhase, oldStatus := n.Phase, n.Status
	var readyAt time.Time
	err := e.tx.QueryRow(ctx, `
UPDATE dagw.nodes
SET phase = $2, status = $3, ready_at = clock_timestamp() + make_interval(secs => $4)
WHERE id = $1
RETURNING ready_at`, n.ID, int16(dw.PhaseScheduled), int16(dw.StatusNew), delaySeconds).Scan(&readyAt)
	if err != nil {
		return fmt.Errorf("postgres: schedule: %w", err)
	}
	n.Phase, n.Status = dw.PhaseScheduled, dw.StatusNew
	n.ReadyAt = &readyAt
	return e.moveBucket(ctx, oldPhase, oldStatus, n.Phase, n.Status)
}

// settle re-evaluates one node against its current dependency tally after an
// edge was added or removed, and moves it to whichever state its trigger
// rule now implies. It is a direct port of memory's settle, including its
// idempotency: a node that is not currently Blocked or Ready is left alone.
func (e *engine) settle(ctx context.Context, n *nodeRow) ([]dw.Effect, error) {
	if n.Phase != dw.PhaseBlocked && n.Phase != dw.PhaseReady {
		return nil, nil
	}
	switch {
	case n.Deps.Unsatisfiable(n.Trigger):
		reason := n.Deps.TerminalReason()
		return e.terminate(ctx, n.ID, dw.StatusError, reason, terminalMessage(reason))
	case n.Deps.Ready(n.Trigger):
		if n.Phase != dw.PhaseReady {
			ef, err := e.makeReady(ctx, n)
			if err != nil {
				return nil, err
			}
			return []dw.Effect{ef}, nil
		}
	default:
		if n.Phase != dw.PhaseBlocked {
			if err := e.makeBlocked(ctx, n); err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}

// terminate drives root to a terminal status and propagates the consequence
// through the graph: a breadth-first walk (a queue, not recursion, for the
// same reason memory's terminate uses one — an implementation detail must
// not impose a maximum DAG depth), locking and updating each successor set in
// ascending id order before touching any of them.
func (e *engine) terminate(ctx context.Context, root int64, status dw.Status, reason dw.Reason, message string) ([]dw.Effect, error) {
	queue := []terminateItem{{root, status, reason, message}}

	var effects []dw.Effect
	for len(queue) > 0 {
		it := queue[0]
		queue = queue[1:]

		n, err := e.loadForUpdate(ctx, it.id)
		if err != nil {
			if isNoRows(err) {
				continue // removed concurrently within this same transaction's own earlier step
			}
			return nil, fmt.Errorf("postgres: terminate: load: %w", err)
		}
		if n.Phase == dw.PhaseDone {
			continue
		}

		oldPhase, oldStatus := n.Phase, n.Status
		var seq int64
		err = e.tx.QueryRow(ctx, `
UPDATE dagw.nodes
SET phase = $2, status = $3, reason = $4, message = $5,
    deadline = NULL, ready_at = NULL, worker = '', seq = seq + 1
WHERE id = $1
RETURNING seq`, n.ID, int16(dw.PhaseDone), int16(it.status), int16(it.reason), it.message).Scan(&seq)
		if err != nil {
			return nil, fmt.Errorf("postgres: terminate: update: %w", err)
		}
		n.Phase, n.Status, n.Reason, n.Message, n.Seq = dw.PhaseDone, it.status, it.reason, it.message, dw.Seq(seq)

		if err := e.moveBucket(ctx, oldPhase, oldStatus, n.Phase, n.Status); err != nil {
			return nil, err
		}
		ef, err := e.insertEvent(ctx, n.NodeID, n.Kind, dw.EventTransition, oldStatus, n.Status, n.Reason, n.Message, n.Attempt, n.Seq)
		if err != nil {
			return nil, err
		}
		effects = append(effects, ef)

		more, err := e.cascadeSuccessors(ctx, &queue, n.ID, n.Status, n.Reason)
		if err != nil {
			return nil, err
		}
		effects = append(effects, more...)
	}
	return effects, nil
}

// terminateItem is one pending termination in terminate's breadth-first
// queue: a node id plus the outcome it should be terminated with.
type terminateItem struct {
	id      int64
	status  dw.Status
	reason  dw.Reason
	message string
}

// cascadeSuccessors is terminate's fan-out step, split out on its own so
// terminate's own loop body stays a single, readable unit of work: for a
// node that just resolved, lock every direct successor in ascending id
// order, mark the edge satisfied, apply the dependency delta, and either
// enqueue the successor for termination (its rule just became unsatisfiable)
// or promote it straight to ready (its rule just became satisfied).
func (e *engine) cascadeSuccessors(ctx context.Context, queue *[]terminateItem, fromID int64, fromStatus dw.Status, fromReason dw.Reason) ([]dw.Effect, error) {
	succs, err := e.successorsForUpdate(ctx, fromID)
	if err != nil {
		return nil, err
	}
	var effects []dw.Effect
	for i := range succs {
		succ := &succs[i]
		flipped, err := e.markEdgeSatisfied(ctx, fromID, succ.ID)
		if err != nil {
			return nil, err
		}
		if !flipped {
			continue
		}
		deps, err := e.applyDepDelta(ctx, succ.ID, fromStatus, fromReason)
		if err != nil {
			return nil, err
		}
		succ.Deps = deps
		if succ.Phase != dw.PhaseBlocked && succ.Phase != dw.PhaseReady {
			continue
		}
		switch {
		case succ.Deps.Unsatisfiable(succ.Trigger):
			rs := succ.Deps.TerminalReason()
			*queue = append(*queue, terminateItem{succ.ID, dw.StatusError, rs, terminalMessage(rs)})
		case succ.Deps.Ready(succ.Trigger):
			if succ.Phase != dw.PhaseReady {
				ef, err := e.makeReady(ctx, succ)
				if err != nil {
					return nil, err
				}
				effects = append(effects, ef)
			}
		}
	}
	return effects, nil
}

// loadForUpdateByExternal locks and returns one node by its caller-facing
// (scope, node_id) identity.
func (e *engine) loadForUpdateByExternal(ctx context.Context, nodeID string) (nodeRow, error) {
	row := e.tx.QueryRow(ctx,
		`SELECT `+nodeColumns+` FROM dagw.nodes WHERE scope = $1 AND node_id = $2 FOR UPDATE`,
		e.scope, nodeID)
	return scanNode(row)
}

// bumpSeq advances one node's per-node sequence with no other change — used
// for [dagworker.EventCreated], which has no accompanying status write to
// piggyback the bump onto.
func (e *engine) bumpSeq(ctx context.Context, id int64) (dw.Seq, error) {
	var seq int64
	err := e.tx.QueryRow(ctx, `UPDATE dagw.nodes SET seq = seq + 1 WHERE id = $1 RETURNING seq`, id).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("postgres: bump seq: %w", err)
	}
	return dw.Seq(seq), nil
}

// createNode inserts a brand-new node in PhaseBlocked/StatusNew — every node
// is born there, dependency-free or not, and pass three's settle call is
// what promotes it to Ready (or straight to a terminal status) once its
// declared dependencies are linked. Mirrors memory's create().
func (e *engine) createNode(ctx context.Context, spec dw.NodeSpec) (nodeRow, error) {
	labels, err := labelsJSON(spec.Labels)
	if err != nil {
		return nodeRow{}, fmt.Errorf("postgres: create node: labels: %w", err)
	}
	var payload []byte
	if len(spec.Payload) > 0 {
		payload = spec.Payload
	}
	row := e.tx.QueryRow(ctx, `
INSERT INTO dagw.nodes (
	scope, node_id, kind, status, reason, phase, priority, trigger_rule,
	retry_max_attempts, retry_base_delay_ns, retry_max_delay_ns, payload, labels,
	created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, clock_timestamp(), clock_timestamp())
RETURNING `+nodeColumns,
		e.scope, string(spec.ID), spec.Kind, int16(dw.StatusNew), int16(dw.ReasonNone), int16(dw.PhaseBlocked),
		spec.Priority, int16(spec.Trigger),
		int32(spec.Retry.MaxAttempts), int64(spec.Retry.BaseDelay), int64(spec.Retry.MaxDelay),
		payload, labels,
	)
	n, err := scanNode(row)
	if err != nil {
		return nodeRow{}, fmt.Errorf("postgres: create node: %w", err)
	}
	if err := e.enterBucket(ctx, n.Phase, n.Status); err != nil {
		return nodeRow{}, err
	}
	if _, err := e.tx.Exec(ctx, `UPDATE dagw.scopes SET stat_total = stat_total + 1 WHERE scope = $1`, e.scope); err != nil {
		return nodeRow{}, fmt.Errorf("postgres: create node: stat_total: %w", err)
	}
	return n, nil
}

// specMatches reports whether an existing node was created from a
// byte-identical definition, which is what makes AddNodes idempotent.
// Runtime state (status, attempt, sequence) is deliberately not compared:
// re-adding a node that has since run is still the same node.
func specMatches(n nodeRow, spec dw.NodeSpec) bool {
	return n.Kind == spec.Kind &&
		n.Priority == spec.Priority &&
		n.Trigger == spec.Trigger &&
		n.RetryMaxAttempts == spec.Retry.MaxAttempts &&
		n.RetryBaseDelay == spec.Retry.BaseDelay &&
		n.RetryMaxDelay == spec.Retry.MaxDelay &&
		bytes.Equal(n.Payload, spec.Payload) &&
		maps.Equal(n.Labels, spec.Labels)
}

// linkDependency records that to depends on from: the edge-insertion half
// every AddNodes dependency and every AddEdges entry shares. It reports a
// *dagworker.CycleError or dagworker.ErrAlreadyTerminal in place, and leaves
// both nodeRow arguments' cached Deps up to date on success so a caller
// chaining several links against the same touched node sees a consistent view.
func (e *engine) linkDependency(ctx context.Context, from, to *nodeRow) error {
	var exists bool
	err := e.tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM dagw.edges WHERE scope = $1 AND from_id = $2 AND to_id = $3)`,
		e.scope, from.ID, to.ID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("postgres: linkDependency: check existing: %w", err)
	}
	if exists {
		return nil
	}
	if to.Phase == dw.PhaseDone {
		return fmt.Errorf("%w: %q", dw.ErrAlreadyTerminal, to.NodeID)
	}

	res, err := e.addEdgeOrder(ctx, from.ID, to.ID)
	if err != nil {
		return fmt.Errorf("postgres: linkDependency: topo: %w", err)
	}
	if res.cyclePath != nil {
		names, err := externalIDs(ctx, e.tx, res.cyclePath)
		if err != nil {
			return fmt.Errorf("postgres: linkDependency: resolve cycle path: %w", err)
		}
		path := make([]dw.NodeID, len(res.cyclePath))
		for i, id := range res.cyclePath {
			path[i] = dw.NodeID(names[id])
		}
		return &dw.CycleError{Scope: dw.Scope(e.scope), From: dw.NodeID(from.NodeID), To: dw.NodeID(to.NodeID), Path: path}
	}

	satisfied := from.Phase == dw.PhaseDone
	if _, err := e.tx.Exec(ctx,
		`INSERT INTO dagw.edges (scope, from_id, to_id, satisfied) VALUES ($1, $2, $3, $4)`,
		e.scope, from.ID, to.ID, satisfied,
	); err != nil {
		return fmt.Errorf("postgres: linkDependency: insert edge: %w", err)
	}

	switch {
	case !satisfied:
		to.Deps.Unsatisfied++
	case from.Status == dw.StatusSuccess:
		to.Deps.Succeeded++
	case from.Reason == dw.ReasonSkipped:
		to.Deps.Skipped++
	default:
		to.Deps.Failed++
	}
	if _, err := e.tx.Exec(ctx,
		`UPDATE dagw.nodes SET deps_unsatisfied = $2, deps_succeeded = $3, deps_skipped = $4, deps_failed = $5 WHERE id = $1`,
		to.ID, to.Deps.Unsatisfied, to.Deps.Succeeded, to.Deps.Skipped, to.Deps.Failed,
	); err != nil {
		return fmt.Errorf("postgres: linkDependency: persist deps: %w", err)
	}
	return nil
}

// unlinkDependency drops the edge from -> to if it exists, reverting to's
// dependency tally, and reports whether anything changed.
func (e *engine) unlinkDependency(ctx context.Context, fromID, toID int64) (bool, error) {
	var satisfied bool
	err := e.tx.QueryRow(ctx,
		`DELETE FROM dagw.edges WHERE scope = $1 AND from_id = $2 AND to_id = $3 RETURNING satisfied`,
		e.scope, fromID, toID).Scan(&satisfied)
	if err != nil {
		if isNoRows(err) {
			return false, nil
		}
		return false, fmt.Errorf("postgres: unlinkDependency: %w", err)
	}

	from, err := e.loadForUpdate(ctx, fromID)
	if err != nil {
		return false, fmt.Errorf("postgres: unlinkDependency: load predecessor: %w", err)
	}
	col := "deps_failed"
	switch {
	case !satisfied:
		col = "deps_unsatisfied"
	case from.Status == dw.StatusSuccess:
		col = "deps_succeeded"
	case from.Reason == dw.ReasonSkipped:
		col = "deps_skipped"
	}
	sql := fmt.Sprintf(`UPDATE dagw.nodes SET %s = GREATEST(%s - 1, 0) WHERE id = $1`, col, col)
	if _, err := e.tx.Exec(ctx, sql, toID); err != nil {
		return false, fmt.Errorf("postgres: unlinkDependency: decrement: %w", err)
	}
	return true, nil
}

// notifyIfDirty issues one pg_notify carrying only the scope name, the
// latency hint layered on top of the durable events table (dossier 04 §3,
// §4): it is a no-op call whenever this engine inserted no event, so an
// operation that touched nothing never wakes a waiter for nothing.
func (e *engine) notifyIfDirty(ctx context.Context) error {
	if !e.notify {
		return nil
	}
	if _, err := e.tx.Exec(ctx, `SELECT pg_notify($1, $2)`, notifyChannel, e.scope); err != nil {
		return fmt.Errorf("postgres: notify: %w", err)
	}
	return nil
}
