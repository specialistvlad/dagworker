package postgres

import (
	"context"
	"fmt"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// claimSQL is the whole hot path in one statement: a CTE over the partial
// ready-set index using SELECT ... FOR UPDATE SKIP LOCKED, chained into the
// UPDATE that grants the lease and RETURNING the claimed row — so that two
// processes racing for work can never observe, let alone lock, the same
// candidate (dossier 04 §1.2, §13, §14.1; T-CLAIM-ATOMIC is what proves it).
//
// phase/status are baked in as literal integers computed once here, not sent
// as bind parameters: the partial index's WHERE clause must match the
// query's WHERE clause verbatim for the planner to use it (dossier 04 §6),
// and a literal integer is textually identical on every call regardless,
// so plan caching is unaffected either way.
var claimSQL = fmt.Sprintf(`
WITH claimed AS (
	SELECT id
	FROM dagw.nodes
	WHERE scope = $1 AND phase = %d AND ($2::text[] IS NULL OR kind = ANY($2))
	ORDER BY priority DESC, fifo
	FOR UPDATE SKIP LOCKED
	LIMIT 1
)
UPDATE dagw.nodes n
SET phase = %d, status = %d, epoch = n.epoch + 1, attempt = n.epoch + 1,
    worker = $3, deadline = clock_timestamp() + make_interval(secs => $4), seq = n.seq + 1
FROM claimed c
WHERE n.id = c.id
RETURNING `+nodeColumnsAliased("n"),
	int16(dw.PhaseReady), int16(dw.PhaseClaimed), int16(dw.StatusInProgress))

// claimOne runs claimSQL and, on a grant, records the transition. It returns
// a nil row with no error when nothing was ready — an ordinary outcome, not
// a failure, exactly like the port's own doc comment on Claim requires.
func (e *engine) claimOne(ctx context.Context, kinds []string, leaseFor time.Duration, workerID string) (*nodeRow, dw.Effect, error) {
	var kindsArg []string
	if len(kinds) > 0 {
		kindsArg = kinds
	}
	row := e.tx.QueryRow(ctx, claimSQL, e.scope, kindsArg, workerID, leaseFor.Seconds())
	n, err := scanNode(row)
	if err != nil {
		if isNoRows(err) {
			return nil, dw.Effect{}, nil
		}
		return nil, dw.Effect{}, fmt.Errorf("postgres: claim: %w", err)
	}
	e.moveBucket(dw.PhaseReady, dw.StatusNew, n.Phase, n.Status)
	ef, err := e.insertEvent(ctx, n.NodeID, n.Kind, dw.EventTransition, dw.StatusNew, n.Status, n.Reason, n.Message, n.Attempt, n.Seq)
	if err != nil {
		return nil, dw.Effect{}, err
	}
	return &n, ef, nil
}

// claimLoop grants up to want leases, honouring MaxInFlight, stopping as
// soon as claimOne finds nothing left ready. Split out of Claim so the
// method itself reads as setup, work, teardown.
func (e *engine) claimLoop(ctx context.Context, cfg dw.ScopeConfig, scopeName string, req dw.ClaimRequest, want int, leaseFor time.Duration) (dw.ClaimResult, error) {
	var res dw.ClaimResult

	var inProgress int64
	if cfg.MaxInFlight > 0 {
		if err := e.tx.QueryRow(ctx, `SELECT stat_in_progress FROM dagw.scopes WHERE scope = $1`, scopeName).Scan(&inProgress); err != nil {
			return dw.ClaimResult{}, fmt.Errorf("postgres: Claim: in-flight count: %w", err)
		}
		// Counters are accumulated and written once at the end of the
		// transaction, so the stored value is missing anything this
		// transaction has already done -- an inline reclaim, for instance.
		inProgress += e.pendingStat("stat_in_progress")
	}

	for len(res.Leases) < want {
		if cfg.MaxInFlight > 0 && inProgress >= int64(cfg.MaxInFlight) {
			break
		}
		n, ef, err := e.claimOne(ctx, req.Kinds, leaseFor, req.WorkerID)
		if err != nil {
			return dw.ClaimResult{}, err
		}
		if n == nil {
			break
		}
		inProgress++
		res.Effects = append(res.Effects, ef)
		res.Leases = append(res.Leases, dw.Lease{
			Scope:    req.Scope,
			NodeID:   dw.NodeID(n.NodeID),
			Epoch:    n.Epoch,
			Deadline: derefTime(n.Deadline),
			Node:     n.snapshot(),
		})
	}
	return res, nil
}

// Claim implements [dagworker.Store].
func (s *Store) Claim(ctx context.Context, req dw.ClaimRequest) (dw.ClaimResult, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return dw.ClaimResult{}, err
	}
	scopeName := string(req.Scope)
	sc, ok, err := loadScope(ctx, s.pool, scopeName)
	if err != nil {
		return dw.ClaimResult{}, err
	}
	if !ok {
		// A scope nobody has written to has no work. Ordinary, not an error.
		return dw.ClaimResult{}, nil
	}
	cfg := sc.Cfg.Resolved()

	tx, err := beginTx(ctx, s)
	if err != nil {
		return dw.ClaimResult{}, fmt.Errorf("postgres: Claim: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	eng := newEngine(tx, scopeName, cfg, s.jitter)

	reclaimEffects, _, _, err := eng.reclaimExpired(ctx, cfg.SweepBatchSize)
	if err != nil {
		return dw.ClaimResult{}, err
	}
	promoted, err := eng.promoteScheduled(ctx)
	if err != nil {
		return dw.ClaimResult{}, err
	}

	res, err := eng.claimLoop(ctx, cfg, scopeName, req, max(req.Max, 1), cfg.ClampLease(req.Timeout))
	if err != nil {
		return dw.ClaimResult{}, err
	}
	res.Effects = append(append(reclaimEffects, promoted...), res.Effects...)

	if err := eng.finalize(ctx); err != nil {
		return dw.ClaimResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return dw.ClaimResult{}, fmt.Errorf("postgres: Claim: commit: %w", err)
	}
	return res, nil
}

// setRetryFields records the reason/message a failed attempt leaves behind
// and clears the worker identity, ahead of the phase move `schedule`
// performs. Split out because memory's failAttempt likewise mutates these
// fields before calling schedule, and the two together are what the single
// EventTransition this produces describes.
func (e *engine) setRetryFields(ctx context.Context, n *nodeRow, reason dw.Reason, message string) error {
	_, err := e.tx.Exec(ctx, `UPDATE dagw.nodes SET reason = $2, message = $3, worker = '' WHERE id = $1`,
		n.ID, int16(reason), message)
	if err != nil {
		return fmt.Errorf("postgres: setRetryFields: %w", err)
	}
	n.Reason, n.Message, n.Worker = reason, message, ""
	return nil
}

// failAttempt records that an attempt did not succeed and decides between
// another attempt and a terminal failure — the single path every way an
// attempt can fail (a worker's Nack, a reclaimed lease) shares, so the two
// can never diverge in how they count attempts, compute backoff, or fan out
// to successors. Direct port of memory's function of the same name.
func (e *engine) failAttempt(ctx context.Context, n *nodeRow, reason dw.Reason, message string) ([]dw.Effect, error) {
	maxAttempts, base, maxDelay := n.retryEffective(e.cfg)
	if n.Attempt >= maxAttempts {
		return e.terminate(ctx, n.ID, dw.StatusError, reason, message)
	}

	from := n.Status
	delay := dw.Backoff(n.Attempt, base, maxDelay, e.jitter)
	if err := e.setRetryFields(ctx, n, reason, message); err != nil {
		return nil, err
	}
	if err := e.schedule(ctx, n, delay.Seconds()); err != nil {
		return nil, err
	}
	seq, err := e.bumpSeq(ctx, n.ID)
	if err != nil {
		return nil, err
	}
	n.Seq = seq
	ef, err := e.insertEvent(ctx, n.NodeID, n.Kind, dw.EventTransition, from, n.Status, n.Reason, n.Message, n.Attempt, n.Seq)
	if err != nil {
		return nil, err
	}
	return []dw.Effect{ef}, nil
}

// reclaimExpired revokes every lease whose deadline has passed, up to limit,
// via the same fenced write path a worker's own Complete uses. SKIP LOCKED
// means a concurrent Sweep or inline Claim-side reclaim never blocks this
// one: whichever gets to a row first wins it, and the other simply moves on
// to the next — duplicate reclaiming is wasted work, never a wrong answer.
func (e *engine) reclaimExpired(ctx context.Context, limit int) ([]dw.Effect, int, bool, error) {
	if limit <= 0 {
		limit = 1
	}
	rows, err := e.tx.Query(ctx, `
SELECT id FROM dagw.nodes
WHERE scope = $1 AND phase = $2 AND deadline < clock_timestamp()
ORDER BY deadline
FOR UPDATE SKIP LOCKED
LIMIT $3`, e.scope, int16(dw.PhaseClaimed), limit)
	if err != nil {
		return nil, 0, false, fmt.Errorf("postgres: reclaimExpired: select: %w", err)
	}
	ids, err := int64s(rows)
	if err != nil {
		return nil, 0, false, fmt.Errorf("postgres: reclaimExpired: select: %w", err)
	}

	var effects []dw.Effect
	reclaimed := 0
	for _, id := range ids {
		n, err := e.loadForUpdate(ctx, id)
		if err != nil {
			if isNoRows(err) {
				continue
			}
			return nil, 0, false, fmt.Errorf("postgres: reclaimExpired: load: %w", err)
		}
		if n.Phase != dw.PhaseClaimed {
			continue
		}
		more, err := e.failAttempt(ctx, &n, dw.ReasonTimeout, "the worker did not acknowledge before the lease deadline")
		if err != nil {
			return nil, 0, false, err
		}
		effects = append(effects, more...)
		reclaimed++
	}

	var hasMore bool
	err = e.tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM dagw.nodes WHERE scope = $1 AND phase = $2 AND deadline < clock_timestamp())`,
		e.scope, int16(dw.PhaseClaimed)).Scan(&hasMore)
	if err != nil {
		return nil, 0, false, fmt.Errorf("postgres: reclaimExpired: more check: %w", err)
	}
	return effects, reclaimed, hasMore, nil
}

// promoteScheduled releases every node whose retry backoff has elapsed, so a
// retry becomes visible without depending on a timer having fired. Runs on
// both the claim and sweep paths, exactly like memory's function of the same
// name.
func (e *engine) promoteScheduled(ctx context.Context) ([]dw.Effect, error) {
	rows, err := e.tx.Query(ctx, `
SELECT id FROM dagw.nodes
WHERE scope = $1 AND phase = $2 AND ready_at <= clock_timestamp()
ORDER BY ready_at
FOR UPDATE SKIP LOCKED`, e.scope, int16(dw.PhaseScheduled))
	if err != nil {
		return nil, fmt.Errorf("postgres: promoteScheduled: select: %w", err)
	}
	ids, err := int64s(rows)
	if err != nil {
		return nil, fmt.Errorf("postgres: promoteScheduled: select: %w", err)
	}

	var effects []dw.Effect
	for _, id := range ids {
		n, err := e.loadForUpdate(ctx, id)
		if err != nil {
			if isNoRows(err) {
				continue
			}
			return nil, fmt.Errorf("postgres: promoteScheduled: load: %w", err)
		}
		if n.Phase != dw.PhaseScheduled {
			continue
		}
		ef, err := e.makeReady(ctx, &n)
		if err != nil {
			return nil, err
		}
		effects = append(effects, ef)
	}
	return effects, nil
}

// completeSuccess is Complete's Ack branch: store the result and drive the
// node's own terminate cascade.
func (e *engine) completeSuccess(ctx context.Context, n *nodeRow, result []byte) ([]dw.Effect, error) {
	if _, err := e.tx.Exec(ctx, `UPDATE dagw.nodes SET result = $2, worker = '' WHERE id = $1`, n.ID, result); err != nil {
		return nil, fmt.Errorf("postgres: Complete: store result: %w", err)
	}
	return e.terminate(ctx, n.ID, dw.StatusSuccess, dw.ReasonNone, "")
}

// completeSkip is Complete's ReasonSkipped branch: terminal on the first
// report, since a retry would just reach the same conclusion.
func (e *engine) completeSkip(ctx context.Context, n *nodeRow, message string) ([]dw.Effect, error) {
	if _, err := e.tx.Exec(ctx, `UPDATE dagw.nodes SET worker = '' WHERE id = $1`, n.ID); err != nil {
		return nil, fmt.Errorf("postgres: Complete: clear worker: %w", err)
	}
	return e.terminate(ctx, n.ID, dw.StatusError, dw.ReasonSkipped, message)
}

// loadLeasedNode locks the node a lease claims to hold and applies the
// fencing check: a worker that was merely paused rather than dead arrives
// here after its lease was reclaimed and reissued, so its epoch no longer
// matches and its write is refused instead of overwriting whatever the
// current holder has since recorded.
func loadLeasedNode(ctx context.Context, eng *engine, lease dw.Lease) (nodeRow, error) {
	n, err := eng.loadForUpdateByExternal(ctx, string(lease.NodeID))
	if err != nil {
		if isNoRows(err) {
			return nodeRow{}, dw.ErrNotFound
		}
		return nodeRow{}, fmt.Errorf("postgres: load leased node: %w", err)
	}
	if n.Phase != dw.PhaseClaimed || n.Epoch != lease.Epoch {
		return nodeRow{}, fmt.Errorf("%w: node %q is at epoch %d, lease presented %d",
			dw.ErrLeaseMismatch, lease.NodeID, n.Epoch, lease.Epoch)
	}
	return n, nil
}

// Complete implements [dagworker.Store].
func (s *Store) Complete(ctx context.Context, req dw.CompleteRequest) (dw.CompleteResult, error) {
	if !req.Lease.Valid() {
		return dw.CompleteResult{}, fmt.Errorf("%w: lease is not well formed", dw.ErrInvalidArgument)
	}
	if err := s.ensureMigrated(ctx); err != nil {
		return dw.CompleteResult{}, err
	}
	scopeName := string(req.Lease.Scope)
	sc, ok, err := loadScope(ctx, s.pool, scopeName)
	if err != nil {
		return dw.CompleteResult{}, err
	}
	if !ok {
		return dw.CompleteResult{}, dw.ErrNotFound
	}
	cfg := sc.Cfg.Resolved()

	tx, err := beginTx(ctx, s)
	if err != nil {
		return dw.CompleteResult{}, fmt.Errorf("postgres: Complete: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	eng := newEngine(tx, scopeName, cfg, s.jitter)

	n, err := loadLeasedNode(ctx, eng, req.Lease)
	if err != nil {
		return dw.CompleteResult{}, err
	}

	out, err := applyOutcome(ctx, eng, &n, req, cfg)
	if err != nil {
		return dw.CompleteResult{}, err
	}

	if err := eng.finalize(ctx); err != nil {
		return dw.CompleteResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return dw.CompleteResult{}, fmt.Errorf("postgres: Complete: commit: %w", err)
	}
	return out, nil
}

// applyOutcome records what the worker reported: success, a skip, or a failed
// attempt that the retry policy may or may not turn into another one.
//
// It is separate from Complete so that the fencing check and the transaction
// management stay legible next to each other, and so the three outcomes sit
// side by side where the difference between them is easy to read.
func applyOutcome(
	ctx context.Context, eng *engine, n *nodeRow, req dw.CompleteRequest, cfg dw.ScopeConfig,
) (dw.CompleteResult, error) {
	var out dw.CompleteResult
	var err error

	switch {
	case req.Success:
		if len(req.Result) > cfg.PayloadCap {
			return dw.CompleteResult{}, &dw.PayloadTooLargeError{Size: len(req.Result), Cap: cfg.PayloadCap}
		}
		out.Effects, err = eng.completeSuccess(ctx, n, req.Result)

	case req.Reason == dw.ReasonSkipped:
		// Skipping is a decision, not a fault: the worker looked and concluded
		// there was nothing to do. Retrying would reach the same conclusion, so
		// it is terminal on the first report -- the only way ReasonSkipped
		// enters a graph.
		out.Effects, err = eng.completeSkip(ctx, n, req.Message)

	default:
		reason := req.Reason
		if reason == dw.ReasonNone {
			reason = dw.ReasonWorkerError
		}
		out.Effects, err = eng.failAttempt(ctx, n, reason, req.Message)
		if err == nil && n.Phase == dw.PhaseScheduled {
			out.Retrying = true
			out.NextAttemptAt = derefTime(n.ReadyAt)
		}
	}

	if err != nil {
		return dw.CompleteResult{}, err
	}
	return out, nil
}

// Extend implements [dagworker.Store]. It is one UPDATE, fenced identically
// to Complete, and it deliberately never touches status, attempt, or seq: a
// heartbeat is not an event in the node's life.
func (s *Store) Extend(ctx context.Context, req dw.ExtendRequest) (time.Time, error) {
	if !req.Lease.Valid() {
		return time.Time{}, fmt.Errorf("%w: lease is not well formed", dw.ErrInvalidArgument)
	}
	if err := s.ensureMigrated(ctx); err != nil {
		return time.Time{}, err
	}
	scopeName := string(req.Lease.Scope)
	sc, ok, err := loadScope(ctx, s.pool, scopeName)
	if err != nil {
		return time.Time{}, err
	}
	if !ok {
		return time.Time{}, dw.ErrNotFound
	}
	cfg := sc.Cfg.Resolved()
	leaseFor := cfg.ClampLease(req.Timeout)

	tx, err := beginTx(ctx, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("postgres: Extend: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var deadline time.Time
	err = tx.QueryRow(ctx, `
UPDATE dagw.nodes
SET deadline = clock_timestamp() + make_interval(secs => $4)
WHERE scope = $1 AND node_id = $2 AND phase = $5 AND epoch = $3
RETURNING deadline`,
		scopeName, string(req.Lease.NodeID), widenI64(req.Lease.Epoch), leaseFor.Seconds(), int16(dw.PhaseClaimed),
	).Scan(&deadline)
	if err != nil {
		if !isNoRows(err) {
			return time.Time{}, fmt.Errorf("postgres: Extend: %w", err)
		}
		var exists bool
		if qerr := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM dagw.nodes WHERE scope = $1 AND node_id = $2)`,
			scopeName, string(req.Lease.NodeID)).Scan(&exists); qerr != nil {
			return time.Time{}, fmt.Errorf("postgres: Extend: existence check: %w", qerr)
		}
		if !exists {
			return time.Time{}, dw.ErrNotFound
		}
		return time.Time{}, fmt.Errorf("%w: node %q lease no longer matches", dw.ErrLeaseMismatch, req.Lease.NodeID)
	}
	if err := tx.Commit(ctx); err != nil {
		return time.Time{}, fmt.Errorf("postgres: Extend: commit: %w", err)
	}
	return deadline, nil
}

// Sweep implements [dagworker.Store].
func (s *Store) Sweep(ctx context.Context, scope dw.Scope, limit int) (dw.SweepResult, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return dw.SweepResult{}, err
	}
	scopeName := string(scope)
	sc, ok, err := loadScope(ctx, s.pool, scopeName)
	if err != nil {
		return dw.SweepResult{}, err
	}
	if !ok {
		return dw.SweepResult{}, nil
	}
	cfg := sc.Cfg.Resolved()
	if limit <= 0 {
		limit = cfg.SweepBatchSize
	}

	tx, err := beginTx(ctx, s)
	if err != nil {
		return dw.SweepResult{}, fmt.Errorf("postgres: Sweep: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	eng := newEngine(tx, scopeName, cfg, s.jitter)

	var res dw.SweepResult
	res.Effects, res.Reclaimed, res.More, err = eng.reclaimExpired(ctx, limit)
	if err != nil {
		return dw.SweepResult{}, err
	}
	promoted, err := eng.promoteScheduled(ctx)
	if err != nil {
		return dw.SweepResult{}, err
	}
	res.Effects = append(res.Effects, promoted...)

	if err := eng.finalize(ctx); err != nil {
		return dw.SweepResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return dw.SweepResult{}, fmt.Errorf("postgres: Sweep: commit: %w", err)
	}
	return res, nil
}
