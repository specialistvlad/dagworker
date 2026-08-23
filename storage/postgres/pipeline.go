package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	dw "github.com/specialistvlad/dagworker"
)

// Pipelining.
//
// The operations below do exactly what their one-at-a-time counterparts do,
// with exactly the same SQL and in exactly the same order. The only difference
// is that they hand the whole set of statements to pgx at once, so a batch of
// N costs one network round trip instead of N.
//
// That difference is the whole performance story for this backend. A round trip
// to a containerised PostgreSQL costs around 185 microseconds; inserting a node
// took six of them — an existence check, the insert, a sequence bump, an event
// insert, a re-read and a settle — so a million nodes took twenty-one minutes,
// essentially all of it waiting on the network. Nothing was slow; there was
// just far too much talking.
//
// Ordering is preserved: pgx sends the queued statements in order and the
// server executes them in order inside the same transaction, so the lock
// acquisition order the deadlock-avoidance argument depends on is unchanged.
// Every batch is bounded by the scope's MaxBatchSize, so none of them can grow
// without limit.

// loadManyForUpdateByExternal locks and returns the nodes named by ids that
// already exist, keyed by external id. Absent ids are simply missing from the
// map, which is what lets the caller tell "update this" from "create this" in
// one round trip rather than one per node.
//
// It uses ANY rather than a batch because a single statement is strictly better
// when the shape allows it, and here it does.
func (e *engine) loadManyForUpdateByExternal(ctx context.Context, ids []string) (map[string]nodeRow, error) {
	out := make(map[string]nodeRow, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := e.tx.Query(ctx,
		`SELECT `+nodeColumns+` FROM dagw.nodes WHERE scope = $1 AND node_id = ANY($2) ORDER BY id FOR UPDATE`,
		e.scope, ids)
	if err != nil {
		return nil, fmt.Errorf("postgres: load nodes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: load nodes: %w", err)
		}
		out[n.NodeID] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: load nodes: %w", err)
	}
	return out, nil
}

// createNodes inserts every spec in one pipelined batch and returns the rows in
// the same order.
func (e *engine) createNodes(ctx context.Context, specs []dw.NodeSpec) ([]nodeRow, error) {
	if len(specs) == 0 {
		return nil, nil
	}

	batch := &pgx.Batch{}
	for i := range specs {
		labels, err := labelsJSON(specs[i].Labels)
		if err != nil {
			return nil, fmt.Errorf("postgres: create node %q: labels: %w", specs[i].ID, err)
		}
		var payload []byte
		if len(specs[i].Payload) > 0 {
			payload = specs[i].Payload
		}
		batch.Queue(insertNodeSQL,
			e.scope, string(specs[i].ID), specs[i].Kind,
			int16(dw.StatusNew), int16(dw.ReasonNone), int16(dw.PhaseBlocked),
			specs[i].Priority, int16(specs[i].Trigger),
			narrowI32(specs[i].Retry.MaxAttempts), int64(specs[i].Retry.BaseDelay), int64(specs[i].Retry.MaxDelay),
			payload, labels,
		)
	}

	br := e.tx.SendBatch(ctx, batch)
	out := make([]nodeRow, 0, len(specs))
	var firstErr error
	for range specs {
		n, err := scanNode(br.QueryRow())
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("postgres: create node: %w", err)
		}
		if err == nil {
			out = append(out, n)
		}
	}
	// Close must run even on error: the batch's remaining results have to be
	// drained before the connection can be used for anything else.
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("postgres: create nodes: %w", err)
	}
	if firstErr != nil {
		return nil, firstErr
	}

	for i := range out {
		e.enterBucket(out[i].Phase, out[i].Status)
		e.addTotal(1)
	}
	return out, nil
}

// bumpSeqMany advances several nodes' sequences in one round trip, returning
// the new values in the order the ids were given.
func (e *engine) bumpSeqMany(ctx context.Context, ids []int64) ([]dw.Seq, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	batch := &pgx.Batch{}
	for _, id := range ids {
		batch.Queue(`UPDATE dagw.nodes SET seq = seq + 1 WHERE id = $1 RETURNING seq`, id)
	}

	br := e.tx.SendBatch(ctx, batch)
	out := make([]dw.Seq, len(ids))
	var firstErr error
	for i := range ids {
		var seq int64
		if err := br.QueryRow().Scan(&seq); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("postgres: bump seq: %w", err)
		} else if err == nil {
			out[i] = dw.Seq(narrowU64(seq))
		}
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("postgres: bump seq: %w", err)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

// pendingEvent is one row insertEvents is about to write.
type pendingEvent struct {
	nodeID   string
	nodeKind string
	kind     dw.EventKind
	from, to dw.Status
	reason   dw.Reason
	message  string
	attempt  uint32
	seq      dw.Seq
}

// insertEvents appends several log rows in one round trip and returns the
// effects in order.
func (e *engine) insertEvents(ctx context.Context, evs []pendingEvent) ([]dw.Effect, error) {
	if len(evs) == 0 {
		return nil, nil
	}
	batch := &pgx.Batch{}
	for _, ev := range evs {
		batch.Queue(insertEventSQL,
			e.scope, ev.nodeID, int16(ev.kind), widenI64(ev.seq),
			int16(ev.from), int16(ev.to), int16(ev.reason), ev.message,
			narrowI32(ev.attempt), ev.nodeKind)
	}

	br := e.tx.SendBatch(ctx, batch)
	out := make([]dw.Effect, 0, len(evs))
	var firstErr error
	for _, ev := range evs {
		eff, err := scanEventEffect(br.QueryRow(), e.scope, ev)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("postgres: insert event: %w", err)
		}
		if err == nil {
			out = append(out, eff)
		}
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("postgres: insert events: %w", err)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	e.notify = true
	return out, nil
}

// loadManyForUpdate locks and returns the nodes named by ids, in the order
// given. Ordering matters: the caller chose it to keep lock acquisition
// consistent, and a batch preserves it.
func (e *engine) loadManyForUpdate(ctx context.Context, ids []int64) ([]nodeRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	batch := &pgx.Batch{}
	for _, id := range ids {
		batch.Queue(`SELECT `+nodeColumns+` FROM dagw.nodes WHERE id = $1 FOR UPDATE`, id)
	}

	br := e.tx.SendBatch(ctx, batch)
	out := make([]nodeRow, 0, len(ids))
	var firstErr error
	for range ids {
		n, err := scanNode(br.QueryRow())
		switch {
		case err == nil:
			out = append(out, n)
		case isNoRows(err):
			// Deleted between being named and being read. The caller settles
			// what it got; a node that no longer exists needs no settling.
		case firstErr == nil:
			firstErr = fmt.Errorf("postgres: load: %w", err)
		}
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("postgres: load: %w", err)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}
