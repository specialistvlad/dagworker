package postgres

import (
	"context"
	"fmt"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// eventBatchSize bounds how many rows one poll of dagw.events fetches at a
// time, so a subscriber resuming after a long gap drains it in bounded
// chunks rather than one unbounded query.
const eventBatchSize = 256

// Watch implements [dagworker.DurableEventStream]. dagw.events is the
// durable source of truth; LISTEN/NOTIFY (via s.notifier) is layered on top
// purely to skip the poll wait when a wakeup arrives — a notifier that never
// connects only costs latency, bounded by s.pollInterval, never correctness
// (dossier 04 §3, §4).
//
// This backend never evicts old events (unlike memory's bounded ring), so a
// resume cursor is never rejected as expired: retention is deliberately
// unbounded, which is the honest posture for the durable-storage tier per
// the port's durability disclosure. An operator that wants retention manages
// it outside this package.
func (s *Store) Watch(ctx context.Context, req dw.WatchRequest) (<-chan dw.Event, error) {
	if req.Scope == "" {
		return nil, dw.ErrUnsupported
	}
	if err := s.ensureMigrated(ctx); err != nil {
		return nil, err
	}
	scopeName := string(req.Scope)
	if err := ensureScope(ctx, s.pool, scopeName, s.defaults); err != nil {
		return nil, err
	}

	var next int64
	switch {
	case req.From > 0:
		next = widenI64(req.From) + 1
	case req.Replay:
		next = 0
	default:
		var cur int64
		if err := s.pool.QueryRow(ctx,
			`SELECT COALESCE(MAX(cursor), 0) FROM dagw.events WHERE scope = $1`, scopeName,
		).Scan(&cur); err != nil {
			return nil, fmt.Errorf("postgres: Watch: current cursor: %w", err)
		}
		next = cur + 1
	}

	out := make(chan dw.Event)
	go s.pump(ctx, scopeName, next, out)
	return out, nil
}

func (s *Store) pump(ctx context.Context, scope string, next int64, out chan<- dw.Event) {
	defer close(out)
	for {
		batch, err := s.readEvents(ctx, scope, next, eventBatchSize)
		if err != nil {
			// A transient read error (connection blip) is not a reason to
			// silently end a subscriber's stream; back off and retry until
			// the context ends or the store closes.
			select {
			case <-time.After(s.pollInterval):
				continue
			case <-ctx.Done():
				return
			case <-s.closed:
				return
			}
		}

		for _, ev := range batch {
			select {
			case out <- ev:
				next = widenI64(ev.Cursor) + 1
			case <-ctx.Done():
				return
			case <-s.closed:
				return
			}
		}
		if len(batch) > 0 {
			continue // more may already be waiting; do not sleep between full batches
		}

		bell := s.notifier.waitChan(scope)
		select {
		case <-bell:
		case <-time.After(s.pollInterval):
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		}
	}
}

func (s *Store) readEvents(ctx context.Context, scope string, from int64, limit int) ([]dw.Event, error) {
	rows, err := s.pool.Query(ctx, `
SELECT cursor, node_id, kind, seq, from_status, to_status, reason, message, attempt, node_kind, at
FROM dagw.events
WHERE scope = $1 AND cursor >= $2
ORDER BY cursor
LIMIT $3`, scope, from, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: readEvents: %w", err)
	}
	defer rows.Close()

	var out []dw.Event
	for rows.Next() {
		var (
			ev                         dw.Event
			cursor, seq                int64
			kind, from16, to16, reason int16
			attempt                    int32
		)
		if err := rows.Scan(&cursor, &ev.NodeID, &kind, &seq, &from16, &to16, &reason,
			&ev.Message, &attempt, &ev.NodeKind, &ev.At); err != nil {
			return nil, fmt.Errorf("postgres: readEvents: scan: %w", err)
		}
		ev.Scope = dw.Scope(scope)
		ev.Cursor = dw.Cursor(narrowU64(cursor))
		ev.Seq = dw.Seq(narrowU64(seq))
		ev.Kind = dw.EventKind(narrowU8(kind))
		ev.From = dw.Status(narrowU8(from16))
		ev.To = dw.Status(narrowU8(to16))
		ev.Reason = dw.Reason(narrowU8(reason))
		ev.Attempt = narrowU32(attempt)
		out = append(out, ev)
	}
	return out, rows.Err()
}

// WaitForWork implements [dagworker.Doorbell]. A wakeup is advisory: this
// method returning nil never asserts work is definitely there, only that the
// caller should look — a spurious wakeup costs one wasted claim attempt,
// which is the same trade memory's doorbell makes.
func (s *Store) WaitForWork(ctx context.Context, scope dw.Scope, kinds []string) error {
	if err := s.ensureMigrated(ctx); err != nil {
		return err
	}
	scopeName := string(scope)
	if err := ensureScope(ctx, s.pool, scopeName, s.defaults); err != nil {
		return err
	}

	ready, err := s.anyReady(ctx, scopeName, kinds)
	if err != nil {
		return err
	}
	if ready {
		return nil
	}

	bell := s.notifier.waitChan(scopeName)
	select {
	case <-bell:
		return nil
	case <-time.After(s.pollInterval):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return dw.ErrClosed
	}
}

func (s *Store) anyReady(ctx context.Context, scope string, kinds []string) (bool, error) {
	var exists bool
	var err error
	if len(kinds) == 0 {
		err = s.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM dagw.nodes WHERE scope = $1 AND phase = $2)`,
			scope, int16(dw.PhaseReady)).Scan(&exists)
	} else {
		err = s.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM dagw.nodes WHERE scope = $1 AND phase = $2 AND kind = ANY($3))`,
			scope, int16(dw.PhaseReady), kinds).Scan(&exists)
	}
	if err != nil {
		return false, fmt.Errorf("postgres: anyReady: %w", err)
	}
	return exists, nil
}
