package postgres

import (
	"context"
	"fmt"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// ensureScope makes name exist, seeding it with defaults, without disturbing
// an existing row. Scopes are created implicitly on first write (the port's
// own rule); ON CONFLICT DO NOTHING is what makes that safe to call from
// every write path without a preceding existence check.
func ensureScope(ctx context.Context, q querier, scope string, defaults dw.ScopeConfig) error {
	_, err := q.Exec(ctx, `
INSERT INTO dagw.scopes (
	scope,
	default_lease_timeout_ns, min_lease_timeout_ns, max_lease_timeout_ns,
	max_attempts, retry_base_delay_ns, retry_max_delay_ns,
	terminal_retention_ns, max_subscriber_lag_ns, max_in_flight,
	payload_cap, max_batch_size, sweep_batch_size, sweep_interval_ns, partition_count
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (scope) DO NOTHING`,
		scope,
		int64(defaults.DefaultLeaseTimeout), int64(defaults.MinLeaseTimeout), int64(defaults.MaxLeaseTimeout),
		narrowI32(defaults.MaxAttempts), int64(defaults.RetryBaseDelay), int64(defaults.RetryMaxDelay),
		int64(defaults.TerminalRetention), int64(defaults.MaxSubscriberLag), narrowI32(defaults.MaxInFlight),
		narrowI32(defaults.PayloadCap), narrowI32(defaults.MaxBatchSize), narrowI32(defaults.SweepBatchSize),
		int64(defaults.SweepInterval), narrowI32(defaults.PartitionCount),
	)
	if err != nil {
		return fmt.Errorf("postgres: ensure scope %q: %w", scope, err)
	}
	return nil
}

// loadScope reads one scope row, or reports ok=false when it does not exist.
func loadScope(ctx context.Context, q querier, scope string) (scopeRow, bool, error) {
	row := q.QueryRow(ctx, `SELECT `+scopeColumns+` FROM dagw.scopes WHERE scope = $1`, scope)
	sc, err := scanScope(row)
	if err != nil {
		if isNoRows(err) {
			return scopeRow{}, false, nil
		}
		return scopeRow{}, false, fmt.Errorf("postgres: load scope %q: %w", scope, err)
	}
	return sc, true, nil
}

// ScopeConfig implements [dagworker.Store]. An unknown scope returns the
// Store's own configured defaults and no error: asking about a scope nobody
// has written to yet is not a failure, because scopes are created implicitly.
func (s *Store) ScopeConfig(ctx context.Context, scope dw.Scope) (dw.ScopeConfig, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return dw.ScopeConfig{}, err
	}
	sc, ok, err := loadScope(ctx, s.pool, string(scope))
	if err != nil {
		return dw.ScopeConfig{}, err
	}
	if !ok {
		return s.defaults, nil
	}
	return sc.Cfg, nil
}

// SetScopeConfig implements [dagworker.Store].
func (s *Store) SetScopeConfig(ctx context.Context, scope dw.Scope, cfg dw.ScopeConfig) error {
	if err := s.ensureMigrated(ctx); err != nil {
		return err
	}
	if err := ensureScope(ctx, s.pool, string(scope), s.defaults); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
UPDATE dagw.scopes SET
	default_lease_timeout_ns = $2, min_lease_timeout_ns = $3, max_lease_timeout_ns = $4,
	max_attempts = $5, retry_base_delay_ns = $6, retry_max_delay_ns = $7,
	terminal_retention_ns = $8, max_subscriber_lag_ns = $9, max_in_flight = $10,
	payload_cap = $11, max_batch_size = $12, sweep_batch_size = $13,
	sweep_interval_ns = $14, partition_count = $15
WHERE scope = $1`,
		string(scope),
		int64(cfg.DefaultLeaseTimeout), int64(cfg.MinLeaseTimeout), int64(cfg.MaxLeaseTimeout),
		narrowI32(cfg.MaxAttempts), int64(cfg.RetryBaseDelay), int64(cfg.RetryMaxDelay),
		int64(cfg.TerminalRetention), int64(cfg.MaxSubscriberLag), narrowI32(cfg.MaxInFlight),
		narrowI32(cfg.PayloadCap), narrowI32(cfg.MaxBatchSize), narrowI32(cfg.SweepBatchSize),
		int64(cfg.SweepInterval), narrowI32(cfg.PartitionCount),
	)
	if err != nil {
		return fmt.Errorf("postgres: SetScopeConfig: %w", err)
	}
	return nil
}

// Seal implements [dagworker.Store]. Sealing an already-sealed scope is a
// no-op because the UPDATE simply writes the same value again; there is
// nothing to branch on.
func (s *Store) Seal(ctx context.Context, scope dw.Scope) error {
	if err := s.ensureMigrated(ctx); err != nil {
		return err
	}
	if err := ensureScope(ctx, s.pool, string(scope), s.defaults); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `UPDATE dagw.scopes SET sealed = true WHERE scope = $1`, string(scope))
	if err != nil {
		return fmt.Errorf("postgres: Seal: %w", err)
	}
	return nil
}

// ScopeStats implements [dagworker.Store]. Every counter is read straight off
// the scope's own row: none is derived by scanning dagw.nodes.
func (s *Store) ScopeStats(ctx context.Context, scope dw.Scope) (dw.ScopeStats, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return dw.ScopeStats{}, err
	}
	sc, ok, err := loadScope(ctx, s.pool, string(scope))
	if err != nil {
		return dw.ScopeStats{}, err
	}
	if !ok {
		return dw.ScopeStats{}, nil
	}
	sc.Stats.Complete = sc.Sealed && sc.Stats.NonTerminal() == 0
	return sc.Stats, nil
}

// GetNode implements [dagworker.Store].
func (s *Store) GetNode(ctx context.Context, scope dw.Scope, id dw.NodeID) (dw.Node, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return dw.Node{}, err
	}
	row := s.pool.QueryRow(ctx,
		`SELECT `+nodeColumns+` FROM dagw.nodes WHERE scope = $1 AND node_id = $2`,
		string(scope), string(id))
	n, err := scanNode(row)
	if err != nil {
		if isNoRows(err) {
			return dw.Node{}, dw.ErrNotFound
		}
		return dw.Node{}, fmt.Errorf("postgres: GetNode: %w", err)
	}
	return n.snapshot(), nil
}

// Inspect implements [dagworker.Store]. It costs one extra round trip beyond
// GetNode for the waiting-predecessor and successor lists, which the doc
// comment on [dagworker.Store.Inspect] explicitly allows: it is a debugging
// facet, not one of the complexity-bounded hot paths.
func (s *Store) Inspect(ctx context.Context, scope dw.Scope, id dw.NodeID) (dw.Inspection, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return dw.Inspection{}, err
	}
	row := s.pool.QueryRow(ctx,
		`SELECT `+nodeColumns+` FROM dagw.nodes WHERE scope = $1 AND node_id = $2`,
		string(scope), string(id))
	n, err := scanNode(row)
	if err != nil {
		if isNoRows(err) {
			return dw.Inspection{}, dw.ErrNotFound
		}
		return dw.Inspection{}, fmt.Errorf("postgres: Inspect: %w", err)
	}

	insp := dw.Inspection{
		Node:          n.snapshot(),
		Phase:         n.Phase,
		Deps:          n.Deps,
		Rank:          n.Rank,
		LeaseDeadline: derefTime(n.Deadline),
		LeaseHolder:   heldBy(n.Phase, n.Worker),
		LeaseEpoch:    n.Epoch,
		ReadyAt:       derefTime(n.ReadyAt),
	}

	waitRows, err := s.pool.Query(ctx, `
SELECT p.node_id
FROM dagw.edges e
JOIN dagw.nodes p ON p.scope = e.scope AND p.id = e.from_id
WHERE e.scope = $1 AND e.to_id = $2 AND e.satisfied = false`,
		string(scope), n.ID)
	if err != nil {
		return dw.Inspection{}, fmt.Errorf("postgres: Inspect: waiting: %w", err)
	}
	insp.Waiting, err = scanNodeIDs(waitRows)
	if err != nil {
		return dw.Inspection{}, fmt.Errorf("postgres: Inspect: waiting: %w", err)
	}

	succRows, err := s.pool.Query(ctx, `
SELECT c.node_id
FROM dagw.edges e
JOIN dagw.nodes c ON c.scope = e.scope AND c.id = e.to_id
WHERE e.scope = $1 AND e.from_id = $2`,
		string(scope), n.ID)
	if err != nil {
		return dw.Inspection{}, fmt.Errorf("postgres: Inspect: successors: %w", err)
	}
	insp.Successors, err = scanNodeIDs(succRows)
	if err != nil {
		return dw.Inspection{}, fmt.Errorf("postgres: Inspect: successors: %w", err)
	}

	return insp, nil
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// heldBy reports the lease holder only while the node is actually claimed.
// Every backend leaves the worker name behind when a lease is reclaimed rather
// than paying a write to clear a field nothing keys on, so the read side is
// where "who holds it now" is decided.
func heldBy(phase dw.Phase, worker string) string {
	if phase != dw.PhaseClaimed {
		return ""
	}
	return worker
}
