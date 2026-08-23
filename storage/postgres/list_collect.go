package postgres

import (
	"context"
	"fmt"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// ListNodes implements [dagworker.Lister]. Pagination is keyset over the
// node's own identifier ordering: the query always fetches one row more than
// the page size to learn whether another page exists, never an OFFSET, which
// would make page N linear in N — precisely the operation this library
// promises not to have.
func (s *Store) ListNodes(ctx context.Context, scope dw.Scope, opts dw.ListOptions) (dw.ListResult, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return dw.ListResult{}, err
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	query := `SELECT ` + nodeColumns + ` FROM dagw.nodes WHERE scope = $1 AND node_id > $2`
	args := []any{string(scope), opts.Cursor}

	if len(opts.Statuses) > 0 {
		vals := make([]int16, len(opts.Statuses))
		for i, st := range opts.Statuses {
			vals[i] = int16(st)
		}
		args = append(args, vals)
		query += fmt.Sprintf(" AND status = ANY($%d)", len(args))
	}
	if len(opts.Kinds) > 0 {
		args = append(args, opts.Kinds)
		query += fmt.Sprintf(" AND kind = ANY($%d)", len(args))
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" ORDER BY node_id LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return dw.ListResult{}, fmt.Errorf("postgres: ListNodes: %w", err)
	}
	defer rows.Close()

	var out dw.ListResult
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return dw.ListResult{}, fmt.Errorf("postgres: ListNodes: scan: %w", err)
		}
		out.Nodes = append(out.Nodes, n.snapshot())
	}
	if err := rows.Err(); err != nil {
		return dw.ListResult{}, fmt.Errorf("postgres: ListNodes: %w", err)
	}

	if len(out.Nodes) > limit {
		out.Next = string(out.Nodes[limit-1].ID)
		out.Nodes = out.Nodes[:limit]
	}
	return out, nil
}

// terminalCandidateWhere is shared between the candidate SELECT and the
// "does another page exist" EXISTS check below, so the two can never drift
// into disagreeing about what counts as collectible.
const terminalCandidateWhere = `
	n.scope = $1 AND n.phase = $2 AND n.updated_at <= $3
	AND NOT EXISTS (SELECT 1 FROM dagw.edges e WHERE e.scope = n.scope AND e.from_id = n.id)`

// terminalCandidate is one row CollectTerminal is about to delete.
type terminalCandidate struct {
	id     int64
	phase  dw.Phase
	status dw.Status
}

// findTerminalCandidates locks up to limit collectible rows, in ascending id
// order, skipping any a concurrent collector already holds — duplicate
// collection is wasted work, never wrong, since each candidate is deleted
// under its own lock.
func findTerminalCandidates(ctx context.Context, tx querier, scope string, cutoff time.Time, limit int) ([]terminalCandidate, error) {
	rows, err := tx.Query(ctx, `
SELECT n.id, n.phase, n.status
FROM dagw.nodes n
WHERE `+terminalCandidateWhere+`
ORDER BY n.id
FOR UPDATE OF n SKIP LOCKED
LIMIT $4`, scope, int16(dw.PhaseDone), cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: CollectTerminal: select: %w", err)
	}
	defer rows.Close()

	var cands []terminalCandidate
	for rows.Next() {
		var c terminalCandidate
		var ph, st int16
		if err := rows.Scan(&c.id, &ph, &st); err != nil {
			return nil, fmt.Errorf("postgres: CollectTerminal: scan: %w", err)
		}
		c.phase, c.status = dw.Phase(narrowU8(ph)), dw.Status(narrowU8(st))
		cands = append(cands, c)
	}
	return cands, rows.Err()
}

// CollectTerminal implements [dagworker.Collector]. A candidate must be
// terminal, past the cutoff, and have no successors — deleting a node that
// something still depends on would corrupt that dependent's tally, exactly
// the hazard memory's len(s.succ[h])>0 guard exists to avoid.
func (s *Store) CollectTerminal(ctx context.Context, scope dw.Scope, cutoff time.Time, limit int) (int, bool, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return 0, false, err
	}
	if limit <= 0 {
		limit = 100
	}
	scopeName := string(scope)

	if _, ok, err := loadScope(ctx, s.pool, scopeName); err != nil {
		return 0, false, err
	} else if !ok {
		return 0, false, nil
	}

	tx, err := beginTx(ctx, s)
	if err != nil {
		return 0, false, fmt.Errorf("postgres: CollectTerminal: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cands, err := findTerminalCandidates(ctx, tx, scopeName, cutoff, limit)
	if err != nil {
		return 0, false, err
	}

	eng := newEngine(tx, scopeName, dw.ScopeConfig{}, s.jitter)
	for _, c := range cands {
		n := nodeRow{ID: c.id, Phase: c.phase, Status: c.status}
		if _, err := tx.Exec(ctx, `DELETE FROM dagw.edges WHERE scope = $1 AND to_id = $2`, scopeName, c.id); err != nil {
			return 0, false, fmt.Errorf("postgres: CollectTerminal: detach: %w", err)
		}
		if err := deleteNodeRow(ctx, tx, eng, scope, n); err != nil {
			return 0, false, err
		}
	}

	var more bool
	if len(cands) == limit {
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM dagw.nodes n WHERE `+terminalCandidateWhere+`)`,
			scopeName, int16(dw.PhaseDone), cutoff).Scan(&more); err != nil {
			return 0, false, fmt.Errorf("postgres: CollectTerminal: more check: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, false, fmt.Errorf("postgres: CollectTerminal: commit: %w", err)
	}
	return len(cands), more, nil
}
