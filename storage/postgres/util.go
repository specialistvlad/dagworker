package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	dw "github.com/specialistvlad/dagworker"
)

// isNoRows reports whether err is exactly "no rows", pgx's signal for a
// QueryRow that matched nothing. It is never itself a [dagworker] sentinel;
// each call site decides whether "nothing" means [dagworker.ErrNotFound] or
// a plain false/zero-value result, the same branch memory's map lookups make
// explicitly at every call site.
func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

// scanNodeIDs drains rows of a single text column into external node IDs. It
// always closes rows, so every caller can treat it as a value-returning
// query even though pgx models it as an iterator.
func scanNodeIDs(rows pgx.Rows) ([]dw.NodeID, error) {
	defer rows.Close()
	var out []dw.NodeID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, dw.NodeID(id))
	}
	return out, rows.Err()
}

// int64s drains rows of a single bigint column, used throughout the graph
// package to collect internal node handles (successor sets, discovery
// frontiers) before locking or updating them.
func int64s(rows pgx.Rows) ([]int64, error) {
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// beginTx starts a transaction. Factored out purely so every mutating method
// begins one identically; it is not a boundary any caller reasons about.
// Returning the pgx.Tx interface rather than a concrete type is required —
// pgxpool.Pool.Begin has no other return shape — not a stylistic choice.
//
//nolint:ireturn
func beginTx(ctx context.Context, s *Store) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

// lockScopeGraph takes a transaction-scoped advisory lock serialising every
// structure-changing operation (AddNodes, AddEdges, RemoveEdges, RemoveNode,
// Cancel, CancelScope) against every other one, for the same scope.
//
// The claim/complete/extend/sweep hot path deliberately never takes this
// lock — SKIP LOCKED and per-row FOR UPDATE already give it real
// cross-process concurrency, which is the entire reason to run on Postgres.
// Structural graph edits are rarer and touch the topological rank in ways
// that are hard to prove correct under fine-grained row locking alone (the
// Pearce-Kelly reorder's affected region is discovered before any row in it
// is locked); serialising them per scope is the simple, obviously-correct
// choice, and it never blocks a claim or a completion, which take no
// advisory lock and are invisible to this one.
func lockScopeGraph(ctx context.Context, q querier, scope string) error {
	_, err := q.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('dagworker.graph'), hashtext($1))`, scope)
	return err
}

// orderedSet keeps insertion order while still deduplicating, mirroring
// memory's helper of the same name: the order nodes are settled in must be
// the order the caller declared them, not whatever order a map produced.
type orderedSet[T comparable] struct {
	items []T
	seen  map[T]struct{}
}

func newOrderedSet[T comparable]() *orderedSet[T] {
	return &orderedSet[T]{seen: make(map[T]struct{})}
}

func (o *orderedSet[T]) add(v T) {
	if _, dup := o.seen[v]; dup {
		return
	}
	o.seen[v] = struct{}{}
	o.items = append(o.items, v)
}

// externalIDs maps a set of internal node ids back to their external
// (scope, node_id) identifiers — used to translate a cycle path discovered
// over internal ids into the [dagworker.NodeID] values a *CycleError reports.
func externalIDs(ctx context.Context, q querier, ids []int64) (map[int64]string, error) {
	rows, err := q.Query(ctx, `SELECT id, node_id FROM dagw.nodes WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int64]string, len(ids))
	for rows.Next() {
		var id int64
		var nodeID string
		if err := rows.Scan(&id, &nodeID); err != nil {
			return nil, err
		}
		out[id] = nodeID
	}
	return out, rows.Err()
}
