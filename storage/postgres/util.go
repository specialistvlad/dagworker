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
func beginTx(ctx context.Context, s *Store) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}
