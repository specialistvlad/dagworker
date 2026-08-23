package postgres

import (
	"context"
	"sort"

	"github.com/jackc/pgx/v5"
)

// Pearce-Kelly incremental topological ordering, ported from
// storage/memory/topo.go onto SQL reads and writes. See that file's doc
// comment for the algorithm itself; this file only replaces slice/array
// access with queries against dagw.nodes/dagw.edges, executed inside the
// scope-wide advisory transaction lock every graph-structure mutation holds
// (graph.go), which is what makes concurrent structural edits to the same
// region safe without per-row locking during the discovery phase.
//
// Every edge insert first asks whether rank(x) < rank(y) already holds; if
// so, the edge cannot close a cycle and no traversal runs at all. Otherwise
// a bounded forward search from y (does y already reach x?) and backward
// search from x delimit the affected region, which is then renumbered in
// place, reusing the exact rank values the region already occupied.

// idRank pairs a node's internal id with its current topological rank.
type idRank struct{ id, rank int64 }

// topoResult reports what an edge insertion did to the ordering.
type topoResult struct {
	// fastPath is true when the invariant already held and no search ran.
	fastPath bool
	// cyclePath, when non-nil, is the existing route from y back to x
	// (internal ids) that the proposed edge x -> y would have closed into a
	// cycle, running from y to x inclusive.
	cyclePath []int64
}

// ranksOf reads the current rank of every id in ids.
func (e *engine) ranksOf(ctx context.Context, ids []int64) (map[int64]int64, error) {
	rows, err := e.tx.Query(ctx, `SELECT id, rank FROM dagw.nodes WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int64]int64, len(ids))
	for rows.Next() {
		var id, r int64
		if err := rows.Scan(&id, &r); err != nil {
			return nil, err
		}
		out[id] = r
	}
	return out, rows.Err()
}

// successorRanks returns the (id, rank) of every direct successor of id.
func (e *engine) successorRanks(ctx context.Context, id int64) ([]idRank, error) {
	rows, err := e.tx.Query(ctx, `
SELECT c.id, c.rank
FROM dagw.edges edg JOIN dagw.nodes c ON c.scope = edg.scope AND c.id = edg.to_id
WHERE edg.scope = $1 AND edg.from_id = $2`, e.scope, id)
	if err != nil {
		return nil, err
	}
	return scanIDRanks(rows)
}

// predecessorRanks returns the (id, rank) of every direct predecessor of id.
func (e *engine) predecessorRanks(ctx context.Context, id int64) ([]idRank, error) {
	rows, err := e.tx.Query(ctx, `
SELECT p.id, p.rank
FROM dagw.edges edg JOIN dagw.nodes p ON p.scope = edg.scope AND p.id = edg.from_id
WHERE edg.scope = $1 AND edg.to_id = $2`, e.scope, id)
	if err != nil {
		return nil, err
	}
	return scanIDRanks(rows)
}

func scanIDRanks(rows pgx.Rows) ([]idRank, error) {
	defer rows.Close()
	var out []idRank
	for rows.Next() {
		var r idRank
		if err := rows.Scan(&r.id, &r.rank); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// addEdgeOrder restores the topological invariant for a proposed edge
// x -> y, or reports the cycle it would create. It mutates rank only.
func (e *engine) addEdgeOrder(ctx context.Context, x, y int64) (topoResult, error) {
	ranks, err := e.ranksOf(ctx, []int64{x, y})
	if err != nil {
		return topoResult{}, err
	}
	rx, ry := ranks[x], ranks[y]
	if rx < ry {
		return topoResult{fastPath: true}, nil
	}
	ub, lb := rx, ry

	cyclePath, deltaF, err := e.forwardSearch(ctx, x, y, ub)
	if err != nil {
		return topoResult{}, err
	}
	if cyclePath != nil {
		return topoResult{cyclePath: cyclePath}, nil
	}

	deltaB, err := e.backwardSearch(ctx, x, lb)
	if err != nil {
		return topoResult{}, err
	}

	if err := e.reorder(ctx, deltaB, deltaF); err != nil {
		return topoResult{}, err
	}
	return topoResult{}, nil
}

// forwardSearch walks successors from y, bounded above by ub (x's rank),
// looking for x by id — not by rank equality, since x's own rank is exactly
// ub and therefore outside the "< ub" frontier the search otherwise walks.
// Reaching x means y already reaches x, so the caller's proposed edge x -> y
// would close a cycle; the returned path runs from y to x inclusive.
func (e *engine) forwardSearch(ctx context.Context, x, y, ub int64) (cyclePath []int64, deltaF []int64, err error) {
	visited := map[int64]bool{y: true}
	deltaF = []int64{y}
	parent := map[int64]int64{}
	stack := []int64{y}

	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		succs, err := e.successorRanks(ctx, n)
		if err != nil {
			return nil, nil, err
		}
		for _, s := range succs {
			if s.id == x {
				parent[s.id] = n
				return tracePath(parent, y, x), deltaF, nil
			}
			if !visited[s.id] && s.rank < ub {
				visited[s.id] = true
				parent[s.id] = n
				deltaF = append(deltaF, s.id)
				stack = append(stack, s.id)
			}
		}
	}
	return nil, deltaF, nil
}

// backwardSearch walks predecessors from x, bounded below by lb (y's rank
// before the search began), collecting the region that must be renumbered
// ahead of deltaF.
func (e *engine) backwardSearch(ctx context.Context, x, lb int64) ([]int64, error) {
	visited := map[int64]bool{x: true}
	deltaB := []int64{x}
	stack := []int64{x}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		preds, err := e.predecessorRanks(ctx, n)
		if err != nil {
			return nil, err
		}
		for _, p := range preds {
			if !visited[p.id] && p.rank > lb {
				visited[p.id] = true
				deltaB = append(deltaB, p.id)
				stack = append(stack, p.id)
			}
		}
	}
	return deltaB, nil
}

// reorder reassigns the rank values already occupied by deltaB and deltaF so
// that every node of deltaB precedes every node of deltaF, preserving
// relative order within each region — identical to memory's reorder. Because
// the two regions are disjoint and the pooled value set is unchanged, no
// node outside them is affected and no new rank value has to be minted.
func (e *engine) reorder(ctx context.Context, deltaB, deltaF []int64) error {
	all := make([]int64, 0, len(deltaB)+len(deltaF))
	all = append(all, deltaB...)
	all = append(all, deltaF...)
	ranks, err := e.ranksOf(ctx, all)
	if err != nil {
		return err
	}

	byRank := func(ids []int64) { sort.Slice(ids, func(i, j int) bool { return ranks[ids[i]] < ranks[ids[j]] }) }
	byRank(deltaB)
	byRank(deltaF)

	pool := make([]int64, 0, len(all))
	for _, id := range deltaB {
		pool = append(pool, ranks[id])
	}
	for _, id := range deltaF {
		pool = append(pool, ranks[id])
	}
	sort.Slice(pool, func(i, j int) bool { return pool[i] < pool[j] })

	i := 0
	assign := func(ids []int64) error {
		for _, id := range ids {
			if _, err := e.tx.Exec(ctx, `UPDATE dagw.nodes SET rank = $2 WHERE id = $1`, id, pool[i]); err != nil {
				return err
			}
			i++
		}
		return nil
	}
	if err := assign(deltaB); err != nil {
		return err
	}
	return assign(deltaF)
}

// tracePath walks parent pointers back from end to start, returning the path
// in forward order (start..end). The parent map was built by a search rooted
// at start, so the walk always terminates.
func tracePath(parent map[int64]int64, start, end int64) []int64 {
	var rev []int64
	for n := end; ; {
		rev = append(rev, n)
		if n == start {
			break
		}
		p, ok := parent[n]
		if !ok {
			break
		}
		n = p
	}
	for l, r := 0, len(rev)-1; l < r; l, r = l+1, r-1 {
		rev[l], rev[r] = rev[r], rev[l]
	}
	return rev
}
