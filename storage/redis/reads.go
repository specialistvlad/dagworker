package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	dw "github.com/specialistvlad/dagworker"
)

// GetNode implements [dagworker.Store]. The hot hash and the blob hash are
// read inside one script so the two can never be observed torn against each
// other — a plain two-command read from Go could, in principle, straddle a
// concurrent write between the two round trips.
func (s *Store) GetNode(ctx context.Context, scope dw.Scope, id dw.NodeID) (dw.Node, error) {
	if s.isClosed() {
		return dw.Node{}, dw.ErrClosed
	}
	res, err := s.scripts.getNode.Run(ctx, s.rdb, []string{s.keyCfg(scope)}, s.prefix(scope), string(id)).Result()
	if err != nil {
		return dw.Node{}, mapScriptErr(err, scope)
	}
	top, ok := res.([]any)
	if !ok || len(top) != 2 {
		return dw.Node{}, fmt.Errorf("redis: GetNode: unexpected reply %#v", res)
	}
	nFlat, _ := top[0].([]any)
	bFlat, _ := top[1].([]any)
	return nodeFromHash(scope, id, hgetallMap(nFlat), hgetallMap(bFlat)), nil
}

// Inspect implements [dagworker.Store].
func (s *Store) Inspect(ctx context.Context, scope dw.Scope, id dw.NodeID) (dw.Inspection, error) {
	if s.isClosed() {
		return dw.Inspection{}, dw.ErrClosed
	}
	node, err := s.GetNode(ctx, scope, id)
	if err != nil {
		return dw.Inspection{}, err
	}
	res, err := s.scripts.inspect.Run(ctx, s.rdb, []string{s.keyCfg(scope)}, s.prefix(scope), string(id)).Result()
	if err != nil {
		return dw.Inspection{}, mapScriptErr(err, scope)
	}
	top, ok := res.([]any)
	if !ok || len(top) != 3 {
		return dw.Inspection{}, fmt.Errorf("redis: Inspect: unexpected reply %#v", res)
	}
	nFlat, _ := top[0].([]any)
	predFlat, _ := top[1].([]any)
	succFlat, _ := top[2].([]any)
	n := hgetallMap(nFlat)

	insp := dw.Inspection{
		Node:          node,
		Phase:         dw.Phase(atoi64(n["phase"])),
		Rank:          atoi64(n["ord"]),
		LeaseDeadline: msToTime(atoi64(n["deadline"])),
		ReadyAt:       msToTime(atoi64(n["readyAt"])),
	}
	insp.Deps = dw.DepCounts{
		Unsatisfied: uint32(atoi64(n["depsUnsatisfied"])),
		Succeeded:   uint32(atoi64(n["depsSucceeded"])),
		Skipped:     uint32(atoi64(n["depsSkipped"])),
		Failed:      uint32(atoi64(n["depsFailed"])),
	}
	pred := hgetallMap(predFlat)
	for from, satisfied := range pred {
		if satisfied == "0" {
			insp.Waiting = append(insp.Waiting, dw.NodeID(from))
		}
	}
	for _, v := range succFlat {
		insp.Successors = append(insp.Successors, dw.NodeID(toStr(v)))
	}
	return insp, nil
}

// ListNodes implements [dagworker.Lister]. Pagination is keyset over the
// node-id ordering the {scope}:idx ZSET already maintains (every member
// scored 0, so ZRANGEBYLEX walks it in exactly NodeID's own byte order) —
// never an offset, and never a scan of the whole scope to build one page:
// each round only reads as many ids as it takes to fill the page, batching
// forward through ZRANGEBYLEX when a status/kind filter excludes some.
func (s *Store) ListNodes(ctx context.Context, scope dw.Scope, opts dw.ListOptions) (dw.ListResult, error) {
	if s.isClosed() {
		return dw.ListResult{}, dw.ErrClosed
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	const batch = 200
	min := "-"
	if opts.Cursor != "" {
		min = "(" + opts.Cursor
	}

	var out dw.ListResult
	for {
		ids, err := s.rdb.ZRangeByLex(ctx, s.keyIdx(scope), &goredis.ZRangeBy{Min: min, Max: "+", Count: batch}).Result()
		if err != nil {
			return dw.ListResult{}, fmt.Errorf("redis: ListNodes: %w", err)
		}
		if len(ids) == 0 {
			return out, nil
		}
		for _, idStr := range ids {
			id := dw.NodeID(idStr)
			node, err := s.GetNode(ctx, scope, id)
			if err != nil {
				continue // deleted between the index read and this read: skip, not an error
			}
			if len(opts.Statuses) > 0 && !containsStatus(opts.Statuses, node.Status) {
				continue
			}
			if len(opts.Kinds) > 0 && !containsStr(opts.Kinds, node.Kind) {
				continue
			}
			if len(out.Nodes) == limit {
				out.Next = string(out.Nodes[len(out.Nodes)-1].ID)
				return out, nil
			}
			out.Nodes = append(out.Nodes, node)
		}
		min = "(" + ids[len(ids)-1]
		if len(ids) < batch {
			// Exhausted the index; whatever survived filtering is the last page.
			return out, nil
		}
	}
}

func containsStatus(xs []dw.Status, x dw.Status) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func containsStr(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// CollectTerminal implements [dagworker.Collector]. Eligibility (terminal,
// updated before cutoff, no remaining successors) is checked and, if true,
// acted on inside one small per-candidate script, so a concurrent AddEdges
// that gives a "leaf" node a brand-new successor between the scan and the
// delete can never race a deletion out from under it.
func (s *Store) CollectTerminal(ctx context.Context, scope dw.Scope, cutoff time.Time, limit int) (int, bool, error) {
	if s.isClosed() {
		return 0, false, dw.ErrClosed
	}
	if limit <= 0 {
		limit = 100
	}
	cutoffMs := cutoff.UnixMilli()

	const batch = 500
	min := "-"
	deleted := 0
	for {
		ids, err := s.rdb.ZRangeByLex(ctx, s.keyIdx(scope), &goredis.ZRangeBy{Min: min, Max: "+", Count: batch}).Result()
		if err != nil {
			return deleted, false, fmt.Errorf("redis: CollectTerminal: %w", err)
		}
		if len(ids) == 0 {
			return deleted, false, nil
		}
		for _, id := range ids {
			if deleted == limit {
				return deleted, true, nil
			}
			n, err := s.scripts.collectIfEligible.Run(ctx, s.rdb, []string{s.keyCfg(scope)}, s.prefix(scope), id, cutoffMs).Int()
			if err != nil {
				return deleted, false, fmt.Errorf("redis: CollectTerminal: %w", err)
			}
			if n == 1 {
				deleted++
			}
		}
		min = "(" + ids[len(ids)-1]
		if len(ids) < batch {
			return deleted, false, nil
		}
	}
}
