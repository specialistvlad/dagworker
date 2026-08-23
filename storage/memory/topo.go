package memory

import "sort"

// Pearce-Kelly incremental topological ordering.
//
// The scheduler maintains an integer rank per node with the invariant that
// ord[u] < ord[v] for every edge u -> v. That invariant does two jobs at once:
// it is the O(1) fast path for accepting an edge, and the bounded search that
// handles the slow path *is* the cycle check, so there is no separate
// reachability query to maintain.
//
// When an edge x -> y arrives with ord[x] < ord[y] the invariant already holds
// and there is nothing to do. This is the common case, because callers build
// graphs roughly in causal order. Otherwise the algorithm bounds the disruption
// to the region between the two endpoints:
//
//	deltaF  nodes reachable forward from y whose rank is below ord[x]
//	deltaB  nodes reaching backward into x whose rank is above ord[y]
//
// Reaching x during the forward search means y already reaches x, so the edge
// would close a cycle. Otherwise the two regions are reordered against each
// other: every node in deltaB is placed before every node in deltaF, reusing
// the exact set of rank values the two regions already occupied. Nothing
// outside the region moves, so the cost tracks how out-of-order the insertion
// actually was rather than the size of the graph.
//
// Reference: David J. Pearce and Paul H. J. Kelly, "A dynamic topological sort
// algorithm for directed acyclic graphs", ACM Journal of Experimental
// Algorithmics 11, 2007.

// topoResult reports what an edge insertion did to the ordering.
type topoResult struct {
	// fastPath is true when the invariant already held and no search ran.
	fastPath bool
	// visited counts the nodes the two searches touched, for the affected-region
	// size metric.
	visited int
	// cyclePath, when non-nil, is the existing route from y back to x that the
	// proposed edge x -> y would have closed into a cycle. It runs from y to x
	// inclusive.
	cyclePath []int32
}

// addEdgeOrder restores the topological invariant for a proposed edge x -> y,
// or reports the cycle it would create. It mutates ord only.
func (s *scope) addEdgeOrder(x, y int32) topoResult {
	if s.ord[x] < s.ord[y] {
		return topoResult{fastPath: true}
	}

	lb, ub := s.ord[y], s.ord[x]

	// Forward search from y, bounded above by ub. Parents are tracked so a
	// cycle can be reported as a concrete path rather than a bare error.
	var deltaF []int32
	parent := make(map[int32]int32, 8)
	visitedF := make(map[int32]bool, 8)

	var cycle []int32
	var dfsF func(n int32) bool
	dfsF = func(n int32) bool {
		visitedF[n] = true
		deltaF = append(deltaF, n)
		for _, w := range s.succ[n] {
			if s.ord[w] == ub {
				// w is x: y already reaches x.
				parent[w] = n
				cycle = tracePath(parent, y, w)
				return true
			}
			if !visitedF[w] && s.ord[w] < ub {
				parent[w] = n
				if dfsF(w) {
					return true
				}
			}
		}
		return false
	}

	if dfsF(y) {
		return topoResult{visited: len(visitedF), cyclePath: cycle}
	}

	// Backward search from x, bounded below by lb.
	var deltaB []int32
	visitedB := make(map[int32]bool, 8)

	var dfsB func(n int32)
	dfsB = func(n int32) {
		visitedB[n] = true
		deltaB = append(deltaB, n)
		for _, e := range s.pred[n] {
			w := e.from
			if !visitedB[w] && s.ord[w] > lb {
				dfsB(w)
			}
		}
	}
	dfsB(x)

	s.reorder(deltaB, deltaF)
	return topoResult{visited: len(visitedF) + len(visitedB)}
}

// reorder reassigns the rank values already occupied by deltaB and deltaF so
// that every node of deltaB precedes every node of deltaF, preserving the
// relative order within each region. Because the two regions are disjoint and
// the value set is unchanged, no node outside them is affected and no new rank
// value has to be minted.
func (s *scope) reorder(deltaB, deltaF []int32) {
	byOrd := func(xs []int32) { sort.Slice(xs, func(i, j int) bool { return s.ord[xs[i]] < s.ord[xs[j]] }) }
	byOrd(deltaB)
	byOrd(deltaF)

	pool := make([]int64, 0, len(deltaB)+len(deltaF))
	for _, n := range deltaB {
		pool = append(pool, s.ord[n])
	}
	for _, n := range deltaF {
		pool = append(pool, s.ord[n])
	}
	sort.Slice(pool, func(i, j int) bool { return pool[i] < pool[j] })

	i := 0
	for _, n := range deltaB {
		s.ord[n] = pool[i]
		i++
	}
	for _, n := range deltaF {
		s.ord[n] = pool[i]
		i++
	}
}

// tracePath walks parent pointers back from end to start, returning the path in
// forward order. The parent map was built by a search rooted at start, so the
// walk always terminates.
func tracePath(parent map[int32]int32, start, end int32) []int32 {
	var rev []int32
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
