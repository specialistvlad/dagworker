package perf_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/test/perf"
)

// The complexity guards answer a question a benchmark cannot: does the cost of
// one operation grow with the size of the graph?
//
// They assert a RATIO between the per-operation cost at the smallest and
// largest graph sizes, measured in the same process, on the same machine, in
// the same run. That is deliberate. An absolute nanosecond threshold is a
// promise about hardware, and on a shared CI runner it is a promise that will
// be broken by a noisy neighbour rather than by a regression. A ratio cancels
// the machine out.
//
// The sweep spans three orders of magnitude, from a thousand nodes to a
// million. Over that span:
//
//	O(1) or O(log n)  ratio stays in single digits, dominated by cache misses
//	O(sqrt n)         ratio around 31
//	O(n)              ratio around 1000
//
// So a bound of 20 is loose enough to absorb the cache-locality penalty that
// any operation pays on a graph too large for L2, and tight enough that
// anything worse than logarithmic fails decisively rather than marginally.
const (
	maxRatio         = 20.0
	networkedRatio   = 30.0 // a round trip dominates and widens the spread
	measureIters     = 2_000
	measureReps      = 5
	warmupIterations = 200
)

// measurement is the per-operation cost at one graph size.
type measurement struct {
	size  int
	perOp time.Duration
}

// timePerOp runs op in batches and returns the median batch's per-operation
// cost. The median, not the mean: one descheduled batch should not move the
// answer, and on a shared runner there is always one.
func timePerOp(tb testing.TB, iters int, op func(i int)) time.Duration {
	tb.Helper()
	for i := range warmupIterations {
		op(i)
	}
	samples := make([]time.Duration, 0, measureReps)
	n := 0
	for range measureReps {
		start := time.Now()
		for range iters {
			op(n)
			n++
		}
		samples = append(samples, time.Since(start)/time.Duration(iters))
	}
	slices.Sort(samples)
	return samples[len(samples)/2]
}

// assertFlat fails when the per-operation cost grew faster than the bound
// allows across the sweep.
func assertFlat(t *testing.T, op string, ms []measurement, bound float64) {
	t.Helper()
	if len(ms) < 2 {
		t.Fatalf("%s: need at least two sizes to compute a ratio, got %d", op, len(ms))
	}
	first, last := ms[0], ms[len(ms)-1]
	// A measurement floored at the clock's resolution makes the ratio
	// meaningless in the safe direction, so treat it as a pass.
	if first.perOp <= 0 {
		t.Logf("%s: cost at n=%d is below the timer resolution; ratio not meaningful", op, first.size)
		return
	}
	ratio := float64(last.perOp) / float64(first.perOp)

	var report strings.Builder
	for _, m := range ms {
		fmt.Fprintf(&report, "\n    n=%-9d %v", m.size, m.perOp)
	}
	t.Logf("%s cost curve:%s\n    ratio(n=%d / n=%d) = %.2fx (bound %.0fx)",
		op, report.String(), last.size, first.size, ratio, bound)

	if ratio > bound {
		t.Errorf("%s: per-operation cost grew %.2fx from n=%d to n=%d, above the %.0fx bound.\n"+
			"This is the signature of a linear scan where the design requires O(1) or O(log n).",
			op, ratio, first.size, last.size, bound)
	}
}

// consumingIters bounds an operation that uses up a node each time, so a
// measurement can never run past the end of the graph it seeded. Measuring
// against an empty ready set would report the cost of finding nothing, which
// looks flat no matter how bad the real operation is.
func consumingIters(n int) int {
	budget := (n - warmupIterations) / measureReps
	return max(1, min(measureIters, budget))
}

// sizes chooses the sweep for one backend.
//
// A networked backend stops at 100,000 by default. Not because the guarantee is
// weaker there -- TestMillionNodes checks a million on every backend -- but
// because re-seeding a million rows once per operation per backend costs many
// minutes of round trips to measure a ratio that 100,000 already establishes.
// Set DAGWORKER_PERF_FULL=1 to sweep the whole range everywhere.
func sizes(t *testing.T, b perf.Backend) []int {
	t.Helper()
	if testing.Short() {
		return perf.SmallSizes
	}
	if b.Networked && os.Getenv("DAGWORKER_PERF_FULL") == "" {
		return perf.SmallSizes
	}
	return perf.Sizes
}

func bound(b perf.Backend) float64 {
	if b.Networked {
		return networkedRatio
	}
	return maxRatio
}

// TestComplexity is the whole ratio sweep: five operations, each of which must
// cost the same at a hundred thousand nodes as at a thousand.
//
// The five used to be five separate tests, and each one seeded its own graph at
// every size -- 5 x (1,000 + 10,000 + 100,000) = 555,000 nodes per backend,
// which on PostgreSQL at a measured 454us per node is four minutes of setup
// before anything is measured. They all seeded the identical shape, so they now
// share it: two fixtures per size instead of five, and the same sizes, the same
// iteration counts and the same bound as before. Nothing about the measurement
// got weaker; there is just far less building of graphs to throw away.
//
// Two fixtures rather than one because claiming CONSUMES the ready set. The
// three read-only operations share the first; the claim measurements get the
// second to themselves, and take it in one pass -- timing the claim and the
// completion separately out of a single loop yields both curves for the price
// of one graph, where before they were two tests each draining their own.
func TestComplexity(t *testing.T) {
	t.Parallel()
	for _, backend := range perf.Backends() {
		t.Run(backend.Name, func(t *testing.T) {
			t.Parallel()
			sweep := sizes(t, backend)

			// The sizes are measured concurrently, and that is not incidental.
			// Seeding a networked backend is round-trip bound, so overlapping
			// the sizes is most of what keeps this suite tolerable: measured
			// serially it took 399s, and concurrently
			// it takes a fraction of that against the same databases.
			//
			// The wrapper subtest is what makes it safe to read `collected`
			// afterwards: t.Run does not return until every parallel child
			// beneath it has finished, so the assertions below see a complete
			// map without any synchronisation of their own.
			var mu sync.Mutex
			collected := make(map[int]map[string]time.Duration, len(sweep))
			record := func(n int, costs map[string]time.Duration) {
				mu.Lock()
				defer mu.Unlock()
				if collected[n] == nil {
					collected[n] = make(map[string]time.Duration, len(guards))
				}
				for name, cost := range costs {
					collected[n][name] = cost
				}
			}
			t.Run("sweep", func(t *testing.T) {
				for _, n := range sweep {
					t.Run(fmt.Sprintf("read/n=%d", n), func(t *testing.T) {
						t.Parallel()
						record(n, measureReads(t, backend, n))
					})
					t.Run(fmt.Sprintf("claim/n=%d", n), func(t *testing.T) {
						t.Parallel()
						record(n, measureClaims(t, backend, n))
					})
				}
			})

			curves := make(map[string][]measurement, len(guards))
			for _, n := range sweep {
				for _, name := range guards {
					curves[name] = append(curves[name], measurement{n, collected[n][name]})
				}
			}
			for _, name := range guards {
				assertFlat(t, name, curves[name], bound(backend))
			}
		})
	}
}

// guards names every operation the sweep asserts on, in a fixed order so the
// report reads the same way every run.
var guards = []string{"ScopeStats", "GetNode", "AddNode(causal)", "Claim", "Claim+Complete"}

// measureReads builds the read fixture for one graph size and measures every
// operation that does not consume the ready set. The insert is here too: it
// only grows the graph, so it cannot disturb what the two reads already
// measured.
func measureReads(t *testing.T, backend perf.Backend, n int) map[string]time.Duration {
	t.Helper()
	ctx := t.Context()
	out := make(map[string]time.Duration, 3)

	st := backend.New(t)
	scope := perf.Scope("read-%d", n)
	perf.SeedWide(t, st, scope, n)

	// Scope statistics are counters maintained by the transitions that change
	// them. If this one ever grows with n, something started scanning.
	out["ScopeStats"] = timePerOp(t, measureIters, func(int) {
		if _, err := st.ScopeStats(ctx, scope); err != nil {
			t.Fatalf("ScopeStats: %v", err)
		}
	})

	// Reading one node must not depend on how many others there are.
	out["GetNode"] = timePerOp(t, measureIters, func(i int) {
		// Stride through the whole keyspace rather than hammering one node, so
		// the measurement includes cache misses a real workload would pay.
		if _, err := st.GetNode(ctx, scope, perf.NodeID((i*7919)%n)); err != nil {
			t.Fatalf("GetNode: %v", err)
		}
	})

	// Inserting a node with one dependency on an existing node is the common
	// case, and the one that must stay O(1): the topological rank invariant
	// already holds, so no search runs.
	next := n
	out["AddNode(causal)"] = timePerOp(t, min(measureIters, 2_000), func(int) {
		id := perf.NodeID(next)
		dep := perf.NodeID(next % n)
		next++
		if _, err := st.AddNodes(ctx, scope, []dw.NodeSpec{{
			ID: id, Deps: []dw.NodeID{dep},
		}}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}
	})

	return out
}

// measureClaims builds the claim fixture and measures the path that consumes
// it. Claiming and completing are timed out of a single pass because both need
// the same graph and each claim can only be made once.
func measureClaims(t *testing.T, backend perf.Backend, n int) map[string]time.Duration {
	t.Helper()
	st := backend.New(t)
	scope := perf.Scope("claim-%d", n)
	perf.SeedWide(t, st, scope, n)
	claim, complete := timeClaimAndComplete(t, st, scope, consumingIters(n))
	return map[string]time.Duration{
		"Claim":          claim,
		"Claim+Complete": claim + complete,
	}
}

// timeClaimAndComplete drains part of a graph, timing the claim and the
// completion separately, and returns the median per-operation cost of each.
//
// Claiming must be a pop from an ordered structure rather than a search, and
// completing must cost what the node's out-degree costs -- constant here --
// rather than what the graph costs. Both are measured from one pass because
// both need the same graph and each claim can only be made once.
func timeClaimAndComplete(tb testing.TB, st dw.Store, scope dw.Scope, iters int) (claim, complete time.Duration) {
	tb.Helper()
	ctx := context.Background()
	req := dw.ClaimRequest{Scope: scope, Max: 1, Timeout: time.Hour}

	step := func() (time.Duration, time.Duration) {
		startClaim := time.Now()
		res, err := st.Claim(ctx, req)
		claimed := time.Since(startClaim)
		if err != nil {
			tb.Fatalf("Claim: %v", err)
		}
		if len(res.Leases) == 0 {
			tb.Fatal("ran out of claimable nodes mid-measurement: " +
				"the sweep would have been timing an empty ready set")
		}
		startComplete := time.Now()
		if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: res.Leases[0], Success: true}); err != nil {
			tb.Fatalf("Complete: %v", err)
		}
		return claimed, time.Since(startComplete)
	}

	for range warmupIterations {
		step()
	}
	claims := make([]time.Duration, 0, measureReps)
	completes := make([]time.Duration, 0, measureReps)
	for range measureReps {
		var c, d time.Duration
		for range iters {
			a, b := step()
			c += a
			d += b
		}
		claims = append(claims, c/time.Duration(iters))
		completes = append(completes, d/time.Duration(iters))
	}
	slices.Sort(claims)
	slices.Sort(completes)
	return claims[len(claims)/2], completes[len(completes)/2]
}

// Draining a chain of a million nodes end to end is the shape that would expose
// a scheduler that rescans on every completion: each step releases exactly one
// successor, so a linear ready-set recomputation would make the whole run
// quadratic and it would never finish.
func TestChainDrainsInLinearTime(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("the whole-graph drain is a long test")
	}
	for _, backend := range perf.Backends() {
		if backend.Networked {
			continue // a million round trips is a different measurement
		}
		t.Run(backend.Name, func(t *testing.T) {
			t.Parallel()
			const n = 200_000
			ctx := t.Context()
			st := backend.New(t)
			perf.SeedChain(t, st, "chain", n)

			start := time.Now()
			req := dw.ClaimRequest{Scope: "chain", Max: 1, Timeout: time.Hour}
			for i := range n {
				res, err := st.Claim(ctx, req)
				if err != nil {
					t.Fatalf("Claim at %d: %v", i, err)
				}
				if len(res.Leases) != 1 {
					t.Fatalf("at step %d the chain stalled with nothing claimable", i)
				}
				if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: res.Leases[0], Success: true}); err != nil {
					t.Fatalf("Complete at %d: %v", i, err)
				}
			}
			elapsed := time.Since(start)
			t.Logf("drained a %d-node chain in %v (%v per node)", n, elapsed, elapsed/n)

			stats, err := st.ScopeStats(ctx, "chain")
			if err != nil {
				t.Fatalf("ScopeStats: %v", err)
			}
			if stats.Succeeded != n {
				t.Fatalf("%d of %d nodes succeeded", stats.Succeeded, n)
			}
		})
	}
}

var _ = context.Background

// millionEnv opts in to TestMillionNodes. `make million` sets it.
const millionEnv = "DAGWORKER_MILLION"

// TestMillionNodes is the headline claim, checked on every backend: a graph of
// a million nodes, and per-operation costs that do not reflect its size.
//
// It seeds once and measures several operations against that one graph, rather
// than letting each guard build its own. On a networked backend the seeding is
// the expensive part -- two thousand round trips -- and paying it once per
// backend instead of once per operation is the difference between a suite that
// runs and one nobody waits for.
//
// The backends run one at a time, and nothing in this test is parallel. That
// costs wall-clock -- the total is now the sum rather than the slowest -- and
// buys the only thing the numbers are for. Run in parallel, Redis's
// per-operation costs were being sampled while PostgreSQL was seeding a
// million rows through the same Docker network stack on the same laptop, which
// inflated them by roughly an order of magnitude and made Redis look slower
// than PostgreSQL at reading one node. A measurement that reports the load
// from its own siblings is not a measurement.
//
//nolint:paralleltest // deliberately serial; see the paragraph above
func TestMillionNodes(t *testing.T) {
	// Opt-in, not merely integration-gated. This seeds three million nodes and
	// takes minutes even on a quiet machine, so the generic integration sweep
	// -- which runs every module's tests under -race with the default ten
	// minute timeout -- must not pick it up. It did, and passed only because
	// the three backends used to overlap; serialising them pushed it past the
	// timeout and turned a measurement into a broken build.
	if os.Getenv(millionEnv) == "" {
		t.Skipf("set %s=1, or run `make million`, to measure at 1,000,000 nodes", millionEnv)
	}
	if testing.Short() {
		t.Skip("seeds a million nodes")
	}
	const n = 1_000_000

	for _, backend := range perf.Backends() {
		//nolint:paralleltest // deliberately serial; see TestMillionNodes' doc comment
		t.Run(backend.Name, func(t *testing.T) {
			ctx := t.Context()
			st := backend.New(t)

			seedStart := time.Now()
			scope := perf.Scope("million")
			perf.SeedWide(t, st, scope, n)
			t.Logf("seeded %d nodes in %v (%v per node)",
				n, time.Since(seedStart), time.Since(seedStart)/n)

			stats, err := st.ScopeStats(ctx, scope)
			if err != nil {
				t.Fatalf("ScopeStats: %v", err)
			}
			if stats.Total != n {
				t.Fatalf("the scope holds %d nodes, want %d", stats.Total, n)
			}

			statsCost := timePerOp(t, 500, func(int) {
				if _, err := st.ScopeStats(ctx, scope); err != nil {
					t.Fatalf("ScopeStats: %v", err)
				}
			})
			getCost := timePerOp(t, 500, func(i int) {
				if _, err := st.GetNode(ctx, scope, perf.NodeID((i*7919)%n)); err != nil {
					t.Fatalf("GetNode: %v", err)
				}
			})

			req := dw.ClaimRequest{Scope: scope, Max: 1, Timeout: time.Hour}
			claimCost := timePerOp(t, 500, func(int) {
				res, err := st.Claim(ctx, req)
				if err != nil {
					t.Fatalf("Claim: %v", err)
				}
				if len(res.Leases) == 0 {
					t.Fatal("a million-node ready set produced no work")
				}
				if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: res.Leases[0], Success: true}); err != nil {
					t.Fatalf("Complete: %v", err)
				}
			})

			t.Logf("at %d nodes: ScopeStats %v, GetNode %v, Claim+Complete %v",
				n, statsCost, getCost, claimCost)

			// ScopeStats is the sharpest signal here: it is a counter read, so
			// any scan hiding behind it shows up as milliseconds rather than
			// microseconds at this size.
			if statsCost > 50*time.Millisecond {
				t.Errorf("ScopeStats took %v on a million-node scope -- that is a scan, not a counter", statsCost)
			}
		})
	}
}
