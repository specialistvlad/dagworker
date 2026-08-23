// Package perf holds dagworker's performance suite: the benchmarks that
// establish absolute throughput and the complexity guards that prove the
// per-operation cost does not grow with the size of the graph.
//
// It is a separate module so that importing dagworker never drags in a
// database driver, and so the suite can depend on every backend at once.
package perf

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
)

// Backend names a store the suite can measure.
type Backend struct {
	Name string
	// New returns a fresh, empty store. It must return nil when the backend is
	// not reachable, so the suite can skip rather than fail on a developer
	// machine with no database running.
	New func(tb testing.TB) dw.Store
	// Networked distinguishes a backend whose every operation crosses a socket.
	// The complexity guards allow a networked backend more slack, because a
	// round trip dominates and makes the measurement noisier.
	Networked bool
}

// Backends returns every backend the suite should measure in this environment.
//
// The in-memory backend is always present. The networked ones are included only
// when DAGWORKER_INTEGRATION is set, so that a plain "go test ./..." on a
// laptop measures what it can reach instead of failing on what it cannot.
func Backends() []Backend {
	out := make([]Backend, 0, 3)
	out = append(out, Backend{
		Name: "memory",
		New: func(tb testing.TB) dw.Store {
			tb.Helper()
			st := memory.New()
			tb.Cleanup(func() { _ = st.Close(context.Background()) })
			return st
		},
	})
	if os.Getenv("DAGWORKER_INTEGRATION") == "" {
		return out
	}
	return append(out, integrationBackends()...)
}

// Sizes are the graph sizes every complexity guard sweeps. The span from a
// thousand to a million is what makes the ratio meaningful: three orders of
// magnitude is enough that a linear term cannot hide inside measurement noise.
var Sizes = []int{1_000, 10_000, 100_000, 1_000_000}

// SmallSizes drops the largest step, for guards whose setup is quadratic in
// wall-clock terms even though the operation under test is not.
var SmallSizes = []int{1_000, 10_000, 100_000}

// Env reads an environment variable with a fallback, so the suite points at the
// docker compose stack by default and at anything else on request.
func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// NodeID builds a fixed-width identifier. The width is fixed so that key
// length -- and therefore hashing and comparison cost -- does not itself vary
// with the graph size being measured, which would quietly contaminate the very
// ratio the guards exist to check.
func NodeID(i int) dw.NodeID { return dw.NodeID(fmt.Sprintf("n%09d", i)) }

// SeedChain fills a scope with n nodes in one long dependency chain. This is
// the worst shape for a scheduler: every completion releases exactly one
// successor, so nothing is ever batched and the ready set stays tiny.
func SeedChain(tb testing.TB, st dw.Store, scope dw.Scope, n int) {
	tb.Helper()
	seed(tb, st, scope, n, func(i int) []dw.NodeID {
		if i == 0 {
			return nil
		}
		return []dw.NodeID{NodeID(i - 1)}
	})
}

// SeedWide fills a scope with n independent nodes: the best shape, where the
// whole graph is claimable at once and the ready set is as large as it gets.
func SeedWide(tb testing.TB, st dw.Store, scope dw.Scope, n int) {
	tb.Helper()
	seed(tb, st, scope, n, func(int) []dw.NodeID { return nil })
}

// SeedFanIn builds a graph where each node depends on its three immediate
// predecessors: in-degree three, out-degree three, which is the shape a real
// pipeline tends to have.
//
// An earlier version pointed every node at the same three roots. That is a
// legitimate graph, but it gives those roots a third of the graph as successors
// each, so completing one of them costs a third of the graph -- and the
// benchmark then measured that rather than the ordinary fan-out it was named
// for. The cost is genuinely proportional to out-degree; the mistake was
// choosing a shape whose out-degree grew with n.
func SeedFanIn(tb testing.TB, st dw.Store, scope dw.Scope, n int) {
	tb.Helper()
	seed(tb, st, scope, n, func(i int) []dw.NodeID {
		switch {
		case i < 3:
			return nil
		default:
			return []dw.NodeID{NodeID(i - 1), NodeID(i - 2), NodeID(i - 3)}
		}
	})
}

func seed(tb testing.TB, st dw.Store, scope dw.Scope, n int, deps func(int) []dw.NodeID) {
	tb.Helper()
	ctx := context.Background()

	// Batches must stay inside the scope's own limit, and large enough that the
	// per-call overhead does not dominate the seeding time.
	const batch = 500
	if err := st.SetScopeConfig(ctx, scope, dw.ScopeConfig{
		MaxBatchSize:        batch,
		DefaultLeaseTimeout: time.Hour,
		MaxAttempts:         1,
	}); err != nil {
		tb.Fatalf("SetScopeConfig: %v", err)
	}

	specs := make([]dw.NodeSpec, 0, batch)
	for i := range n {
		specs = append(specs, dw.NodeSpec{ID: NodeID(i), Deps: deps(i)})
		if len(specs) == batch {
			if _, err := st.AddNodes(ctx, scope, specs); err != nil {
				tb.Fatalf("AddNodes at %d: %v", i, err)
			}
			specs = specs[:0]
		}
	}
	if len(specs) > 0 {
		if _, err := st.AddNodes(ctx, scope, specs); err != nil {
			tb.Fatalf("AddNodes (final): %v", err)
		}
	}
}
