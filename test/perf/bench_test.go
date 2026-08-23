package perf_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/test/perf"
)

// Graph sizes the throughput benchmarks run at, chosen per backend rather than
// from one shared list.
//
// The shared list was {1e3, 1e5, 1e6} for everything, which meant every
// benchmark seeded a million nodes into PostgreSQL before measuring anything --
// at a measured 454us per node that is seven and a half minutes of setup, per
// benchmark, to produce a number about an operation that takes milliseconds.
// Five benchmarks and three backends made `make bench` a half-hour job with a
// 30-minute timeout, which is why nobody ran it.
//
// Absolute throughput on a shared machine says more about that machine's
// neighbours than about this code, so the size only has to be large enough that
// the data structure under test is not trivially small. The guard that actually
// protects complexity is the ratio sweep in complexity_test.go, and the
// million-node figures come from `make million`.
// The sizes must also exceed the iteration count, because three of these
// benchmarks CONSUME the graph -- a claim removes a node from the ready set --
// and skip rather than lie once it is exhausted. `make benchmark` pins the
// iteration count with -benchtime=1000x precisely so the graph can be sized
// against it instead of against a wall-clock budget nobody can predict.
var (
	// memorySizes can afford to be large: seeding is ~1us per node.
	memorySizes = []int{100_000}
	// networkedSizes cannot. Every node is a round trip, ~454us on PostgreSQL.
	networkedSizes = []int{5_000}
)

func sizesFor(backend perf.Backend) []int {
	if os.Getenv("DAGWORKER_PERF_FULL") != "" {
		return []int{1_000, 100_000, 1_000_000}
	}
	if backend.Networked {
		return networkedSizes
	}
	return memorySizes
}

func eachBackend(b *testing.B, fn func(b *testing.B, backend perf.Backend, n int)) {
	b.Helper()
	for _, backend := range perf.Backends() {
		for _, n := range sizesFor(backend) {
			b.Run(fmt.Sprintf("%s/n=%d", backend.Name, n), func(b *testing.B) {
				b.Helper()
				fn(b, backend, n)
			})
		}
	}
}

func BenchmarkClaim(b *testing.B) {
	eachBackend(b, func(b *testing.B, backend perf.Backend, n int) {
		b.Helper()
		ctx := b.Context()
		st := backend.New(b)
		scope := perf.Scope("bench")
		perf.SeedWide(b, st, scope, n)
		req := dw.ClaimRequest{Scope: scope, Max: 1, Timeout: time.Hour}

		b.ReportAllocs()
		b.ResetTimer()
		claimed := 0
		for b.Loop() {
			res, err := st.Claim(ctx, req)
			if err != nil {
				b.Fatalf("Claim: %v", err)
			}
			if len(res.Leases) == 0 {
				// The graph is exhausted; stopping the timer keeps the empty
				// claims out of the measurement rather than flattering it.
				b.StopTimer()
				b.Skipf("exhausted the graph after %d claims", claimed)
			}
			claimed++
		}
	})
}

// Batched claiming is the shape a networked backend must use: one round trip
// that returns many nodes costs about what one that returns a single node
// costs, so a worker pool claiming singly spends its life on the wire.
func BenchmarkClaimBatch(b *testing.B) {
	const batch = 100
	eachBackend(b, func(b *testing.B, backend perf.Backend, n int) {
		b.Helper()
		ctx := b.Context()
		st := backend.New(b)
		scope := perf.Scope("bench")
		perf.SeedWide(b, st, scope, n)
		req := dw.ClaimRequest{Scope: scope, Max: batch, Timeout: time.Hour}

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			res, err := st.Claim(ctx, req)
			if err != nil {
				b.Fatalf("Claim: %v", err)
			}
			if len(res.Leases) == 0 {
				b.StopTimer()
				b.Skip("exhausted the graph")
			}
		}
	})
}

func BenchmarkGetNode(b *testing.B) {
	eachBackend(b, func(b *testing.B, backend perf.Backend, n int) {
		b.Helper()
		ctx := b.Context()
		st := backend.New(b)
		scope := perf.Scope("bench")
		perf.SeedWide(b, st, scope, n)

		b.ReportAllocs()
		b.ResetTimer()
		i := 0
		for b.Loop() {
			// Stride through the keyspace so the measurement pays the cache
			// misses a real workload pays.
			if _, err := st.GetNode(ctx, scope, perf.NodeID((i*7919)%n)); err != nil {
				b.Fatalf("GetNode: %v", err)
			}
			i++
		}
	})
}

func BenchmarkClaimComplete(b *testing.B) {
	eachBackend(b, func(b *testing.B, backend perf.Backend, n int) {
		b.Helper()
		ctx := b.Context()
		st := backend.New(b)
		scope := perf.Scope("bench")
		perf.SeedFanIn(b, st, scope, n)
		req := dw.ClaimRequest{Scope: scope, Max: 1, Timeout: time.Hour}

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			res, err := st.Claim(ctx, req)
			if err != nil {
				b.Fatalf("Claim: %v", err)
			}
			if len(res.Leases) == 0 {
				b.StopTimer()
				b.Skip("exhausted the graph")
			}
			if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: res.Leases[0], Success: true}); err != nil {
				b.Fatalf("Complete: %v", err)
			}
		}
	})
}

func BenchmarkAddNodesBatch(b *testing.B) {
	const batch = 100
	for _, backend := range perf.Backends() {
		b.Run(backend.Name, func(b *testing.B) {
			b.Helper()
			ctx := b.Context()
			st := backend.New(b)
			scope := perf.Scope("bench")
			if err := st.SetScopeConfig(ctx, scope, dw.ScopeConfig{MaxBatchSize: batch}); err != nil {
				b.Fatalf("SetScopeConfig: %v", err)
			}

			specs := make([]dw.NodeSpec, batch)
			next := 0
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for i := range specs {
					specs[i] = dw.NodeSpec{ID: perf.NodeID(next)}
					next++
				}
				if _, err := st.AddNodes(ctx, scope, specs); err != nil {
					b.Fatalf("AddNodes: %v", err)
				}
			}
			b.ReportMetric(float64(batch), "nodes/op")
		})
	}
}

// TestMemoryFootprint is not an assertion so much as a published number: how
// much memory a million-node graph actually costs, measured rather than
// estimated. It runs only for in-process backends, where the question means
// anything.
//
//nolint:paralleltest // measures process-wide heap; a concurrent test would poison the reading
func TestMemoryFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a million nodes")
	}
	const n = 1_000_000
	for _, backend := range perf.Backends() {
		if backend.Networked {
			continue
		}
		t.Run(backend.Name, func(t *testing.T) {
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			st := backend.New(t)
			perf.SeedChain(t, st, perf.Scope("footprint"), n)

			runtime.GC()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			used := after.HeapAlloc - before.HeapAlloc
			perNode := float64(used) / float64(n)
			t.Logf("%d nodes in a chain: %.1f MiB heap, %.0f bytes per node, %d GC cycles",
				n, float64(used)/(1<<20), perNode, after.NumGC-before.NumGC)

			// A loose sanity bound rather than a tuning target: if a node ever
			// costs kilobytes, something is holding a reference it should not.
			if perNode > 2048 {
				t.Errorf("a node costs %.0f bytes, which is far more than its fields explain", perNode)
			}
			runtime.KeepAlive(st)
		})
	}
}

var _ = context.Background
