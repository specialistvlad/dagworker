package perf_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/test/perf"
)

// benchSizes are the graph sizes the throughput benchmarks run at. Absolute
// numbers belong here and in a benchstat-tracked nightly job, never in a CI
// gate: a shared runner's absolute throughput says more about its neighbours
// than about this code. The CI gate is the ratio guard in complexity_test.go.
var benchSizes = []int{1_000, 100_000, 1_000_000}

func eachBackend(b *testing.B, fn func(b *testing.B, backend perf.Backend, n int)) {
	b.Helper()
	for _, backend := range perf.Backends() {
		for _, n := range benchSizes {
			b.Run(fmt.Sprintf("%s/n=%d", backend.Name, n), func(b *testing.B) {
				fn(b, backend, n)
			})
		}
	}
}

func BenchmarkClaim(b *testing.B) {
	eachBackend(b, func(b *testing.B, backend perf.Backend, n int) {
		ctx := b.Context()
		st := backend.New(b)
		perf.SeedWide(b, st, "bench", n)
		req := dw.ClaimRequest{Scope: "bench", Max: 1, Timeout: time.Hour}

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
		ctx := b.Context()
		st := backend.New(b)
		perf.SeedWide(b, st, "bench", n)
		req := dw.ClaimRequest{Scope: "bench", Max: batch, Timeout: time.Hour}

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
		ctx := b.Context()
		st := backend.New(b)
		perf.SeedWide(b, st, "bench", n)

		b.ReportAllocs()
		b.ResetTimer()
		i := 0
		for b.Loop() {
			// Stride through the keyspace so the measurement pays the cache
			// misses a real workload pays.
			if _, err := st.GetNode(ctx, "bench", perf.NodeID((i*7919)%n)); err != nil {
				b.Fatalf("GetNode: %v", err)
			}
			i++
		}
	})
}

func BenchmarkClaimComplete(b *testing.B) {
	eachBackend(b, func(b *testing.B, backend perf.Backend, n int) {
		ctx := b.Context()
		st := backend.New(b)
		perf.SeedFanIn(b, st, "bench", n)
		req := dw.ClaimRequest{Scope: "bench", Max: 1, Timeout: time.Hour}

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
			ctx := b.Context()
			st := backend.New(b)
			if err := st.SetScopeConfig(ctx, "bench", dw.ScopeConfig{MaxBatchSize: batch}); err != nil {
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
				if _, err := st.AddNodes(ctx, "bench", specs); err != nil {
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
			perf.SeedChain(t, st, "footprint", n)

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
