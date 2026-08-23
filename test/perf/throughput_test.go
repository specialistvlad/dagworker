package perf_test

import (
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/test/perf"
)

// TestThroughput publishes the absolute per-operation cost of each backend at a
// size big enough to be representative and small enough to run often.
//
// It asserts almost nothing. The complexity guards already assert the property
// that matters — that cost does not grow with the graph — and an absolute
// threshold here would be a promise about hardware that a shared runner breaks
// for reasons unrelated to the code. What this produces is the number a user
// comparing backends actually wants, measured rather than estimated.
//
// The one thing it does assert is a sanity floor: an operation that takes more
// than a second at this size is not slow, it is broken.
//
// Backends are measured one at a time, not in parallel: they share this
// machine, and three of them competing for it produces numbers that describe
// the contention rather than the backends.
//
//nolint:paralleltest // measurement quality depends on not competing with itself
func TestThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds tens of thousands of nodes")
	}
	const n = 30_000

	for _, backend := range perf.Backends() {
		t.Run(backend.Name, func(t *testing.T) {
			ctx := t.Context()
			st := backend.New(t)
			scope := dw.Scope("throughput")

			start := time.Now()
			perf.SeedWide(t, st, scope, n)
			insert := time.Since(start) / n

			req := dw.ClaimRequest{Scope: scope, Max: 1, Timeout: time.Hour}
			cycle := timePerOp(t, 300, func(int) {
				res, err := st.Claim(ctx, req)
				if err != nil {
					t.Fatalf("Claim: %v", err)
				}
				if len(res.Leases) == 0 {
					t.Fatal("ran out of work mid-measurement")
				}
				if _, err := st.Complete(ctx, dw.CompleteRequest{
					Lease: res.Leases[0], Success: true,
				}); err != nil {
					t.Fatalf("Complete: %v", err)
				}
			})

			// Batched claiming is what a networked backend should actually do:
			// one round trip that returns many nodes costs about what one that
			// returns a single node costs.
			batch := dw.ClaimRequest{Scope: scope, Max: 100, Timeout: time.Hour}
			var batched time.Duration
			if res, err := st.Claim(ctx, batch); err == nil && len(res.Leases) > 0 {
				bs := time.Now()
				const rounds = 20
				got := len(res.Leases)
				for range rounds {
					r, err := st.Claim(ctx, batch)
					if err != nil || len(r.Leases) == 0 {
						break
					}
					got += len(r.Leases)
				}
				if got > 0 {
					batched = time.Since(bs) / time.Duration(got)
				}
			}

			t.Logf("%s at n=%d: insert %v/node, claim+complete %v/op, batched claim %v/node",
				backend.Name, n, insert, cycle, batched)

			if insert > time.Second || cycle > time.Second {
				t.Errorf("%s: insert %v and claim+complete %v at only %d nodes — that is broken, not slow",
					backend.Name, insert, cycle, n)
			}
		})
	}
}
