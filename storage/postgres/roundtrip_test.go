package postgres_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	dw "github.com/specialistvlad/dagworker"
	postgresstore "github.com/specialistvlad/dagworker/storage/postgres"
)

// Round-trip budgets.
//
// This backend's cost is not computation, it is conversation. A round trip to
// PostgreSQL costs around 185 microseconds here; a node used to take six of
// them, which is why inserting a million nodes took twenty-one minutes with
// the server essentially idle.
//
// Wall-clock benchmarks cannot guard that, because they measure the machine as
// much as the code — under load the same run varies twofold. Counting round
// trips is deterministic: it is a property of the code and nothing else. So the
// budget is asserted here, and a change that reintroduces a per-node query
// fails this test rather than showing up months later as "PostgreSQL is slow".
//
// A pgx.Batch counts as ONE round trip however many statements it carries,
// which is exactly the thing being measured.
type tripCounter struct {
	queries atomic.Int64
	batches atomic.Int64
}

func (c *tripCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	c.queries.Add(1)
	return ctx
}

func (c *tripCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (c *tripCounter) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	c.batches.Add(1)
	return ctx
}

// TraceBatchQuery deliberately counts nothing: the statements inside a batch
// travel together, and counting them would measure the wrong thing.
func (c *tripCounter) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (c *tripCounter) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

// trips is the number of network exchanges: a batch is one, a query is one.
func (c *tripCounter) trips() int64 { return c.queries.Load() + c.batches.Load() }

func (c *tripCounter) reset() {
	c.queries.Store(0)
	c.batches.Store(0)
}

func tracedStore(t *testing.T) (*postgresstore.Store, *tripCounter) {
	t.Helper()
	if os.Getenv("DAGWORKER_INTEGRATION") == "" {
		t.Skip("set DAGWORKER_INTEGRATION=1 and start the compose stack")
	}
	dsn := os.Getenv("DAGWORKER_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://dagworker:dagworker@127.0.0.1:15432/dagworker?sslmode=disable"
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	counter := &tripCounter{}
	cfg.ConnConfig.Tracer = counter

	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Skipf("postgres unreachable: %v", err)
	}
	t.Cleanup(pool.Close)

	st := postgresstore.New(pool)
	t.Cleanup(func() { _ = st.Close(context.Background()) })
	return st, counter
}

//nolint:paralleltest // counts round trips on a shared connection; a parallel sibling would be counted too
func TestRoundTripBudget_AddNodes(t *testing.T) {
	st, counter := tracedStore(t)
	ctx := t.Context()
	scope := dw.Scope(fmt.Sprintf("trips-add-%d", time.Now().UnixNano()))

	// The first call also migrates and creates the scope, so measure a later one.
	if _, err := st.AddNodes(ctx, scope, []dw.NodeSpec{{ID: "warmup"}}); err != nil {
		t.Fatalf("AddNodes(warmup): %v", err)
	}

	const batch = 200
	specs := make([]dw.NodeSpec, batch)
	for i := range specs {
		specs[i] = dw.NodeSpec{ID: dw.NodeID(fmt.Sprintf("n%05d", i))}
	}

	counter.reset()
	if _, err := st.AddNodes(ctx, scope, specs); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	trips := counter.trips()
	perNode := float64(trips) / float64(batch)

	t.Logf("AddNodes(%d): %d round trips (%d queries, %d batches) = %.2f per node",
		batch, trips, counter.queries.Load(), counter.batches.Load(), perNode)

	// Two per node is the budget: settleTouched re-reads and settles each node,
	// and its re-read is a correctness mechanism rather than overhead (a settle
	// can cascade into a node whose snapshot is then stale). Everything else --
	// the existence check, the inserts, the sequence bumps, the event writes --
	// is pipelined into a handful of exchanges for the whole batch.
	//
	// Six per node was the original cost and is what made a million nodes take
	// twenty-one minutes.
	const budget = 2.5
	if perNode > budget {
		t.Errorf("AddNodes costs %.2f round trips per node, budget is %.2f.\n"+
			"Something on this path stopped being pipelined; at ~185us per trip this is the "+
			"difference between minutes and tens of minutes for a large graph.", perNode, budget)
	}
}

//nolint:paralleltest // counts round trips on a shared connection
func TestRoundTripBudget_ClaimComplete(t *testing.T) {
	st, counter := tracedStore(t)
	ctx := t.Context()
	scope := dw.Scope(fmt.Sprintf("trips-claim-%d", time.Now().UnixNano()))

	specs := make([]dw.NodeSpec, 50)
	for i := range specs {
		specs[i] = dw.NodeSpec{ID: dw.NodeID(fmt.Sprintf("n%03d", i))}
	}
	if _, err := st.AddNodes(ctx, scope, specs); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}

	req := dw.ClaimRequest{Scope: scope, Max: 1, Timeout: time.Hour}
	// Warm the plan cache before measuring.
	res, err := st.Claim(ctx, req)
	if err != nil || len(res.Leases) == 0 {
		t.Fatalf("Claim(warmup): %v", err)
	}
	if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: res.Leases[0], Success: true}); err != nil {
		t.Fatalf("Complete(warmup): %v", err)
	}

	const cycles = 10
	counter.reset()
	for range cycles {
		res, err := st.Claim(ctx, req)
		if err != nil || len(res.Leases) == 0 {
			t.Fatalf("Claim: %v", err)
		}
		if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: res.Leases[0], Success: true}); err != nil {
			t.Fatalf("Complete: %v", err)
		}
	}
	perCycle := float64(counter.trips()) / float64(cycles)
	t.Logf("Claim+Complete: %.1f round trips per cycle", perCycle)

	// Both are transactions with real work in them, so this will never be one
	// or two. The budget exists to catch a regression, not to describe an
	// ideal: anything much above this means a loop crept onto the hot path.
	const budget = 26.0
	if perCycle > budget {
		t.Errorf("Claim+Complete costs %.1f round trips, budget is %.1f — something is looping",
			perCycle, budget)
	}
}

// The claim path must not scan. A sweep for expired leases runs before every
// claim, and if its query stops using the partial index the cost grows with the
// scope: this asserts the cost does not move between a small graph and one
// fifty times larger.
//
//nolint:paralleltest // counts round trips on a shared connection
func TestClaimCostDoesNotGrowWithScope(t *testing.T) {
	st, counter := tracedStore(t)
	ctx := t.Context()

	measure := func(nodes int) float64 {
		scope := dw.Scope(fmt.Sprintf("trips-scale-%d-%d", nodes, time.Now().UnixNano()))
		specs := make([]dw.NodeSpec, nodes)
		for i := range specs {
			specs[i] = dw.NodeSpec{ID: dw.NodeID(fmt.Sprintf("n%06d", i))}
		}
		const chunk = 500
		for start := 0; start < nodes; start += chunk {
			if _, err := st.AddNodes(ctx, scope, specs[start:min(start+chunk, nodes)]); err != nil {
				t.Fatalf("AddNodes: %v", err)
			}
		}

		req := dw.ClaimRequest{Scope: scope, Max: 1, Timeout: time.Hour}
		if _, err := st.Claim(ctx, req); err != nil {
			t.Fatalf("Claim(warmup): %v", err)
		}
		counter.reset()
		const cycles = 10
		for range cycles {
			if _, err := st.Claim(ctx, req); err != nil {
				t.Fatalf("Claim: %v", err)
			}
		}
		return float64(counter.trips()) / float64(cycles)
	}

	small := measure(200)
	large := measure(10_000)
	t.Logf("Claim round trips: %.1f at 200 nodes, %.1f at 10,000 nodes", small, large)

	if large > small*1.5 {
		t.Errorf("claiming costs %.1f trips at 10,000 nodes against %.1f at 200 — "+
			"the claim path is doing work proportional to the scope", large, small)
	}
}

var _ = sync.Once{}
