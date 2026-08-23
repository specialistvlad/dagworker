package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	dw "github.com/specialistvlad/dagworker"
)

// TestDoorbellSharesOneSubscription pins down a resource leak that no pool
// limit catches.
//
// WaitForWork opened its own Pub/Sub subscription, and go-redis gives each one
// its own connection, tracked separately from PoolSize — so N workers blocked
// on a claim opened N TCP connections to Redis, with nothing bounding N.
// Measured before the fix: 40 waiters, 40 Pub/Sub connections,
// connected_clients 44 against a PoolSize of 4. The pool itself never
// starves, which is precisely why this is easy to miss; what runs out is
// Redis's own maxclients and the per-connection memory behind it, at a fleet
// size that is entirely ordinary.
//
// One subscription per scope, fanned out to every waiter, is what memory and
// PostgreSQL already do.
func TestDoorbellSharesOneSubscription(t *testing.T) {
	skipUnlessIntegration(t)
	t.Parallel()

	addr := redisAddr()
	// A pool deliberately far smaller than the number of waiters: with one
	// connection per waiter this test cannot pass, and with one shared
	// subscription the pool size is irrelevant.
	const waiters = 40
	client := goredis.NewClient(&goredis.Options{
		Addr:        addr,
		PoolSize:    4,
		PoolTimeout: 2 * time.Second,
	})
	t.Cleanup(func() { _ = client.Close() })

	ns := randomKeyspace(t)
	st := New(client, withKeyspace(ns))
	t.Cleanup(func() {
		_ = st.Close(context.Background())
		cleanupKeyspace(t, client, ns)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	const scope = dw.Scope("busy")
	if _, err := st.AddNodes(ctx, scope, []dw.NodeSpec{{ID: "seed"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	// Take the one ready node so every waiter below genuinely parks.
	if _, err := st.Claim(ctx, dw.ClaimRequest{Scope: scope, Max: 1, Timeout: time.Hour}); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, waiters)
	parked := make(chan struct{}, waiters)
	for range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			parked <- struct{}{}
			if err := st.WaitForWork(ctx, scope, nil); err != nil {
				errs <- err
			}
		}()
	}
	for range waiters {
		<-parked
	}

	// Every waiter is parked. However many there are, they share one
	// subscription per scope -- the number of connections is a property of the
	// number of scopes being watched, never of the size of the fleet.
	time.Sleep(300 * time.Millisecond)
	if active := client.PoolStats().PubSubStats.Active; active > 1 {
		t.Errorf("%d workers parked on one scope's doorbell hold %d Pub/Sub connections, want 1",
			waiters, active)
	}
	if _, err := st.GetNode(ctx, scope, "seed"); err != nil {
		t.Fatalf("with %d workers parked on the doorbell, an ordinary read failed: %v", waiters, err)
	}

	// And the doorbell still rings for all of them.
	if _, err := st.AddNodes(ctx, scope, []dw.NodeSpec{{ID: "work"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("waiters did not all wake after a node became ready")
	}
	close(errs)
	for err := range errs {
		t.Errorf("WaitForWork: %v", err)
	}
}
