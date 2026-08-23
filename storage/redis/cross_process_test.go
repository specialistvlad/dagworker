package redis

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	dw "github.com/specialistvlad/dagworker"
)

// TestCrossProcessClaimIsExclusive is T-CLAIM-ATOMIC generalized across two
// independent *Store values, each on its own client connection, standing in
// for two separate host processes — the scenario the whole atomicity
// requirement exists for, which a single-process conformance run (however
// many goroutines it races) cannot by itself distinguish from "one process's
// in-memory mutex happened to work."
func TestCrossProcessClaimIsExclusive(t *testing.T) {
	skipUnlessIntegration(t)
	t.Parallel()

	addr := redisAddr()
	ns := randomKeyspace(t)
	scope := dw.Scope("race")

	clientA := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = clientA.Close() })
	clientB := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = clientB.Close() })

	storeA := New(clientA, withKeyspace(ns))
	storeB := New(clientB, withKeyspace(ns))
	t.Cleanup(func() {
		_ = storeA.Close(context.Background())
		_ = storeB.Close(context.Background())
		cleanupKeyspace(t, clientA, ns)
	})

	ctx := context.Background()
	const n = 40
	specs := make([]dw.NodeSpec, n)
	for i := range specs {
		specs[i] = dw.NodeSpec{ID: dw.NodeID(fmt.Sprintf("node-%03d", i))}
	}
	if _, err := storeA.AddNodes(ctx, scope, specs); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}

	var mu sync.Mutex
	seen := make(map[dw.NodeID]int)
	var wg sync.WaitGroup

	race := func(st *Store) {
		defer wg.Done()
		for {
			res, err := st.Claim(ctx, dw.ClaimRequest{
				Scope: scope, Max: 3, Timeout: 5 * time.Second, WorkerID: "racer",
			})
			if err != nil {
				t.Errorf("Claim: %v", err)
				return
			}
			if len(res.Leases) == 0 {
				return
			}
			mu.Lock()
			for _, l := range res.Leases {
				seen[l.NodeID]++
			}
			mu.Unlock()
		}
	}

	for range 4 {
		wg.Add(2)
		go race(storeA)
		go race(storeB)
	}
	wg.Wait()

	if len(seen) != n {
		t.Fatalf("two independent *Store values claimed %d distinct nodes, want %d", len(seen), n)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("node %q was granted %d times across two independent *Store values sharing one Redis", id, count)
		}
	}
}
