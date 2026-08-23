package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/memory"
)

func newStore(t *testing.T, opts ...memory.Option) (*memory.Store, *dagstoretest.FakeClock) {
	t.Helper()
	clk := dagstoretest.NewFakeClock()
	st := memory.New(append([]memory.Option{
		memory.WithClock(clk),
		memory.WithJitter(func(int64) int64 { return 0 }),
	}, opts...)...)
	t.Cleanup(func() { _ = st.Close(context.Background()) })
	return st, clk
}

// A closed store must refuse work rather than operating on state nobody can
// reach, and must unblock anything parked inside it.
func TestClosedStoreRefusesWork(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	ctx := t.Context()

	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{{ID: "a"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if err := st.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{{ID: "b"}}); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("AddNodes after Close gave %v", err)
	}
	if _, err := st.GetNode(ctx, "s", "a"); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("GetNode after Close gave %v", err)
	}
	if _, err := st.Scopes(ctx); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("Scopes after Close gave %v", err)
	}
	if _, err := st.Claim(ctx, dw.ClaimRequest{Scope: "s"}); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("Claim after Close gave %v", err)
	}
	if _, err := st.ScopeStats(ctx, "s"); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("ScopeStats after Close gave %v", err)
	}
	if _, err := st.ScopeConfig(ctx, "s"); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("ScopeConfig after Close gave %v", err)
	}
	if err := st.SetScopeConfig(ctx, "s", dw.ScopeConfig{}); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("SetScopeConfig after Close gave %v", err)
	}
	if err := st.Seal(ctx, "s"); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("Seal after Close gave %v", err)
	}
	if _, err := st.Inspect(ctx, "s", "a"); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("Inspect after Close gave %v", err)
	}
	if _, err := st.ListNodes(ctx, "s", dw.ListOptions{}); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("ListNodes after Close gave %v", err)
	}
	if _, err := st.Sweep(ctx, "s", 10); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("Sweep after Close gave %v", err)
	}
	if _, _, err := st.CollectTerminal(ctx, "s", time.Now(), 10); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("CollectTerminal after Close gave %v", err)
	}
	if err := st.WaitForWork(ctx, "s", nil); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("WaitForWork after Close gave %v", err)
	}
	if _, err := st.Watch(ctx, dw.WatchRequest{Scope: "s"}); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("Watch after Close gave %v", err)
	}
}

// Close must release anything already parked, not just refuse new callers.
func TestCloseUnblocksAWaiter(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	ctx := t.Context()

	done := make(chan error, 1)
	go func() { done <- st.WaitForWork(ctx, "quiet", nil) }()
	time.Sleep(30 * time.Millisecond)

	if err := st.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, dw.ErrClosed) {
			t.Fatalf("a parked waiter woke with %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not release a parked waiter")
	}
}

func TestUnknownScopeAndNodeReads(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	ctx := t.Context()

	// Asking about a scope nobody has written to is not an error.
	if _, err := st.ScopeStats(ctx, "ghost"); err != nil {
		t.Fatalf("ScopeStats on an unknown scope: %v", err)
	}
	if cfg, err := st.ScopeConfig(ctx, "ghost"); err != nil || cfg != (dw.ScopeConfig{}) {
		t.Fatalf("ScopeConfig on an unknown scope gave %+v, %v", cfg, err)
	}
	if page, err := st.ListNodes(ctx, "ghost", dw.ListOptions{}); err != nil || len(page.Nodes) != 0 {
		t.Fatalf("ListNodes on an unknown scope gave %+v, %v", page, err)
	}
	if res, err := st.Claim(ctx, dw.ClaimRequest{Scope: "ghost"}); err != nil || len(res.Leases) != 0 {
		t.Fatalf("Claim on an unknown scope gave %+v, %v", res, err)
	}
	if res, err := st.Sweep(ctx, "ghost", 10); err != nil || res.Reclaimed != 0 {
		t.Fatalf("Sweep on an unknown scope gave %+v, %v", res, err)
	}
	if res, err := st.CancelScope(ctx, "ghost"); err != nil || len(res) != 0 {
		t.Fatalf("CancelScope on an unknown scope gave %v", err)
	}
	if n, more, err := st.CollectTerminal(ctx, "ghost", time.Now(), 10); err != nil || n != 0 || more {
		t.Fatalf("CollectTerminal on an unknown scope gave %d, %v, %v", n, more, err)
	}

	// But asking for a node inside one is.
	if _, err := st.GetNode(ctx, "ghost", "a"); !errors.Is(err, dw.ErrNotFound) {
		t.Fatalf("GetNode in an unknown scope gave %v", err)
	}
	if _, err := st.Inspect(ctx, "ghost", "a"); !errors.Is(err, dw.ErrNotFound) {
		t.Fatalf("Inspect in an unknown scope gave %v", err)
	}
}

func TestLeaseOperationsOnMissingNodes(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	ctx := t.Context()
	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{{ID: "a"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}

	ghost := dw.Lease{Scope: "s", NodeID: "nope", Epoch: 1}
	if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: ghost, Success: true}); !errors.Is(err, dw.ErrNotFound) {
		t.Fatalf("completing a missing node gave %v", err)
	}
	if _, err := st.Extend(ctx, dw.ExtendRequest{Lease: ghost, Timeout: time.Second}); !errors.Is(err, dw.ErrNotFound) {
		t.Fatalf("extending a missing node gave %v", err)
	}

	malformed := dw.Lease{}
	if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: malformed}); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("completing with a zero lease gave %v", err)
	}
	if _, err := st.Extend(ctx, dw.ExtendRequest{Lease: malformed}); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("extending with a zero lease gave %v", err)
	}

	// Completing a node that is not claimed at all is a fencing failure, not a
	// missing node: the node exists, the caller just does not hold it.
	unclaimed := dw.Lease{Scope: "s", NodeID: "a", Epoch: 1}
	if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: unclaimed, Success: true}); !errors.Is(err, dw.ErrLeaseMismatch) {
		t.Fatalf("completing an unclaimed node gave %v", err)
	}
	if _, err := st.Extend(ctx, dw.ExtendRequest{Lease: unclaimed, Timeout: time.Second}); !errors.Is(err, dw.ErrLeaseMismatch) {
		t.Fatalf("extending an unclaimed node gave %v", err)
	}
}

func TestBatchAndPayloadLimits(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	ctx := t.Context()

	if err := st.SetScopeConfig(ctx, "s", dw.ScopeConfig{MaxBatchSize: 2, PayloadCap: 4}); err != nil {
		t.Fatalf("SetScopeConfig: %v", err)
	}
	tooMany := []dw.NodeSpec{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	if _, err := st.AddNodes(ctx, "s", tooMany); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("an oversized batch gave %v", err)
	}
	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{{ID: "a", Payload: []byte("toolong")}}); !errors.Is(err, dw.ErrPayloadTooLarge) {
		t.Fatalf("an oversized payload gave %v", err)
	}
	// An empty batch is a no-op, not an error.
	if eff, err := st.AddNodes(ctx, "s", nil); err != nil || len(eff) != 0 {
		t.Fatalf("an empty batch gave %v, %v", eff, err)
	}
	if eff, err := st.AddEdges(ctx, "s", nil); err != nil || len(eff) != 0 {
		t.Fatalf("an empty edge batch gave %v, %v", eff, err)
	}
	if eff, err := st.RemoveEdges(ctx, "s", nil); err != nil || len(eff) != 0 {
		t.Fatalf("an empty edge removal gave %v, %v", eff, err)
	}
	if eff, err := st.Cancel(ctx, "s", nil); err != nil || len(eff) != 0 {
		t.Fatalf("cancelling nothing gave %v, %v", eff, err)
	}

	// An oversized result on Ack is rejected the same way an oversized payload is.
	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{{ID: "ok"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	res, err := st.Claim(ctx, dw.ClaimRequest{Scope: "s", Max: 1})
	if err != nil || len(res.Leases) != 1 {
		t.Fatalf("Claim gave %v, %v", res, err)
	}
	_, err = st.Complete(ctx, dw.CompleteRequest{Lease: res.Leases[0], Success: true, Result: []byte("far too long")})
	if !errors.Is(err, dw.ErrPayloadTooLarge) {
		t.Fatalf("an oversized result gave %v", err)
	}
}

func TestStoreOptions(t *testing.T) {
	t.Parallel()
	st := memory.New(
		memory.WithScopeDefaults(dw.ScopeConfig{MaxAttempts: 11}),
		memory.WithEventLogSize(0),  // ignored: not a usable size
		memory.WithEventLogSize(16), // applied
		memory.WithJitter(nil),      // ignored: nil would panic on use
		memory.WithClock(nil),       // a nil clock is refused rather than stored
	)
	t.Cleanup(func() { _ = st.Close(context.Background()) })

	cfg, err := st.ScopeConfig(t.Context(), "fresh")
	if err != nil {
		t.Fatalf("ScopeConfig: %v", err)
	}
	if cfg.MaxAttempts != 11 {
		t.Fatalf("scope defaults were not applied: %+v", cfg)
	}
	// The store must still work with a nil clock option supplied.
	if _, err := st.AddNodes(t.Context(), "fresh", []dw.NodeSpec{{ID: "a"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
}

func TestCapabilitiesAreHonest(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	caps := st.Capabilities()
	for _, c := range []dw.Capability{dw.CapList, dw.CapDurableEvents, dw.CapDoorbell, dw.CapCollect} {
		if !caps.Has(c) {
			t.Fatalf("the in-memory store does not report capability %d, which it implements", c)
		}
	}
	// It must NOT claim what it cannot do: state dies with the process, and no
	// other process can reach it.
	if caps.Has(dw.CapDurableStorage) {
		t.Fatal("the in-memory store claims durable storage")
	}
	if caps.Has(dw.CapCrossProcess) {
		t.Fatal("the in-memory store claims cross-process sharing")
	}
}

// Handles are recycled after a node is deleted; a recycled slot must not
// inherit anything from its previous occupant.
func TestHandleReuseAfterRemoval(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	ctx := t.Context()

	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{{ID: "first", Kind: "k", Priority: 9}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if _, err := st.RemoveNode(ctx, "s", "first", dw.CascadeReject); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{{ID: "second"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}

	n, err := st.GetNode(ctx, "s", "second")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if n.Kind != "" || n.Priority != 0 || n.Attempt != 0 || n.Status != dw.StatusNew {
		t.Fatalf("a recycled slot carried state from its previous occupant: %+v", n)
	}
	if st2, err := st.ScopeStats(ctx, "s"); err != nil || st2.Total != 1 {
		t.Fatalf("stats after reuse: %+v, %v", st2, err)
	}
}

// Waiting on a specific kind must not be woken by work of a different kind:
// a GPU worker that wakes for every CPU node burns a claim attempt each time.
func TestWaitForWorkFiltersByKind(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	ctx := t.Context()

	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{{ID: "c", Kind: "cpu"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	// A waiter for an existing kind returns at once.
	if err := st.WaitForWork(ctx, "s", []string{"cpu"}); err != nil {
		t.Fatalf("WaitForWork(cpu): %v", err)
	}
	// A waiter for a kind with nothing ready must park.
	short, cancel := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancel()
	if err := st.WaitForWork(short, "s", []string{"gpu"}); err == nil {
		t.Fatal("WaitForWork(gpu) returned while only cpu work existed")
	}
	// And with no kind named, any work counts.
	if err := st.WaitForWork(ctx, "s", nil); err != nil {
		t.Fatalf("WaitForWork(any): %v", err)
	}
}

func TestListNodesFiltersByKind(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	ctx := t.Context()
	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{
		{ID: "a", Kind: "cpu"}, {ID: "b", Kind: "gpu"}, {ID: "c", Kind: "cpu"},
	}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	page, err := st.ListNodes(ctx, "s", dw.ListOptions{Kinds: []string{"cpu"}, Limit: 10})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(page.Nodes) != 2 {
		t.Fatalf("kind filter returned %d nodes, want 2", len(page.Nodes))
	}
	for _, n := range page.Nodes {
		if n.Kind != "cpu" {
			t.Fatalf("kind filter returned a %q node", n.Kind)
		}
	}
}

// Collection must never take a node that still has a successor, and must
// detach the ones it does take from their predecessors -- otherwise the
// survivors keep dangling references to nodes that no longer exist.
func TestCollectSkipsNodesWithLiveSuccessors(t *testing.T) {
	t.Parallel()
	st, clk := newStore(t)
	ctx := t.Context()

	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{
		{ID: "up", Kind: "up"},
		{ID: "down", Kind: "down", Trigger: dw.TriggerAllDone, Deps: []dw.NodeID{"up"}},
	}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}

	// Finish only the predecessor. It is terminal and old, but its successor
	// is still live, so it must survive collection.
	res, err := st.Claim(ctx, dw.ClaimRequest{Scope: "s", Kinds: []string{"up"}, Max: 1})
	if err != nil || len(res.Leases) != 1 {
		t.Fatalf("Claim: %v, %v", res, err)
	}
	if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: res.Leases[0], Success: true}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	clk.Advance(time.Hour)

	n, _, err := st.CollectTerminal(ctx, "s", clk.Now(), 10)
	if err != nil {
		t.Fatalf("CollectTerminal: %v", err)
	}
	if n != 0 {
		t.Fatalf("collected %d nodes while one still had a live successor", n)
	}
	if _, err := st.GetNode(ctx, "s", "up"); err != nil {
		t.Fatalf("a node with a live successor was collected: %v", err)
	}

	// Finish the successor too, and now the whole chain is collectable. How
	// many passes that takes is not specified -- map order decides whether the
	// predecessor becomes eligible within the same pass that removes its
	// successor -- so the assertion is on the end state.
	down, err := st.Claim(ctx, dw.ClaimRequest{Scope: "s", Kinds: []string{"down"}, Max: 1})
	if err != nil || len(down.Leases) != 1 {
		t.Fatalf("Claim: %v, %v", down, err)
	}
	if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: down.Leases[0], Success: true}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	clk.Advance(time.Hour)

	for range 5 {
		if _, _, err := st.CollectTerminal(ctx, "s", clk.Now(), 10); err != nil {
			t.Fatalf("CollectTerminal: %v", err)
		}
	}
	stats, err := st.ScopeStats(ctx, "s")
	if err != nil {
		t.Fatalf("ScopeStats: %v", err)
	}
	if stats.Total != 0 {
		t.Fatalf("%d nodes survived collection of a fully finished chain", stats.Total)
	}
}

func TestCollectRespectsItsLimit(t *testing.T) {
	t.Parallel()
	st, clk := newStore(t)
	ctx := t.Context()
	specs := make([]dw.NodeSpec, 5)
	for i := range specs {
		specs[i] = dw.NodeSpec{ID: dw.NodeID(string(rune('a' + i)))}
	}
	if _, err := st.AddNodes(ctx, "s", specs); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	res, err := st.Claim(ctx, dw.ClaimRequest{Scope: "s", Max: 5})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	for _, l := range res.Leases {
		if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: l, Success: true}); err != nil {
			t.Fatalf("Complete: %v", err)
		}
	}
	clk.Advance(time.Hour)

	n, more, err := st.CollectTerminal(ctx, "s", clk.Now(), 2)
	if err != nil {
		t.Fatalf("CollectTerminal: %v", err)
	}
	if n != 2 || !more {
		t.Fatalf("a limited collection took %d with more=%v, want 2 and true", n, more)
	}
}

// Unlinking must undo whichever tally the edge contributed to, so a removed
// dependency does not leave a successor permanently mis-counted.
func TestRemoveEdgeUndoesEveryOutcomeTally(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		complete func(t *testing.T, st *memory.Store, ctx context.Context, l dw.Lease)
	}{
		{"succeeded", func(t *testing.T, st *memory.Store, ctx context.Context, l dw.Lease) {
			if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: l, Success: true}); err != nil {
				t.Fatalf("Complete: %v", err)
			}
		}},
		{"skipped", func(t *testing.T, st *memory.Store, ctx context.Context, l dw.Lease) {
			if _, err := st.Complete(ctx, dw.CompleteRequest{
				Lease: l, Success: false, Reason: dw.ReasonSkipped,
			}); err != nil {
				t.Fatalf("Complete: %v", err)
			}
		}},
		{"failed", func(t *testing.T, st *memory.Store, ctx context.Context, l dw.Lease) {
			if _, err := st.Complete(ctx, dw.CompleteRequest{
				Lease: l, Success: false, Reason: dw.ReasonWorkerError,
			}); err != nil {
				t.Fatalf("Complete: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := newStore(t)
			ctx := t.Context()
			if err := st.SetScopeConfig(ctx, "s", dw.ScopeConfig{MaxAttempts: 1}); err != nil {
				t.Fatalf("SetScopeConfig: %v", err)
			}
			// The successor uses all_done so it survives whatever the
			// predecessor did, leaving the edge there to remove.
			if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{
				{ID: "up", Kind: "up"},
				{ID: "down", Kind: "down", Trigger: dw.TriggerAllDone, Deps: []dw.NodeID{"up"}},
			}); err != nil {
				t.Fatalf("AddNodes: %v", err)
			}
			res, err := st.Claim(ctx, dw.ClaimRequest{Scope: "s", Kinds: []string{"up"}, Max: 1})
			if err != nil || len(res.Leases) != 1 {
				t.Fatalf("Claim: %v, %v", res, err)
			}
			tc.complete(t, st, ctx, res.Leases[0])

			insp, err := st.Inspect(ctx, "s", "down")
			if err != nil {
				t.Fatalf("Inspect: %v", err)
			}
			if insp.Deps.Total() != 1 {
				t.Fatalf("the successor tallies %d dependencies, want 1", insp.Deps.Total())
			}

			if _, err := st.RemoveEdges(ctx, "s", []dw.Edge{{From: "up", To: "down"}}); err != nil {
				t.Fatalf("RemoveEdges: %v", err)
			}
			after, err := st.Inspect(ctx, "s", "down")
			if err != nil {
				t.Fatalf("Inspect: %v", err)
			}
			if after.Deps.Total() != 0 {
				t.Fatalf("after removal the successor still tallies %+v", after.Deps)
			}
		})
	}
}

func TestRemoveEdgeThatDoesNotExist(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	ctx := t.Context()
	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{{ID: "a"}, {ID: "b"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	// Removing an edge that was never there is a no-op, not an error: removal
	// should be idempotent so a retry after an ambiguous failure is safe.
	if _, err := st.RemoveEdges(ctx, "s", []dw.Edge{{From: "a", To: "b"}}); err != nil {
		t.Fatalf("removing an absent edge: %v", err)
	}
}

// A node's own retry bounds override the scope's, field by field.
func TestNodeLevelRetryBounds(t *testing.T) {
	t.Parallel()
	st, clk := newStore(t)
	ctx := t.Context()
	if err := st.SetScopeConfig(ctx, "s", dw.ScopeConfig{
		MaxAttempts: 5, RetryBaseDelay: time.Hour, RetryMaxDelay: time.Hour,
	}); err != nil {
		t.Fatalf("SetScopeConfig: %v", err)
	}
	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{{
		ID:    "quick",
		Retry: dw.RetryPolicy{BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	res, err := st.Claim(ctx, dw.ClaimRequest{Scope: "s", Max: 1})
	if err != nil || len(res.Leases) != 1 {
		t.Fatalf("Claim: %v, %v", res, err)
	}
	out, err := st.Complete(ctx, dw.CompleteRequest{
		Lease: res.Leases[0], Success: false, Reason: dw.ReasonWorkerError,
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if !out.Retrying {
		t.Fatal("the node did not schedule a retry")
	}
	// The node's millisecond backoff applies, not the scope's hour.
	clk.Advance(time.Second)
	again, err := st.Claim(ctx, dw.ClaimRequest{Scope: "s", Max: 1})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(again.Leases) != 1 {
		t.Fatal("the retry did not become claimable, so the scope's backoff was used")
	}
}

func TestCancelledAndRemovedCarryTheirReason(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	ctx := t.Context()
	if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{
		{ID: "a"}, {ID: "b", Deps: []dw.NodeID{"a"}}, {ID: "x"}, {ID: "y", Deps: []dw.NodeID{"x"}},
	}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if _, err := st.Cancel(ctx, "s", []dw.NodeID{"a"}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	n, err := st.GetNode(ctx, "s", "a")
	if err != nil || n.Reason != dw.ReasonCancelled || n.Message == "" {
		t.Fatalf("a cancelled node is %+v", n)
	}
	if _, err := st.RemoveNode(ctx, "s", "x", dw.CascadeFail); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	y, err := st.GetNode(ctx, "s", "y")
	if err != nil || y.Reason != dw.ReasonRemoved || y.Message == "" {
		t.Fatalf("a successor of a removed node is %+v", y)
	}
}
