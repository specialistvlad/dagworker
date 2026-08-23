package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/test/e2e"
)

func newManager(tb testing.TB, b e2e.Backend, opts ...dw.Option) *dw.Manager {
	tb.Helper()
	st := b.New(tb)
	base := []dw.Option{dw.WithPollInterval(30 * time.Millisecond)}
	m, err := dw.New(st, append(base, opts...)...)
	if err != nil {
		tb.Fatalf("New: %v", err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			tb.Errorf("Close: %v", err)
		}
	})
	return m
}

func eachBackend(t *testing.T, fn func(t *testing.T, b e2e.Backend)) {
	t.Helper()
	for _, b := range e2e.Backends() {
		t.Run(b.Name, func(t *testing.T) {
			t.Parallel()
			fn(t, b)
		})
	}
}

// A diamond drained by a pool of workers: the shape most real pipelines are,
// and the one that catches a scheduler that releases a join too early.
func TestDiamondDrains(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		m := newManager(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := m.AddNodes(ctx, scope, []dw.NodeSpec{
			{ID: "root"},
			{ID: "left", Deps: []dw.NodeID{"root"}},
			{ID: "right", Deps: []dw.NodeID{"root"}},
			{ID: "join", Deps: []dw.NodeID{"left", "right"}},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}
		if err := m.Seal(ctx, scope); err != nil {
			t.Fatalf("Seal: %v", err)
		}

		var mu sync.Mutex
		order := map[dw.NodeID]int{}
		step := 0

		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					done, err := m.IsComplete(ctx, scope)
					if err != nil || done {
						return
					}
					lease, err := m.TryClaim(ctx, scope)
					if errors.Is(err, dw.ErrNoWork) {
						time.Sleep(5 * time.Millisecond)
						continue
					}
					if err != nil {
						return
					}
					mu.Lock()
					step++
					order[lease.NodeID] = step
					mu.Unlock()
					if err := m.Ack(ctx, lease, nil); err != nil {
						t.Errorf("Ack %q: %v", lease.NodeID, err)
						return
					}
				}
			}()
		}
		wg.Wait()

		if len(order) != 4 {
			t.Fatalf("ran %d of 4 nodes: %v", len(order), order)
		}
		// The join must not have run before both of its predecessors: that is
		// the entire promise of the library.
		if order["join"] < order["left"] || order["join"] < order["right"] {
			t.Fatalf("join ran at step %d, before left (%d) or right (%d)",
				order["join"], order["left"], order["right"])
		}
		if order["root"] != 1 {
			t.Fatalf("root ran at step %d, want first", order["root"])
		}

		done, err := m.IsComplete(ctx, scope)
		if err != nil || !done {
			t.Fatalf("IsComplete is %v (err %v)", done, err)
		}
	})
}

// A failure part-way down does not strand the rest of the graph, and the
// all_done cleanup node still runs.
func TestFailurePropagatesAndCleanupRuns(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		m := newManager(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := m.Configure(ctx, scope, dw.ScopeConfig{MaxAttempts: 1}); err != nil {
			t.Fatalf("Configure: %v", err)
		}
		if err := m.AddNodes(ctx, scope, []dw.NodeSpec{
			{ID: "extract", Kind: "extract"},
			{ID: "transform", Kind: "transform", Deps: []dw.NodeID{"extract"}},
			{ID: "load", Kind: "load", Deps: []dw.NodeID{"transform"}},
			{ID: "cleanup", Kind: "cleanup", Deps: []dw.NodeID{"load"}, Trigger: dw.TriggerAllDone},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}

		lease, err := m.TryClaim(ctx, scope, dw.OfKind("extract"))
		if err != nil {
			t.Fatalf("TryClaim: %v", err)
		}
		if err := m.Nack(ctx, lease, errors.New("source unavailable")); err != nil {
			t.Fatalf("Nack: %v", err)
		}

		for id, want := range map[dw.NodeID]dw.Reason{
			"extract":   dw.ReasonWorkerError,
			"transform": dw.ReasonUpstreamFailed,
			"load":      dw.ReasonUpstreamFailed,
		} {
			n, err := m.GetNode(ctx, scope, id)
			if err != nil {
				t.Fatalf("GetNode(%q): %v", id, err)
			}
			if n.Status != dw.StatusError || n.Reason != want {
				t.Fatalf("%q is %v/%v, want error/%v", id, n.Status, n.Reason, want)
			}
		}

		// cleanup declared all_done, so a failed chain still lets it run.
		cleanup, err := m.TryClaim(ctx, scope, dw.OfKind("cleanup"))
		if err != nil {
			t.Fatalf("cleanup was not claimable after the chain failed: %v", err)
		}
		if err := m.Ack(ctx, cleanup, nil); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})
}

// A worker that never answers loses its node, and the retry policy decides
// whether another attempt happens. This is the guarantee the whole lease
// protocol exists to provide.
func TestAbandonedWorkIsRecovered(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		m := newManager(t, b, dw.WithScopeDefaults(dw.ScopeConfig{SweepInterval: 50 * time.Millisecond}))
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := m.Configure(ctx, scope, dw.ScopeConfig{
			MaxAttempts:         2,
			MinLeaseTimeout:     10 * time.Millisecond,
			DefaultLeaseTimeout: 200 * time.Millisecond,
			RetryBaseDelay:      time.Millisecond,
			RetryMaxDelay:       time.Millisecond,
		}); err != nil {
			t.Fatalf("Configure: %v", err)
		}
		if err := m.AddNode(ctx, scope, "flaky", nil); err != nil {
			t.Fatalf("AddNode: %v", err)
		}

		// A worker takes the node and is never heard from again.
		abandoned, err := m.Claim(ctx, scope, dw.WithLeaseTimeout(200*time.Millisecond))
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}

		// A second worker picks it up once the lease lapses.
		recovered, err := m.Claim(ctx, scope, dw.WithLeaseTimeout(5*time.Second))
		if err != nil {
			t.Fatalf("the abandoned node was never recovered: %v", err)
		}
		if recovered.Epoch <= abandoned.Epoch {
			t.Fatalf("recovered at epoch %d, not above the abandoned %d", recovered.Epoch, abandoned.Epoch)
		}

		// The first worker returns and tries to report success. Refused.
		err = m.Ack(ctx, abandoned, nil)
		if !errors.Is(err, dw.ErrLeaseMismatch) {
			t.Fatalf("the superseded worker's ack gave %v, want ErrLeaseMismatch", err)
		}

		if err := m.Ack(ctx, recovered, nil); err != nil {
			t.Fatalf("the current holder's ack: %v", err)
		}
		n, _ := m.GetNode(ctx, scope, "flaky")
		if n.Status != dw.StatusSuccess {
			t.Fatalf("node is %v/%v", n.Status, n.Reason)
		}
	})
}

// The graph may grow while it is running, which is the feature that separates
// this from a job queue.
func TestGraphGrowsWhileRunning(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		m := newManager(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := m.AddNode(ctx, scope, "seed", nil, dw.WithKind("seed")); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		lease, err := m.TryClaim(ctx, scope, dw.OfKind("seed"))
		if err != nil {
			t.Fatalf("TryClaim: %v", err)
		}

		// The running node discovers more work and adds it, depending on itself.
		fanout := make([]dw.NodeSpec, 10)
		for i := range fanout {
			fanout[i] = dw.NodeSpec{
				ID:   dw.NodeID(fmt.Sprintf("child-%02d", i)),
				Kind: "child",
				Deps: []dw.NodeID{"seed"},
			}
		}
		if err := m.AddNodes(ctx, scope, fanout); err != nil {
			t.Fatalf("AddNodes while running: %v", err)
		}

		// None of them may run until the parent finishes.
		if _, err := m.TryClaim(ctx, scope, dw.OfKind("child")); !errors.Is(err, dw.ErrNoWork) {
			t.Fatalf("a child was claimable before its parent finished: %v", err)
		}
		if err := m.Ack(ctx, lease, nil); err != nil {
			t.Fatalf("Ack: %v", err)
		}

		claimed := 0
		for range 10 {
			l, err := m.TryClaim(ctx, scope, dw.OfKind("child"))
			if err != nil {
				break
			}
			claimed++
			if err := m.Ack(ctx, l, nil); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
		if claimed != 10 {
			t.Fatalf("claimed %d of 10 children after the parent finished", claimed)
		}
	})
}

// Cancelling a scope stops everything, including work already in a worker's
// hands: its acknowledgement is refused rather than resurrecting the node.
func TestCancelScopeStopsInFlightWork(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		m := newManager(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		specs := make([]dw.NodeSpec, 5)
		for i := range specs {
			specs[i] = dw.NodeSpec{ID: dw.NodeID(fmt.Sprintf("n%d", i))}
		}
		if err := m.AddNodes(ctx, scope, specs); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}
		inFlight, err := m.TryClaim(ctx, scope)
		if err != nil {
			t.Fatalf("TryClaim: %v", err)
		}

		if err := m.CancelScope(ctx, scope); err != nil {
			t.Fatalf("CancelScope: %v", err)
		}
		if err := m.Ack(ctx, inFlight, nil); !errors.Is(err, dw.ErrLeaseMismatch) {
			t.Fatalf("acking a cancelled node gave %v, want ErrLeaseMismatch", err)
		}
		stats, err := m.Stats(ctx, scope)
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if stats.NonTerminal() != 0 {
			t.Fatalf("%d nodes survived CancelScope: %+v", stats.NonTerminal(), stats)
		}
	})
}

// Subscribers see the run happen. The counts are what matter: every node is
// created once, transitions to in-progress once, and terminates once.
func TestSubscriberSeesTheWholeRun(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		m := newManager(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		sub, err := m.Subscribe(ctx, dw.SubscribeOptions{Scope: scope, BufferSize: 256})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		defer func() { _ = sub.Close() }()

		var created, succeeded atomic.Int64
		collected := make(chan struct{})
		go func() {
			defer close(collected)
			for ev := range sub.Events() {
				switch {
				case ev.Kind == dw.EventCreated:
					created.Add(1)
				case ev.Kind == dw.EventTransition && ev.To == dw.StatusSuccess:
					if succeeded.Add(1) == 3 {
						return
					}
				}
			}
		}()

		if err := m.AddNodes(ctx, scope, []dw.NodeSpec{
			{ID: "a"}, {ID: "b", Deps: []dw.NodeID{"a"}}, {ID: "c", Deps: []dw.NodeID{"b"}},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}
		for range 3 {
			l, err := m.Claim(ctx, scope)
			if err != nil {
				t.Fatalf("Claim: %v", err)
			}
			if err := m.Ack(ctx, l, nil); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}

		select {
		case <-collected:
		case <-time.After(10 * time.Second):
			t.Fatalf("saw %d creations and %d successes before timing out",
				created.Load(), succeeded.Load())
		}
		if got := created.Load(); got != 3 {
			t.Fatalf("saw %d creation events, want 3", got)
		}
	})
}
