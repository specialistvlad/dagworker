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

// pair builds two Managers over two independently created stores on the same
// backend. On a shared backend they are the same data seen by what may as well
// be two processes; that is the scenario these tests exist for.
func pair(tb testing.TB, b e2e.Backend, opts ...dw.Option) (*dw.Manager, *dw.Manager) {
	tb.Helper()
	return newManager(tb, b, opts...), newManager(tb, b, opts...)
}

func eachSharedBackend(t *testing.T, fn func(t *testing.T, b e2e.Backend)) {
	t.Helper()
	ran := false
	for _, b := range e2e.Backends() {
		if !b.Shared {
			continue
		}
		ran = true
		t.Run(b.Name, func(t *testing.T) {
			t.Parallel()
			fn(t, b)
		})
	}
	if !ran {
		t.Skip("no shared backend reachable; set DAGWORKER_INTEGRATION and start the compose stack")
	}
}

// The headline claim: two instances, no coordinator, no leader, no membership
// protocol -- and no node ever handed to both.
func TestE2E_MultiInstance_NeverDoubleDispatch(t *testing.T) {
	t.Parallel()
	eachSharedBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		a, c := pair(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		const nodes = 120
		specs := make([]dw.NodeSpec, nodes)
		for i := range specs {
			specs[i] = dw.NodeSpec{ID: dw.NodeID(fmt.Sprintf("n%04d", i))}
		}
		if err := a.AddNodes(ctx, scope, specs); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}

		var mu sync.Mutex
		claimedBy := map[dw.NodeID][]string{}

		var wg sync.WaitGroup
		drain := func(name string, m *dw.Manager) {
			defer wg.Done()
			for {
				leases, err := m.ClaimBatch(ctx, scope, 5)
				if err != nil || len(leases) == 0 {
					return
				}
				mu.Lock()
				for _, l := range leases {
					claimedBy[l.NodeID] = append(claimedBy[l.NodeID], name)
				}
				mu.Unlock()
				for _, l := range leases {
					if err := m.Ack(ctx, l, nil); err != nil {
						t.Errorf("%s Ack %q: %v", name, l.NodeID, err)
						return
					}
				}
			}
		}
		for i := range 3 {
			wg.Add(2)
			go drain(fmt.Sprintf("a%d", i), a)
			go drain(fmt.Sprintf("c%d", i), c)
		}
		wg.Wait()

		if len(claimedBy) != nodes {
			t.Fatalf("%d of %d nodes were claimed", len(claimedBy), nodes)
		}
		for id, holders := range claimedBy {
			if len(holders) != 1 {
				t.Fatalf("node %q was granted to %v -- concurrently, by two instances", id, holders)
			}
		}
	})
}

// One instance writes the graph, another runs it. Neither knows about the
// other; the backend is the only thing they share.
func TestE2E_MultiInstance_OneWritesAnotherRuns(t *testing.T) {
	t.Parallel()
	eachSharedBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		writer, runner := pair(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := writer.AddNodes(ctx, scope, []dw.NodeSpec{
			{ID: "first"},
			{ID: "second", Deps: []dw.NodeID{"first"}},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}
		if err := writer.Seal(ctx, scope); err != nil {
			t.Fatalf("Seal: %v", err)
		}

		for range 2 {
			lease, err := runner.Claim(ctx, scope)
			if err != nil {
				t.Fatalf("Claim: %v", err)
			}
			if err := runner.Ack(ctx, lease, []byte(`{"ok":true}`)); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}

		// The writer sees the runner's work without being told about it.
		done, err := writer.IsComplete(ctx, scope)
		if err != nil {
			t.Fatalf("IsComplete: %v", err)
		}
		if !done {
			t.Fatal("the writing instance does not see the running instance's completions")
		}
	})
}

// An instance that dies mid-job loses its leases to whoever is still running.
// Nothing coordinates this: the deadline is in storage and the fencing epoch
// makes the recovery safe.
func TestE2E_MultiInstance_SurvivorRecoversDeadWork(t *testing.T) {
	t.Parallel()
	eachSharedBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		doomed, survivor := pair(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := survivor.Configure(ctx, scope, dw.ScopeConfig{
			MaxAttempts:         3,
			MinLeaseTimeout:     10 * time.Millisecond,
			DefaultLeaseTimeout: 250 * time.Millisecond,
			RetryBaseDelay:      time.Millisecond,
			RetryMaxDelay:       time.Millisecond,
		}); err != nil {
			t.Fatalf("Configure: %v", err)
		}
		if err := survivor.AddNode(ctx, scope, "important", []byte("payload")); err != nil {
			t.Fatalf("AddNode: %v", err)
		}

		stolen, err := doomed.Claim(ctx, scope, dw.WithLeaseTimeout(250*time.Millisecond))
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		// The instance goes away entirely, mid-job.
		if err := doomed.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}

		recovered, err := survivor.Claim(ctx, scope, dw.WithLeaseTimeout(10*time.Second))
		if err != nil {
			t.Fatalf("the survivor never recovered the dead instance's node: %v", err)
		}
		if recovered.NodeID != "important" || recovered.Epoch <= stolen.Epoch {
			t.Fatalf("recovered %+v, expected a higher epoch on the same node", recovered)
		}
		if err := survivor.Ack(ctx, recovered, nil); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	})
}

// A node completed on one instance releases its successors for every instance,
// because readiness lives in storage rather than in any process's memory.
func TestE2E_MultiInstance_FanOutCrosses(t *testing.T) {
	t.Parallel()
	eachSharedBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		a, c := pair(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := a.AddNodes(ctx, scope, []dw.NodeSpec{
			{ID: "gate", Kind: "gate"},
			{ID: "x", Kind: "fan", Deps: []dw.NodeID{"gate"}},
			{ID: "y", Kind: "fan", Deps: []dw.NodeID{"gate"}},
			{ID: "z", Kind: "fan", Deps: []dw.NodeID{"gate"}},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}

		if _, err := c.TryClaim(ctx, scope, dw.OfKind("fan")); !errors.Is(err, dw.ErrNoWork) {
			t.Fatalf("a gated node was claimable from the other instance: %v", err)
		}

		gate, err := a.TryClaim(ctx, scope, dw.OfKind("gate"))
		if err != nil {
			t.Fatalf("TryClaim: %v", err)
		}
		if err := a.Ack(ctx, gate, nil); err != nil {
			t.Fatalf("Ack: %v", err)
		}

		var got atomic.Int64
		for range 3 {
			l, err := c.TryClaim(ctx, scope, dw.OfKind("fan"))
			if err != nil {
				break
			}
			got.Add(1)
			if err := c.Ack(ctx, l, nil); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
		if got.Load() != 3 {
			t.Fatalf("the other instance saw %d of 3 released successors", got.Load())
		}
	})
}

// Scopes are isolated: two instances working different scopes in the same
// database never see each other's nodes.
func TestE2E_MultiInstance_ScopesAreIsolated(t *testing.T) {
	t.Parallel()
	eachSharedBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		a, c := pair(t, b)
		ctx := t.Context()
		left, right := e2e.UniqueScope(t)+"-l", e2e.UniqueScope(t)+"-r"

		if err := a.AddNode(ctx, left, "only-left", nil); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := c.AddNode(ctx, right, "only-right", nil); err != nil {
			t.Fatalf("AddNode: %v", err)
		}

		l, err := c.TryClaim(ctx, left)
		if err != nil {
			t.Fatalf("TryClaim: %v", err)
		}
		if l.NodeID != "only-left" {
			t.Fatalf("claiming from the left scope produced %q", l.NodeID)
		}
		if _, err := c.TryClaim(ctx, left); !errors.Is(err, dw.ErrNoWork) {
			t.Fatalf("the left scope leaked a node from another scope: %v", err)
		}

		if _, err := a.GetNode(ctx, left, "only-right"); !errors.Is(err, dw.ErrNotFound) {
			t.Fatalf("a node from another scope was visible: %v", err)
		}
	})
}

var _ = context.Background
