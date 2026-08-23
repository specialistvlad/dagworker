package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/test/e2e"
)

// Every operation the library exposes, driven through the Manager against every
// backend. The scenario tests show the library being used; this one makes sure
// nothing in the surface is only ever exercised by a unit test with a fake.
//
// One subtest per operation, each on its own scope so they can run in parallel
// against a shared database.
func TestE2E_Operations(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()

		t.Run("insert-is-idempotent", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)

			spec := dw.NodeSpec{ID: "a", Kind: "k", Payload: []byte(`{"v":1}`)}
			if err := m.AddNodes(ctx, scope, []dw.NodeSpec{spec}); err != nil {
				t.Fatalf("AddNodes: %v", err)
			}
			// Retrying an insert after an ambiguous failure must not duplicate.
			if err := m.AddNodes(ctx, scope, []dw.NodeSpec{spec}); err != nil {
				t.Fatalf("re-adding an identical node: %v", err)
			}
			if st, _ := m.Stats(ctx, scope); st.Total != 1 {
				t.Fatalf("scope holds %d nodes, want 1", st.Total)
			}
			// The same identity with different content is a conflict, not a
			// silent overwrite.
			err := m.AddNodes(ctx, scope, []dw.NodeSpec{{ID: "a", Kind: "k", Payload: []byte(`{"v":2}`)}})
			if !errors.Is(err, dw.ErrIDConflict) {
				t.Fatalf("re-adding with different content gave %v", err)
			}
		})

		t.Run("edges-added-and-removed-at-runtime", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			mustAdd(ctx, t, m, scope,
				dw.NodeSpec{ID: "a", Kind: "a"}, dw.NodeSpec{ID: "b", Kind: "b"})

			if err := m.AddEdge(ctx, scope, "a", "b"); err != nil {
				t.Fatalf("AddEdge: %v", err)
			}
			if _, err := m.TryClaim(ctx, scope, dw.OfKind("b")); !errors.Is(err, dw.ErrNoWork) {
				t.Fatalf("b was claimable behind a new edge: %v", err)
			}
			if err := m.RemoveEdge(ctx, scope, "a", "b"); err != nil {
				t.Fatalf("RemoveEdge: %v", err)
			}
			if _, err := m.TryClaim(ctx, scope, dw.OfKind("b")); err != nil {
				t.Fatalf("b did not become claimable after its edge was removed: %v", err)
			}
		})

		t.Run("cycles-are-refused", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			mustAdd(ctx, t, m, scope,
				dw.NodeSpec{ID: "a"}, dw.NodeSpec{ID: "b", Deps: []dw.NodeID{"a"}},
				dw.NodeSpec{ID: "c", Deps: []dw.NodeID{"b"}})

			err := m.AddEdge(ctx, scope, "c", "a")
			var ce *dw.CycleError
			if !errors.As(err, &ce) {
				t.Fatalf("closing a cycle gave %v, want a *CycleError", err)
			}
			// The graph is untouched: the chain still runs end to end.
			if st, _ := m.Stats(ctx, scope); st.Ready != 1 {
				t.Fatalf("after a refused edge: %+v", st)
			}
		})

		t.Run("remove-node-policies", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			mustAdd(ctx, t, m, scope,
				dw.NodeSpec{ID: "up", Kind: "up"},
				dw.NodeSpec{ID: "down", Kind: "down", Deps: []dw.NodeID{"up"}})

			// A cleanup call must not cascade a failure by accident.
			if err := m.RemoveNode(ctx, scope, "up", dw.CascadeReject); !errors.Is(err, dw.ErrHasSuccessors) {
				t.Fatalf("removing a node with successors gave %v", err)
			}
			// Detach: the successor loses the dependency and may run.
			if err := m.RemoveNode(ctx, scope, "up", dw.CascadeDetach); err != nil {
				t.Fatalf("RemoveNode(detach): %v", err)
			}
			if _, err := m.TryClaim(ctx, scope, dw.OfKind("down")); err != nil {
				t.Fatalf("down did not become claimable after detach: %v", err)
			}
		})

		t.Run("remove-node-cascading-failure", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			mustAdd(ctx, t, m, scope,
				dw.NodeSpec{ID: "up"},
				dw.NodeSpec{ID: "mid", Deps: []dw.NodeID{"up"}},
				dw.NodeSpec{ID: "leaf", Deps: []dw.NodeID{"mid"}})

			if err := m.RemoveNode(ctx, scope, "up", dw.CascadeFail); err != nil {
				t.Fatalf("RemoveNode(fail): %v", err)
			}
			for _, id := range []dw.NodeID{"mid", "leaf"} {
				n, err := m.GetNode(ctx, scope, id)
				if err != nil {
					t.Fatalf("GetNode(%q): %v", id, err)
				}
				if n.Status != dw.StatusError {
					t.Errorf("%q is %v, want it failed behind the removed node", id, n.Status)
				}
			}
		})

		t.Run("claim-by-kind-and-priority", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			mustAdd(ctx, t, m, scope,
				dw.NodeSpec{ID: "cpu-low", Kind: "cpu", Priority: 1},
				dw.NodeSpec{ID: "cpu-high", Kind: "cpu", Priority: 100},
				dw.NodeSpec{ID: "gpu-only", Kind: "gpu"})

			l, err := m.TryClaim(ctx, scope, dw.OfKind("cpu"))
			if err != nil {
				t.Fatalf("TryClaim: %v", err)
			}
			if l.NodeID != "cpu-high" {
				t.Errorf("claimed %q, want the higher priority cpu node", l.NodeID)
			}
			g, err := m.TryClaim(ctx, scope, dw.OfKind("gpu"))
			if err != nil || g.NodeID != "gpu-only" {
				t.Fatalf("claiming kind gpu gave %q, %v", g.NodeID, err)
			}
		})

		t.Run("batch-claim", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			specs := make([]dw.NodeSpec, 10)
			for i := range specs {
				specs[i] = dw.NodeSpec{ID: dw.NodeID(fmt.Sprintf("n%02d", i))}
			}
			mustAdd(ctx, t, m, scope, specs...)

			leases, err := m.ClaimBatch(ctx, scope, 10)
			if err != nil {
				t.Fatalf("ClaimBatch: %v", err)
			}
			if len(leases) != 10 {
				t.Fatalf("ClaimBatch returned %d of 10", len(leases))
			}
			for _, l := range leases {
				if err := m.Ack(ctx, l, nil); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			}
		})

		t.Run("result-round-trips", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			mustAdd(ctx, t, m, scope, dw.NodeSpec{ID: "a"})

			l, err := m.TryClaim(ctx, scope)
			if err != nil {
				t.Fatalf("TryClaim: %v", err)
			}
			if err := m.Ack(ctx, l, []byte(`{"artifact":"out.tar.gz"}`)); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			n, err := m.GetNode(ctx, scope, "a")
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			if n.Status != dw.StatusSuccess {
				t.Fatalf("node is %v", n.Status)
			}
		})

		t.Run("retry-then-succeed", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			if err := m.Configure(ctx, scope, dw.ScopeConfig{
				MaxAttempts: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
			}); err != nil {
				t.Fatalf("Configure: %v", err)
			}
			mustAdd(ctx, t, m, scope, dw.NodeSpec{ID: "flaky"})

			l, err := m.TryClaim(ctx, scope)
			if err != nil {
				t.Fatalf("TryClaim: %v", err)
			}
			outcome, err := m.Nack(ctx, l, errors.New("transient"))
			if err != nil {
				t.Fatalf("Nack: %v", err)
			}
			if !outcome.Retrying {
				t.Fatal("a failure with attempts remaining did not schedule a retry")
			}

			second := claimEventually(ctx, t, m, scope)
			if second.Epoch <= l.Epoch {
				t.Fatalf("retry claimed at epoch %d, not above %d", second.Epoch, l.Epoch)
			}
			if err := m.Ack(ctx, second, nil); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			if n, _ := m.GetNode(ctx, scope, "flaky"); n.Status != dw.StatusSuccess {
				t.Fatalf("node is %v after a successful retry", n.Status)
			}
		})

		t.Run("retries-exhaust-and-propagate", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			if err := m.Configure(ctx, scope, dw.ScopeConfig{
				MaxAttempts: 2, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
			}); err != nil {
				t.Fatalf("Configure: %v", err)
			}
			mustAdd(ctx, t, m, scope,
				dw.NodeSpec{ID: "doomed"},
				dw.NodeSpec{ID: "downstream", Deps: []dw.NodeID{"doomed"}})

			for attempt := 1; attempt <= 2; attempt++ {
				l := claimEventually(ctx, t, m, scope)
				if _, err := m.Nack(ctx, l, fmt.Errorf("attempt %d failed", attempt)); err != nil { //nolint:err113 // test message
					t.Fatalf("Nack: %v", err)
				}
			}
			n, err := m.GetNode(ctx, scope, "doomed")
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			if n.Status != dw.StatusError || n.Attempt != 2 {
				t.Fatalf("after exhausting retries: %v/%v attempt=%d", n.Status, n.Reason, n.Attempt)
			}
			d, _ := m.GetNode(ctx, scope, "downstream")
			if d.Reason != dw.ReasonUpstreamFailed {
				t.Errorf("downstream is %v/%v, want upstream_failed", d.Status, d.Reason)
			}
		})

		t.Run("heartbeat-keeps-long-work-alive", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			if err := m.Configure(ctx, scope, dw.ScopeConfig{
				MinLeaseTimeout: 50 * time.Millisecond, DefaultLeaseTimeout: 300 * time.Millisecond,
			}); err != nil {
				t.Fatalf("Configure: %v", err)
			}
			mustAdd(ctx, t, m, scope, dw.NodeSpec{ID: "slow"})

			l, err := m.TryClaim(ctx, scope, dw.WithLeaseTimeout(300*time.Millisecond))
			if err != nil {
				t.Fatalf("TryClaim: %v", err)
			}
			// Work for longer than the lease, renewing as we go.
			deadline := time.Now().Add(900 * time.Millisecond)
			for time.Now().Before(deadline) {
				time.Sleep(100 * time.Millisecond)
				extended, err := m.Extend(ctx, l, 300*time.Millisecond)
				if err != nil {
					t.Fatalf("Extend: %v", err)
				}
				l = extended
			}
			// Still ours, long after the original lease would have lapsed.
			if err := m.Ack(ctx, l, nil); err != nil {
				t.Fatalf("Ack after heartbeating: %v", err)
			}
		})

		t.Run("cancel-revokes-in-flight-work", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			mustAdd(ctx, t, m, scope, dw.NodeSpec{ID: "a"}, dw.NodeSpec{ID: "b"})

			l, err := m.TryClaim(ctx, scope)
			if err != nil {
				t.Fatalf("TryClaim: %v", err)
			}
			if err := m.CancelScope(ctx, scope); err != nil {
				t.Fatalf("CancelScope: %v", err)
			}
			if err := m.Ack(ctx, l, nil); !errors.Is(err, dw.ErrLeaseMismatch) {
				t.Fatalf("acking a cancelled node gave %v", err)
			}
			if st, _ := m.Stats(ctx, scope); st.NonTerminal() != 0 {
				t.Fatalf("%d nodes survived CancelScope", st.NonTerminal())
			}
		})

		t.Run("inspect-explains-a-stuck-node", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			mustAdd(ctx, t, m, scope,
				dw.NodeSpec{ID: "a", Kind: "a"}, dw.NodeSpec{ID: "b", Kind: "b"},
				dw.NodeSpec{ID: "join", Deps: []dw.NodeID{"a", "b"}})

			la, err := m.TryClaim(ctx, scope, dw.OfKind("a"))
			if err != nil {
				t.Fatalf("TryClaim: %v", err)
			}
			if err := m.Ack(ctx, la, nil); err != nil {
				t.Fatalf("Ack: %v", err)
			}

			insp, err := m.Inspect(ctx, scope, "join")
			if err != nil {
				t.Fatalf("Inspect: %v", err)
			}
			if insp.Phase != dw.PhaseBlocked {
				t.Errorf("join is in phase %v, want blocked", insp.Phase)
			}
			if len(insp.Waiting) != 1 || insp.Waiting[0] != "b" {
				t.Errorf("join is waiting on %v, want [b] — this is the first question an operator asks",
					insp.Waiting)
			}
		})

		t.Run("scope-config-limits-in-flight", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			if err := m.Configure(ctx, scope, dw.ScopeConfig{MaxInFlight: 2}); err != nil {
				t.Fatalf("Configure: %v", err)
			}
			mustAdd(ctx, t, m, scope,
				dw.NodeSpec{ID: "a"}, dw.NodeSpec{ID: "b"}, dw.NodeSpec{ID: "c"})

			for range 2 {
				if _, err := m.TryClaim(ctx, scope, dw.WithLeaseTimeout(time.Hour)); err != nil {
					t.Fatalf("TryClaim: %v", err)
				}
			}
			if l, err := m.TryClaim(ctx, scope, dw.WithLeaseTimeout(time.Hour)); err == nil {
				t.Fatalf("claimed %q with two already in flight and a cap of two", l.NodeID)
			}
		})

		t.Run("payload-cap-is-enforced", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			if err := m.Configure(ctx, scope, dw.ScopeConfig{PayloadCap: 64}); err != nil {
				t.Fatalf("Configure: %v", err)
			}
			big := make([]byte, 128)
			err := m.AddNodes(ctx, scope, []dw.NodeSpec{{ID: "fat", Payload: big}})
			if !errors.Is(err, dw.ErrPayloadTooLarge) {
				t.Fatalf("an oversized payload gave %v", err)
			}
		})

		t.Run("list-nodes-pages", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			if !m.Capabilities().Has(dw.CapList) {
				t.Skip("backend does not list")
			}
			specs := make([]dw.NodeSpec, 7)
			for i := range specs {
				specs[i] = dw.NodeSpec{ID: dw.NodeID(fmt.Sprintf("n%02d", i))}
			}
			mustAdd(ctx, t, m, scope, specs...)

			seen := map[dw.NodeID]bool{}
			cursor := ""
			for range 10 {
				page, err := m.ListNodes(ctx, scope, dw.ListOptions{Cursor: cursor, Limit: 3})
				if err != nil {
					t.Fatalf("ListNodes: %v", err)
				}
				for _, n := range page.Nodes {
					if seen[n.ID] {
						t.Fatalf("node %q appeared on two pages", n.ID)
					}
					seen[n.ID] = true
				}
				if page.Next == "" {
					break
				}
				cursor = page.Next
			}
			if len(seen) != 7 {
				t.Fatalf("paging saw %d of 7", len(seen))
			}
		})

		t.Run("subscribe-observes-the-run", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)

			sub, err := m.Subscribe(ctx, dw.SubscribeOptions{Scope: scope, BufferSize: 64})
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			defer func() { _ = sub.Close() }()

			mustAdd(ctx, t, m, scope, dw.NodeSpec{ID: "a"})
			l, err := m.TryClaim(ctx, scope)
			if err != nil {
				t.Fatalf("TryClaim: %v", err)
			}
			if err := m.Ack(ctx, l, nil); err != nil {
				t.Fatalf("Ack: %v", err)
			}

			var sawCreated, sawSuccess bool
			deadline := time.After(10 * time.Second)
			for !sawCreated || !sawSuccess {
				select {
				case ev := <-sub.Events():
					if ev.Kind == dw.EventCreated {
						sawCreated = true
					}
					if ev.Kind == dw.EventTransition && ev.To == dw.StatusSuccess {
						sawSuccess = true
					}
				case <-deadline:
					t.Fatalf("timed out: created=%v success=%v", sawCreated, sawSuccess)
				}
			}
		})

		t.Run("durable-subscription-replays", func(t *testing.T) {
			t.Parallel()
			m, ctx, scope := fixture(t, b)
			if !m.Capabilities().Has(dw.CapDurableEvents) {
				t.Skip("backend has no durable event stream")
			}
			// Write first, subscribe after: a durable stream can deliver what
			// happened before the subscriber existed.
			mustAdd(ctx, t, m, scope, dw.NodeSpec{ID: "early"})

			sub, err := m.Subscribe(ctx, dw.SubscribeOptions{Scope: scope, Durable: true, Replay: true})
			if err != nil {
				t.Fatalf("Subscribe(durable): %v", err)
			}
			defer func() { _ = sub.Close() }()

			select {
			case ev := <-sub.Events():
				if ev.NodeID != "early" {
					t.Fatalf("replay began with %q", ev.NodeID)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("a replaying subscription delivered nothing")
			}
		})
	})
}

// fixture builds a Manager on an isolated scope.
func fixture(t *testing.T, b e2e.Backend) (*dw.Manager, context.Context, dw.Scope) {
	t.Helper()
	return newManager(t, b), t.Context(), e2e.UniqueScope(t)
}

func mustAdd(ctx context.Context, t *testing.T, m *dw.Manager, scope dw.Scope, specs ...dw.NodeSpec) {
	t.Helper()
	if err := m.AddNodes(ctx, scope, specs); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
}

// claimEventually retries a non-blocking claim for a short while, which is what
// a caller must do after a retry has been scheduled behind a backoff.
func claimEventually(ctx context.Context, t *testing.T, m *dw.Manager, scope dw.Scope) dw.Lease {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		l, err := m.TryClaim(ctx, scope, dw.WithLeaseTimeout(time.Hour))
		if err == nil {
			return l
		}
		if !errors.Is(err, dw.ErrNoWork) {
			t.Fatalf("TryClaim: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("nothing became claimable")
	return dw.Lease{}
}
