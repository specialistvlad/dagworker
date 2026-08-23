package dagworker_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/memory"
)

// bareStore hides every optional facet, so tests can exercise the paths a
// minimal backend takes: no doorbell, no listing, no durable stream.
type bareStore struct{ dw.Store }

type fixture struct {
	t     *testing.T
	m     *dw.Manager
	store *memory.Store
	clock *dagstoretest.FakeClock
	ctx   context.Context
}

func newFixture(t *testing.T, opts ...dw.Option) *fixture {
	t.Helper()
	clk := dagstoretest.NewFakeClock()
	st := memory.New(memory.WithClock(clk), memory.WithJitter(func(n int64) int64 { return 0 }))
	base := []dw.Option{
		dw.WithPollInterval(20 * time.Millisecond),
		dw.WithoutBackgroundSweeper(),
	}
	m, err := dw.New(st, append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
		if err := st.Close(ctx); err != nil {
			t.Errorf("store Close: %v", err)
		}
	})
	return &fixture{t: t, m: m, store: st, clock: clk, ctx: t.Context()}
}

func (f *fixture) add(id dw.NodeID, opts ...dw.NodeOption) {
	f.t.Helper()
	if err := f.m.AddNode(f.ctx, "s", id, []byte(`{"n":1}`), opts...); err != nil {
		f.t.Fatalf("AddNode(%q): %v", id, err)
	}
}

func (f *fixture) claim(opts ...dw.ClaimOption) dw.Lease {
	f.t.Helper()
	l, err := f.m.TryClaim(f.ctx, "s", opts...)
	if err != nil {
		f.t.Fatalf("TryClaim: %v", err)
	}
	return l
}

func (f *fixture) status(id dw.NodeID) dw.Status {
	f.t.Helper()
	n, err := f.m.GetNode(f.ctx, "s", id)
	if err != nil {
		f.t.Fatalf("GetNode(%q): %v", id, err)
	}
	return n.Status
}

func TestNewRejectsBadInput(t *testing.T) {
	t.Parallel()
	if _, err := dw.New(nil); !errors.Is(err, dw.ErrNilStore) {
		t.Fatalf("New(nil) gave %v, want ErrNilStore", err)
	}
	st := memory.New()
	t.Cleanup(func() { _ = st.Close(context.Background()) })

	if _, err := dw.New(st, nil); !errors.Is(err, dw.ErrInvalidConfig) {
		t.Fatalf("a nil option gave %v", err)
	}
	if _, err := dw.New(st, dw.WithSubscriberBuffer(0)); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a zero buffer gave %v", err)
	}
	if _, err := dw.New(st, dw.WithClock(nil)); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a nil clock gave %v", err)
	}
	if _, err := dw.New(st, dw.WithLogger(nil)); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a nil logger gave %v", err)
	}
	if _, err := dw.New(st, dw.WithOverflowPolicy(dw.OverflowPolicy(99))); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("an unknown overflow policy gave %v", err)
	}
	bad := dw.ScopeConfig{MinLeaseTimeout: time.Hour, MaxLeaseTimeout: time.Second}
	if _, err := dw.New(st, dw.WithScopeDefaults(bad)); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("inverted lease bounds gave %v", err)
	}
}

// The whole point of the library, end to end: a dependency gates its successor
// until it succeeds, and then does not.
func TestDependencyGatesSuccessor(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.add("build")
	f.add("test", dw.WithDeps("build"))

	if _, err := f.m.TryClaim(f.ctx, "s", dw.OfKind()); err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	// build is now claimed; test must not be available.
	if _, err := f.m.TryClaim(f.ctx, "s"); !errors.Is(err, dw.ErrNoWork) {
		t.Fatalf("TryClaim with only a blocked node gave %v, want ErrNoWork", err)
	}

	lease, err := f.m.GetNode(f.ctx, "s", "build")
	if err != nil || lease.Status != dw.StatusInProgress {
		t.Fatalf("build is %v", lease.Status)
	}
}

func TestFullLifecycle(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.add("a")
	f.add("b", dw.WithDeps("a"))

	l := f.claim()
	if l.NodeID != "a" {
		t.Fatalf("claimed %q, want a", l.NodeID)
	}
	if err := f.m.Ack(f.ctx, l, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if got := f.status("a"); got != dw.StatusSuccess {
		t.Fatalf("a is %v after Ack", got)
	}

	l2 := f.claim()
	if l2.NodeID != "b" {
		t.Fatalf("claimed %q, want b to be released by a's success", l2.NodeID)
	}
	if err := f.m.Ack(f.ctx, l2, nil); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	if err := f.m.Seal(f.ctx, "s"); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	done, err := f.m.IsComplete(f.ctx, "s")
	if err != nil || !done {
		t.Fatalf("IsComplete is %v (err %v), want true", done, err)
	}
}

func TestClaimBlocksUntilWorkAppears(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	type result struct {
		lease dw.Lease
		err   error
	}
	got := make(chan result, 1)
	go func() {
		l, err := f.m.Claim(f.ctx, "s")
		got <- result{l, err}
	}()

	// The claim must be parked, not spinning and not returning empty.
	select {
	case r := <-got:
		t.Fatalf("Claim returned %+v before any work existed", r)
	case <-time.After(100 * time.Millisecond):
	}

	f.add("late")

	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("Claim: %v", r.err)
		}
		if r.lease.NodeID != "late" {
			t.Fatalf("claimed %q, want late", r.lease.NodeID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Claim did not wake after work appeared")
	}
}

// Without a doorbell the poll fallback is the only wakeup path, and it must
// still work.
func TestClaimPollsWhenBackendHasNoDoorbell(t *testing.T) {
	t.Parallel()
	st := memory.New(memory.WithClock(dagstoretest.NewFakeClock()))
	m, err := dw.New(bareStore{st}, dw.WithPollInterval(20*time.Millisecond), dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})
	if m.Capabilities() != 0 {
		t.Fatalf("a store with no reporter advertised %v", m.Capabilities())
	}

	ctx := t.Context()
	done := make(chan dw.Lease, 1)
	go func() {
		l, err := m.Claim(ctx, "s")
		if err == nil {
			done <- l
		}
	}()
	time.Sleep(50 * time.Millisecond)
	if err := m.AddNode(ctx, "s", "n", nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	select {
	case l := <-done:
		if l.NodeID != "n" {
			t.Fatalf("claimed %q", l.NodeID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the polling fallback never found the work")
	}
}

func TestClaimRespectsContext(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(f.ctx, 80*time.Millisecond)
	defer cancel()
	if _, err := f.m.Claim(ctx, "s"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Claim with an expiring context gave %v", err)
	}
}

func TestAckIsFencedAgainstAStaleLease(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	if err := f.m.Configure(f.ctx, "s", dw.ScopeConfig{
		DefaultLeaseTimeout: time.Second,
		MinLeaseTimeout:     time.Millisecond,
		RetryBaseDelay:      time.Nanosecond,
		RetryMaxDelay:       time.Nanosecond,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	f.add("a")
	stale := f.claim()

	f.clock.Advance(2 * time.Second) // the lease expires

	fresh := f.claim() // reclaimed inline and reissued
	if fresh.Epoch <= stale.Epoch {
		t.Fatalf("reissued epoch %d is not above the stale %d", fresh.Epoch, stale.Epoch)
	}
	if err := f.m.Ack(f.ctx, stale, nil); !errors.Is(err, dw.ErrLeaseMismatch) {
		t.Fatalf("acking a superseded lease gave %v", err)
	}
	if err := f.m.Ack(f.ctx, fresh, nil); err != nil {
		t.Fatalf("acking the current lease: %v", err)
	}
}

func TestNackSkipAndExtend(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	if err := f.m.Configure(f.ctx, "s", dw.ScopeConfig{MaxAttempts: 1}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	f.add("fail")
	f.add("skip")

	l1 := f.claim()
	if err := f.m.Nack(f.ctx, l1, errors.New("exploded")); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	n, _ := f.m.GetNode(f.ctx, "s", l1.NodeID)
	if n.Status != dw.StatusError || n.Reason != dw.ReasonWorkerError || n.Message != "exploded" {
		t.Fatalf("after Nack the node is %+v", n)
	}

	l2 := f.claim()
	if err := f.m.Skip(f.ctx, l2, "nothing to do"); err != nil {
		t.Fatalf("Skip: %v", err)
	}
	n2, _ := f.m.GetNode(f.ctx, "s", l2.NodeID)
	if n2.Status != dw.StatusError || n2.Reason != dw.ReasonSkipped {
		t.Fatalf("after Skip the node is %+v", n2)
	}

	// Nack with a nil cause must not panic.
	f.add("third")
	l3 := f.claim()
	if err := f.m.Nack(f.ctx, l3, nil); err != nil {
		t.Fatalf("Nack(nil): %v", err)
	}
}

func TestExtendMovesTheDeadline(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.add("a")
	l := f.claim()

	extended, err := f.m.Extend(f.ctx, l, time.Hour)
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if !extended.Deadline.After(l.Deadline) {
		t.Fatalf("Extend gave %s, not after %s", extended.Deadline, l.Deadline)
	}

	// Past the original deadline the lease is still held.
	f.clock.Advance(5 * time.Minute)
	if got := f.status("a"); got != dw.StatusInProgress {
		t.Fatalf("the extended node is %v", got)
	}
	if err := f.m.Ack(f.ctx, extended, nil); err != nil {
		t.Fatalf("Ack after Extend: %v", err)
	}
}

func TestOperationsValidateInput(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	bad := dw.Scope("")

	if err := f.m.AddNode(f.ctx, bad, "a", nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("an empty scope gave %v", err)
	}
	if err := f.m.AddNode(f.ctx, "s", "", nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("an empty node id gave %v", err)
	}
	if err := f.m.AddEdge(f.ctx, "s", "a", "a"); !errors.Is(err, dw.ErrCycle) {
		t.Fatalf("a self edge gave %v", err)
	}
	if err := f.m.AddEdge(f.ctx, "s", "", "b"); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("an empty edge source gave %v", err)
	}
	if err := f.m.AddEdge(f.ctx, "s", "a", ""); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("an empty edge target gave %v", err)
	}
	if err := f.m.RemoveEdge(f.ctx, "s", "", "b"); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("removing an edge with an empty source gave %v", err)
	}
	if err := f.m.RemoveNode(f.ctx, "s", "a", dw.CascadePolicy(99)); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("an unknown cascade policy gave %v", err)
	}
	if err := f.m.Cancel(f.ctx, "s", ""); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("cancelling an empty id gave %v", err)
	}
	if _, err := f.m.GetNode(f.ctx, "s", ""); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("getting an empty id gave %v", err)
	}
	if _, err := f.m.Inspect(f.ctx, "s", ""); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("inspecting an empty id gave %v", err)
	}
	if err := f.m.AddNode(f.ctx, "s", "a", nil, nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a nil node option gave %v", err)
	}
	if _, err := f.m.TryClaim(f.ctx, "s", nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a nil claim option gave %v", err)
	}
	if _, err := f.m.TryClaim(f.ctx, "s", dw.WithLeaseTimeout(-time.Second)); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a negative lease timeout gave %v", err)
	}
	if _, err := f.m.TryClaim(f.ctx, "s", dw.OfKind(string(make([]byte, 200)))); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("an oversized kind gave %v", err)
	}
	if err := f.m.Ack(f.ctx, dw.Lease{}, nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("acking a zero lease gave %v", err)
	}
	if _, err := f.m.Extend(f.ctx, dw.Lease{}, time.Second); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("extending a zero lease gave %v", err)
	}
	if _, err := f.m.Extend(f.ctx, dw.Lease{Scope: "s", NodeID: "a", Epoch: 1}, -time.Second); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a negative extension gave %v", err)
	}
	if err := f.m.Configure(f.ctx, "s", dw.ScopeConfig{TerminalRetention: -1}); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a negative retention gave %v", err)
	}
	if err := f.m.AddNodes(f.ctx, "s", []dw.NodeSpec{{ID: ""}}); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a malformed spec gave %v", err)
	}
}

func TestInspectExplainsWhyANodeIsBlocked(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.add("a")
	f.add("b")
	f.add("c", dw.WithDeps("a", "b"))

	insp, err := f.m.Inspect(f.ctx, "s", "c")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if insp.Phase != dw.PhaseBlocked {
		t.Fatalf("c is in phase %v, want blocked", insp.Phase)
	}
	if len(insp.Waiting) != 2 {
		t.Fatalf("c is waiting on %v, want two predecessors", insp.Waiting)
	}
	if insp.Deps.Unsatisfied != 2 {
		t.Fatalf("c has %d unsatisfied dependencies, want 2", insp.Deps.Unsatisfied)
	}

	up, err := f.m.Inspect(f.ctx, "s", "a")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if len(up.Successors) != 1 || up.Successors[0] != "c" {
		t.Fatalf("a's successors are %v, want [c]", up.Successors)
	}
	if up.Rank == 0 {
		t.Fatal("a has no topological rank")
	}
}

func TestClosedManagerRefusesEverything(t *testing.T) {
	t.Parallel()
	clk := dagstoretest.NewFakeClock()
	st := memory.New(memory.WithClock(clk))
	m, err := dw.New(st, dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close(context.Background()) })

	ctx := t.Context()
	if err := m.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := m.Close(ctx); err != nil {
		t.Fatalf("Close twice: %v", err)
	}

	checks := map[string]error{
		"AddNode":     m.AddNode(ctx, "s", "a", nil),
		"AddEdge":     m.AddEdge(ctx, "s", "a", "b"),
		"RemoveEdge":  m.RemoveEdge(ctx, "s", "a", "b"),
		"RemoveNode":  m.RemoveNode(ctx, "s", "a", dw.CascadeReject),
		"Cancel":      m.Cancel(ctx, "s", "a"),
		"CancelScope": m.CancelScope(ctx, "s"),
		"Seal":        m.Seal(ctx, "s"),
		"Configure":   m.Configure(ctx, "s", dw.ScopeConfig{}),
		"Ack":         m.Ack(ctx, dw.Lease{Scope: "s", NodeID: "a", Epoch: 1}, nil),
	}
	for name, err := range checks {
		if !errors.Is(err, dw.ErrClosed) {
			t.Fatalf("%s after Close gave %v, want ErrClosed", name, err)
		}
	}
	if _, err := m.GetNode(ctx, "s", "a"); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("GetNode after Close gave %v", err)
	}
	if _, err := m.TryClaim(ctx, "s"); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("TryClaim after Close gave %v", err)
	}
	if _, err := m.Claim(ctx, "s"); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("Claim after Close gave %v", err)
	}
	if _, err := m.Scopes(ctx); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("Scopes after Close gave %v", err)
	}
	if _, err := m.Subscribe(ctx, dw.SubscribeOptions{}); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("Subscribe after Close gave %v", err)
	}
	if _, err := m.Extend(ctx, dw.Lease{Scope: "s", NodeID: "a", Epoch: 1}, time.Second); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("Extend after Close gave %v", err)
	}
	if _, err := m.Stats(ctx, "s"); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("Stats after Close gave %v", err)
	}
	if _, err := m.ListNodes(ctx, "s", dw.ListOptions{}); !errors.Is(err, dw.ErrClosed) {
		t.Fatalf("ListNodes after Close gave %v", err)
	}
}

// Close must return only once every goroutine the Manager started has exited,
// so a caller can discard it knowing nothing will fire afterwards.
func TestCloseLeavesNoGoroutines(t *testing.T) {
	clk := dagstoretest.NewFakeClock()
	st := memory.New(memory.WithClock(clk))
	before := runtime.NumGoroutine()

	ctx := t.Context()
	m, err := dw.New(st, dw.WithScopeDefaults(dw.ScopeConfig{SweepInterval: 5 * time.Millisecond}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := range 5 {
		if _, err := m.Subscribe(ctx, dw.SubscribeOptions{Scope: dw.Scope(string(rune('a' + i)))}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}
	if err := m.AddNode(ctx, "s", "a", nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = st.Close(closeCtx)

	// Goroutine teardown is observable but not instantaneous; give the
	// scheduler a bounded chance to settle before declaring a leak.
	for range 100 {
		if runtime.NumGoroutine() <= before+1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines went from %d to %d across a Manager's lifetime", before, runtime.NumGoroutine())
}

func TestListNodesNeedsTheCapability(t *testing.T) {
	t.Parallel()
	st := memory.New()
	m, err := dw.New(bareStore{st}, dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})
	if _, err := m.ListNodes(t.Context(), "s", dw.ListOptions{}); !errors.Is(err, dw.ErrUnsupported) {
		t.Fatalf("listing on a backend without the facet gave %v", err)
	}
	if _, err := m.Subscribe(t.Context(), dw.SubscribeOptions{Scope: "s", Durable: true}); !errors.Is(err, dw.ErrUnsupported) {
		t.Fatalf("a durable subscription on a backend without the facet gave %v", err)
	}
}

func TestListNodesPages(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	for _, id := range []dw.NodeID{"a", "b", "c"} {
		f.add(id)
	}
	page, err := f.m.ListNodes(f.ctx, "s", dw.ListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(page.Nodes) != 2 || page.Next == "" {
		t.Fatalf("first page has %d nodes, next %q", len(page.Nodes), page.Next)
	}
	rest, err := f.m.ListNodes(f.ctx, "s", dw.ListOptions{Limit: 2, Cursor: page.Next})
	if err != nil {
		t.Fatalf("ListNodes(page 2): %v", err)
	}
	if len(rest.Nodes) != 1 {
		t.Fatalf("second page has %d nodes, want 1", len(rest.Nodes))
	}
}

// The background maintenance loop must reclaim a dead worker's node without
// anyone asking for work.
func TestBackgroundSweeperReclaims(t *testing.T) {
	t.Parallel()
	clk := dagstoretest.NewFakeClock()
	st := memory.New(memory.WithClock(clk), memory.WithJitter(func(int64) int64 { return 0 }))
	m, err := dw.New(st, dw.WithScopeDefaults(dw.ScopeConfig{SweepInterval: time.Millisecond}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})

	ctx := t.Context()
	if err := m.Configure(ctx, "s", dw.ScopeConfig{
		MaxAttempts: 1, DefaultLeaseTimeout: time.Second, MinLeaseTimeout: time.Millisecond,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if err := m.AddNode(ctx, "s", "a", nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := m.TryClaim(ctx, "s"); err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	clk.Advance(10 * time.Second)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n, err := m.GetNode(ctx, "s", "a")
		if err == nil && n.Status == dw.StatusError && n.Reason == dw.ReasonTimeout {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the background sweeper never reclaimed the expired lease")
}
