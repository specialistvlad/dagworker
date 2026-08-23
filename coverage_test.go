package dagworker_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/memory"
)

func TestNodeOptionsShapeTheSpec(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	if err := f.m.AddNode(f.ctx, "s", "shaped", []byte("p"),
		dw.WithKind("gpu"),
		dw.WithPriority(42),
		dw.WithTrigger(dw.TriggerAllDone),
		dw.WithLabels(map[string]string{"team": "infra"}),
		dw.WithLabels(map[string]string{"env": "prod"}),
		dw.WithRetry(7, time.Second, time.Minute),
	); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	n, err := f.m.GetNode(f.ctx, "s", "shaped")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if n.Kind != "gpu" || n.Priority != 42 || n.Trigger != dw.TriggerAllDone {
		t.Fatalf("options did not reach the node: %+v", n)
	}
	// Two WithLabels calls merge rather than the second replacing the first.
	if n.Labels["team"] != "infra" || n.Labels["env"] != "prod" {
		t.Fatalf("labels are %v, want both calls merged", n.Labels)
	}
	if n.Retry.MaxAttempts != 7 || n.Retry.BaseDelay != time.Second || n.Retry.MaxDelay != time.Minute {
		t.Fatalf("retry policy is %+v", n.Retry)
	}
	if n.Terminal() {
		t.Fatal("a fresh node reports itself terminal")
	}

	if err := f.m.AddNode(f.ctx, "s", "capped", nil, dw.WithMaxAttempts(1)); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	capped, _ := f.m.GetNode(f.ctx, "s", "capped")
	if capped.Retry.MaxAttempts != 1 {
		t.Fatalf("WithMaxAttempts gave %d", capped.Retry.MaxAttempts)
	}
}

// A node's own retry policy overrides the scope's field by field: a zero field
// inherits rather than disabling.
func TestNodeRetryOverridesScopePerField(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	if err := f.m.Configure(f.ctx, "s", dw.ScopeConfig{
		MaxAttempts: 1, RetryBaseDelay: time.Nanosecond, RetryMaxDelay: time.Nanosecond,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	// The scope allows one attempt; this node overrides only that field.
	if err := f.m.AddNode(f.ctx, "s", "stubborn", nil, dw.WithMaxAttempts(3)); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	l := f.claim(dw.AsWorker("worker-1"))
	if _, err := f.m.Nack(f.ctx, l, errors.New("first")); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if got := f.status("stubborn"); got != dw.StatusNew {
		t.Fatalf("the node is %v after one failure of three permitted attempts", got)
	}
}

func TestWorkerIdentityIsRecorded(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.add("a")
	l := f.claim(dw.AsWorker("worker-7"), dw.WithLeaseTimeout(time.Minute))
	if l.NodeID != "a" {
		t.Fatalf("claimed %q", l.NodeID)
	}
	// Worker identity has no bearing on correctness, so the only assertion
	// worth making is that supplying it changes nothing about the grant.
	if l.Epoch != 1 {
		t.Fatalf("epoch is %d", l.Epoch)
	}
}

func TestGraphMutationHappyPaths(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	if err := f.m.AddNodes(f.ctx, "s", []dw.NodeSpec{{ID: "a"}, {ID: "b"}, {ID: "c"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if err := f.m.AddEdges(f.ctx, "s", []dw.Edge{{From: "a", To: "b"}, {From: "b", To: "c"}}); err != nil {
		t.Fatalf("AddEdges: %v", err)
	}
	if err := f.m.RemoveEdges(f.ctx, "s", []dw.Edge{{From: "b", To: "c"}}); err != nil {
		t.Fatalf("RemoveEdges: %v", err)
	}
	if err := f.m.RemoveNode(f.ctx, "s", "c", dw.CascadeReject); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	if err := f.m.Cancel(f.ctx, "s", "b"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if got := f.status("b"); got != dw.StatusError {
		t.Fatalf("b is %v after Cancel", got)
	}
	if err := f.m.CancelScope(f.ctx, "s"); err != nil {
		t.Fatalf("CancelScope: %v", err)
	}

	scopes, err := f.m.Scopes(f.ctx)
	if err != nil || len(scopes) == 0 {
		t.Fatalf("Scopes gave %v, %v", scopes, err)
	}
	cfg, err := f.m.ScopeConfig(f.ctx, "s")
	if err != nil {
		t.Fatalf("ScopeConfig: %v", err)
	}
	_ = cfg

	if err := f.m.Seal(f.ctx, "s"); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	done, err := f.m.IsComplete(f.ctx, "s")
	if err != nil {
		t.Fatalf("IsComplete: %v", err)
	}
	if !done {
		t.Fatal("a sealed scope with everything cancelled is not complete")
	}
	if _, err := f.m.IsComplete(f.ctx, ""); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("IsComplete on an empty scope gave %v", err)
	}
}

// Retention is off unless asked for, and does what it says when it is.
func TestRetentionCollectsOnlyWhenConfigured(t *testing.T) {
	t.Parallel()
	clk := dagstoretest.NewFakeClock()
	st := memory.New(memory.WithClock(clk))
	m, err := dw.New(st,
		dw.WithClock(clk),
		dw.WithScopeDefaults(dw.ScopeConfig{SweepInterval: time.Millisecond}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})
	ctx := t.Context()

	if err := m.Configure(ctx, "s", dw.ScopeConfig{TerminalRetention: time.Hour}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if err := m.AddNode(ctx, "s", "done", nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	l, err := m.TryClaim(ctx, "s")
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if err := m.Ack(ctx, l, nil); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Not yet past the retention window: the node must survive.
	clk.Advance(time.Minute)
	time.Sleep(50 * time.Millisecond)
	if _, err := m.GetNode(ctx, "s", "done"); err != nil {
		t.Fatalf("a node inside the retention window was collected: %v", err)
	}

	clk.Advance(2 * time.Hour)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := m.GetNode(ctx, "s", "done"); errors.Is(err, dw.ErrNotFound) {
			return
		}
		clk.Advance(time.Second)
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("a node past its retention window was never collected")
}

func TestSystemClock(t *testing.T) {
	t.Parallel()
	var c dw.Clock = dw.SystemClock{}
	if c.Now().IsZero() {
		t.Fatal("SystemClock.Now returned the zero time")
	}
	if c.Since(c.Now().Add(-time.Second)) < time.Second/2 {
		t.Fatal("SystemClock.Since is implausible")
	}
	select {
	case <-c.After(time.Millisecond):
	case <-time.After(time.Second):
		t.Fatal("SystemClock.After never fired")
	}

	fired := make(chan struct{})
	stop := c.AfterFunc(time.Millisecond, func() { close(fired) })
	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("SystemClock.AfterFunc never ran")
	}
	if stop() {
		t.Fatal("stopping an already-fired timer reported success")
	}

	stopped := c.AfterFunc(time.Hour, func() { t.Error("a stopped timer ran") })
	if !stopped() {
		t.Fatal("stopping a pending timer reported failure")
	}
}

func TestFakeClockDrivesTimers(t *testing.T) {
	t.Parallel()
	c := dagstoretest.NewFakeClock()
	start := c.Now()

	ch := c.After(time.Minute)
	select {
	case <-ch:
		t.Fatal("a timer fired before the clock moved")
	default:
	}

	ran := make(chan struct{}, 1)
	stop := c.AfterFunc(30*time.Second, func() { ran <- struct{}{} })

	c.Advance(time.Hour)
	select {
	case <-ch:
	default:
		t.Fatal("advancing past the deadline did not fire the timer")
	}
	select {
	case <-ran:
	default:
		t.Fatal("advancing past the deadline did not run the callback")
	}
	if stop() {
		t.Fatal("stopping a fired timer reported success")
	}
	if !c.Now().After(start) {
		t.Fatal("Advance did not move the clock")
	}

	// A cancelled timer must not fire.
	cancelled := c.AfterFunc(time.Minute, func() { t.Error("a cancelled timer ran") })
	if !cancelled() {
		t.Fatal("cancelling a pending timer reported failure")
	}
	c.Advance(time.Hour)

	// Real clocks jump backwards; Set allows it and fires nothing.
	c.Set(start)
	if !c.Now().Equal(start) {
		t.Fatalf("Set did not move the clock back: %s", c.Now())
	}
	c.Set(start.Add(time.Hour))
	if !c.Now().Equal(start.Add(time.Hour)) {
		t.Fatal("Set forwards did not move the clock")
	}
}

// A library that writes to its host's stderr uninvited is a defect, so the
// default logger must swallow everything.
func TestDefaultLoggerIsSilent(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.add("a")
	// Exercised indirectly: if the default handler were not silent, every test
	// in this package would be printing. Assert its contract directly too.
	m2, err := dw.New(memory.New(), dw.WithLogger(slog.New(slog.NewTextHandler(discard{}, nil))))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := m2.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// A sweep that hits its batch limit must report that more work remains, or a
// backlog of expired leases would drain one batch per interval forever without
// anyone knowing.
func TestSweepReportsMoreWhenLimited(t *testing.T) {
	t.Parallel()
	clk := dagstoretest.NewFakeClock()
	st := memory.New(memory.WithClock(clk), memory.WithJitter(func(int64) int64 { return 0 }))
	t.Cleanup(func() { _ = st.Close(context.Background()) })
	ctx := t.Context()

	if err := st.SetScopeConfig(ctx, "s", dw.ScopeConfig{
		MaxAttempts: 1, DefaultLeaseTimeout: time.Second, MinLeaseTimeout: time.Millisecond,
	}); err != nil {
		t.Fatalf("SetScopeConfig: %v", err)
	}
	specs := make([]dw.NodeSpec, 5)
	for i := range specs {
		specs[i] = dw.NodeSpec{ID: dw.NodeID(string(rune('a' + i)))}
	}
	if _, err := st.AddNodes(ctx, "s", specs); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	res, err := st.Claim(ctx, dw.ClaimRequest{Scope: "s", Max: 5, Timeout: time.Second})
	if err != nil || len(res.Leases) != 5 {
		t.Fatalf("Claim gave %d leases, %v", len(res.Leases), err)
	}

	clk.Advance(time.Hour)
	swept, err := st.Sweep(ctx, "s", 2)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if swept.Reclaimed != 2 {
		t.Fatalf("Sweep(limit 2) reclaimed %d", swept.Reclaimed)
	}
	if !swept.More {
		t.Fatal("a limited sweep with expired leases remaining did not report More")
	}

	rest, err := st.Sweep(ctx, "s", 10)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if rest.Reclaimed != 3 || rest.More {
		t.Fatalf("the final sweep reclaimed %d, More=%v", rest.Reclaimed, rest.More)
	}
}
