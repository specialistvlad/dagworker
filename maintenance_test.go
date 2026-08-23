package dagworker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/memory"
)

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

// TestPerScopeSweepIntervalIsHonoured pins down a per-scope setting that was
// accepted, validated, persisted by every backend and echoed through both
// adapters, and then read by nobody: the maintenance loop used the
// Manager-wide construction-time default for every scope.
//
// The failure mode is the quiet kind. A scope configured to reclaim dead
// workers every 200ms on a Manager whose default is a minute does exactly what
// it was told to do in its configuration and nothing at all in practice, and
// the only symptom is leases sitting expired for far longer than asked.
func TestPerScopeSweepIntervalIsHonoured(t *testing.T) {
	t.Parallel()

	clk := dagstoretest.NewFakeClock()
	st := memory.New(memory.WithClock(clk))
	// A Manager-wide default far longer than the scope's own setting: if the
	// scope's value is ignored, nothing is swept for a simulated hour.
	m, err := dw.New(st,
		dw.WithClock(clk),
		dw.WithScopeDefaults(dw.ScopeConfig{SweepInterval: time.Hour}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})
	ctx := t.Context()

	if err := m.Configure(ctx, "brisk", dw.ScopeConfig{
		SweepInterval:       200 * time.Millisecond,
		DefaultLeaseTimeout: time.Second,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if err := m.AddNode(ctx, "brisk", "abandoned", nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := m.TryClaim(ctx, "brisk"); err != nil {
		t.Fatalf("TryClaim: %v", err)
	}

	// The worker dies. Past the lease deadline, and past several of the
	// scope's own sweep intervals, but nowhere near the Manager's default.
	clk.Advance(2 * time.Second)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		insp, err := m.Inspect(ctx, "brisk", "abandoned")
		if err != nil {
			t.Fatalf("Inspect: %v", err)
		}
		if insp.Phase != dw.PhaseClaimed {
			return // reclaimed, which is the whole point
		}
		clk.Advance(200 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("a scope configured to sweep every 200ms was still holding an expired lease " +
		"after simulated hours: its SweepInterval is being ignored")
}
