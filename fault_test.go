package dagworker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/memory"
)

var errInjected = errors.New("injected fault")

// faultStore wraps a real store and fails whichever operations a test asks it
// to. The Manager's maintenance loops and its blocking claim must survive a
// backend that is misbehaving — a scheduler that dies when its database hiccups
// is worse than one that keeps trying — and these paths are otherwise
// unreachable from the public API.
type faultStore struct {
	dw.Store
	failScopes  atomic.Bool
	failSweep   atomic.Bool
	failConfig  atomic.Bool
	failCollect atomic.Bool
	failClaim   atomic.Bool
	failDoor    atomic.Bool
	sweeps      atomic.Int64
}

func (f *faultStore) Scopes(ctx context.Context) ([]dw.Scope, error) {
	if f.failScopes.Load() {
		return nil, errInjected
	}
	return f.Store.Scopes(ctx)
}

func (f *faultStore) Sweep(ctx context.Context, scope dw.Scope, limit int) (dw.SweepResult, error) {
	f.sweeps.Add(1)
	if f.failSweep.Load() {
		return dw.SweepResult{}, errInjected
	}
	return f.Store.Sweep(ctx, scope, limit)
}

func (f *faultStore) ScopeConfig(ctx context.Context, scope dw.Scope) (dw.ScopeConfig, error) {
	if f.failConfig.Load() {
		return dw.ScopeConfig{}, errInjected
	}
	return f.Store.ScopeConfig(ctx, scope)
}

func (f *faultStore) CollectTerminal(ctx context.Context, scope dw.Scope, cutoff time.Time, limit int) (int, bool, error) {
	if f.failCollect.Load() {
		return 0, false, errInjected
	}
	return f.Store.(dw.Collector).CollectTerminal(ctx, scope, cutoff, limit)
}

func (f *faultStore) Claim(ctx context.Context, req dw.ClaimRequest) (dw.ClaimResult, error) {
	if f.failClaim.Load() {
		return dw.ClaimResult{}, errInjected
	}
	return f.Store.Claim(ctx, req)
}

func (f *faultStore) WaitForWork(ctx context.Context, scope dw.Scope, kinds []string) error {
	if f.failDoor.Load() {
		return errInjected
	}
	return f.Store.(dw.Doorbell).WaitForWork(ctx, scope, kinds)
}

func (f *faultStore) Capabilities() dw.Capabilities {
	return f.Store.(dw.CapabilityReporter).Capabilities()
}

func newFaulty(t *testing.T, opts ...dw.Option) (*faultStore, *dw.Manager, *dagstoretest.FakeClock) {
	t.Helper()
	clk := dagstoretest.NewFakeClock()
	inner := memory.New(memory.WithClock(clk), memory.WithJitter(func(int64) int64 { return 0 }))
	fs := &faultStore{Store: inner}
	base := []dw.Option{
		dw.WithClock(clk),
		dw.WithPollInterval(30 * time.Millisecond),
		dw.WithScopeDefaults(dw.ScopeConfig{SweepInterval: time.Millisecond}),
	}
	m, err := dw.New(fs, append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = inner.Close(context.Background())
	})
	return fs, m, clk
}

// A backend that cannot even list its scopes must not stop the maintenance
// loop: the next tick has to try again.
func TestMaintenanceSurvivesAFailingBackend(t *testing.T) {
	t.Parallel()
	fs, m, clk := newFaulty(t)
	ctx := t.Context()

	fs.failScopes.Store(true)
	if err := m.AddNode(ctx, "s", "a", nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	clk.Advance(time.Second)
	time.Sleep(50 * time.Millisecond)

	// Now let listing work but make sweeping fail.
	fs.failScopes.Store(false)
	fs.failSweep.Store(true)
	before := fs.sweeps.Load()
	for range 20 {
		clk.Advance(time.Second)
		time.Sleep(5 * time.Millisecond)
	}
	if fs.sweeps.Load() <= before {
		t.Fatal("the maintenance loop stopped sweeping after a failure")
	}

	// And finally let everything work: the node must still be claimable.
	fs.failSweep.Store(false)
	if _, err := m.TryClaim(ctx, "s"); err != nil {
		t.Fatalf("TryClaim after the faults cleared: %v", err)
	}
}

func TestRetentionSurvivesAFailingBackend(t *testing.T) {
	t.Parallel()
	fs, m, clk := newFaulty(t)
	ctx := t.Context()

	if err := m.Configure(ctx, "s", dw.ScopeConfig{TerminalRetention: time.Second}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if err := m.AddNode(ctx, "s", "a", nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	l, err := m.TryClaim(ctx, "s")
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if err := m.Ack(ctx, l, nil); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Reading the scope's retention policy fails: collection is skipped, not
	// retried against a guessed policy.
	fs.failConfig.Store(true)
	clk.Advance(time.Hour)
	time.Sleep(30 * time.Millisecond)
	if _, err := m.GetNode(ctx, "s", "a"); err != nil {
		t.Fatalf("the node was collected while the policy was unreadable: %v", err)
	}

	// The policy reads but the collection itself fails: still no crash.
	fs.failConfig.Store(false)
	fs.failCollect.Store(true)
	for range 10 {
		clk.Advance(time.Minute)
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := m.GetNode(ctx, "s", "a"); err != nil {
		t.Fatalf("the node vanished while collection was failing: %v", err)
	}

	// Faults clear: it is collected.
	fs.failCollect.Store(false)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := m.GetNode(ctx, "s", "a"); errors.Is(err, dw.ErrNotFound) {
			return
		}
		clk.Advance(time.Minute)
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("collection never resumed after the fault cleared")
}

// A doorbell that fails is a degraded doorbell, not a broken claim: the poll
// underneath must still find the work.
func TestClaimFallsBackWhenTheDoorbellFails(t *testing.T) {
	t.Parallel()
	fs, m, _ := newFaulty(t)
	fs.failDoor.Store(true)
	ctx := t.Context()

	got := make(chan dw.Lease, 1)
	go func() {
		if l, err := m.Claim(ctx, "s"); err == nil {
			got <- l
		}
	}()

	time.Sleep(60 * time.Millisecond)
	if err := m.AddNode(ctx, "s", "n", nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	select {
	case l := <-got:
		if l.NodeID != "n" {
			t.Fatalf("claimed %q", l.NodeID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("with a failing doorbell the poll never found the work")
	}
}

// A store error during a claim is surfaced, not swallowed into an endless
// retry loop that looks like a hang.
func TestClaimSurfacesStoreErrors(t *testing.T) {
	t.Parallel()
	fs, m, _ := newFaulty(t)
	fs.failClaim.Store(true)
	ctx := t.Context()

	if _, err := m.Claim(ctx, "s"); !errors.Is(err, errInjected) {
		t.Fatalf("Claim gave %v, want the injected fault", err)
	}
	if _, err := m.TryClaim(ctx, "s"); !errors.Is(err, errInjected) {
		t.Fatalf("TryClaim gave %v", err)
	}
	if _, err := m.ClaimBatch(ctx, "s", 3); !errors.Is(err, errInjected) {
		t.Fatalf("ClaimBatch gave %v", err)
	}
}

func TestSweepSurfacesStoreErrors(t *testing.T) {
	t.Parallel()
	fs, m, _ := newFaulty(t)
	fs.failSweep.Store(true)
	if _, err := m.Sweep(t.Context(), "s"); !errors.Is(err, errInjected) {
		t.Fatalf("Sweep gave %v", err)
	}
	fs.failScopes.Store(true)
	if _, err := m.Scopes(t.Context()); !errors.Is(err, errInjected) {
		t.Fatalf("Scopes gave %v", err)
	}
}
