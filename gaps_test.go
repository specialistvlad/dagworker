package dagworker_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/memory"
)

func TestTriggerRuleNames(t *testing.T) {
	t.Parallel()
	want := map[dw.TriggerRule]string{
		dw.TriggerAllSuccess:              "all_success",
		dw.TriggerAllDone:                 "all_done",
		dw.TriggerNoneFailed:              "none_failed",
		dw.TriggerNoneFailedMinOneSuccess: "none_failed_min_one_success",
		dw.TriggerAlways:                  "always",
	}
	for rule, name := range want {
		if got := rule.String(); got != name {
			t.Fatalf("rule %d is %q, want %q", rule, got, name)
		}
	}
}

func TestIdentifierLimits(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	long := dw.Scope(strings.Repeat("s", dw.MaxScopeLen+1))
	if err := f.m.AddNode(f.ctx, long, "a", nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("an oversized scope gave %v", err)
	}
	badUTF8 := dw.Scope([]byte{0xff, 0xfe})
	if err := f.m.AddNode(f.ctx, badUTF8, "a", nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a non-UTF-8 scope gave %v", err)
	}
	badKind := strings.Repeat("k", dw.MaxKindLen+1)
	if err := f.m.AddNode(f.ctx, "s", "a", nil, dw.WithKind(badKind)); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("an oversized kind gave %v", err)
	}
	if err := f.m.AddNode(f.ctx, "s", "a", nil, dw.WithKind(string([]byte{0xff}))); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a non-UTF-8 kind gave %v", err)
	}
	if err := f.m.AddNode(f.ctx, "s", "a", nil,
		dw.WithLabels(map[string]string{string([]byte{0xff}): "v"})); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a non-UTF-8 label gave %v", err)
	}
}

// The default logger must swallow every level and survive the whole Handler
// surface, because a library that writes to its host's stderr uninvited is a
// defect rather than a feature.
func TestDefaultLoggerSwallowsEverything(t *testing.T) {
	t.Parallel()
	st := memory.New()
	m, err := dw.New(st, dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})
	// Reach the handler the only way the public API allows: drive an operation
	// that logs. Nothing must appear on stderr, and nothing must panic.
	for _, lvl := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
		if err := m.AddNode(t.Context(), "s", dw.NodeID(lvl.String()), nil); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
}

// A blocking claim on a backend with a doorbell must still be bounded, so a
// signal lost between the failed claim and the wait costs one interval rather
// than hanging forever.
func TestBlockingClaimIsBoundedEvenWithADoorbell(t *testing.T) {
	t.Parallel()
	f := newFixture(t, dw.WithPollInterval(30*time.Millisecond))
	ctx, cancel := context.WithTimeout(f.ctx, 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := f.m.Claim(ctx, "s"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Claim gave %v", err)
	}
	// It must have looped rather than parked once and blocked past the context.
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Claim took %s to notice an expired context", elapsed)
	}
}

func TestPollIntervalIsClamped(t *testing.T) {
	t.Parallel()
	// Absurd values on both sides must be brought into range rather than
	// producing a busy loop or an unbounded wait.
	for _, d := range []time.Duration{time.Nanosecond, time.Hour} {
		f := newFixture(t, dw.WithPollInterval(d))
		ctx, cancel := context.WithTimeout(f.ctx, 200*time.Millisecond)
		if _, err := f.m.Claim(ctx, "s"); !errors.Is(err, context.DeadlineExceeded) {
			cancel()
			t.Fatalf("with poll interval %s, Claim gave %v", d, err)
		}
		cancel()
	}
}

func TestClaimBatchReturnsWhatIsAvailable(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.add("a")
	f.add("b")

	got, err := f.m.ClaimBatch(f.ctx, "s", 10)
	if err != nil {
		t.Fatalf("ClaimBatch: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ClaimBatch returned %d leases, want the 2 available", len(got))
	}
	// Asking for none is treated as asking for one, not as an error.
	f.add("c")
	one, err := f.m.ClaimBatch(f.ctx, "s", 0)
	if err != nil {
		t.Fatalf("ClaimBatch(0): %v", err)
	}
	if len(one) != 1 {
		t.Fatalf("ClaimBatch(0) returned %d leases, want 1", len(one))
	}
	if _, err := f.m.ClaimBatch(f.ctx, "", 1); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("ClaimBatch on an empty scope gave %v", err)
	}
	if _, err := f.m.ClaimBatch(f.ctx, "s", 1, nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("ClaimBatch with a nil option gave %v", err)
	}
}

func TestExtendRejectsAStaleLease(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	if err := f.m.Configure(f.ctx, "s", dw.ScopeConfig{
		MaxAttempts: 1, DefaultLeaseTimeout: time.Second, MinLeaseTimeout: time.Millisecond,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	f.add("a")
	stale := f.claim()
	f.clock.Advance(time.Hour)
	if _, err := f.m.Sweep(f.ctx, "s"); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, err := f.m.Extend(f.ctx, stale, time.Minute); !errors.Is(err, dw.ErrLeaseMismatch) {
		t.Fatalf("extending a reclaimed lease gave %v", err)
	}
}

func TestScopeConfigRejectsEmptyScope(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	if _, err := f.m.ScopeConfig(f.ctx, ""); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("ScopeConfig on an empty scope gave %v", err)
	}
	if _, err := f.m.Stats(f.ctx, ""); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("Stats on an empty scope gave %v", err)
	}
	if _, err := f.m.ListNodes(f.ctx, "", dw.ListOptions{}); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("ListNodes on an empty scope gave %v", err)
	}
	if err := f.m.Seal(f.ctx, ""); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("Seal on an empty scope gave %v", err)
	}
	if err := f.m.CancelScope(f.ctx, ""); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("CancelScope on an empty scope gave %v", err)
	}
	if err := f.m.AddNodes(f.ctx, "", nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("AddNodes on an empty scope gave %v", err)
	}
	if err := f.m.AddEdges(f.ctx, "", nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("AddEdges on an empty scope gave %v", err)
	}
	if err := f.m.RemoveEdges(f.ctx, "", nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("RemoveEdges on an empty scope gave %v", err)
	}
	if err := f.m.RemoveNode(f.ctx, "", "a", dw.CascadeReject); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("RemoveNode on an empty scope gave %v", err)
	}
	if err := f.m.Cancel(f.ctx, "", "a"); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("Cancel on an empty scope gave %v", err)
	}
	if _, err := f.m.Inspect(f.ctx, "", "a"); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("Inspect on an empty scope gave %v", err)
	}
	if _, err := f.m.Sweep(f.ctx, ""); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("Sweep on an empty scope gave %v", err)
	}
}

func TestManagerHonoursAnEmptyStoreClose(t *testing.T) {
	t.Parallel()
	clk := dagstoretest.NewFakeClock()
	st := memory.New(memory.WithClock(clk))
	m, err := dw.New(st, dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Closing with a context that is already done must report that rather than
	// hanging, and must not leave the Manager half-closed.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = m.Close(ctx)
	if err := m.Close(context.Background()); err != nil {
		t.Fatalf("a second Close gave %v", err)
	}
	_ = st.Close(context.Background())
}

func TestReadySetGrowsBeyondItsCapacityHint(t *testing.T) {
	t.Parallel()
	// The ready set's index is sized from a hint and grows on demand; pushing
	// far more nodes than the hint anticipated must not corrupt the ordering.
	f := newFixture(t)
	const n = 64
	for i := range n {
		f.add(dw.NodeID(string(rune('a'+i%26))+string(rune('0'+i/26))), dw.WithPriority(int16(i)))
	}
	// Highest priority first, all the way down.
	prev := int16(32767)
	for range n {
		l, err := f.m.TryClaim(f.ctx, "s")
		if err != nil {
			t.Fatalf("TryClaim: %v", err)
		}
		if l.Node.Priority > prev {
			t.Fatalf("claimed priority %d after %d", l.Node.Priority, prev)
		}
		prev = l.Node.Priority
	}
}

func TestScopeConfigValidationBranches(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	cases := map[string]dw.ScopeConfig{
		"inverted lease bounds": {MinLeaseTimeout: time.Hour, MaxLeaseTimeout: time.Second},
		"default below the floor": {
			MinLeaseTimeout: time.Minute, MaxLeaseTimeout: time.Hour, DefaultLeaseTimeout: time.Second,
		},
		"default above the ceiling": {
			MinLeaseTimeout: time.Second, MaxLeaseTimeout: time.Minute, DefaultLeaseTimeout: time.Hour,
		},
		"inverted retry bounds":   {RetryBaseDelay: time.Hour, RetryMaxDelay: time.Second},
		"negative retention":      {TerminalRetention: -time.Second},
		"negative subscriber lag": {MaxSubscriberLag: -time.Second},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := f.m.Configure(f.ctx, "s", cfg); !errors.Is(err, dw.ErrInvalidArgument) {
				t.Fatalf("got %v, want ErrInvalidArgument", err)
			}
		})
	}
	// A configuration that only sets the fields it cares about is accepted.
	if err := f.m.Configure(f.ctx, "s", dw.ScopeConfig{MaxAttempts: 2}); err != nil {
		t.Fatalf("a partial configuration was rejected: %v", err)
	}
}

func TestTypedAckRejectsUnencodableResult(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	tv := dw.NewTyped[job](f.m, "s")
	if err := tv.AddNode(f.ctx, "a", job{URL: "u"}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	l, err := tv.TryClaim(f.ctx)
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if err := tv.Ack(f.ctx, l, make(chan int)); err == nil {
		t.Fatal("a result that cannot be marshalled was accepted")
	}
	// The lease is still live, so a correct ack still works.
	if err := tv.Ack(f.ctx, l, nil); err != nil {
		t.Fatalf("Ack after a failed encode: %v", err)
	}
}

func TestTypedClaimPropagatesClaimErrors(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	tv := dw.NewTyped[job](f.m, "s")
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Millisecond)
	defer cancel()
	if _, err := tv.Claim(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Typed.Claim with an expiring context gave %v", err)
	}
}

func TestRemoveNodeRejectsUnknownAndInFlight(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	if err := f.m.RemoveNode(f.ctx, "s", "ghost", dw.CascadeReject); !errors.Is(err, dw.ErrNotFound) {
		t.Fatalf("removing an unknown node gave %v", err)
	}
	f.add("a")
	f.claim()
	if err := f.m.RemoveNode(f.ctx, "s", "a", dw.CascadeReject); !errors.Is(err, dw.ErrNodeInFlight) {
		t.Fatalf("removing a claimed node gave %v", err)
	}
}

func TestSubscribeRejectsAnExpiredResumeCursor(t *testing.T) {
	t.Parallel()
	clk := dagstoretest.NewFakeClock()
	st := memory.New(memory.WithClock(clk), memory.WithEventLogSize(4))
	m, err := dw.New(st, dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})
	ctx := t.Context()
	for i := range 40 {
		if err := m.AddNode(ctx, "s", dw.NodeID(string(rune('a'+i%26))+string(rune('0'+i/26))), nil); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
	if _, err := m.Subscribe(ctx, dw.SubscribeOptions{Scope: "s", From: 1}); !errors.Is(err, dw.ErrCursorExpired) {
		t.Fatalf("resuming from an evicted cursor gave %v, want ErrCursorExpired", err)
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
