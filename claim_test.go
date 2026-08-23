package dagworker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

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
