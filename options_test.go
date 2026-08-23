package dagworker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
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
