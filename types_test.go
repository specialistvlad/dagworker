package dagworker_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

func TestStatusText(t *testing.T) {
	t.Parallel()
	for _, s := range []dw.Status{dw.StatusNew, dw.StatusInProgress, dw.StatusSuccess, dw.StatusError} {
		text, err := s.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText(%v): %v", s, err)
		}
		var back dw.Status
		if err := back.UnmarshalText(text); err != nil {
			t.Fatalf("UnmarshalText(%q): %v", text, err)
		}
		if back != s {
			t.Fatalf("round trip gave %v, want %v", back, s)
		}
		if s.String() != string(text) {
			t.Fatalf("String is %q but MarshalText is %q", s.String(), text)
		}
	}
	if _, err := dw.Status(99).MarshalText(); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("marshalling an out-of-range status gave %v", err)
	}
	var s dw.Status
	if err := s.UnmarshalText([]byte("nonsense")); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("unmarshalling nonsense gave %v", err)
	}
	if dw.Status(99).String() == "" {
		t.Fatal("an out-of-range status has no string form")
	}
}

func TestStatusTerminal(t *testing.T) {
	t.Parallel()
	if dw.StatusNew.Terminal() || dw.StatusInProgress.Terminal() {
		t.Fatal("a non-final status reports itself terminal")
	}
	if !dw.StatusSuccess.Terminal() || !dw.StatusError.Terminal() {
		t.Fatal("a final status does not report itself terminal")
	}
}

func TestReasonText(t *testing.T) {
	t.Parallel()
	all := []dw.Reason{
		dw.ReasonNone, dw.ReasonWorkerError, dw.ReasonTimeout, dw.ReasonUpstreamFailed,
		dw.ReasonSkipped, dw.ReasonCancelled, dw.ReasonRemoved,
	}
	for _, r := range all {
		text, err := r.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText(%v): %v", r, err)
		}
		var back dw.Reason
		if err := back.UnmarshalText(text); err != nil {
			t.Fatalf("UnmarshalText(%q): %v", text, err)
		}
		if back != r {
			t.Fatalf("round trip gave %v, want %v", back, r)
		}
	}
	if _, err := dw.Reason(99).MarshalText(); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("marshalling an out-of-range reason gave %v", err)
	}
	var r dw.Reason
	if err := r.UnmarshalText([]byte("nonsense")); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("unmarshalling nonsense gave %v", err)
	}
	if dw.Reason(99).String() == "" {
		t.Fatal("an out-of-range reason has no string form")
	}
}

// Phase is internal detail, but its mapping onto the public status is the one
// place the two vocabularies meet, so it must be total.
func TestPhaseMapsToStatus(t *testing.T) {
	t.Parallel()
	cases := map[dw.Phase]dw.Status{
		dw.PhaseBlocked:   dw.StatusNew,
		dw.PhaseScheduled: dw.StatusNew,
		dw.PhaseReady:     dw.StatusNew,
		dw.PhaseClaimed:   dw.StatusInProgress,
		// PhaseDone alone cannot distinguish success from failure, so it maps
		// to the pessimistic answer; asserted again below for the reason.
		dw.PhaseDone: dw.StatusError,
	}
	for p, want := range cases {
		if got := p.Status(); got != want {
			t.Fatalf("%v maps to %v, want %v", p, got, want)
		}
		if p.String() == "" {
			t.Fatalf("%v has no string form", p)
		}
	}
	// PhaseDone alone cannot distinguish success from failure, and reports the
	// pessimistic answer so a bug cannot make a failed node look successful.
	if got := dw.PhaseDone.Status(); got != dw.StatusError {
		t.Fatalf("PhaseDone maps to %v, want the pessimistic StatusError", got)
	}
	if dw.Phase(99).String() == "" {
		t.Fatal("an out-of-range phase has no string form")
	}
}

func TestTriggerRuleEvaluation(t *testing.T) {
	t.Parallel()
	type want struct{ ready, unsatisfiable bool }
	cases := []struct {
		name  string
		rule  dw.TriggerRule
		deps  dw.DepCounts
		want  want
		after dw.Reason
	}{
		{"all_success waits", dw.TriggerAllSuccess, dw.DepCounts{Unsatisfied: 1, Succeeded: 1}, want{false, false}, 0},
		{"all_success runs", dw.TriggerAllSuccess, dw.DepCounts{Succeeded: 2}, want{true, false}, 0},
		{"all_success stops on failure", dw.TriggerAllSuccess, dw.DepCounts{Succeeded: 1, Failed: 1}, want{false, true}, dw.ReasonUpstreamFailed},
		{"all_success stops on skip", dw.TriggerAllSuccess, dw.DepCounts{Succeeded: 1, Skipped: 1}, want{false, true}, dw.ReasonSkipped},

		{"all_done waits", dw.TriggerAllDone, dw.DepCounts{Unsatisfied: 1}, want{false, false}, 0},
		{"all_done runs after failure", dw.TriggerAllDone, dw.DepCounts{Failed: 2}, want{true, false}, 0},

		{"none_failed accepts a skip", dw.TriggerNoneFailed, dw.DepCounts{Succeeded: 1, Skipped: 1}, want{true, false}, 0},
		{"none_failed rejects a failure", dw.TriggerNoneFailed, dw.DepCounts{Failed: 1}, want{false, true}, dw.ReasonUpstreamFailed},

		{"min_one_success needs one", dw.TriggerNoneFailedMinOneSuccess, dw.DepCounts{Skipped: 2}, want{false, true}, dw.ReasonSkipped},
		{"min_one_success satisfied", dw.TriggerNoneFailedMinOneSuccess, dw.DepCounts{Succeeded: 1, Skipped: 1}, want{true, false}, 0},
		{"min_one_success rejects a failure", dw.TriggerNoneFailedMinOneSuccess, dw.DepCounts{Succeeded: 1, Failed: 1}, want{false, true}, dw.ReasonUpstreamFailed},

		{"always ignores everything", dw.TriggerAlways, dw.DepCounts{Unsatisfied: 5, Failed: 3}, want{true, false}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.deps.Ready(tc.rule); got != tc.want.ready {
				t.Fatalf("Ready is %v, want %v", got, tc.want.ready)
			}
			if got := tc.deps.Unsatisfiable(tc.rule); got != tc.want.unsatisfiable {
				t.Fatalf("Unsatisfiable is %v, want %v", got, tc.want.unsatisfiable)
			}
			if tc.want.unsatisfiable {
				if got := tc.deps.TerminalReason(); got != tc.after {
					t.Fatalf("TerminalReason is %v, want %v", got, tc.after)
				}
			}
		})
	}
}

func TestDepCountsTotal(t *testing.T) {
	t.Parallel()
	d := dw.DepCounts{Unsatisfied: 1, Succeeded: 2, Skipped: 3, Failed: 4}
	if d.Total() != 10 {
		t.Fatalf("Total is %d, want 10", d.Total())
	}
	if dw.TriggerRule(99).String() == "" {
		t.Fatal("an out-of-range trigger rule has no string form")
	}
	empty := dw.DepCounts{}
	if empty.Ready(dw.TriggerRule(99)) || empty.Unsatisfiable(dw.TriggerRule(99)) {
		t.Fatal("an unknown rule should be inert, not accidentally satisfied")
	}
}

// Full jitter must stay inside the exponentially growing window and must never
// overflow into a negative duration however high the attempt count goes.
func TestBackoffFullJitter(t *testing.T) {
	t.Parallel()
	base, ceiling := time.Second, time.Minute

	// With the jitter forced to its maximum, the window is what doubles.
	maxJitter := func(n int64) int64 { return n - 1 }
	prev := time.Duration(0)
	for attempt := uint32(1); attempt <= 10; attempt++ {
		got := dw.Backoff(attempt, base, ceiling, maxJitter)
		if got < 0 {
			t.Fatalf("attempt %d gave a negative delay %s", attempt, got)
		}
		if got > ceiling {
			t.Fatalf("attempt %d gave %s, above the cap %s", attempt, got, ceiling)
		}
		if got < prev {
			t.Fatalf("attempt %d gave %s, below the previous %s", attempt, got, prev)
		}
		prev = got
	}

	// A very large attempt count must saturate at the cap rather than wrap.
	if got := dw.Backoff(1_000_000, base, ceiling, maxJitter); got < 0 || got > ceiling {
		t.Fatalf("a huge attempt count gave %s", got)
	}
	// Zero is treated as the first attempt.
	if dw.Backoff(0, base, ceiling, nil) != base {
		t.Fatalf("attempt 0 gave %s, want the base delay", dw.Backoff(0, base, ceiling, nil))
	}
	// Without a jitter function the full window is returned.
	if got := dw.Backoff(1, base, ceiling, nil); got != base {
		t.Fatalf("with no jitter, attempt 1 gave %s, want %s", got, base)
	}
	// Zero bounds fall back to the library defaults rather than producing zero.
	if got := dw.Backoff(1, 0, 0, nil); got != dw.DefaultRetryBaseDelay {
		t.Fatalf("zero bounds gave %s, want the default %s", got, dw.DefaultRetryBaseDelay)
	}
}

func TestScopeConfigResolved(t *testing.T) {
	t.Parallel()
	got := dw.ScopeConfig{}.Resolved()
	if got.DefaultLeaseTimeout != dw.DefaultLeaseTimeout ||
		got.MaxAttempts != dw.DefaultMaxAttempts ||
		got.PayloadCap != dw.DefaultPayloadCap ||
		got.PartitionCount != 1 {
		t.Fatalf("the zero config resolved to %+v", got)
	}
	// The three fields whose zero means "disabled" must stay zero: a library
	// that deletes a caller's data because they did not configure retention is
	// a defect.
	if got.TerminalRetention != 0 || got.MaxSubscriberLag != 0 || got.MaxInFlight != 0 {
		t.Fatalf("a disabled-by-default field was given a value: %+v", got)
	}

	// An explicit value survives resolution.
	explicit := dw.ScopeConfig{MaxAttempts: 9, PayloadCap: 7}.Resolved()
	if explicit.MaxAttempts != 9 || explicit.PayloadCap != 7 {
		t.Fatalf("explicit values were overwritten: %+v", explicit)
	}
}

func TestScopeConfigClampLease(t *testing.T) {
	t.Parallel()
	cfg := dw.ScopeConfig{
		DefaultLeaseTimeout: 30 * time.Second,
		MinLeaseTimeout:     time.Second,
		MaxLeaseTimeout:     time.Minute,
	}
	cases := []struct{ in, want time.Duration }{
		{0, 30 * time.Second},                // zero takes the default
		{time.Millisecond, time.Second},      // below the floor
		{time.Hour, time.Minute},             // above the ceiling
		{10 * time.Second, 10 * time.Second}, // inside the bounds, untouched
	}
	for _, tc := range cases {
		if got := cfg.ClampLease(tc.in); got != tc.want {
			t.Fatalf("ClampLease(%s) is %s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestCapabilities(t *testing.T) {
	t.Parallel()
	cs := dw.Capabilities(dw.CapList | dw.CapDoorbell)
	if !cs.Has(dw.CapList) || !cs.Has(dw.CapDoorbell) {
		t.Fatal("a set does not report a capability it holds")
	}
	if cs.Has(dw.CapDurableEvents) {
		t.Fatal("a set reports a capability it does not hold")
	}
	// Has takes a set, not a single bit, so it must require every bit present.
	if cs.Has(dw.CapList | dw.CapCollect) {
		t.Fatal("Has reported true for a set only partly held")
	}
	if !cs.Has(dw.CapList | dw.CapDoorbell) {
		t.Fatal("Has reported false for a set fully held")
	}
}

func TestErrorTypes(t *testing.T) {
	t.Parallel()
	ce := &dw.CycleError{Scope: "s", From: "a", To: "b", Path: []dw.NodeID{"b", "c", "a"}}
	if !errors.Is(ce, dw.ErrCycle) {
		t.Fatal("CycleError does not unwrap to ErrCycle")
	}
	if ce.Error() == "" {
		t.Fatal("CycleError has an empty message")
	}
	bare := &dw.CycleError{Scope: "s", From: "a", To: "b"}
	if bare.Error() == ce.Error() {
		t.Fatal("a cycle with a known path reads the same as one without")
	}

	pe := &dw.PayloadTooLargeError{Size: 10, Cap: 5}
	if !errors.Is(pe, dw.ErrPayloadTooLarge) || pe.Error() == "" {
		t.Fatalf("PayloadTooLargeError is wrong: %v", pe)
	}

	ie := &dw.InvalidArgumentError{Field: "scope", Detail: "empty"}
	if !errors.Is(ie, dw.ErrInvalidArgument) || ie.Error() == "" {
		t.Fatalf("InvalidArgumentError is wrong: %v", ie)
	}
}

func TestLeaseValid(t *testing.T) {
	t.Parallel()
	if (dw.Lease{}).Valid() {
		t.Fatal("the zero lease claims to be valid")
	}
	if (dw.Lease{Scope: "s", NodeID: "n"}).Valid() {
		t.Fatal("a lease with no epoch claims to be valid")
	}
	if !(dw.Lease{Scope: "s", NodeID: "n", Epoch: 1}).Valid() {
		t.Fatal("a well-formed lease is reported invalid")
	}
}

func TestScopeStatsNonTerminal(t *testing.T) {
	t.Parallel()
	st := dw.ScopeStats{Blocked: 1, Scheduled: 2, Ready: 3, InProgress: 4, Succeeded: 5, Failed: 6}
	if got := st.NonTerminal(); got != 10 {
		t.Fatalf("NonTerminal is %d, want 10", got)
	}
}

func TestEventKindAndOverflowText(t *testing.T) {
	t.Parallel()
	for _, k := range []dw.EventKind{dw.EventCreated, dw.EventTransition, dw.EventReady} {
		if k.String() == "" {
			t.Fatalf("event kind %d has no string form", k)
		}
	}
	if dw.EventKind(99).String() == "" {
		t.Fatal("an out-of-range event kind has no string form")
	}
	for _, p := range []dw.OverflowPolicy{dw.OverflowDropOldest, dw.OverflowCloseSlow} {
		if p.String() == "" {
			t.Fatalf("overflow policy %d has no string form", p)
		}
	}
	if dw.OverflowPolicy(99).String() == "" {
		t.Fatal("an out-of-range overflow policy has no string form")
	}
	for _, c := range []dw.CascadePolicy{dw.CascadeReject, dw.CascadeDetach, dw.CascadeFail} {
		if c.String() == "" {
			t.Fatalf("cascade policy %d has no string form", c)
		}
	}
	if dw.CascadePolicy(99).String() == "" {
		t.Fatal("an out-of-range cascade policy has no string form")
	}
}

func TestNodeSpecValidate(t *testing.T) {
	t.Parallel()
	long := string(make([]byte, 300))
	cases := []struct {
		name string
		spec dw.NodeSpec
	}{
		{"empty id", dw.NodeSpec{}},
		{"oversized id", dw.NodeSpec{ID: dw.NodeID(long)}},
		{"invalid utf8 id", dw.NodeSpec{ID: dw.NodeID([]byte{0xff, 0xfe})}},
		{"oversized kind", dw.NodeSpec{ID: "a", Kind: long}},
		{"self dependency", dw.NodeSpec{ID: "a", Deps: []dw.NodeID{"a"}}},
		{"duplicate dependency", dw.NodeSpec{ID: "a", Deps: []dw.NodeID{"b", "b"}}},
		{"empty dependency", dw.NodeSpec{ID: "a", Deps: []dw.NodeID{""}}},
		{"unknown trigger", dw.NodeSpec{ID: "a", Trigger: dw.TriggerRule(99)}},
		{"negative retry base", dw.NodeSpec{ID: "a", Retry: dw.RetryPolicy{BaseDelay: -1}}},
		{"negative retry max", dw.NodeSpec{ID: "a", Retry: dw.RetryPolicy{MaxDelay: -1}}},
		{"inverted retry bounds", dw.NodeSpec{ID: "a", Retry: dw.RetryPolicy{BaseDelay: time.Minute, MaxDelay: time.Second}}},
		{"too many labels", dw.NodeSpec{ID: "a", Labels: manyLabels(dw.MaxLabels + 1)}},
		{"empty label key", dw.NodeSpec{ID: "a", Labels: map[string]string{"": "v"}}},
		{"oversized label key", dw.NodeSpec{ID: "a", Labels: map[string]string{long: "v"}}},
		{"oversized label value", dw.NodeSpec{ID: "a", Labels: map[string]string{"k": long + long}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.spec.Validate(); err == nil {
				t.Fatal("a malformed spec was accepted")
			} else if !errors.Is(err, dw.ErrInvalidArgument) {
				t.Fatalf("got %v, want it to wrap ErrInvalidArgument", err)
			}
		})
	}

	good := dw.NodeSpec{ID: "a", Kind: "k", Deps: []dw.NodeID{"b", "c"}, Labels: map[string]string{"x": "y"}}
	if err := good.Validate(); err != nil {
		t.Fatalf("a well-formed spec was rejected: %v", err)
	}
}

func manyLabels(n int) map[string]string {
	out := make(map[string]string, n)
	for i := range n {
		out[string(rune('a'+i%26))+string(rune('a'+i/26))] = "v"
	}
	return out
}

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
