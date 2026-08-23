// Package dagstoretest is the conformance suite every dagworker storage backend
// must pass.
//
// The suite is the definition of correct backend behaviour. Prose in the
// documentation describes the contract; this package decides it. A backend is
// finished when RunConformance passes, and a disagreement between a backend and
// this suite is a bug in the backend unless the suite is changed deliberately.
//
// Each test has a stable identifier — T-CLAIM-ATOMIC, T-FENCE-STALE-ACK and so
// on — that backend documentation can cite. Tests for optional facets skip when
// the backend does not report the capability; they never silently pass, so a
// facet that is claimed but broken fails rather than disappearing.
//
// Usage from a backend's own test file:
//
//	func TestConformance(t *testing.T) {
//		dagstoretest.RunConformance(t, dagstoretest.Harness{
//			Name: "memory",
//			New: func(t *testing.T) (dagworker.Store, func(time.Duration)) {
//				clk := dagstoretest.NewFakeClock()
//				st := memory.New(memory.WithClock(clk))
//				t.Cleanup(func() { _ = st.Close(context.Background()) })
//				return st, clk.Advance
//			},
//		})
//	}
package dagstoretest

import (
	"context"
	"errors"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// Harness tells the suite how to obtain a backend to test.
type Harness struct {
	// Name identifies the backend in test output.
	Name string

	// New returns a fresh, empty store and, when the backend's clock can be
	// driven, a function that advances it.
	//
	// A backend whose clock lives in a server it does not control returns nil
	// for the advance function; the suite then uses short real durations and
	// genuine sleeps for the handful of tests that need time to pass. Every
	// other test is timing-independent.
	//
	// New must return a store isolated from any other, so that subtests can run
	// in parallel.
	New func(t *testing.T) (dw.Store, func(time.Duration))
}

// realLease is the lease duration used when the clock cannot be advanced. It is
// short enough to keep the suite quick and long enough to survive a loaded CI
// machine between the claim and the assertion that the lease is still held.
const (
	realLease = 400 * time.Millisecond
	realSlack = 250 * time.Millisecond
	fakeLease = 30 * time.Second
)

type suite struct {
	t       *testing.T
	st      dw.Store
	advance func(time.Duration)
	scope   dw.Scope
	ctx     context.Context
}

func (h Harness) begin(t *testing.T) *suite {
	t.Helper()
	st, adv := h.New(t)
	s := &suite{t: t, st: st, advance: adv, scope: "s", ctx: t.Context()}
	// Retry backoff is not what most of these tests are about, and a default
	// measured in seconds would make every timeout test either slow or flaky.
	// A one-nanosecond window means a reclaimed node is claimable again
	// immediately, so a test can observe the reclaim without also having to
	// wait out a delay it does not care about. Tests about backoff set their own.
	if err := st.SetScopeConfig(s.ctx, s.scope, dw.ScopeConfig{
		RetryBaseDelay: time.Nanosecond,
		RetryMaxDelay:  time.Nanosecond,
	}); err != nil {
		t.Fatalf("dagstoretest: SetScopeConfig: %v", err)
	}
	return s
}

// fake reports whether the suite can control time. Tests that would otherwise
// have to sleep for a long time use it to choose their durations.
func (s *suite) fake() bool { return s.advance != nil }

func (s *suite) lease() time.Duration {
	if s.fake() {
		return fakeLease
	}
	return realLease
}

// passLease moves past the current lease deadline, by driving the clock when
// possible and by sleeping when not.
func (s *suite) passLease() {
	s.t.Helper()
	if s.fake() {
		s.advance(s.lease() + time.Second)
		return
	}
	time.Sleep(realLease + realSlack)
}

// ---------------------------------------------------------------- helpers

func (s *suite) add(specs ...dw.NodeSpec) []dw.Effect {
	s.t.Helper()
	eff, err := s.st.AddNodes(s.ctx, s.scope, specs)
	if err != nil {
		s.t.Fatalf("AddNodes(%v): %v", ids(specs), err)
	}
	return eff
}

func (s *suite) addErr(specs ...dw.NodeSpec) error {
	s.t.Helper()
	_, err := s.st.AddNodes(s.ctx, s.scope, specs)
	return err
}

func (s *suite) node(id dw.NodeID) dw.Node {
	s.t.Helper()
	n, err := s.st.GetNode(s.ctx, s.scope, id)
	if err != nil {
		s.t.Fatalf("GetNode(%q): %v", id, err)
	}
	return n
}

func (s *suite) statusIs(id dw.NodeID, want dw.Status) {
	s.t.Helper()
	if got := s.node(id).Status; got != want {
		s.t.Fatalf("node %q: status is %v, want %v", id, got, want)
	}
}

func (s *suite) reasonIs(id dw.NodeID, want dw.Reason) {
	s.t.Helper()
	if got := s.node(id).Reason; got != want {
		s.t.Fatalf("node %q: reason is %v, want %v", id, got, want)
	}
}

func (s *suite) stats() dw.ScopeStats {
	s.t.Helper()
	st, err := s.st.ScopeStats(s.ctx, s.scope)
	if err != nil {
		s.t.Fatalf("ScopeStats: %v", err)
	}
	return st
}

// claim takes exactly one node and fails the test if none was available.
func (s *suite) claim() dw.Lease {
	s.t.Helper()
	l, ok := s.tryClaim()
	if !ok {
		s.t.Fatal("Claim returned no lease, want one")
	}
	return l
}

func (s *suite) tryClaim(kinds ...string) (dw.Lease, bool) {
	s.t.Helper()
	res, err := s.st.Claim(s.ctx, dw.ClaimRequest{
		Scope: s.scope, Kinds: kinds, Max: 1, Timeout: s.lease(), WorkerID: "w",
	})
	if err != nil {
		s.t.Fatalf("Claim: %v", err)
	}
	if len(res.Leases) == 0 {
		return dw.Lease{}, false
	}
	return res.Leases[0], true
}

func (s *suite) claimNone() {
	s.t.Helper()
	if l, ok := s.tryClaim(); ok {
		s.t.Fatalf("Claim returned %q, want nothing ready", l.NodeID)
	}
}

func (s *suite) ack(l dw.Lease) dw.CompleteResult {
	s.t.Helper()
	res, err := s.st.Complete(s.ctx, dw.CompleteRequest{Lease: l, Success: true})
	if err != nil {
		s.t.Fatalf("Complete(ack %q): %v", l.NodeID, err)
	}
	return res
}

func (s *suite) nack(l dw.Lease) dw.CompleteResult {
	s.t.Helper()
	res, err := s.st.Complete(s.ctx, dw.CompleteRequest{
		Lease: l, Success: false, Reason: dw.ReasonWorkerError, Message: "boom",
	})
	if err != nil {
		s.t.Fatalf("Complete(nack %q): %v", l.NodeID, err)
	}
	return res
}

// wantErr asserts that err wraps target, naming what was being attempted.
func (s *suite) wantErr(what string, err, target error) {
	s.t.Helper()
	if err == nil {
		s.t.Fatalf("%s: got nil error, want %v", what, target)
	}
	if !errors.Is(err, target) {
		s.t.Fatalf("%s: got %v, want it to wrap %v", what, err, target)
	}
}

func ids(specs []dw.NodeSpec) []dw.NodeID {
	out := make([]dw.NodeID, len(specs))
	for i, sp := range specs {
		out[i] = sp.ID
	}
	return out
}

func spec(id dw.NodeID, deps ...dw.NodeID) dw.NodeSpec {
	return dw.NodeSpec{ID: id, Deps: deps}
}

// specK gives a node its own kind, named after its id, so a test can claim
// exactly that node. Without it a test wanting a particular node would have to
// claim others and give them back, and there is deliberately no way to return a
// lease without recording an attempt.
func specK(id dw.NodeID, deps ...dw.NodeID) dw.NodeSpec {
	return dw.NodeSpec{ID: id, Kind: string(id), Deps: deps}
}

func caps(st dw.Store) dw.Capabilities {
	if r, ok := st.(dw.CapabilityReporter); ok {
		return r.Capabilities()
	}
	return 0
}

func hasEffect(effs []dw.Effect, id dw.NodeID, kind dw.EventKind) bool {
	for _, e := range effs {
		if e.NodeID == id && e.Kind == kind {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- runner

type conformanceTest struct {
	id string
	fn func(*suite)
}

// RunConformance runs the whole suite against the backend the harness builds.
func RunConformance(t *testing.T, h Harness) {
	t.Helper()
	if h.New == nil {
		t.Fatal("dagstoretest: Harness.New must not be nil")
	}
	name := h.Name
	if name == "" {
		name = "backend"
	}

	all := make([]conformanceTest, 0, 64)
	all = append(all, scopeTests()...)
	all = append(all, graphTests()...)
	all = append(all, triggerTests()...)
	all = append(all, leaseTests()...)
	all = append(all, removalTests()...)
	all = append(all, facetTests()...)

	seen := make(map[string]bool, len(all))
	for _, tc := range all {
		if seen[tc.id] {
			t.Fatalf("dagstoretest: duplicate test id %q", tc.id)
		}
		seen[tc.id] = true
	}

	t.Run(name, func(t *testing.T) {
		for _, tc := range all {
			t.Run(tc.id, func(t *testing.T) {
				t.Parallel()
				tc.fn(h.begin(t))
			})
		}
	})
}

func contextWithCancel(s *suite) (context.Context, context.CancelFunc) {
	return context.WithCancel(s.ctx)
}

func contextWithTimeout(s *suite, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(s.ctx, d)
}

// recv takes the next event or fails the test rather than hanging until the
// package timeout, which produces a far less useful failure.
func recv(s *suite, ch <-chan dw.Event) dw.Event {
	s.t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			s.t.Fatal("event channel closed before delivering an event")
		}
		return ev
	case <-time.After(5 * time.Second):
		s.t.Fatal("timed out waiting for an event")
		return dw.Event{}
	}
}
