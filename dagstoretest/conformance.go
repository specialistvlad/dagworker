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
	realSlack = 400 * time.Millisecond
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
	//
	// MinLeaseTimeout must be set explicitly, and this is not cosmetic. A store
	// clamps every requested lease into the scope's bounds, and an unset
	// MinLeaseTimeout resolves to the library default of one second -- which
	// silently inflates the 400ms lease the real-clock path asks for, so
	// passLease's wait ends while the lease is still very much alive and every
	// timeout test fails for a reason that has nothing to do with the backend.
	// A backend driving a fake clock never noticed, because its lease is 30s
	// and never hits the floor.
	if err := st.SetScopeConfig(s.ctx, s.scope, baseConfig()); err != nil {
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

// tick moves time forward by a little: enough for the suite's deliberately
// tiny retry backoff to elapse, and no more.
func (s *suite) tick() {
	s.t.Helper()
	if s.fake() {
		s.advance(time.Second)
		return
	}
	time.Sleep(40 * time.Millisecond)
}

// reclaimExpired drives the store past the current lease deadline and past the
// retry backoff that reclaiming schedules, leaving the node claimable again.
//
// The backoff step is not incidental. A reclaimed node with attempts remaining
// does not become claimable at the instant its lease expires -- it waits out a
// retry delay first. A test that claims immediately after the deadline is
// asserting something the contract does not promise, and it only appeared to
// hold on a backend whose configured backoff had silently rounded to zero.
func (s *suite) reclaimExpired() {
	s.t.Helper()
	s.passLease()
	// Either the sweeper or the claim path may do the reclaiming; running the
	// sweeper here makes the surrounding test independent of which.
	if _, err := s.st.Sweep(s.ctx, s.scope, 1000); err != nil {
		s.t.Fatalf("Sweep: %v", err)
	}
	s.tick()
}

// baseConfig is the scope policy every test starts from. Two of its fields are
// load-bearing for the suite itself rather than for any one test:
//
//   - MinLeaseTimeout must be well below the lease a real-clock backend is
//     given, or the store clamps the request up to the library's one-second
//     floor and every timeout test waits for an expiry that has not happened.
//   - The retry delays are as short as a backend can faithfully store. A
//     sub-millisecond value is not portable: a backend keeping durations in
//     milliseconds rounds it to zero, zero is how ScopeConfig spells "unset",
//     and the library default of one second comes back instead -- so the
//     "negligible" backoff becomes the longest thing in the test.
func baseConfig() dw.ScopeConfig {
	return dw.ScopeConfig{
		MinLeaseTimeout: time.Millisecond,
		RetryBaseDelay:  time.Millisecond,
		RetryMaxDelay:   time.Millisecond,
	}
}

// configure applies a scope policy on top of the suite's baseline.
//
// It exists because SetScopeConfig replaces the whole struct: a test that sets
// only MaxAttempts would silently discard the baseline and get the library
// defaults back, including the one-second lease floor. That failure is
// invisible on a backend driving a fake clock, whose lease never approaches the
// floor, and deterministic on every backend that owns its own clock -- which is
// precisely the kind of asymmetry a shared suite exists to prevent.
func (s *suite) configure(cfg dw.ScopeConfig) {
	s.t.Helper()
	merged := baseConfig()
	if cfg.DefaultLeaseTimeout != 0 {
		merged.DefaultLeaseTimeout = cfg.DefaultLeaseTimeout
	}
	if cfg.MinLeaseTimeout != 0 {
		merged.MinLeaseTimeout = cfg.MinLeaseTimeout
	}
	if cfg.MaxLeaseTimeout != 0 {
		merged.MaxLeaseTimeout = cfg.MaxLeaseTimeout
	}
	if cfg.MaxAttempts != 0 {
		merged.MaxAttempts = cfg.MaxAttempts
	}
	if cfg.RetryBaseDelay != 0 {
		merged.RetryBaseDelay = cfg.RetryBaseDelay
	}
	if cfg.RetryMaxDelay != 0 {
		merged.RetryMaxDelay = cfg.RetryMaxDelay
	}
	if cfg.PayloadCap != 0 {
		merged.PayloadCap = cfg.PayloadCap
	}
	if cfg.MaxBatchSize != 0 {
		merged.MaxBatchSize = cfg.MaxBatchSize
	}
	if cfg.MaxInFlight != 0 {
		merged.MaxInFlight = cfg.MaxInFlight
	}
	if err := s.st.SetScopeConfig(s.ctx, s.scope, merged); err != nil {
		s.t.Fatalf("SetScopeConfig(%+v): %v", merged, err)
	}
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
