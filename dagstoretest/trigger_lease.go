package dagstoretest

import (
	"sync"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// skip completes a lease as "nothing to do", which is terminal on the first
// report and is the only way ReasonSkipped enters a graph.
func (s *suite) skip(l dw.Lease) {
	s.t.Helper()
	if _, err := s.st.Complete(s.ctx, dw.CompleteRequest{
		Lease: l, Success: false, Reason: dw.ReasonSkipped, Message: "nothing to do",
	}); err != nil {
		s.t.Fatalf("Complete(skip %q): %v", l.NodeID, err)
	}
}

// claimByID claims one specific node, using the per-node kind that specK
// assigns. Claiming by kind is the only way to target a node: the ready set
// serves whatever the priority rules put first, and there is deliberately no
// operation that hands a lease back without recording an attempt.
func (s *suite) claimByID(id dw.NodeID) dw.Lease {
	s.t.Helper()
	l, ok := s.tryClaim(string(id))
	if !ok {
		s.t.Fatalf("node %q never became claimable", id)
	}
	if l.NodeID != id {
		s.t.Fatalf("claiming kind %q produced node %q", id, l.NodeID)
	}
	return l
}

// withRule builds two predecessors and one successor governed by rule. Each
// node gets its own kind so the test can drive them individually.
func (s *suite) withRule(rule dw.TriggerRule) {
	s.t.Helper()
	s.configure(dw.ScopeConfig{
		MaxAttempts:    1,
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  time.Millisecond,
	})
	s.add(
		specK("p1"),
		specK("p2"),
		dw.NodeSpec{ID: "child", Kind: "child", Trigger: rule, Deps: []dw.NodeID{"p1", "p2"}},
	)
}

func triggerTests() []conformanceTest {
	return []conformanceTest{
		{"T-TRIGGER-ALL-SUCCESS-RUNS", func(s *suite) {
			s.withRule(dw.TriggerAllSuccess)
			s.ack(s.claimByID("p1"))
			s.ack(s.claimByID("p2"))
			s.statusIs("child", dw.StatusNew)
			s.claimByID("child")
		}},

		{"T-TRIGGER-ALL-SUCCESS-BLOCKS-ON-FAILURE", func(s *suite) {
			s.withRule(dw.TriggerAllSuccess)
			s.ack(s.claimByID("p1"))
			s.nack(s.claimByID("p2"))
			s.statusIs("child", dw.StatusError)
			s.reasonIs("child", dw.ReasonUpstreamFailed)
		}},

		{"T-TRIGGER-ALL-SUCCESS-BLOCKS-ON-SKIP", func(s *suite) {
			// A skipped predecessor is not a failure, but all_success still
			// requires success, so the child is terminated as skipped rather
			// than as an upstream failure. Conflating the two is the complaint
			// every engine that does it eventually collects.
			s.withRule(dw.TriggerAllSuccess)
			s.ack(s.claimByID("p1"))
			s.skip(s.claimByID("p2"))
			s.statusIs("child", dw.StatusError)
			s.reasonIs("child", dw.ReasonSkipped)
		}},

		{"T-TRIGGER-ALL-DONE", func(s *suite) {
			// all_done runs whatever happened upstream: the cleanup case.
			s.withRule(dw.TriggerAllDone)
			s.ack(s.claimByID("p1"))
			s.nack(s.claimByID("p2"))
			s.statusIs("child", dw.StatusNew)
			s.claimByID("child")
		}},

		{"T-TRIGGER-NONE-FAILED", func(s *suite) {
			// Distinguishes itself from all_success exactly here: a skipped
			// predecessor is acceptable, a failed one is not.
			s.withRule(dw.TriggerNoneFailed)
			s.ack(s.claimByID("p1"))
			s.skip(s.claimByID("p2"))
			s.claimByID("child")
		}},

		{"T-TRIGGER-NONE-FAILED-STOPS-ON-FAILURE", func(s *suite) {
			s.withRule(dw.TriggerNoneFailed)
			s.ack(s.claimByID("p1"))
			s.nack(s.claimByID("p2"))
			s.statusIs("child", dw.StatusError)
			s.reasonIs("child", dw.ReasonUpstreamFailed)
		}},

		{"T-TRIGGER-NONE-FAILED-MIN-ONE-SUCCESS", func(s *suite) {
			s.withRule(dw.TriggerNoneFailedMinOneSuccess)
			s.ack(s.claimByID("p1"))
			s.skip(s.claimByID("p2"))
			s.claimByID("child")
		}},

		{"T-TRIGGER-NONE-FAILED-MIN-ONE-NEEDS-A-SUCCESS", func(s *suite) {
			s.withRule(dw.TriggerNoneFailedMinOneSuccess)
			s.skip(s.claimByID("p1"))
			s.skip(s.claimByID("p2"))
			s.statusIs("child", dw.StatusError)
			s.reasonIs("child", dw.ReasonSkipped)
		}},

		{"T-TRIGGER-ALWAYS", func(s *suite) {
			// always ignores predecessors entirely: claimable from the moment
			// it exists, even though p1 and p2 have not run.
			s.withRule(dw.TriggerAlways)
			// Claimable at once, with both predecessors still untouched.
			s.claimByID("child")
			s.statusIs("p1", dw.StatusNew)
			s.statusIs("p2", dw.StatusNew)
		}},

		{"T-UPSTREAM-FAILED-CASCADE", func(s *suite) {
			// The consequence of a failure propagates the whole way down, not
			// one level, or a deep graph would leave nodes blocked forever.
			s.configure(dw.ScopeConfig{MaxAttempts: 1})
			s.add(spec("a"), spec("b", "a"), spec("c", "b"), spec("d", "c"))
			s.nack(s.claim())
			for _, id := range []dw.NodeID{"b", "c", "d"} {
				s.statusIs(id, dw.StatusError)
				s.reasonIs(id, dw.ReasonUpstreamFailed)
			}
			if st := s.stats(); st.NonTerminal() != 0 {
				s.t.Fatalf("%d nodes left non-terminal after the cascade: %+v", st.NonTerminal(), st)
			}
		}},
	}
}

func leaseTests() []conformanceTest {
	return []conformanceTest{
		{"T-CLAIM-EMPTY", func(s *suite) {
			// Having no work is an ordinary answer, never an error.
			res, err := s.st.Claim(s.ctx, dw.ClaimRequest{Scope: s.scope, Max: 1})
			if err != nil {
				s.t.Fatalf("Claim on an empty scope: %v", err)
			}
			if len(res.Leases) != 0 {
				s.t.Fatalf("Claim on an empty scope returned %d leases", len(res.Leases))
			}
		}},

		{"T-CLAIM-ATOMIC", func(s *suite) {
			// The central guarantee: however many instances race, no node is
			// ever handed to two of them.
			const n = 50
			specs := make([]dw.NodeSpec, n)
			for i := range specs {
				specs[i] = spec(dw.NodeID(string(rune('a'+i/26)) + string(rune('a'+i%26))))
			}
			s.add(specs...)

			var mu sync.Mutex
			seen := make(map[dw.NodeID]int)
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						res, err := s.st.Claim(s.ctx, dw.ClaimRequest{
							Scope: s.scope, Max: 3, Timeout: longLease, WorkerID: "racer",
						})
						if err != nil || len(res.Leases) == 0 {
							return
						}
						mu.Lock()
						for _, l := range res.Leases {
							seen[l.NodeID]++
						}
						mu.Unlock()
					}
				}()
			}
			wg.Wait()

			if len(seen) != n {
				s.t.Fatalf("claimed %d distinct nodes, want %d", len(seen), n)
			}
			for id, count := range seen {
				if count != 1 {
					s.t.Fatalf("node %q was granted to %d claimants at once", id, count)
				}
			}
		}},

		{"T-CLAIM-PRIORITY", func(s *suite) {
			s.add(
				dw.NodeSpec{ID: "low", Priority: 1},
				dw.NodeSpec{ID: "high", Priority: 100},
				dw.NodeSpec{ID: "mid", Priority: 50},
			)
			for _, want := range []dw.NodeID{"high", "mid", "low"} {
				if got := s.claim().NodeID; got != want {
					s.t.Fatalf("claimed %q, want %q", got, want)
				}
			}
		}},

		{"T-CLAIM-FIFO", func(s *suite) {
			// Equal priority is served in insertion order, so a node cannot be
			// starved by a steady stream of equally urgent arrivals.
			s.add(spec("first"), spec("second"), spec("third"))
			for _, want := range []dw.NodeID{"first", "second", "third"} {
				if got := s.claim().NodeID; got != want {
					s.t.Fatalf("claimed %q, want %q", got, want)
				}
			}
		}},

		{"T-CLAIM-KIND", func(s *suite) {
			s.add(
				dw.NodeSpec{ID: "cpu1", Kind: "cpu"},
				dw.NodeSpec{ID: "gpu1", Kind: "gpu"},
			)
			l, ok := s.tryClaim("gpu")
			if !ok || l.NodeID != "gpu1" {
				s.t.Fatalf("claiming kind gpu produced %+v", l)
			}
			if _, ok := s.tryClaim("gpu"); ok {
				s.t.Fatal("a second gpu claim succeeded, want nothing left of that kind")
			}
			if l, ok := s.tryClaim("cpu"); !ok || l.NodeID != "cpu1" {
				s.t.Fatalf("claiming kind cpu produced %+v", l)
			}
		}},

		{"T-CLAIM-MAX-INFLIGHT", func(s *suite) {
			s.configure(dw.ScopeConfig{MaxInFlight: 2})
			s.add(spec("a"), spec("b"), spec("c"))
			// A long lease: this test is about the in-flight cap, and a lease
			// lapsing mid-test would let the third claim through for a reason
			// that has nothing to do with the cap.
			if _, ok := s.tryClaim(); !ok {
				s.t.Fatal("first claim found nothing")
			}
			if _, ok := s.tryClaim(); !ok {
				s.t.Fatal("second claim found nothing")
			}
			if l, ok := s.tryClaim(); ok {
				s.t.Fatalf("Claim returned %q with two already in flight and a cap of 2", l.NodeID)
			}
		}},

		{"T-CLAIM-BATCH", func(s *suite) {
			s.add(spec("a"), spec("b"), spec("c"))
			res, err := s.st.Claim(s.ctx, dw.ClaimRequest{Scope: s.scope, Max: 3, Timeout: longLease})
			if err != nil {
				s.t.Fatalf("Claim: %v", err)
			}
			if len(res.Leases) != 3 {
				s.t.Fatalf("batch claim returned %d leases, want 3", len(res.Leases))
			}
		}},

		{"T-CLAIM-STAMPS-LEASE", func(s *suite) {
			// The deadline is set by the store from its own clock at the moment
			// of the grant, not computed by the caller.
			s.add(spec("a"))
			l := s.claimExpiring()
			if l.Epoch != 1 {
				s.t.Fatalf("first claim has epoch %d, want 1", l.Epoch)
			}
			if l.Deadline.IsZero() {
				s.t.Fatal("claim returned no deadline")
			}
			held := l.Deadline.Sub(l.Node.UpdatedAt)
			if held < s.lease()/2 || held > s.lease()*2 {
				s.t.Fatalf("deadline is %s after the grant, want about %s", held, s.lease())
			}
			if l.Node.Attempt != 1 {
				s.t.Fatalf("attempt is %d after the first claim, want 1", l.Node.Attempt)
			}
		}},

		{"T-ACK-SUCCESS", func(s *suite) {
			s.add(spec("a"))
			s.ack(s.claim())
			s.statusIs("a", dw.StatusSuccess)
			s.reasonIs("a", dw.ReasonNone)
		}},

		{"T-ACK-FANOUT", func(s *suite) {
			// Completing a node and releasing its successors is one operation:
			// the effects come back from the same call, with no second query.
			s.add(spec("a"), spec("b", "a"), spec("c", "a"))
			res := s.ack(s.claim())
			for _, id := range []dw.NodeID{"b", "c"} {
				if !hasEffect(res.Effects, id, dw.EventReady) {
					s.t.Fatalf("ack did not report %q becoming ready: %+v", id, res.Effects)
				}
			}
		}},

		{"T-FENCE-STALE-ACK", func(s *suite) {
			// The whole point of the epoch: a worker that was paused rather
			// than dead must not be able to write after being superseded.
			s.add(spec("a"))
			stale := s.claimExpiring()
			s.reclaimExpired()
			fresh, ok := s.tryClaim()
			if !ok {
				s.t.Fatal("the expired lease was not reclaimed")
			}
			if fresh.Epoch <= stale.Epoch {
				s.t.Fatalf("reissued lease has epoch %d, want it above %d", fresh.Epoch, stale.Epoch)
			}
			_, err := s.st.Complete(s.ctx, dw.CompleteRequest{Lease: stale, Success: true})
			s.wantErr("acking with a superseded lease", err, dw.ErrLeaseMismatch)
			s.statusIs("a", dw.StatusInProgress)
			s.ack(fresh)
			s.statusIs("a", dw.StatusSuccess)
		}},

		{"T-FENCE-DOUBLE-ACK", func(s *suite) {
			s.add(spec("a"))
			l := s.claim()
			s.ack(l)
			_, err := s.st.Complete(s.ctx, dw.CompleteRequest{Lease: l, Success: true})
			s.wantErr("acking the same lease twice", err, dw.ErrLeaseMismatch)
		}},

		{"T-NACK-RETRY", func(s *suite) {
			s.configure(dw.ScopeConfig{
				MaxAttempts: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: 2 * time.Millisecond,
			})
			s.add(spec("a"))
			res := s.nack(s.claim())
			if !res.Retrying {
				s.t.Fatal("a failure with attempts remaining did not report Retrying")
			}
			s.statusIs("a", dw.StatusNew)
			s.reasonIs("a", dw.ReasonWorkerError) // the last attempt's reason survives on a retrying node

			s.tick() // clear the retry backoff
			second := s.claim()
			if second.Epoch != 2 {
				s.t.Fatalf("retry claimed at epoch %d, want 2", second.Epoch)
			}
			s.ack(second)
			s.statusIs("a", dw.StatusSuccess)
		}},

		{"T-NACK-EXHAUSTED", func(s *suite) {
			s.configure(dw.ScopeConfig{
				MaxAttempts: 2, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
			})
			s.add(spec("a"))
			s.nack(s.claim())
			s.tick()
			res := s.nack(s.claim())
			if res.Retrying {
				s.t.Fatal("the final attempt reported Retrying")
			}
			s.statusIs("a", dw.StatusError)
			s.reasonIs("a", dw.ReasonWorkerError)
		}},

		{"T-TIMEOUT-RECLAIM", func(s *suite) {
			s.configure(dw.ScopeConfig{MaxAttempts: 1})
			s.add(spec("a"))
			s.claimExpiring()
			s.statusIs("a", dw.StatusInProgress)
			s.passLease()
			res, err := s.st.Sweep(s.ctx, s.scope, 10)
			if err != nil {
				s.t.Fatalf("Sweep: %v", err)
			}
			if res.Reclaimed != 1 {
				s.t.Fatalf("Sweep reclaimed %d leases, want 1", res.Reclaimed)
			}
			s.statusIs("a", dw.StatusError)
			s.reasonIs("a", dw.ReasonTimeout)
		}},

		{"T-TIMEOUT-RETRIES", func(s *suite) {
			s.configure(dw.ScopeConfig{
				MaxAttempts: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
			})
			s.add(spec("a"))
			s.claimExpiring()
			s.passLease()
			if _, err := s.st.Sweep(s.ctx, s.scope, 10); err != nil {
				s.t.Fatalf("Sweep: %v", err)
			}
			// Attempts remain, so a timeout returns the node for another try
			// rather than failing it.
			s.statusIs("a", dw.StatusNew)
			s.reasonIs("a", dw.ReasonTimeout)
			s.tick()
			if l := s.claim(); l.Epoch != 2 {
				s.t.Fatalf("re-claim after timeout has epoch %d, want 2", l.Epoch)
			}
		}},

		{"T-CLAIM-RECLAIMS-INLINE", func(s *suite) {
			// A dead worker's node must come back without a background sweeper
			// ever running: whoever asks for work also reclaims what it finds
			// expired. This test never calls Sweep, which is the whole point.
			s.add(spec("a"))
			first := s.claimExpiring()
			s.passLease()

			l, ok := s.tryClaim()
			if !ok {
				// Whether the reclaimed node is offered by the very call that
				// reclaimed it, or only after its retry backoff, is not
				// specified -- it depends on the backend's time granularity,
				// and both are correct. What is specified is that no sweeper
				// was needed. So allow one backoff and ask again.
				s.tick()
				l, ok = s.tryClaim()
			}
			if !ok {
				s.t.Fatal("an expired lease was never reclaimed by the claim path")
			}
			if l.Epoch <= first.Epoch {
				s.t.Fatalf("reclaimed lease has epoch %d, want it above the expired %d",
					l.Epoch, first.Epoch)
			}
		}},

		{"T-EXTEND", func(s *suite) {
			s.add(spec("a"))
			l := s.claimExpiring()
			longer := s.lease() * 3
			deadline, err := s.st.Extend(s.ctx, dw.ExtendRequest{Lease: l, Timeout: longer})
			if err != nil {
				s.t.Fatalf("Extend: %v", err)
			}
			if !deadline.After(l.Deadline) {
				s.t.Fatalf("Extend returned %s, want it after the original %s", deadline, l.Deadline)
			}
			// Past the original deadline, the lease must still be held.
			s.passLease()
			s.statusIs("a", dw.StatusInProgress)
			s.ack(l)
			s.statusIs("a", dw.StatusSuccess)
		}},

		{"T-EXTEND-DOES-NOT-TRANSITION", func(s *suite) {
			s.add(spec("a"))
			l := s.claim()
			before := s.node("a")
			if _, err := s.st.Extend(s.ctx, dw.ExtendRequest{Lease: l, Timeout: s.lease()}); err != nil {
				s.t.Fatalf("Extend: %v", err)
			}
			after := s.node("a")
			if after.Seq != before.Seq || after.Attempt != before.Attempt || after.Status != before.Status {
				s.t.Fatalf("Extend changed node state: %+v then %+v", before, after)
			}
		}},

		{"T-EXTEND-FENCED", func(s *suite) {
			s.add(spec("a"))
			stale := s.claimExpiring()
			s.reclaimExpired()
			if _, ok := s.tryClaim(); !ok {
				s.t.Fatal("the expired lease was not reclaimed")
			}
			_, err := s.st.Extend(s.ctx, dw.ExtendRequest{Lease: stale, Timeout: s.lease()})
			s.wantErr("extending a superseded lease", err, dw.ErrLeaseMismatch)
		}},

		{"T-SWEEP-EMPTY", func(s *suite) {
			res, err := s.st.Sweep(s.ctx, s.scope, 10)
			if err != nil {
				s.t.Fatalf("Sweep on an empty scope: %v", err)
			}
			if res.Reclaimed != 0 {
				s.t.Fatalf("Sweep on an empty scope reclaimed %d", res.Reclaimed)
			}
		}},
	}
}

var _ = testing.Short
