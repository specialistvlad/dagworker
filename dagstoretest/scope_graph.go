package dagstoretest

import (
	"bytes"
	"errors"
	"testing"

	dw "github.com/specialistvlad/dagworker"
)

func scopeTests() []conformanceTest {
	return []conformanceTest{
		{"T-SCOPE-IMPLICIT", func(s *suite) {
			// A scope must not need creating. Writing to a name that has never
			// been used brings it into existence.
			s.scope = "never-configured"
			s.add(spec("a"))
			s.statusIs("a", dw.StatusNew)
			names, err := s.st.Scopes(s.ctx)
			if err != nil {
				s.t.Fatalf("Scopes: %v", err)
			}
			if !containsScope(names, s.scope) {
				s.t.Fatalf("Scopes returned %v, want it to include %q", names, s.scope)
			}
		}},

		{"T-SCOPE-UNKNOWN-READ", func(s *suite) {
			// Asking about a scope that does not exist is not an error, but
			// asking for a node inside it is.
			if _, err := s.st.ScopeStats(s.ctx, "never-used"); err != nil {
				s.t.Fatalf("ScopeStats on an unused scope: %v", err)
			}
			_, err := s.st.GetNode(s.ctx, "never-used", "a")
			s.wantErr("GetNode in an unused scope", err, dw.ErrNotFound)
		}},

		{"T-SCOPE-CONFIG-ROUNDTRIP", func(s *suite) {
			want := dw.ScopeConfig{MaxAttempts: 7, PayloadCap: 1024}
			if err := s.st.SetScopeConfig(s.ctx, s.scope, want); err != nil {
				s.t.Fatalf("SetScopeConfig: %v", err)
			}
			got, err := s.st.ScopeConfig(s.ctx, s.scope)
			if err != nil {
				s.t.Fatalf("ScopeConfig: %v", err)
			}
			if got.MaxAttempts != want.MaxAttempts || got.PayloadCap != want.PayloadCap {
				s.t.Fatalf("config round-trip: got %+v, want MaxAttempts=%d PayloadCap=%d",
					got, want.MaxAttempts, want.PayloadCap)
			}
		}},

		{"T-SCOPE-SEAL-REJECTS", func(s *suite) {
			s.add(spec("a"))
			if err := s.st.Seal(s.ctx, s.scope); err != nil {
				s.t.Fatalf("Seal: %v", err)
			}
			s.wantErr("AddNodes after Seal", s.addErr(spec("b")), dw.ErrScopeSealed)
			// Sealing twice is a no-op, not an error.
			if err := s.st.Seal(s.ctx, s.scope); err != nil {
				s.t.Fatalf("Seal twice: %v", err)
			}
		}},

		{"T-SCOPE-COMPLETE", func(s *suite) {
			s.add(spec("a"), spec("b", "a"))
			if s.stats().Complete {
				s.t.Fatal("scope reports complete before anything ran")
			}
			if err := s.st.Seal(s.ctx, s.scope); err != nil {
				s.t.Fatalf("Seal: %v", err)
			}
			if s.stats().Complete {
				s.t.Fatal("sealed scope with outstanding work reports complete")
			}
			s.ack(s.claim())
			s.ack(s.claim())
			if !s.stats().Complete {
				s.t.Fatalf("sealed scope with no outstanding work is not complete: %+v", s.stats())
			}
		}},

		{"T-SCOPE-STATS", func(s *suite) {
			s.add(spec("a"), spec("b", "a"), spec("c", "a"))
			st := s.stats()
			if st.Total != 3 || st.Ready != 1 || st.Blocked != 2 {
				s.t.Fatalf("after insert: %+v, want Total=3 Ready=1 Blocked=2", st)
			}
			l := s.claim()
			if st = s.stats(); st.InProgress != 1 || st.Ready != 0 {
				s.t.Fatalf("after claim: %+v, want InProgress=1 Ready=0", st)
			}
			s.ack(l)
			if st = s.stats(); st.Succeeded != 1 || st.Ready != 2 || st.Blocked != 0 {
				s.t.Fatalf("after ack: %+v, want Succeeded=1 Ready=2 Blocked=0", st)
			}
			if st.NonTerminal() != 2 {
				s.t.Fatalf("NonTerminal is %d, want 2", st.NonTerminal())
			}
		}},
	}
}

func graphTests() []conformanceTest {
	return []conformanceTest{
		{"T-NODE-CREATE", func(s *suite) {
			eff := s.add(dw.NodeSpec{
				ID: "a", Kind: "k", Priority: 5,
				Payload: []byte("hello"), Labels: map[string]string{"x": "y"},
			})
			if !hasEffect(eff, "a", dw.EventCreated) {
				s.t.Fatalf("AddNodes returned %v, want an EventCreated for a", eff)
			}
			n := s.node("a")
			if n.Kind != "k" || n.Priority != 5 || !bytes.Equal(n.Payload, []byte("hello")) ||
				n.Labels["x"] != "y" || n.Status != dw.StatusNew || n.Attempt != 0 {
				s.t.Fatalf("round-tripped node is %+v", n)
			}
			if n.Seq == 0 {
				s.t.Fatal("a created node has sequence 0, want a stamped sequence")
			}
		}},

		{"T-NODE-NOTFOUND", func(s *suite) {
			s.add(spec("a"))
			_, err := s.st.GetNode(s.ctx, s.scope, "nope")
			s.wantErr("GetNode on an unknown node", err, dw.ErrNotFound)
		}},

		{"T-NODE-IDEMPOTENT", func(s *suite) {
			// Re-adding a byte-identical node is a no-op. This is what lets a
			// caller retry an insert after an ambiguous failure.
			sp := dw.NodeSpec{ID: "a", Kind: "k", Payload: []byte("p")}
			s.add(sp)
			first := s.node("a")
			s.add(sp)
			if got := s.stats().Total; got != 1 {
				s.t.Fatalf("re-adding an identical node produced %d nodes, want 1", got)
			}
			if s.node("a").Seq != first.Seq {
				s.t.Fatal("re-adding an identical node bumped its sequence")
			}
		}},

		{"T-NODE-CONFLICT", func(s *suite) {
			s.add(dw.NodeSpec{ID: "a", Payload: []byte("one")})
			err := s.addErr(dw.NodeSpec{ID: "a", Payload: []byte("two")})
			s.wantErr("AddNodes with a differing spec", err, dw.ErrIDConflict)
		}},

		{"T-NODE-PAYLOAD-CAP", func(s *suite) {
			if err := s.st.SetScopeConfig(s.ctx, s.scope, dw.ScopeConfig{PayloadCap: 8}); err != nil {
				s.t.Fatalf("SetScopeConfig: %v", err)
			}
			err := s.addErr(dw.NodeSpec{ID: "a", Payload: bytes.Repeat([]byte("x"), 9)})
			s.wantErr("AddNodes over the payload cap", err, dw.ErrPayloadTooLarge)
			var tooLarge *dw.PayloadTooLargeError
			if !errors.As(err, &tooLarge) || tooLarge.Cap != 8 {
				s.t.Fatalf("error %v does not carry the cap that rejected it", err)
			}
		}},

		{"T-NODE-BATCH-DUP", func(s *suite) {
			err := s.addErr(spec("a"), spec("a"))
			s.wantErr("a batch naming the same node twice", err, dw.ErrInvalidArgument)
		}},

		{"T-EDGE-BLOCKS", func(s *suite) {
			s.add(spec("a"), spec("b", "a"))
			l := s.claim()
			if l.NodeID != "a" {
				s.t.Fatalf("claimed %q, want the only unblocked node a", l.NodeID)
			}
			s.claimNone()
		}},

		{"T-EDGE-SATISFIED-AT-INSERT", func(s *suite) {
			// A dependency that has already succeeded is born satisfied, so the
			// new node is claimable at once rather than waiting for an event
			// that already happened.
			s.add(spec("a"))
			s.ack(s.claim())
			s.add(spec("b", "a"))
			if l := s.claim(); l.NodeID != "b" {
				s.t.Fatalf("claimed %q, want b to be immediately ready", l.NodeID)
			}
		}},

		{"T-EDGE-FAILED-AT-INSERT", func(s *suite) {
			// The mirror case: a node inserted behind an already-failed
			// dependency is born terminal, not blocked forever.
			if err := s.st.SetScopeConfig(s.ctx, s.scope, dw.ScopeConfig{MaxAttempts: 1}); err != nil {
				s.t.Fatalf("SetScopeConfig: %v", err)
			}
			s.add(spec("a"))
			s.nack(s.claim())
			s.statusIs("a", dw.StatusError)
			s.add(spec("b", "a"))
			s.statusIs("b", dw.StatusError)
			s.reasonIs("b", dw.ReasonUpstreamFailed)
		}},

		{"T-EDGE-CYCLE-DIRECT", func(s *suite) {
			s.add(spec("a"), spec("b", "a"))
			_, err := s.st.AddEdges(s.ctx, s.scope, []dw.Edge{{From: "b", To: "a"}})
			s.wantErr("an edge closing a two-node cycle", err, dw.ErrCycle)
			var ce *dw.CycleError
			if !errors.As(err, &ce) {
				s.t.Fatalf("error %v is not a *CycleError", err)
			}
		}},

		{"T-EDGE-CYCLE-INDIRECT", func(s *suite) {
			s.add(spec("a"), spec("b", "a"), spec("c", "b"), spec("d", "c"))
			_, err := s.st.AddEdges(s.ctx, s.scope, []dw.Edge{{From: "d", To: "a"}})
			s.wantErr("an edge closing a four-node cycle", err, dw.ErrCycle)
		}},

		{"T-EDGE-CYCLE-LEAVES-GRAPH-INTACT", func(s *suite) {
			// A rejected edge must leave nothing behind: the graph is exactly as
			// it was, and every node still reaches the state it should.
			s.add(spec("a"), spec("b", "a"), spec("c", "b"))
			before := s.stats()
			_, err := s.st.AddEdges(s.ctx, s.scope, []dw.Edge{{From: "c", To: "a"}})
			s.wantErr("cycle", err, dw.ErrCycle)

			after := s.stats()
			after.Cursor = before.Cursor // a rejected write may still have burned no cursor
			if after != before {
				s.t.Fatalf("stats changed after a rejected edge: %+v, want %+v", after, before)
			}
			// The chain still runs end to end, so no counter was left skewed.
			s.ack(s.claim()) // a
			s.ack(s.claim()) // b
			if l := s.claim(); l.NodeID != "c" {
				s.t.Fatalf("claimed %q, want c", l.NodeID)
			}
		}},

		{"T-EDGE-BATCH-ATOMIC", func(s *suite) {
			// One bad edge in a batch discards the whole batch, including the
			// good edges that preceded it.
			s.add(specK("a"), specK("b", "a"), specK("c"))
			_, err := s.st.AddEdges(s.ctx, s.scope, []dw.Edge{
				{From: "c", To: "b"}, // fine on its own
				{From: "b", To: "a"}, // closes a cycle
			})
			s.wantErr("a batch containing a cycle", err, dw.ErrCycle)
			// c -> b must not have survived: b depends only on a, so acking a
			// makes b claimable without c ever running.
			s.ack(s.claimByID("a"))
			s.claimByID("b")
		}},

		{"T-EDGE-READY-TO-BLOCKED", func(s *suite) {
			// Adding an unresolved dependency to a claimable node must pull it
			// out of the ready set in the same operation that records the edge,
			// or a worker claims it through the gap.
			s.add(spec("a"), spec("b"))
			if _, err := s.st.AddEdges(s.ctx, s.scope, []dw.Edge{{From: "a", To: "b"}}); err != nil {
				s.t.Fatalf("AddEdges: %v", err)
			}
			l := s.claim()
			if l.NodeID != "a" {
				s.t.Fatalf("claimed %q, want a; b should have been pulled out of the ready set", l.NodeID)
			}
			s.claimNone()
		}},

		{"T-EDGE-TO-TERMINAL", func(s *suite) {
			s.add(spec("a"), spec("b"))
			s.ack(s.claim()) // a
			// b is claimable; finish it too, then try to give it a dependency.
			s.ack(s.claim())
			_, err := s.st.AddEdges(s.ctx, s.scope, []dw.Edge{{From: "a", To: "b"}})
			s.wantErr("an edge into a terminal node", err, dw.ErrAlreadyTerminal)
		}},

		{"T-EDGE-MISSING-DEP", func(s *suite) {
			s.wantErr("a dependency that does not exist", s.addErr(spec("b", "ghost")), dw.ErrNotFound)
			_, err := s.st.AddEdges(s.ctx, s.scope, []dw.Edge{{From: "ghost", To: "also-ghost"}})
			s.wantErr("AddEdges between unknown nodes", err, dw.ErrNotFound)
		}},

		{"T-EDGE-IDEMPOTENT", func(s *suite) {
			s.add(spec("a"), spec("b", "a"))
			if _, err := s.st.AddEdges(s.ctx, s.scope, []dw.Edge{{From: "a", To: "b"}}); err != nil {
				s.t.Fatalf("re-adding an existing edge: %v", err)
			}
			s.ack(s.claim()) // a
			// If the duplicate edge had been counted twice, b would still be blocked.
			if l := s.claim(); l.NodeID != "b" {
				s.t.Fatalf("claimed %q, want b; the duplicate edge was double counted", l.NodeID)
			}
		}},

		{"T-EDGE-SELF", func(s *suite) {
			s.wantErr("a node depending on itself", s.addErr(spec("a", "a")), dw.ErrInvalidArgument)
		}},

		{"T-SEQ-MONOTONIC", func(s *suite) {
			s.add(spec("a"))
			first := s.node("a").Seq
			l := s.claim()
			second := s.node("a").Seq
			if second <= first {
				s.t.Fatalf("sequence did not advance on claim: %d then %d", first, second)
			}
			s.ack(l)
			if third := s.node("a").Seq; third <= second {
				s.t.Fatalf("sequence did not advance on ack: %d then %d", second, third)
			}
		}},

		{"T-CURSOR-MONOTONIC", func(s *suite) {
			// Cursors order the scope's whole stream, across nodes, which is
			// what makes a subscription resumable.
			eff := s.add(spec("a"), spec("b"))
			var last dw.Cursor
			for _, e := range eff {
				if e.Cursor <= last {
					s.t.Fatalf("cursors are not increasing: %d after %d", e.Cursor, last)
				}
				last = e.Cursor
			}
			more := s.add(spec("c"))
			if len(more) == 0 || more[0].Cursor <= last {
				s.t.Fatalf("cursor did not continue across calls: %v after %d", more, last)
			}
		}},
	}
}

func containsScope(xs []dw.Scope, x dw.Scope) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

var _ = testing.Verbose
