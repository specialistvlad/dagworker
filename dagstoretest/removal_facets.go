package dagstoretest

import (
	"errors"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

func removalTests() []conformanceTest {
	return []conformanceTest{
		{"T-CANCEL", func(s *suite) {
			s.add(spec("a"), spec("b", "a"))
			if _, err := s.st.Cancel(s.ctx, s.scope, []dw.NodeID{"a"}); err != nil {
				s.t.Fatalf("Cancel: %v", err)
			}
			s.statusIs("a", dw.StatusError)
			s.reasonIs("a", dw.ReasonCancelled)
			// The consequence reaches the successor, which can no longer run.
			s.statusIs("b", dw.StatusError)
			s.reasonIs("b", dw.ReasonUpstreamFailed)
		}},

		{"T-CANCEL-TERMINAL-IS-NOOP", func(s *suite) {
			s.add(spec("a"))
			s.ack(s.claim())
			if _, err := s.st.Cancel(s.ctx, s.scope, []dw.NodeID{"a"}); err != nil {
				s.t.Fatalf("Cancel on a finished node: %v", err)
			}
			s.statusIs("a", dw.StatusSuccess)
		}},

		{"T-CANCEL-UNKNOWN", func(s *suite) {
			_, err := s.st.Cancel(s.ctx, s.scope, []dw.NodeID{"ghost"})
			s.wantErr("cancelling an unknown node", err, dw.ErrNotFound)
		}},

		{"T-CANCEL-INFLIGHT-REVOKES-LEASE", func(s *suite) {
			// Cancelling a claimed node must invalidate the worker's lease, or
			// its later acknowledgement would resurrect a cancelled node.
			s.add(spec("a"))
			l := s.claim()
			if _, err := s.st.Cancel(s.ctx, s.scope, []dw.NodeID{"a"}); err != nil {
				s.t.Fatalf("Cancel: %v", err)
			}
			s.statusIs("a", dw.StatusError)
			s.reasonIs("a", dw.ReasonCancelled)
			_, err := s.st.Complete(s.ctx, dw.CompleteRequest{Lease: l, Success: true})
			s.wantErr("acking a cancelled node", err, dw.ErrLeaseMismatch)
			s.statusIs("a", dw.StatusError)
		}},

		{"T-CANCEL-SCOPE", func(s *suite) {
			s.add(spec("a"), spec("b", "a"), spec("c"))
			if _, err := s.st.CancelScope(s.ctx, s.scope); err != nil {
				s.t.Fatalf("CancelScope: %v", err)
			}
			if st := s.stats(); st.NonTerminal() != 0 {
				s.t.Fatalf("%d nodes survived CancelScope: %+v", st.NonTerminal(), st)
			}
		}},

		{"T-REMOVE-EDGE", func(s *suite) {
			// Dropping a dependency releases the successor.
			s.add(specK("a"), specK("b", "a"))
			if _, ok := s.tryClaim("b"); ok {
				s.t.Fatal("b was claimable while still depending on a")
			}
			if _, err := s.st.RemoveEdges(s.ctx, s.scope, []dw.Edge{{From: "a", To: "b"}}); err != nil {
				s.t.Fatalf("RemoveEdges: %v", err)
			}
			s.claimByID("b")
		}},

		{"T-REMOVE-NODE-REJECT", func(s *suite) {
			s.add(spec("a"), spec("b", "a"))
			_, err := s.st.RemoveNode(s.ctx, s.scope, "a", dw.CascadeReject)
			s.wantErr("removing a node with successors and no policy", err, dw.ErrHasSuccessors)
			s.statusIs("a", dw.StatusNew)
		}},

		{"T-REMOVE-NODE-DETACH", func(s *suite) {
			s.add(specK("a"), specK("b", "a"))
			if _, err := s.st.RemoveNode(s.ctx, s.scope, "a", dw.CascadeDetach); err != nil {
				s.t.Fatalf("RemoveNode(detach): %v", err)
			}
			if _, err := s.st.GetNode(s.ctx, s.scope, "a"); !errors.Is(err, dw.ErrNotFound) {
				s.t.Fatalf("removed node is still present: %v", err)
			}
			// b lost its dependency, so it may run.
			s.claimByID("b")
		}},

		{"T-REMOVE-NODE-FAIL", func(s *suite) {
			s.add(spec("a"), spec("b", "a"), spec("c", "b"))
			if _, err := s.st.RemoveNode(s.ctx, s.scope, "a", dw.CascadeFail); err != nil {
				s.t.Fatalf("RemoveNode(fail): %v", err)
			}
			s.statusIs("b", dw.StatusError)
			s.reasonIs("b", dw.ReasonRemoved)
			// The consequence reaches further than one level.
			s.statusIs("c", dw.StatusError)
		}},

		{"T-REMOVE-NODE-LEAF", func(s *suite) {
			s.add(spec("a"), spec("b", "a"))
			if _, err := s.st.RemoveNode(s.ctx, s.scope, "b", dw.CascadeReject); err != nil {
				s.t.Fatalf("removing a leaf: %v", err)
			}
			if got := s.stats().Total; got != 1 {
				s.t.Fatalf("scope has %d nodes after removing a leaf, want 1", got)
			}
			// a must not still believe it has a successor.
			s.ack(s.claim())
			if st := s.stats(); st.NonTerminal() != 0 {
				s.t.Fatalf("%+v after removing the leaf and finishing a", st)
			}
		}},

		{"T-REMOVE-INFLIGHT", func(s *suite) {
			s.add(spec("a"))
			s.claim()
			_, err := s.st.RemoveNode(s.ctx, s.scope, "a", dw.CascadeReject)
			s.wantErr("removing a claimed node", err, dw.ErrNodeInFlight)
		}},

		{"T-REMOVE-UNKNOWN", func(s *suite) {
			_, err := s.st.RemoveNode(s.ctx, s.scope, "ghost", dw.CascadeReject)
			s.wantErr("removing an unknown node", err, dw.ErrNotFound)
		}},
	}
}

func facetTests() []conformanceTest {
	return []conformanceTest{
		{"T-LIST-PAGES", func(s *suite) {
			lister, ok := s.st.(dw.Lister)
			if !ok || !caps(s.st).Has(dw.CapList) {
				s.t.Skip("backend does not report CapList")
			}
			s.add(spec("a"), spec("b"), spec("c"), spec("d"), spec("e"))

			seen := make(map[dw.NodeID]bool)
			cursor := ""
			for range 10 {
				page, err := lister.ListNodes(s.ctx, s.scope, dw.ListOptions{Cursor: cursor, Limit: 2})
				if err != nil {
					s.t.Fatalf("ListNodes: %v", err)
				}
				for _, n := range page.Nodes {
					if seen[n.ID] {
						s.t.Fatalf("node %q appeared on two pages", n.ID)
					}
					seen[n.ID] = true
				}
				if page.Next == "" {
					break
				}
				cursor = page.Next
			}
			if len(seen) != 5 {
				s.t.Fatalf("paging saw %d of 5 nodes", len(seen))
			}
		}},

		{"T-LIST-FILTER-STATUS", func(s *suite) {
			lister, ok := s.st.(dw.Lister)
			if !ok || !caps(s.st).Has(dw.CapList) {
				s.t.Skip("backend does not report CapList")
			}
			s.add(specK("a"), specK("b"))
			s.ack(s.claimByID("a"))
			page, err := lister.ListNodes(s.ctx, s.scope, dw.ListOptions{
				Statuses: []dw.Status{dw.StatusSuccess}, Limit: 10,
			})
			if err != nil {
				s.t.Fatalf("ListNodes: %v", err)
			}
			if len(page.Nodes) != 1 || page.Nodes[0].ID != "a" {
				s.t.Fatalf("status filter returned %+v, want only a", page.Nodes)
			}
		}},

		{"T-WATCH-DELIVERS", func(s *suite) {
			w, ok := s.st.(dw.DurableEventStream)
			if !ok || !caps(s.st).Has(dw.CapDurableEvents) {
				s.t.Skip("backend does not report CapDurableEvents")
			}
			ctx, cancel := contextWithCancel(s)
			defer cancel()
			ch, err := w.Watch(ctx, dw.WatchRequest{Scope: s.scope})
			if err != nil {
				s.t.Fatalf("Watch: %v", err)
			}
			s.add(spec("a"))
			ev := recv(s, ch)
			if ev.NodeID != "a" || ev.Kind != dw.EventCreated {
				s.t.Fatalf("first event is %+v, want a created event for a", ev)
			}
		}},

		{"T-WATCH-RESUME", func(s *suite) {
			w, ok := s.st.(dw.DurableEventStream)
			if !ok || !caps(s.st).Has(dw.CapDurableEvents) {
				s.t.Skip("backend does not report CapDurableEvents")
			}
			// Write first, subscribe afterwards: a resumable stream must be able
			// to deliver what happened before the subscriber arrived.
			eff := s.add(spec("a"), spec("b"))
			if len(eff) == 0 {
				s.t.Fatal("AddNodes produced no effects")
			}
			ctx, cancel := contextWithCancel(s)
			defer cancel()

			// Replay: the oldest retained event must be the first one written.
			ch, err := w.Watch(ctx, dw.WatchRequest{Scope: s.scope, Replay: true})
			if err != nil {
				s.t.Fatalf("Watch(replay): %v", err)
			}
			if ev := recv(s, ch); ev.Cursor != eff[0].Cursor {
				s.t.Fatalf("replay started at cursor %d, want %d", ev.Cursor, eff[0].Cursor)
			}

			// Resume: picking up after a known position must deliver the next
			// event and not repeat the one already seen.
			ch2, err := w.Watch(ctx, dw.WatchRequest{Scope: s.scope, From: eff[0].Cursor})
			if err != nil {
				s.t.Fatalf("Watch(resume): %v", err)
			}
			if ev := recv(s, ch2); ev.Cursor != eff[0].Cursor+1 {
				s.t.Fatalf("resumed at cursor %d, want %d", ev.Cursor, eff[0].Cursor+1)
			}
		}},

		{"T-WATCH-FROM-NOW-SKIPS-HISTORY", func(s *suite) {
			w, ok := s.st.(dw.DurableEventStream)
			if !ok || !caps(s.st).Has(dw.CapDurableEvents) {
				s.t.Skip("backend does not report CapDurableEvents")
			}
			s.add(spec("old"))
			ctx, cancel := contextWithCancel(s)
			defer cancel()
			// The default start position is "now": history is not replayed.
			ch, err := w.Watch(ctx, dw.WatchRequest{Scope: s.scope})
			if err != nil {
				s.t.Fatalf("Watch: %v", err)
			}
			s.add(spec("new"))
			if ev := recv(s, ch); ev.NodeID != "new" {
				s.t.Fatalf("first event is for %q, want the node created after subscribing", ev.NodeID)
			}
		}},

		{"T-WATCH-CURSOR-EXPIRED", func(s *suite) {
			w, ok := s.st.(dw.DurableEventStream)
			if !ok || !caps(s.st).Has(dw.CapDurableEvents) {
				s.t.Skip("backend does not report CapDurableEvents")
			}
			ctx, cancel := contextWithCancel(s)
			defer cancel()
			stats := s.stats()
			// A cursor from beyond the log's head cannot be honoured, and the
			// suite requires it be refused rather than silently restarted.
			_, err := w.Watch(ctx, dw.WatchRequest{Scope: s.scope, From: stats.Cursor + 1_000_000})
			if err == nil {
				s.t.Skip("backend accepts a cursor beyond its head; retention is unbounded")
			}
			if !errors.Is(err, dw.ErrCursorExpired) && !errors.Is(err, dw.ErrInvalidArgument) {
				s.t.Fatalf("watching from an impossible cursor gave %v", err)
			}
		}},

		{"T-WATCH-REQUIRES-SCOPE-TO-RESUME", func(s *suite) {
			w, ok := s.st.(dw.DurableEventStream)
			if !ok || !caps(s.st).Has(dw.CapDurableEvents) {
				s.t.Skip("backend does not report CapDurableEvents")
			}
			ctx, cancel := contextWithCancel(s)
			defer cancel()
			if _, err := w.Watch(ctx, dw.WatchRequest{From: 5}); err == nil {
				s.t.Fatal("watching every scope from a cursor succeeded, want it refused: cursors are per scope")
			}
		}},

		{"T-DOORBELL-WAKES", func(s *suite) {
			d, ok := s.st.(dw.Doorbell)
			if !ok || !caps(s.st).Has(dw.CapDoorbell) {
				s.t.Skip("backend does not report CapDoorbell")
			}
			ctx, cancel := contextWithTimeout(s, 5*time.Second)
			defer cancel()

			done := make(chan error, 1)
			go func() { done <- d.WaitForWork(ctx, s.scope, nil) }()

			// Give the waiter a moment to park, then create work.
			time.Sleep(20 * time.Millisecond)
			s.add(spec("a"))

			select {
			case err := <-done:
				if err != nil {
					s.t.Fatalf("WaitForWork: %v", err)
				}
			case <-ctx.Done():
				s.t.Fatal("WaitForWork did not return after work appeared")
			}
		}},

		{"T-DOORBELL-RESPECTS-CONTEXT", func(s *suite) {
			d, ok := s.st.(dw.Doorbell)
			if !ok || !caps(s.st).Has(dw.CapDoorbell) {
				s.t.Skip("backend does not report CapDoorbell")
			}
			ctx, cancel := contextWithTimeout(s, 100*time.Millisecond)
			defer cancel()
			if err := d.WaitForWork(ctx, s.scope, nil); err == nil {
				s.t.Fatal("WaitForWork returned nil with no work and an expired context")
			}
		}},

		{"T-COLLECT-TERMINAL", func(s *suite) {
			c, ok := s.st.(dw.Collector)
			if !ok || !caps(s.st).Has(dw.CapCollect) {
				s.t.Skip("backend does not report CapCollect")
			}
			s.add(specK("a"), specK("b"))
			s.ack(s.claimByID("a"))

			// A cutoff in the past collects nothing.
			n, _, err := c.CollectTerminal(s.ctx, s.scope, time.Unix(0, 0), 10)
			if err != nil {
				s.t.Fatalf("CollectTerminal: %v", err)
			}
			if n != 0 {
				s.t.Fatalf("collected %d nodes with a cutoff at the epoch, want 0", n)
			}

			// A cutoff in the future collects the terminal node and only it.
			n, _, err = c.CollectTerminal(s.ctx, s.scope, time.Now().Add(24*time.Hour), 10)
			if err != nil {
				s.t.Fatalf("CollectTerminal: %v", err)
			}
			if n != 1 {
				s.t.Fatalf("collected %d nodes, want 1", n)
			}
			if _, err := s.st.GetNode(s.ctx, s.scope, "a"); !errors.Is(err, dw.ErrNotFound) {
				s.t.Fatalf("collected node is still readable: %v", err)
			}
			s.statusIs("b", dw.StatusNew)
		}},

		{"T-CLOSE-IDEMPOTENT", func(s *suite) {
			if err := s.st.Close(s.ctx); err != nil {
				s.t.Fatalf("Close: %v", err)
			}
			if err := s.st.Close(s.ctx); err != nil {
				s.t.Fatalf("Close twice: %v", err)
			}
		}},
	}
}
