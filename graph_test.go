package dagworker_test

import (
	"errors"
	"testing"

	dw "github.com/specialistvlad/dagworker"
)

func TestGraphMutationHappyPaths(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	if err := f.m.AddNodes(f.ctx, "s", []dw.NodeSpec{{ID: "a"}, {ID: "b"}, {ID: "c"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if err := f.m.AddEdges(f.ctx, "s", []dw.Edge{{From: "a", To: "b"}, {From: "b", To: "c"}}); err != nil {
		t.Fatalf("AddEdges: %v", err)
	}
	if err := f.m.RemoveEdges(f.ctx, "s", []dw.Edge{{From: "b", To: "c"}}); err != nil {
		t.Fatalf("RemoveEdges: %v", err)
	}
	if err := f.m.RemoveNode(f.ctx, "s", "c", dw.CascadeReject); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	if err := f.m.Cancel(f.ctx, "s", "b"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if got := f.status("b"); got != dw.StatusError {
		t.Fatalf("b is %v after Cancel", got)
	}
	if err := f.m.CancelScope(f.ctx, "s"); err != nil {
		t.Fatalf("CancelScope: %v", err)
	}

	scopes, err := f.m.Scopes(f.ctx)
	if err != nil || len(scopes) == 0 {
		t.Fatalf("Scopes gave %v, %v", scopes, err)
	}
	cfg, err := f.m.ScopeConfig(f.ctx, "s")
	if err != nil {
		t.Fatalf("ScopeConfig: %v", err)
	}
	_ = cfg

	if err := f.m.Seal(f.ctx, "s"); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	done, err := f.m.IsComplete(f.ctx, "s")
	if err != nil {
		t.Fatalf("IsComplete: %v", err)
	}
	if !done {
		t.Fatal("a sealed scope with everything cancelled is not complete")
	}
	if _, err := f.m.IsComplete(f.ctx, ""); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("IsComplete on an empty scope gave %v", err)
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
