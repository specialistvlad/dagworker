package file_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/file"
)

// snapshot reads the whole visible state through the public port, which is the
// only surface a caller can tell two stores apart with. If these match after a
// restart, the restart preserved everything that is observable.
func snapshot(t *testing.T, st dw.Store) map[string]dw.Inspection {
	t.Helper()
	ctx := t.Context()
	out := map[string]dw.Inspection{}
	scopes, err := st.Scopes(ctx)
	if err != nil {
		t.Fatalf("Scopes: %v", err)
	}
	lister, ok := st.(dw.Lister)
	if !ok {
		t.Fatal("the file backend must implement Lister")
	}
	for _, sc := range scopes {
		res, err := lister.ListNodes(ctx, sc, dw.ListOptions{Limit: 10_000})
		if err != nil {
			t.Fatalf("ListNodes(%s): %v", sc, err)
		}
		for _, n := range res.Nodes {
			insp, err := st.Inspect(ctx, sc, n.ID)
			if err != nil {
				t.Fatalf("Inspect(%s/%s): %v", sc, n.ID, err)
			}
			out[string(sc)+"/"+string(n.ID)] = insp
		}
	}
	return out
}

// exercise drives a graph through every kind of mutation, so the restart below
// is not just replaying inserts. It deliberately leaves work in every state:
// succeeded, failed-and-retrying, still claimed, cancelled and blocked.
func exercise(t *testing.T, st dw.Store) {
	t.Helper()
	ctx := t.Context()
	const scope = dw.Scope("release")

	if err := st.SetScopeConfig(ctx, scope, dw.ScopeConfig{
		MaxAttempts: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: 5 * time.Millisecond,
		DefaultLeaseTimeout: time.Hour,
	}); err != nil {
		t.Fatalf("SetScopeConfig: %v", err)
	}
	if _, err := st.AddNodes(ctx, scope, []dw.NodeSpec{
		{ID: "build"},
		{ID: "test", Deps: []dw.NodeID{"build"}},
		{ID: "lint"},
		{ID: "flaky"},
		{ID: "doomed"},
		{ID: "held"},
		{ID: "notify", Deps: []dw.NodeID{"test"}, Trigger: dw.TriggerAllDone},
	}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if _, err := st.AddEdges(ctx, scope, []dw.Edge{{From: "lint", To: "notify"}}); err != nil {
		t.Fatalf("AddEdges: %v", err)
	}

	claim := func() dw.Lease {
		t.Helper()
		res, err := st.Claim(ctx, dw.ClaimRequest{Scope: scope, Max: 1, Timeout: time.Hour, WorkerID: "w"})
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if len(res.Leases) == 0 {
			t.Fatal("nothing claimable")
		}
		return res.Leases[0]
	}
	done := func(l dw.Lease, ok bool) {
		t.Helper()
		if _, err := st.Complete(ctx, dw.CompleteRequest{Lease: l, Success: ok, Reason: dw.ReasonWorkerError}); err != nil {
			t.Fatalf("Complete: %v", err)
		}
	}

	// Drain the ready set into a mixture of outcomes.
	for range 4 {
		l := claim()
		switch l.NodeID {
		case "flaky":
			done(l, false) // fails, schedules a retry -- exercises jitter
		case "doomed":
			done(l, false)
		case "held":
			// left claimed on purpose: the lease and its epoch must survive
			_, _ = st.Extend(ctx, dw.ExtendRequest{Lease: l, Timeout: 2 * time.Hour})
		default:
			done(l, true)
		}
	}
	if _, err := st.Cancel(ctx, scope, []dw.NodeID{"doomed"}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := st.Sweep(ctx, scope, 0); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
}

// TestStateSurvivesRestart is the requirement, stated as a test: files only, no
// database, state survives a restart.
func TestStateSurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	clk := dagstoretest.NewFakeClock()

	first, rec, err := file.Open(t.Context(), dir, file.WithClock(clk))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if rec.Records != 0 {
		t.Fatalf("a fresh directory replayed %d records", rec.Records)
	}
	exercise(t, first)
	before := snapshot(t, first)
	if len(before) == 0 {
		t.Fatal("the exercise produced no state to compare")
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A different process, a different clock instance, the same directory.
	second, rec2, err := file.Open(t.Context(), dir, file.WithClock(dagstoretest.NewFakeClock()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = second.Close(context.Background()) }()
	if rec2.Records == 0 {
		t.Fatal("the reopened store replayed nothing")
	}
	after := snapshot(t, second)

	if len(before) != len(after) {
		t.Fatalf("restart changed the node count: %d -> %d", len(before), len(after))
	}
	for k, b := range before {
		a, ok := after[k]
		if !ok {
			t.Errorf("%s vanished across the restart", k)
			continue
		}
		// The whole observable node, including the fields a naive command
		// replay would get wrong: the lease deadline, the fencing epoch, the
		// attempt count and the retry's scheduled time all come from the
		// clock and the jitter, and all must come back identical.
		switch {
		case a.Node.Status != b.Node.Status:
			t.Errorf("%s: status %v -> %v", k, b.Node.Status, a.Node.Status)
		case a.Node.Attempt != b.Node.Attempt:
			t.Errorf("%s: attempt %d -> %d", k, b.Node.Attempt, a.Node.Attempt)
		case a.Node.Reason != b.Node.Reason:
			t.Errorf("%s: reason %v -> %v", k, b.Node.Reason, a.Node.Reason)
		case a.Phase != b.Phase:
			t.Errorf("%s: phase %v -> %v", k, b.Phase, a.Phase)
		case a.LeaseEpoch != b.LeaseEpoch:
			t.Errorf("%s: lease epoch %d -> %d", k, b.LeaseEpoch, a.LeaseEpoch)
		case a.LeaseHolder != b.LeaseHolder:
			t.Errorf("%s: lease holder %q -> %q", k, b.LeaseHolder, a.LeaseHolder)
		case !a.LeaseDeadline.Equal(b.LeaseDeadline):
			t.Errorf("%s: lease deadline %v -> %v", k, b.LeaseDeadline, a.LeaseDeadline)
		case !a.ReadyAt.Equal(b.ReadyAt):
			t.Errorf("%s: retry scheduled for %v -> %v", k, b.ReadyAt, a.ReadyAt)
		case a.Deps != b.Deps:
			t.Errorf("%s: dep counts %+v -> %+v", k, b.Deps, a.Deps)
		}
	}
}

// TestTornTrailingRecordIsRecovered: a process killed mid-append leaves a
// partial frame. That is the ordinary shape of a crash, and it must cost the
// partial write and nothing else.
func TestTornTrailingRecordIsRecovered(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	first, _, err := file.Open(t.Context(), dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	exercise(t, first)
	before := snapshot(t, first)
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate the kill: append a half-written frame.
	path := filepath.Join(dir, "dagworker.log")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	if _, err := f.Write([]byte{0x40, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef, 0x01, 0x02}); err != nil {
		t.Fatalf("write torn frame: %v", err)
	}
	_ = f.Close()

	second, rec, err := file.Open(t.Context(), dir)
	if err != nil {
		t.Fatalf("reopen after a torn write: %v", err)
	}
	defer func() { _ = second.Close(context.Background()) }()
	if rec.DiscardedBytes != 10 {
		t.Errorf("discarded %d bytes, want the 10 of the torn frame", rec.DiscardedBytes)
	}
	if got, want := len(snapshot(t, second)), len(before); got != want {
		t.Fatalf("recovered %d nodes, want %d", got, want)
	}

	// And the truncated log must be writable again, not poisoned.
	if _, err := second.AddNodes(t.Context(), "release", []dw.NodeSpec{{ID: "after-recovery"}}); err != nil {
		t.Fatalf("writing after recovery: %v", err)
	}
}

// TestCompactionPreservesStateExactly is the property compaction must have and
// the one it is easy to get wrong: the snapshot has to carry everything the
// replayed log carried, including the parts a caller cannot set directly.
func TestCompactionPreservesStateExactly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	first, _, err := file.Open(t.Context(), dir, file.WithClock(dagstoretest.NewFakeClock()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	exercise(t, first)
	before := snapshot(t, first)

	if err := first.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// The live store must be unchanged by compacting it.
	if got := snapshot(t, first); len(got) != len(before) {
		t.Fatalf("compaction changed the live store: %d nodes -> %d", len(before), len(got))
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, rec, err := file.Open(t.Context(), dir, file.WithClock(dagstoretest.NewFakeClock()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = second.Close(context.Background()) }()

	if !rec.FromSnapshot {
		t.Error("the reopened store did not load the snapshot")
	}
	if rec.Records != 0 {
		t.Errorf("replayed %d records after compaction; the log should have been empty", rec.Records)
	}
	assertSameState(t, before, snapshot(t, second))
}

// TestCompactionBoundsTheLog: the reason compaction exists.
func TestCompactionBoundsTheLog(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st, _, err := file.Open(t.Context(), dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close(context.Background()) }()

	for i := range 200 {
		if _, err := st.AddNodes(t.Context(), "bulk", []dw.NodeSpec{
			{ID: dw.NodeID(fmt.Sprintf("n%03d", i))},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}
	}
	grown := logSize(t, dir)
	if grown == 0 {
		t.Fatal("the log did not grow")
	}
	if err := st.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if after := logSize(t, dir); after != 0 {
		t.Fatalf("the log is %d bytes after compaction, want 0", after)
	}

	// And writing continues normally afterwards.
	if _, err := st.AddNodes(t.Context(), "bulk", []dw.NodeSpec{{ID: "after"}}); err != nil {
		t.Fatalf("AddNodes after compaction: %v", err)
	}
	if logSize(t, dir) == 0 {
		t.Fatal("a write after compaction did not reach the log")
	}
}

// TestSnapshotPlusLogReplaysTogether: the ordinary steady state is a snapshot
// with newer records on top of it, and the two have to compose.
func TestSnapshotPlusLogReplaysTogether(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	first, _, err := file.Open(t.Context(), dir, file.WithClock(dagstoretest.NewFakeClock()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	exercise(t, first)
	if err := first.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// Work done AFTER the snapshot, which only the log knows about.
	if _, err := first.AddNodes(t.Context(), "release", []dw.NodeSpec{{ID: "post-snapshot"}}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	l, err := first.Claim(t.Context(), dw.ClaimRequest{Scope: "release", Max: 1, Timeout: time.Hour, WorkerID: "late"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(l.Leases) == 0 {
		t.Fatal("nothing claimable after the snapshot")
	}
	before := snapshot(t, first)
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, rec, err := file.Open(t.Context(), dir, file.WithClock(dagstoretest.NewFakeClock()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = second.Close(context.Background()) }()
	if !rec.FromSnapshot || rec.Records == 0 {
		t.Fatalf("expected a snapshot plus records, got FromSnapshot=%v Records=%d",
			rec.FromSnapshot, rec.Records)
	}
	assertSameState(t, before, snapshot(t, second))
}

// TestCorruptSnapshotFallsBackToTheLog: the log is authoritative, so a damaged
// snapshot must cost startup time and nothing else.
func TestCorruptSnapshotFallsBackToTheLog(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	first, _, err := file.Open(t.Context(), dir, file.WithClock(dagstoretest.NewFakeClock()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	exercise(t, first)
	before := snapshot(t, first)
	// A snapshot exists, but the log has NOT been truncated, so it still
	// covers everything.
	if err := first.WriteSnapshotForTest(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "dagworker.snapshot"), []byte("not a snapshot"), 0o600); err != nil {
		t.Fatalf("corrupt: %v", err)
	}

	second, rec, err := file.Open(t.Context(), dir, file.WithClock(dagstoretest.NewFakeClock()))
	if err != nil {
		t.Fatalf("a corrupt snapshot must not stop the store from opening: %v", err)
	}
	defer func() { _ = second.Close(context.Background()) }()
	if rec.FromSnapshot {
		t.Error("a corrupt snapshot was reported as loaded")
	}
	assertSameState(t, before, snapshot(t, second))
}

func assertSameState(t *testing.T, before, after map[string]dw.Inspection) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("node count %d -> %d", len(before), len(after))
	}
	for k, b := range before {
		a, ok := after[k]
		if !ok {
			t.Errorf("%s vanished", k)
			continue
		}
		switch {
		case a.Node.Status != b.Node.Status:
			t.Errorf("%s: status %v -> %v", k, b.Node.Status, a.Node.Status)
		case a.Node.Attempt != b.Node.Attempt:
			t.Errorf("%s: attempt %d -> %d", k, b.Node.Attempt, a.Node.Attempt)
		case a.Node.Reason != b.Node.Reason:
			t.Errorf("%s: reason %v -> %v", k, b.Node.Reason, a.Node.Reason)
		case a.Phase != b.Phase:
			t.Errorf("%s: phase %v -> %v", k, b.Phase, a.Phase)
		case a.LeaseEpoch != b.LeaseEpoch:
			t.Errorf("%s: lease epoch %d -> %d", k, b.LeaseEpoch, a.LeaseEpoch)
		case a.LeaseHolder != b.LeaseHolder:
			t.Errorf("%s: lease holder %q -> %q", k, b.LeaseHolder, a.LeaseHolder)
		case !a.LeaseDeadline.Equal(b.LeaseDeadline):
			t.Errorf("%s: lease deadline %v -> %v", k, b.LeaseDeadline, a.LeaseDeadline)
		case !a.ReadyAt.Equal(b.ReadyAt):
			t.Errorf("%s: retry time %v -> %v", k, b.ReadyAt, a.ReadyAt)
		case a.Deps != b.Deps:
			t.Errorf("%s: deps %+v -> %+v", k, b.Deps, a.Deps)
		case len(a.Successors) != len(b.Successors):
			t.Errorf("%s: %d successors -> %d", k, len(b.Successors), len(a.Successors))
		case len(a.Waiting) != len(b.Waiting):
			t.Errorf("%s: %d unsatisfied predecessors -> %d", k, len(b.Waiting), len(a.Waiting))
		case a.Rank != b.Rank:
			t.Errorf("%s: topological rank %d -> %d", k, b.Rank, a.Rank)
		}
	}
}

func logSize(t *testing.T, dir string) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, "dagworker.log"))
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	return fi.Size()
}
