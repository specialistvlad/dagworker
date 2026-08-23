package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/test/e2e"
)

// A release pipeline: the shape most people picture when they think "DAG of
// work". It is here because it exercises, in one run, nearly everything a real
// host program does — heterogeneous worker pools claiming by kind, a fan-out
// and a join, a branch that is not taken, a flaky step that retries, and a
// cleanup step that must run whatever happened upstream.
//
//	checkout ──▶ build ──┬──▶ test:unit ────────┐
//	                     ├──▶ test:integration ─┼──▶ package ──▶ publish ──▶ notify
//	                     └──▶ lint ─────────────┘                              ▲
//	                                                                           │
//	                     (notify uses all_done, so it runs even if publish fails)
func TestE2E_Scenario_ReleasePipeline(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		m := newManager(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := m.Configure(ctx, scope, dw.ScopeConfig{
			MaxAttempts:    3,
			RetryBaseDelay: time.Millisecond,
			RetryMaxDelay:  5 * time.Millisecond,
		}); err != nil {
			t.Fatalf("Configure: %v", err)
		}

		// The graph. Kinds route each node to the pool provisioned for it:
		// builders are expensive, testers are many, publishing is serialised.
		if err := m.AddNodes(ctx, scope, []dw.NodeSpec{
			{ID: "checkout", Kind: "vcs"},
			{ID: "build", Kind: "build", Deps: []dw.NodeID{"checkout"}},

			{ID: "test:unit", Kind: "test", Deps: []dw.NodeID{"build"}},
			{ID: "test:integration", Kind: "test", Deps: []dw.NodeID{"build"}},
			{ID: "lint", Kind: "test", Deps: []dw.NodeID{"build"}},

			{ID: "package", Kind: "build", Deps: []dw.NodeID{"test:unit", "test:integration", "lint"}},
			{ID: "publish", Kind: "publish", Deps: []dw.NodeID{"package"}},

			// all_done: a release that failed still has to tell somebody.
			{ID: "notify", Kind: "notify", Deps: []dw.NodeID{"publish"}, Trigger: dw.TriggerAllDone},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}
		if err := m.Seal(ctx, scope); err != nil {
			t.Fatalf("Seal: %v", err)
		}

		var mu sync.Mutex
		finished := map[dw.NodeID]int{}
		step := 0
		attempts := map[dw.NodeID]int{}

		record := func(id dw.NodeID) {
			mu.Lock()
			defer mu.Unlock()
			step++
			finished[id] = step
		}

		handler := func(_ context.Context, node dw.Node) ([]byte, error) {
			mu.Lock()
			attempts[node.ID]++
			n := attempts[node.ID]
			mu.Unlock()

			// The integration suite is flaky on its first run, the way real
			// integration suites are.
			if node.ID == "test:integration" && n == 1 {
				return nil, errors.New("flaky: connection reset")
			}
			record(node.ID)
			return fmt.Appendf(nil, `{"step":%q,"attempt":%d}`, node.ID, n), nil
		}

		// Four pools, provisioned differently, exactly as a real deployment
		// would be: one publisher because artefact registries dislike
		// concurrency, three testers because tests are the long pole.
		pools := []*e2e.Pool{
			{Manager: m, Scope: scope, Kinds: []string{"vcs"}, Workers: 1, Handle: handler},
			{Manager: m, Scope: scope, Kinds: []string{"build"}, Workers: 2, Handle: handler},
			{Manager: m, Scope: scope, Kinds: []string{"test"}, Workers: 3, Handle: handler},
			{Manager: m, Scope: scope, Kinds: []string{"publish"}, Workers: 1, Handle: handler},
			{Manager: m, Scope: scope, Kinds: []string{"notify"}, Workers: 1, Handle: handler},
		}
		runPools(ctx, t, pools, 60*time.Second)

		done, err := m.IsComplete(ctx, scope)
		if err != nil {
			t.Fatalf("IsComplete: %v", err)
		}
		if !done {
			st, _ := m.Stats(ctx, scope)
			t.Fatalf("pipeline did not finish: %+v", st)
		}

		mu.Lock()
		defer mu.Unlock()

		if len(finished) != 8 {
			t.Fatalf("%d of 8 steps ran: %v", len(finished), finished)
		}
		// The flaky step really did retry rather than being quietly skipped.
		if attempts["test:integration"] < 2 {
			t.Fatalf("test:integration ran %d times, want a retry", attempts["test:integration"])
		}

		// Every dependency edge must show up as an ordering. This is the whole
		// promise, checked against the graph rather than a hardcoded sequence.
		for _, edge := range [][2]dw.NodeID{
			{"checkout", "build"},
			{"build", "test:unit"},
			{"build", "test:integration"},
			{"build", "lint"},
			{"test:unit", "package"},
			{"test:integration", "package"},
			{"lint", "package"},
			{"package", "publish"},
			{"publish", "notify"},
		} {
			if finished[edge[0]] >= finished[edge[1]] {
				t.Errorf("%s finished at step %d, not before %s at step %d",
					edge[0], finished[edge[0]], edge[1], finished[edge[1]])
			}
		}
	})
}

// The same pipeline, but publishing fails for good. The release does not
// happen, everything downstream of the failure is accounted for, and the
// notifier still runs — which is the entire reason all_done exists.
func TestE2E_Scenario_ReleaseFailsButNotifies(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		m := newManager(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := m.Configure(ctx, scope, dw.ScopeConfig{MaxAttempts: 2, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond}); err != nil {
			t.Fatalf("Configure: %v", err)
		}
		if err := m.AddNodes(ctx, scope, []dw.NodeSpec{
			{ID: "build", Kind: "work"},
			{ID: "publish", Kind: "work", Deps: []dw.NodeID{"build"}},
			{ID: "announce", Kind: "work", Deps: []dw.NodeID{"publish"}},
			{ID: "notify", Kind: "notify", Deps: []dw.NodeID{"publish"}, Trigger: dw.TriggerAllDone},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}
		if err := m.Seal(ctx, scope); err != nil {
			t.Fatalf("Seal: %v", err)
		}

		var notified bool
		var mu sync.Mutex
		handler := func(_ context.Context, node dw.Node) ([]byte, error) {
			if node.ID == "publish" {
				return nil, errors.New("registry rejected the artefact")
			}
			if node.ID == "notify" {
				mu.Lock()
				notified = true
				mu.Unlock()
			}
			return nil, nil
		}

		runPools(ctx, t, []*e2e.Pool{
			{Manager: m, Scope: scope, Kinds: []string{"work"}, Workers: 2, Handle: handler},
			{Manager: m, Scope: scope, Kinds: []string{"notify"}, Workers: 1, Handle: handler},
		}, 60*time.Second)

		mu.Lock()
		defer mu.Unlock()
		if !notified {
			t.Fatal("the notifier never ran, though it was declared all_done")
		}

		for id, want := range map[dw.NodeID]dw.Reason{
			"publish":  dw.ReasonWorkerError,
			"announce": dw.ReasonUpstreamFailed,
		} {
			n, err := m.GetNode(ctx, scope, id)
			if err != nil {
				t.Fatalf("GetNode(%q): %v", id, err)
			}
			if n.Status != dw.StatusError || n.Reason != want {
				t.Errorf("%q is %v/%v, want error/%v", id, n.Status, n.Reason, want)
			}
		}

		// Nothing is left in limbo: a sealed scope whose work all resolved is
		// complete even though the release failed.
		if done, err := m.IsComplete(ctx, scope); err != nil || !done {
			st, _ := m.Stats(ctx, scope)
			t.Fatalf("scope not complete after a failed release: %+v (err %v)", st, err)
		}
	})
}

// Branching. A worker that finds nothing to do reports a skip rather than a
// failure, and downstream nodes distinguish the two through their trigger rule:
// the deploy is skipped along with its branch, while the summary still runs.
func TestE2E_Scenario_SkippedBranch(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		m := newManager(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := m.AddNodes(ctx, scope, []dw.NodeSpec{
			{ID: "detect-changes", Kind: "work"},
			// Nothing changed, so this branch is skipped rather than failed.
			{ID: "deploy-docs", Kind: "work", Deps: []dw.NodeID{"detect-changes"}},
			// all_success will not accept a skipped predecessor.
			{ID: "purge-cdn", Kind: "work", Deps: []dw.NodeID{"deploy-docs"}},
			// none_failed will: a skip is not a failure.
			{ID: "summary", Kind: "work", Deps: []dw.NodeID{"deploy-docs"}, Trigger: dw.TriggerNoneFailed},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}
		if err := m.Seal(ctx, scope); err != nil {
			t.Fatalf("Seal: %v", err)
		}

		var summaryRan bool
		var mu sync.Mutex
		handler := func(_ context.Context, node dw.Node) ([]byte, error) {
			if node.ID == "deploy-docs" {
				return nil, e2e.ErrSkip
			}
			if node.ID == "summary" {
				mu.Lock()
				summaryRan = true
				mu.Unlock()
			}
			return nil, nil
		}
		runPools(ctx, t, []*e2e.Pool{
			{Manager: m, Scope: scope, Workers: 2, Handle: handler},
		}, 60*time.Second)

		mu.Lock()
		ran := summaryRan
		mu.Unlock()
		if !ran {
			t.Error("summary did not run, though none_failed accepts a skipped predecessor")
		}

		docs, err := m.GetNode(ctx, scope, "deploy-docs")
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if docs.Reason != dw.ReasonSkipped {
			t.Errorf("deploy-docs is %v/%v, want a skip", docs.Status, docs.Reason)
		}
		purge, err := m.GetNode(ctx, scope, "purge-cdn")
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if purge.Status != dw.StatusError || purge.Reason != dw.ReasonSkipped {
			t.Errorf("purge-cdn is %v/%v, want it skipped behind its branch", purge.Status, purge.Reason)
		}
	})
}

// runPools runs every pool to completion, failing the test on error or timeout.
func runPools(ctx context.Context, t *testing.T, pools []*e2e.Pool, within time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, within)
	defer cancel()

	var wg sync.WaitGroup
	errs := make([]error, len(pools))
	for i, p := range pools {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = p.Run(ctx)
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(within + 5*time.Second):
		t.Fatal("worker pools did not stop")
	}

	if err := errors.Join(errs...); err != nil && !strings.Contains(err.Error(), "context") {
		t.Fatalf("pool: %v", err)
	}
}
