package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/test/e2e"
)

// Media transcoding: the case a task queue cannot express and a workflow engine
// is too heavy for.
//
// The shape of the graph is not known when it is submitted. Probing the source
// decides how many renditions there are, and each rendition is a separate unit
// of work that a separate machine should pick up. Only when all of them finish
// can the manifest be written.
//
//	ingest ──▶ probe ──┬──▶ rendition:240p ──┐
//	                   ├──▶ rendition:720p ──┼──▶ manifest ──▶ publish
//	                   └──▶ rendition:1080p ─┘
//	                   ▲
//	                   └── these three do not exist until probe runs
//
// A task queue has no edges, so the manifest step would have to poll or the
// probe would have to block a worker until every rendition finished. A workflow
// engine can express it, at the cost of running your code inside its runtime.
// Here the fan-out is three lines in the probe handler.
func TestE2E_Scenario_DynamicFanOut(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		m := newManager(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := m.AddNodes(ctx, scope, []dw.NodeSpec{
			{ID: "ingest", Kind: "io"},
			{ID: "probe", Kind: "io", Deps: []dw.NodeID{"ingest"}},
			// manifest and publish exist from the start, but manifest will
			// acquire dependencies that do not exist yet.
			{ID: "manifest", Kind: "io", Deps: []dw.NodeID{"probe"}},
			{ID: "publish", Kind: "io", Deps: []dw.NodeID{"manifest"}},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}

		// What probing "discovers". A real probe reads the source's resolution.
		renditions := []string{"240p", "480p", "720p", "1080p", "2160p"}

		var mu sync.Mutex
		transcoded := map[string]bool{}
		manifestSaw := 0

		handler := func(hctx context.Context, node dw.Node) ([]byte, error) {
			switch {
			case node.ID == "probe":
				// The graph grows here, while it is running. Each rendition is
				// inserted with manifest as its successor, so manifest is
				// pulled back out of the ready set until they all finish.
				specs := make([]dw.NodeSpec, 0, len(renditions))
				for _, r := range renditions {
					specs = append(specs, dw.NodeSpec{
						ID:      dw.NodeID("rendition:" + r),
						Kind:    "transcode",
						Payload: fmt.Appendf(nil, `{"rendition":%q}`, r),
						Deps:    []dw.NodeID{"probe"},
					})
				}
				if err := m.AddNodes(hctx, scope, specs); err != nil {
					return nil, fmt.Errorf("fan out: %w", err)
				}
				edges := make([]dw.Edge, 0, len(renditions))
				for _, r := range renditions {
					edges = append(edges, dw.Edge{From: dw.NodeID("rendition:" + r), To: "manifest"})
				}
				if err := m.AddEdges(hctx, scope, edges); err != nil {
					return nil, fmt.Errorf("join: %w", err)
				}
				// The shape is now known, so the scope can be sealed: no more
				// nodes will be added, which is what makes "is this finished"
				// answerable at all. Sealing blocks new nodes, not the ones
				// already in flight.
				if err := m.Seal(hctx, scope); err != nil {
					return nil, fmt.Errorf("seal: %w", err)
				}
				return nil, nil

			case node.Kind == "transcode":
				var p struct {
					Rendition string `json:"rendition"`
				}
				if err := json.Unmarshal(node.Payload, &p); err != nil {
					return nil, fmt.Errorf("payload: %w", err)
				}
				mu.Lock()
				transcoded[p.Rendition] = true
				mu.Unlock()
				return nil, nil

			case node.ID == "manifest":
				mu.Lock()
				manifestSaw = len(transcoded)
				mu.Unlock()
				return nil, nil
			}
			return nil, nil
		}

		// Two pools: cheap IO work, and a wider pool of transcoders.
		runPools(ctx, t, []*e2e.Pool{
			{Manager: m, Scope: scope, Kinds: []string{"io"}, Workers: 1, Handle: handler},
			{Manager: m, Scope: scope, Kinds: []string{"transcode"}, Workers: 4, Handle: handler},
		}, 60*time.Second)

		mu.Lock()
		defer mu.Unlock()

		if len(transcoded) != len(renditions) {
			t.Fatalf("transcoded %d of %d renditions: %v", len(transcoded), len(renditions), transcoded)
		}
		// The point of the join: the manifest must not have been written until
		// every rendition it depends on was finished.
		if manifestSaw != len(renditions) {
			t.Fatalf("manifest ran when only %d of %d renditions were done",
				manifestSaw, len(renditions))
		}
		for _, id := range []dw.NodeID{"ingest", "probe", "manifest", "publish"} {
			n, err := m.GetNode(ctx, scope, id)
			if err != nil {
				t.Fatalf("GetNode(%q): %v", id, err)
			}
			if n.Status != dw.StatusSuccess {
				t.Errorf("%q is %v/%v", id, n.Status, n.Reason)
			}
		}
	})
}

// Adding an edge into a node that is already claimable must pull it back out
// before anyone can take it. This is the race the dynamic fan-out above depends
// on being closed, tested directly rather than inferred.
func TestE2E_Scenario_JoinBlocksWhileFanningOut(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, b e2e.Backend) {
		t.Helper()
		m := newManager(t, b)
		ctx := t.Context()
		scope := e2e.UniqueScope(t)

		if err := m.AddNodes(ctx, scope, []dw.NodeSpec{
			{ID: "gate", Kind: "gate"},
			{ID: "join", Kind: "join", Deps: []dw.NodeID{"gate"}},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}

		// Finish the gate: join is now claimable.
		gate, err := m.TryClaim(ctx, scope, dw.OfKind("gate"))
		if err != nil {
			t.Fatalf("TryClaim: %v", err)
		}
		if err := m.Ack(ctx, gate, nil); err != nil {
			t.Fatalf("Ack: %v", err)
		}

		// Now grow the graph underneath it.
		if err := m.AddNodes(ctx, scope, []dw.NodeSpec{
			{ID: "extra", Kind: "work", Deps: []dw.NodeID{"gate"}},
		}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}
		if err := m.AddEdges(ctx, scope, []dw.Edge{{From: "extra", To: "join"}}); err != nil {
			t.Fatalf("AddEdges: %v", err)
		}

		// join must have been pulled back out of the ready set.
		if l, err := m.TryClaim(ctx, scope, dw.OfKind("join")); err == nil {
			t.Fatalf("claimed %q while it had an unfinished new dependency", l.NodeID)
		}

		extra, err := m.TryClaim(ctx, scope, dw.OfKind("work"))
		if err != nil {
			t.Fatalf("TryClaim: %v", err)
		}
		if err := m.Ack(ctx, extra, nil); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		if _, err := m.TryClaim(ctx, scope, dw.OfKind("join")); err != nil {
			t.Fatalf("join did not become claimable once its new dependency finished: %v", err)
		}
	})
}
