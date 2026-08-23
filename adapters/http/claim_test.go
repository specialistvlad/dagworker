package httpadapter

import (
	"net/http"
	"testing"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/memory"
)

// TestClaim_TimeoutIsNoContent covers the blocking-query 204: a claim against
// an empty scope must return 204 with no body once wait elapses, never 200
// with an empty leases array (adapter contract §2).
func TestClaim_TimeoutIsNoContent(t *testing.T) {
	t.Parallel()
	base, _, shutdown := testServer(t)
	defer shutdown()

	start := time.Now()
	resp := doRequest(t, http.MethodPost, base+"/v1/scopes/empty-scope/nodes:claim",
		map[string]any{"wait": "200ms"})
	defer func() { _ = resp.Body.Close() }()
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if resp.ContentLength > 0 {
		t.Fatalf("expected no body on 204, Content-Length = %d", resp.ContentLength)
	}
	// It should have genuinely waited close to the requested window, not
	// returned instantly (which would suggest the wait was ignored) and not
	// hung well past it either (which would suggest the deadline was not
	// wired through).
	if elapsed < 100*time.Millisecond {
		t.Errorf("returned after only %s, wanted to actually wait close to 200ms", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("took %s, wanted well under 5s", elapsed)
	}
}

// TestClaim_HappyPathThenAck covers claim -> ack: PUT a node, claim it,
// complete it, and confirm the node's terminal state through GET.
func TestClaim_HappyPathThenAck(t *testing.T) {
	t.Parallel()
	base, _, shutdown := testServer(t)
	defer shutdown()

	putResp := doRequest(t, http.MethodPut, base+"/v1/scopes/proj/nodes/task-1", map[string]any{
		"payload_encoding": "base64",
		"payload":          "aGVsbG8=", // "hello"
	})
	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT node status = %d, want 201", putResp.StatusCode)
	}
	_ = putResp.Body.Close()

	claimResp := doRequest(t, http.MethodPost, base+"/v1/scopes/proj/nodes:claim",
		map[string]any{"worker_id": "worker-1"})
	defer func() { _ = claimResp.Body.Close() }()
	if claimResp.StatusCode != http.StatusOK {
		t.Fatalf("claim status = %d, want 200", claimResp.StatusCode)
	}
	var claimed claimResponse
	decodeBody(t, claimResp, &claimed)
	if len(claimed.Leases) != 1 {
		t.Fatalf("got %d leases, want exactly 1", len(claimed.Leases))
	}
	lease := claimed.Leases[0]
	if lease.Node.ID != "task-1" {
		t.Errorf("claimed node id = %q, want task-1", lease.Node.ID)
	}
	if lease.Node.Status != dagworker.StatusInProgress {
		t.Errorf("claimed node status = %v, want in_progress", lease.Node.Status)
	}
	if lease.LeaseID == "" {
		t.Fatal("lease_id must not be empty")
	}

	completePath := base + "/v1/scopes/proj/leases/" + lease.LeaseID + ":complete"
	completeResp := doRequest(t, http.MethodPost, completePath, map[string]any{
		"result_encoding": "base64",
		"result":          "b2s=", // "ok"
	})
	defer func() { _ = completeResp.Body.Close() }()
	if completeResp.StatusCode != http.StatusOK {
		t.Fatalf(":complete status = %d, want 200", completeResp.StatusCode)
	}
	var result completeResponse
	decodeBody(t, completeResp, &result)
	if result.Status != dagworker.StatusSuccess {
		t.Errorf("completion status = %v, want success", result.Status)
	}

	getResp := doRequest(t, http.MethodGet, base+"/v1/scopes/proj/nodes/task-1", nil)
	defer func() { _ = getResp.Body.Close() }()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET node status = %d, want 200", getResp.StatusCode)
	}
	var node nodeWire
	decodeBody(t, getResp, &node)
	if node.Status != dagworker.StatusSuccess {
		t.Errorf("final node status = %v, want success", node.Status)
	}
}

// TestClaim_StaleAckIsFencedWith409 forces a lease to be superseded — by
// advancing a fake clock past its deadline and reclaiming it — and confirms
// the original lease's :complete is refused as a 409 with the
// "lease-superseded" problem type, per the adapter contract's error table.
// This is the one behavior the assignment brief calls out by name: a stale
// ack must never be allowed to silently overwrite whoever holds the lease
// now.
func TestClaim_StaleAckIsFencedWith409(t *testing.T) {
	t.Parallel()

	clk := dagstoretest.NewFakeClock()
	store := memory.New(memory.WithClock(clk))
	mgr, err := dagworker.New(store, dagworker.WithClock(clk), dagworker.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("dagworker.New: %v", err)
	}
	srv, err := New(mgr, withJitter(func(time.Duration) time.Duration { return 0 }))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	base, stopServer := startServer(t, srv)
	defer func() {
		stopServer()
		_ = mgr.Close(t.Context())
	}()

	putResp := doRequest(t, http.MethodPut, base+"/v1/scopes/proj/nodes/flaky", nil)
	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT node status = %d, want 201", putResp.StatusCode)
	}
	_ = putResp.Body.Close()

	firstClaim := doRequest(t, http.MethodPost, base+"/v1/scopes/proj/nodes:claim", nil)
	defer func() { _ = firstClaim.Body.Close() }()
	var first claimResponse
	decodeBody(t, firstClaim, &first)
	if len(first.Leases) != 1 {
		t.Fatalf("first claim: got %d leases, want 1", len(first.Leases))
	}
	staleLeaseID := first.Leases[0].LeaseID

	// Past the default 30s lease timeout: the worker holding staleLeaseID has
	// gone silent from the store's point of view.
	clk.Advance(31 * time.Second)
	if _, err := mgr.Sweep(t.Context(), "proj"); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// Past the maximum full-jitter retry backoff (5m default), so the
	// reclaimed node is guaranteed ready again regardless of how much jitter
	// the backoff drew.
	clk.Advance(6 * time.Minute)

	secondClaim := doRequest(t, http.MethodPost, base+"/v1/scopes/proj/nodes:claim", nil)
	defer func() { _ = secondClaim.Body.Close() }()
	var second claimResponse
	decodeBody(t, secondClaim, &second)
	if len(second.Leases) != 1 {
		t.Fatalf("second claim: got %d leases, want 1 (the reclaimed node)", len(second.Leases))
	}
	if second.Leases[0].FencingEpoch != first.Leases[0].FencingEpoch+1 {
		t.Fatalf("second claim epoch = %d, want %d (one more than the first)",
			second.Leases[0].FencingEpoch, first.Leases[0].FencingEpoch+1)
	}

	// The original worker, unaware it was ever reclaimed, finally gets around
	// to acknowledging its (now stale) lease.
	staleAck := doRequest(t, http.MethodPost, base+"/v1/scopes/proj/leases/"+staleLeaseID+":complete", nil)
	defer func() { _ = staleAck.Body.Close() }()
	if staleAck.StatusCode != http.StatusConflict {
		t.Fatalf("stale ack status = %d, want 409", staleAck.StatusCode)
	}
	if ct := staleAck.Header.Get("Content-Type"); ct != problemContentType {
		t.Errorf("Content-Type = %q, want %q", ct, problemContentType)
	}
	var p problem
	decodeBody(t, staleAck, &p)
	wantType := defaultProblemBaseURI + "lease-superseded"
	if p.Type != wantType {
		t.Errorf("problem type = %q, want %q", p.Type, wantType)
	}
	if p.Status != http.StatusConflict {
		t.Errorf("problem status field = %d, want 409", p.Status)
	}
}
