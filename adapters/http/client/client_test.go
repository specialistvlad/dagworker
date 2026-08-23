package client_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
	httpadapter "github.com/specialistvlad/dagworker/adapters/http"
	"github.com/specialistvlad/dagworker/adapters/http/client"
	"github.com/specialistvlad/dagworker/storage/memory"
)

// noKeepAliveClient is used throughout: keeping keep-alive connections out of
// the picture is deliberate, not an oversight. net/http.Server.Shutdown
// reclaims idle keep-alive connections via its own background polling, whose
// promptness is stdlib's concern and already covered, for this adapter's own
// contractual "must not hang" requirement, by the SSE shutdown test
// (shutdown_test.go) — a genuinely long-lived stream, not an ordinary
// request/response connection sitting idle between polls. Sharing that
// polling's timing with these tests would make them flaky under parallel
// load for a reason unrelated to what each one actually verifies.
func noKeepAliveClient() *http.Client {
	return &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
}

// startTestServer wires an in-memory backend and the real HTTP adapter on a
// loopback listener, so the client is exercised against the actual wire
// protocol rather than a mock — the same reason the server's own tests avoid
// httptest.NewServer(mux) in favor of a real Serve/Shutdown round trip.
func startTestServer(t *testing.T) (baseURL string, mgr *dagworker.Manager) {
	t.Helper()

	store := memory.New()
	mgr, err := dagworker.New(store, dagworker.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("dagworker.New: %v", err)
	}
	srv, err := httpadapter.New(mgr)
	if err != nil {
		t.Fatalf("httpadapter.New: %v", err)
	}

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(serveCtx, lis); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		ctx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelShutdown()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		cancel()
		<-done
		if err := mgr.Close(context.Background()); err != nil {
			t.Errorf("Manager.Close: %v", err)
		}
	})

	return "http://" + lis.Addr().String() + "/v1", mgr
}

// TestClient_CreateClaimComplete exercises the reference client end to end
// against a real server: create a node, claim it, and acknowledge success.
func TestClient_CreateClaimComplete(t *testing.T) {
	t.Parallel()
	base, _ := startTestServer(t)
	c := client.New(base, client.WithHTTPClient(noKeepAliveClient()))
	ctx := t.Context()

	node, err := c.CreateNode(ctx, "proj", "task-1", client.CreateNodeOptions{
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	if string(node.Payload) != "hello" {
		t.Errorf("payload = %q, want %q", node.Payload, "hello")
	}

	leases, err := c.Claim(ctx, "proj", client.ClaimOptions{WorkerID: "w1"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("got %d leases, want 1", len(leases))
	}
	lease := leases[0]
	if lease.Node.ID != "task-1" {
		t.Errorf("claimed node id = %q, want task-1", lease.Node.ID)
	}

	result, err := c.Complete(ctx, "proj", lease.ID, []byte("done"))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if result.Status != "success" {
		t.Errorf("status = %q, want success", result.Status)
	}

	// A retry of the same (now consumed) lease is refused, per the fencing
	// contract, as a *StatusError rather than a silently-accepted no-op.
	if _, err := c.Complete(ctx, "proj", lease.ID, []byte("done")); err == nil {
		t.Fatal("expected an error completing an already-consumed lease, got nil")
	} else if statusErr, ok := asStatusError(err); !ok {
		t.Fatalf("expected a *client.StatusError, got %T: %v", err, err)
	} else if statusErr.StatusCode != http.StatusConflict {
		t.Errorf("status code = %d, want 409", statusErr.StatusCode)
	}
}

// TestClient_ClaimNoWorkReturnsNilNotError covers the 204 contract on the
// client side: no work available is a nil slice and a nil error, never an
// error a caller would have to special-case.
func TestClient_ClaimNoWorkReturnsNilNotError(t *testing.T) {
	t.Parallel()
	base, _ := startTestServer(t)
	c := client.New(base, client.WithHTTPClient(noKeepAliveClient()))

	leases, err := c.Claim(t.Context(), "empty-scope", client.ClaimOptions{Wait: 150 * time.Millisecond})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if leases != nil {
		t.Errorf("leases = %v, want nil", leases)
	}
}

// TestHandle_AutoRenewExtendsDeadline covers the reference client's owned
// renew loop: a Handle's lease deadline advances on its own, on a background
// context distinct from the one used for the original Claim call.
func TestHandle_AutoRenewExtendsDeadline(t *testing.T) {
	t.Parallel()
	base, _ := startTestServer(t)
	c := client.New(base, client.WithHTTPClient(noKeepAliveClient()), client.WithRenewTimeout(2*time.Second))

	if _, err := c.CreateNode(t.Context(), "proj", "long-task", client.CreateNodeOptions{}); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	// A short-lived claim context: it expires almost immediately, proving the
	// renew loop cannot be using it (if it were, the very first renew tick
	// would fail with a context-deadline error instead of succeeding).
	claimCtx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	handles, err := c.ClaimAndRenew(claimCtx, "proj",
		client.ClaimOptions{LeaseTimeout: 500 * time.Millisecond}, 100*time.Millisecond)
	cancel()
	if err != nil {
		t.Fatalf("ClaimAndRenew: %v", err)
	}
	if len(handles) != 1 {
		t.Fatalf("got %d handles, want 1", len(handles))
	}
	h := handles[0]
	initialDeadline := h.Lease().Deadline

	// Long past claimCtx's own deadline, but well within a couple of renew
	// ticks — if the renew loop depended on claimCtx, it would have stopped
	// renewing (and canceled h.Context()) the instant claimCtx expired.
	select {
	case <-h.Context().Done():
		t.Fatalf("Handle.Context() was canceled unexpectedly: %v", h.Err())
	case <-time.After(500 * time.Millisecond):
	}

	if !h.Lease().Deadline.After(initialDeadline) {
		t.Errorf("lease deadline did not advance: still %v", h.Lease().Deadline)
	}

	if _, err := h.Complete(t.Context(), nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	select {
	case <-h.Context().Done():
	case <-time.After(time.Second):
		t.Error("Handle.Context() should be canceled after Complete")
	}
}

func asStatusError(err error) (*client.StatusError, bool) {
	se, ok := err.(*client.StatusError)
	return se, ok
}
