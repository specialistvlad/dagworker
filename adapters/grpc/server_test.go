package grpcadapter_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	grpcadapter "github.com/specialistvlad/dagworker/adapters/grpc"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
)

const bufSize = 1 << 20

// harness wires one Manager, over a fresh in-memory Store, behind one
// grpcadapter.Server reachable only through an in-process bufconn listener —
// no real port, so every test in this file can run in parallel without
// fighting over one.
type harness struct {
	worker  pb.WorkerServiceClient
	control pb.ControlServiceClient
	server  *grpcadapter.Server
	clock   *dagstoretest.FakeClock
}

// newHarness starts a server and returns clients dialed against it. t.Cleanup
// tears everything down in the order the adapter contract requires: the
// server first (Shutdown drains in-flight RPCs), then the Manager, then the
// Store it never took ownership of.
func newHarness(t *testing.T, opts ...grpcadapter.Option) *harness {
	t.Helper()

	clock := dagstoretest.NewFakeClock()
	store := memory.New(memory.WithClock(clock))
	mgr, err := dw.New(store, dw.WithClock(clock), dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("dagworker.New: %v", err)
	}

	srv, err := grpcadapter.New(mgr, opts...)
	if err != nil {
		t.Fatalf("grpcadapter.New: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(serveCtx, lis) }()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Server.Shutdown: %v", err)
		}
		cancelServe()
		if err := <-serveErr; err != nil {
			t.Errorf("Server.Serve returned %v, want nil", err)
		}
		closeCtx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel2()
		if err := mgr.Close(closeCtx); err != nil {
			t.Errorf("Manager.Close: %v", err)
		}
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Store.Close: %v", err)
		}
	})

	return &harness{
		worker:  pb.NewWorkerServiceClient(conn),
		control: pb.NewControlServiceClient(conn),
		server:  srv,
		clock:   clock,
	}
}

func addOneNode(t *testing.T, h *harness, scope, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := h.control.AddNodes(ctx, &pb.AddNodesRequest{
		Scope: scope,
		Nodes: []*pb.NewNode{{Id: id}},
	})
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
}

// TestClaimAckRoundTrip is the happy path: add a node, claim it, complete it,
// and see the terminal state through GetNode.
func TestClaimAckRoundTrip(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addOneNode(t, h, "scope-a", "n1")

	claimResp, err := h.worker.ClaimNode(ctx, &pb.ClaimNodeRequest{
		Scope:       "scope-a",
		WorkerId:    "w1",
		PollTimeout: durationpb.New(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("ClaimNode: %v", err)
	}
	lease := claimResp.GetLease()
	if lease == nil {
		t.Fatal("ClaimNode returned no lease for a ready node")
	}
	if lease.GetNode().GetId() != "n1" {
		t.Fatalf("claimed node id = %q, want n1", lease.GetNode().GetId())
	}

	if _, err := h.worker.CompleteNode(ctx, &pb.CompleteNodeRequest{
		TaskToken: lease.GetTaskToken(),
		Result:    []byte("ok"),
	}); err != nil {
		t.Fatalf("CompleteNode: %v", err)
	}

	got, err := h.control.GetNode(ctx, &pb.GetNodeRequest{Scope: "scope-a", NodeId: "n1"})
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.GetNode().GetStatus() != pb.NodeStatus_NODE_STATUS_SUCCESS {
		t.Fatalf("status = %v, want SUCCESS", got.GetNode().GetStatus())
	}
}

// TestStaleAckRejectedAsAborted is the fenced-write scenario the adapter
// contract exists to prevent from silently corrupting state: a worker whose
// lease was reclaimed and reissued presents its old task_token, and the
// server must refuse it with ABORTED (lease-superseded), never accept it.
//
// The FakeClock makes this deterministic instead of racing a real sleep:
// advancing it past the lease deadline is what makes the second Claim
// reclaim the node inline, exactly as Store.Claim's doc comment promises.
func TestStaleAckRejectedAsAborted(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addOneNode(t, h, "scope-b", "n1")

	first, err := h.worker.ClaimNode(ctx, &pb.ClaimNodeRequest{
		Scope:         "scope-b",
		LeaseDuration: durationpb.New(1 * time.Second),
		PollTimeout:   durationpb.New(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("first ClaimNode: %v", err)
	}
	staleToken := first.GetLease().GetTaskToken()
	if staleToken == nil {
		t.Fatal("first ClaimNode returned no lease")
	}

	// Push the store's own clock past the lease deadline: the node is now
	// eligible for inline reclaim by the next claimant, per Store.Claim's
	// contract ("should reclaim any expired lease they encounter"). A short,
	// real poll_timeout keeps this probe call from actually blocking: there
	// is nothing ready for it to find yet, only the reclaim side effect this
	// call triggers.
	//
	// The reclaimed node does not become ready in this same call: a timed-out
	// attempt is recorded through the scope's ordinary retry policy, which
	// schedules the next attempt behind a full-jitter backoff (ADR-0012)
	// rather than making it claimable the instant it is reclaimed — the same
	// backoff a genuine worker failure gets. This probe call is what makes
	// that scheduling happen; it is expected to find no ready node itself.
	h.clock.Advance(2 * time.Second)
	if _, err := h.worker.ClaimNode(ctx, &pb.ClaimNodeRequest{
		Scope:       "scope-b",
		PollTimeout: durationpb.New(50 * time.Millisecond),
	}); err != nil {
		t.Fatalf("probe ClaimNode: %v", err)
	}

	// Clear the backoff window (bounded by the scope's default retry base
	// delay, one second) so the rescheduled attempt is unconditionally ready.
	h.clock.Advance(2 * time.Second)

	second, err := h.worker.ClaimNode(ctx, &pb.ClaimNodeRequest{
		Scope:         "scope-b",
		LeaseDuration: durationpb.New(10 * time.Second),
		PollTimeout:   durationpb.New(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("second ClaimNode: %v", err)
	}
	if second.GetLease() == nil {
		t.Fatal("second ClaimNode did not reclaim the expired lease")
	}
	if second.GetLease().GetFencingToken() == first.GetLease().GetFencingToken() {
		t.Fatalf("fencing token did not advance across reclaim: %d", second.GetLease().GetFencingToken())
	}

	_, err = h.worker.CompleteNode(ctx, &pb.CompleteNodeRequest{TaskToken: staleToken})
	if err == nil {
		t.Fatal("CompleteNode with a superseded task_token succeeded, want ABORTED")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Aborted {
		t.Fatalf("CompleteNode with stale token: code = %v, want Aborted", err)
	}
}

// TestClaimNodeLongPollTimeoutReturnsEmpty is the adapter contract's "having
// no work is ordinary" rule: a poll on an empty scope must come back as a
// successful, empty response once poll_timeout elapses — never an error.
func TestClaimNodeLongPollTimeoutReturnsEmpty(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	resp, err := h.worker.ClaimNode(ctx, &pb.ClaimNodeRequest{
		Scope:       "scope-empty",
		PollTimeout: durationpb.New(300 * time.Millisecond),
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ClaimNode on an empty scope returned an error, want empty response: %v", err)
	}
	if resp.GetLease() != nil {
		t.Fatalf("ClaimNode on an empty scope returned a lease: %+v", resp.GetLease())
	}
	if elapsed < 250*time.Millisecond {
		t.Fatalf("ClaimNode returned after %s, before its poll_timeout even elapsed", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("ClaimNode returned after %s, far past its 300ms poll_timeout", elapsed)
	}
}

// TestWatchDeliversEvents opens a watch before the node exists, then confirms
// both a creation and a completion transition arrive on it in order.
func TestWatchDeliversEvents(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := h.control.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if err := stream.Send(&pb.WatchRequest{Request: &pb.WatchRequest_Create{
		Create: &pb.WatchCreateRequest{WatchId: 1, Scope: "scope-w"},
	}}); err != nil {
		t.Fatalf("Send(create): %v", err)
	}

	created, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv(created ack): %v", err)
	}
	if !created.GetCreated() || created.GetWatchId() != 1 {
		t.Fatalf("first response = %+v, want created=true watch_id=1", created)
	}

	addOneNode(t, h, "scope-w", "n1")

	ev := recvEventOfKind(t, stream, pb.WatchEventKind_WATCH_EVENT_KIND_CREATED)
	if ev.GetNode().GetId() != "n1" {
		t.Fatalf("created event node id = %q, want n1", ev.GetNode().GetId())
	}

	claimResp, err := h.worker.ClaimNode(ctx, &pb.ClaimNodeRequest{
		Scope:       "scope-w",
		PollTimeout: durationpb.New(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("ClaimNode: %v", err)
	}
	if _, err := h.worker.CompleteNode(ctx, &pb.CompleteNodeRequest{
		TaskToken: claimResp.GetLease().GetTaskToken(),
	}); err != nil {
		t.Fatalf("CompleteNode: %v", err)
	}

	transition := recvEventOfKind(t, stream, pb.WatchEventKind_WATCH_EVENT_KIND_TRANSITION)
	if transition.GetNode().GetStatus() != pb.NodeStatus_NODE_STATUS_SUCCESS {
		t.Fatalf("transition status = %v, want SUCCESS", transition.GetNode().GetStatus())
	}
}

// recvEventOfKind skips WORK_AVAILABLE doorbell notifications, which are
// best-effort and may or may not appear alongside a real transition, and
// returns the first event of the requested kind.
func recvEventOfKind(t *testing.T, stream pb.ControlService_WatchClient, kind pb.WatchEventKind) *pb.WatchEvent {
	t.Helper()
	for i := 0; i < 10; i++ {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		for _, ev := range resp.GetEvents() {
			if ev.GetKind() == kind {
				return ev
			}
		}
	}
	t.Fatalf("did not observe a %v event within 10 responses", kind)
	return nil
}

// TestGracefulShutdownDrainsPromptly pins the one behavior the whole "prompt
// draining" section of the adapter contract exists for: a long poll parked
// on an empty scope must not make Shutdown wait out its full poll_timeout.
func TestGracefulShutdownDrainsPromptly(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	claimCtx, cancelClaim := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelClaim()

	claimDone := make(chan struct {
		resp *pb.ClaimNodeResponse
		err  error
	}, 1)
	go func() {
		resp, err := h.worker.ClaimNode(claimCtx, &pb.ClaimNodeRequest{
			Scope:       "scope-drain",
			PollTimeout: durationpb.New(30 * time.Second), // far longer than this test should ever take
		})
		claimDone <- struct {
			resp *pb.ClaimNodeResponse
			err  error
		}{resp, err}
	}()

	// Give the ClaimNode call a moment to actually reach the server and start
	// blocking before shutting down, so this test exercises the drain path
	// rather than a lucky race where Shutdown wins before ClaimNode arrives.
	time.Sleep(100 * time.Millisecond)

	shutdownStart := time.Now()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := h.server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	shutdownElapsed := time.Since(shutdownStart)

	if shutdownElapsed > 3*time.Second {
		t.Fatalf("Shutdown took %s, want well under its 30s poll_timeout", shutdownElapsed)
	}

	select {
	case res := <-claimDone:
		if res.err != nil && !errors.Is(res.err, io.EOF) {
			// A drained long poll returns an empty, successful response
			// (see docs/spec/02-adapter-contract.md); accept either that or
			// a transport-level signal that the connection was going away,
			// since bufconn's client-side behavior on GracefulStop can
			// surface as either depending on timing.
			st, ok := status.FromError(res.err)
			if !ok || (st.Code() != codes.Unavailable && st.Code() != codes.Canceled) {
				t.Fatalf("ClaimNode during shutdown returned %v, want empty response or Unavailable/Canceled", res.err)
			}
			return
		}
		if res.resp.GetLease() != nil {
			t.Fatalf("ClaimNode during shutdown returned a lease: %+v", res.resp.GetLease())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ClaimNode did not return within 3s of Shutdown returning")
	}
}
