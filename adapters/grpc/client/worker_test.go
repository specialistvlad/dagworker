package client_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	grpcadapter "github.com/specialistvlad/dagworker/adapters/grpc"
	"github.com/specialistvlad/dagworker/adapters/grpc/client"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	"github.com/specialistvlad/dagworker/storage/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

// newTestConn starts a real grpcadapter.Server over an in-memory backend,
// reachable only through an in-process bufconn listener, and returns a
// connection to it plus a teardown func. It is deliberately independent of
// ../server_test.go's harness: this package tests the client SDK's own
// behavior (the heartbeat loop, context discipline), not the server, so it
// needs nothing from that package's internal test helpers.
func newTestConn(t *testing.T) *grpc.ClientConn {
	t.Helper()

	store := memory.New()
	mgr, err := dw.New(store, dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("dagworker.New: %v", err)
	}
	srv, err := grpcadapter.New(mgr)
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
		_ = srv.Shutdown(shutdownCtx)
		cancelServe()
		<-serveErr
		_ = mgr.Close(context.Background())
		_ = store.Close(context.Background())
	})

	return conn
}

// TestWorkerRunClaimsAndCompletes exercises the whole reference-SDK loop
// end to end: a Worker.Run call claims a node created through the same
// connection's ControlService, hands it to a Handler, and reports the
// Handler's Outcome back — the "a host writes only the work function"
// promise this package exists for.
func TestWorkerRunClaimsAndCompletes(t *testing.T) {
	t.Parallel()
	conn := newTestConn(t)
	ctx := context.Background()

	control := pb.NewControlServiceClient(conn)
	if _, err := control.AddNodes(ctx, &pb.AddNodesRequest{
		Scope: "scope-c",
		Nodes: []*pb.NewNode{{Id: "n1"}},
	}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}

	w := client.NewWorker(conn, "scope-c",
		client.WithPollTimeout(2*time.Second),
		client.WithLeaseDuration(5*time.Second),
	)

	var handled atomic.Bool
	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		runErr <- w.Run(runCtx, func(_ context.Context, node *pb.Node) client.Outcome {
			handled.Store(true)
			if node.GetId() != "n1" {
				t.Errorf("handler got node id %q, want n1", node.GetId())
			}
			return client.Complete([]byte("done"))
		})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for !handled.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !handled.Load() {
		t.Fatal("Worker.Run never invoked the handler")
	}

	got, err := control.GetNode(ctx, &pb.GetNodeRequest{Scope: "scope-c", NodeId: "n1"})
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.GetNode().GetStatus() != pb.NodeStatus_NODE_STATUS_SUCCESS {
		t.Fatalf("status = %v, want SUCCESS", got.GetNode().GetStatus())
	}

	cancel()
	select {
	case err := <-runErr:
		if err == nil {
			t.Fatal("Run returned nil after ctx was canceled, want a context error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within 3s of its context being canceled")
	}
}

// TestWorkerRunFailReportsFailure confirms the Fail outcome reaches the
// server as a FailNode call rather than a CompleteNode.
func TestWorkerRunFailReportsFailure(t *testing.T) {
	t.Parallel()
	conn := newTestConn(t)
	ctx := context.Background()

	control := pb.NewControlServiceClient(conn)
	if _, err := control.AddNodes(ctx, &pb.AddNodesRequest{
		Scope: "scope-f",
		Nodes: []*pb.NewNode{{Id: "n1", Retry: &pb.RetryPolicy{MaxAttempts: 1}}},
	}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}

	w := client.NewWorker(conn, "scope-f", client.WithPollTimeout(2*time.Second))

	runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- w.Run(runCtx, func(context.Context, *pb.Node) client.Outcome {
			return client.Fail("boom")
		})
	}()

	deadline := time.Now().Add(4 * time.Second)
	var status pb.NodeStatus
	for time.Now().Before(deadline) {
		got, err := control.GetNode(ctx, &pb.GetNodeRequest{Scope: "scope-f", NodeId: "n1"})
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		status = got.GetNode().GetStatus()
		if status == pb.NodeStatus_NODE_STATUS_ERROR {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status != pb.NodeStatus_NODE_STATUS_ERROR {
		t.Fatalf("status = %v, want ERROR after a one-attempt Fail", status)
	}
	cancel()
	<-done
}
