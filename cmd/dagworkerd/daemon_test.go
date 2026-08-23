package main

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	httpclient "github.com/specialistvlad/dagworker/adapters/http/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

// testLogger discards everything: these tests assert on behavior, not log
// output, and a library or daemon that writes to a test's stderr uninvited
// makes failures harder to read, not easier.
func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// startDaemon builds and runs a daemon on ephemeral loopback ports for the
// duration of the test, returning it once every listener is bound. The
// returned cancel function triggers the ordered shutdown; startDaemon itself
// registers a t.Cleanup that calls it and waits for Run to return, so a test
// that forgets to shut down explicitly still leaves no goroutine behind.
func startDaemon(t *testing.T, cfg Config) *daemon {
	t.Helper()

	cfg.AdminAddr = "127.0.0.1:0"
	if cfg.GRPCAddr != "" {
		cfg.GRPCAddr = "127.0.0.1:0"
	}
	if cfg.HTTPAddr != "" {
		cfg.HTTPAddr = "127.0.0.1:0"
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}

	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelBuild()
	d, err := newDaemon(buildCtx, cfg, testLogger())
	if err != nil {
		t.Fatalf("newDaemon: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()

	t.Cleanup(func() {
		cancelRun()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("daemon.Run returned an error during cleanup: %v", err)
			}
		case <-time.After(cfg.ShutdownTimeout + 5*time.Second):
			t.Errorf("daemon.Run did not return within its shutdown timeout during cleanup")
		}
	})

	waitForAdmin(t, d)
	return d
}

// waitForAdmin blocks until the admin listener actually answers, so a test
// issuing its first real request never races the Serve goroutine's startup.
func waitForAdmin(t *testing.T, d *daemon) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	url := "http://" + d.AdminAddr().String() + "/healthz"
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx,gosec // a short bounded retry loop against a test's own ephemeral loopback listener
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("admin listener at %s never became healthy", url)
}

func TestDaemon_FullClaimAckCycle_HTTP(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.HTTPAddr = "enabled" // rewritten to an ephemeral loopback addr by startDaemon
	if err := cfg.validate(); err != nil {
		t.Fatalf("test config failed validation: %v", err)
	}
	d := startDaemon(t, cfg)

	baseURL := "http://" + d.HTTPAddr().String() + "/v1"
	c := httpclient.New(baseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	scope := "http-e2e"
	if _, err := c.CreateNode(ctx, scope, "n1", httpclient.CreateNodeOptions{Payload: []byte("work")}); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	leases, err := c.Claim(ctx, scope, httpclient.ClaimOptions{MaxNodes: 1, Wait: 2 * time.Second})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("Claim returned %d leases, want 1", len(leases))
	}

	result, err := c.Complete(ctx, scope, leases[0].ID, []byte("done"))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if result.Status != "success" {
		t.Errorf("Complete result status = %q, want success", result.Status)
	}

	node, err := c.GetNode(ctx, scope, "n1")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if node.Status != "success" {
		t.Errorf("node status after ack = %q, want success", node.Status)
	}
}

func TestDaemon_FullClaimAckCycle_GRPC(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.GRPCAddr = "enabled"
	if err := cfg.validate(); err != nil {
		t.Fatalf("test config failed validation: %v", err)
	}
	d := startDaemon(t, cfg)

	conn, err := grpc.NewClient(d.GRPCAddr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()

	control := pb.NewControlServiceClient(conn)
	worker := pb.NewWorkerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const scope = "grpc-e2e"
	if _, err := control.AddNodes(ctx, &pb.AddNodesRequest{
		Scope: scope,
		Nodes: []*pb.NewNode{{Id: "n1", Payload: []byte("work")}},
	}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}

	claimResp, err := worker.ClaimNode(ctx, &pb.ClaimNodeRequest{
		Scope:       scope,
		PollTimeout: durationSeconds(2),
	})
	if err != nil {
		t.Fatalf("ClaimNode: %v", err)
	}
	if claimResp.GetLease() == nil {
		t.Fatalf("ClaimNode returned no lease")
	}

	if _, err := worker.CompleteNode(ctx, &pb.CompleteNodeRequest{
		TaskToken: claimResp.GetLease().GetTaskToken(),
		Result:    []byte("done"),
	}); err != nil {
		t.Fatalf("CompleteNode: %v", err)
	}

	got, err := control.GetNode(ctx, &pb.GetNodeRequest{Scope: scope, NodeId: "n1"})
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.GetNode().GetStatus() != pb.NodeStatus_NODE_STATUS_SUCCESS {
		t.Errorf("node status after ack = %v, want NODE_STATUS_SUCCESS", got.GetNode().GetStatus())
	}
}

// TestDaemon_GracefulShutdownDrainsPromptlyWithRequestInFlight is the
// requirement docs/research/15 Part 2 §2.4 exists to satisfy: a long-poll
// claim parked server-side must not keep shutdown waiting anywhere near its
// own poll window once the shutdown signal fires — the adapter's own prompt
// drain (docs/spec/02-adapter-contract.md "draining is prompt") should
// unblock it almost immediately, and Run must return well inside
// cfg.ShutdownTimeout.
func TestDaemon_GracefulShutdownDrainsPromptlyWithRequestInFlight(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.HTTPAddr = "enabled"
	cfg.ShutdownTimeout = 3 * time.Second
	if err := cfg.validate(); err != nil {
		t.Fatalf("test config failed validation: %v", err)
	}

	cfg.AdminAddr = "127.0.0.1:0"
	cfg.HTTPAddr = "127.0.0.1:0"

	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelBuild()
	d, err := newDaemon(buildCtx, cfg, testLogger())
	if err != nil {
		t.Fatalf("newDaemon: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- d.Run(runCtx) }()
	waitForAdmin(t, d)

	baseURL := "http://" + d.HTTPAddr().String() + "/v1"
	c := httpclient.New(baseURL)

	// A claim with a long wait and nothing to claim: this parks the request
	// server-side for up to 10s unless shutdown interrupts it.
	claimDone := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = c.Claim(ctx, "never-claimed", httpclient.ClaimOptions{MaxNodes: 1, Wait: 10 * time.Second})
		claimDone <- time.Since(start)
	}()

	// Give the claim time to actually be parked server-side before triggering
	// shutdown.
	time.Sleep(200 * time.Millisecond)

	shutdownStart := time.Now()
	cancelRun()

	var runErr error
	select {
	case runErr = <-runDone:
	case <-time.After(cfg.ShutdownTimeout + 5*time.Second):
		t.Fatalf("daemon.Run did not return within its shutdown timeout")
	}
	shutdownElapsed := time.Since(shutdownStart)
	if runErr != nil {
		t.Errorf("daemon.Run returned an error: %v", runErr)
	}
	if shutdownElapsed > cfg.ShutdownTimeout {
		t.Errorf("shutdown took %s, wanted at most cfg.ShutdownTimeout (%s)", shutdownElapsed, cfg.ShutdownTimeout)
	}

	select {
	case claimElapsed := <-claimDone:
		if claimElapsed > cfg.ShutdownTimeout {
			t.Errorf("in-flight claim took %s to return, wanted well under the 10s it asked to wait "+
				"(the adapter's own prompt-drain rule should have interrupted it almost immediately)", claimElapsed)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("in-flight claim goroutine never returned after daemon.Run completed")
	}
}

func durationSeconds(s int64) *durationpb.Duration {
	return durationpb.New(time.Duration(s) * time.Second)
}
