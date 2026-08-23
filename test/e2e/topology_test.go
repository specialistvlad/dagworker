package e2e_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dw "github.com/specialistvlad/dagworker"
	grpcadapter "github.com/specialistvlad/dagworker/adapters/grpc"
	grpcclient "github.com/specialistvlad/dagworker/adapters/grpc/client"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	httpadapter "github.com/specialistvlad/dagworker/adapters/http"
	httpclient "github.com/specialistvlad/dagworker/adapters/http/client"
	"github.com/specialistvlad/dagworker/test/e2e"
)

// Worker topologies. The library's whole proposition is that the workers are
// yours and live wherever you like, so every arrangement a host might actually
// deploy is exercised here: one worker, a pool, several pools routed by kind,
// workers in another process reached over gRPC or HTTP, and — the one that
// matters most — several of those at once on the same scope.
//
// Every topology asserts the same invariant: each node runs exactly once.

const topologyNodes = 60

func seedFlat(ctx context.Context, t *testing.T, m *dw.Manager, scope dw.Scope, n int, kind string) {
	t.Helper()
	specs := make([]dw.NodeSpec, n)
	for i := range specs {
		specs[i] = dw.NodeSpec{ID: dw.NodeID(fmt.Sprintf("n%04d", i)), Kind: kind}
	}
	if err := m.AddNodes(ctx, scope, specs); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if err := m.Seal(ctx, scope); err != nil {
		t.Fatalf("Seal: %v", err)
	}
}

// tally records which node each execution touched, so "exactly once" is a
// checked fact rather than a hope.
type tally struct {
	mu  sync.Mutex
	ran map[string]int
}

func newTally() *tally { return &tally{ran: map[string]int{}} }

func (x *tally) record(id string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.ran[id]++
}

func (x *tally) assertExactlyOnce(t *testing.T, want int) {
	t.Helper()
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.ran) != want {
		t.Fatalf("%d of %d nodes ran", len(x.ran), want)
	}
	for id, n := range x.ran {
		if n != 1 {
			t.Fatalf("node %q ran %d times — exactly once is the whole guarantee", id, n)
		}
	}
}

func TestE2E_Topology_InProcess(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		pools func(m *dw.Manager, scope dw.Scope, h e2e.Handler) []*e2e.Pool
	}{
		{"one-worker", func(m *dw.Manager, scope dw.Scope, h e2e.Handler) []*e2e.Pool {
			return []*e2e.Pool{{Manager: m, Scope: scope, Workers: 1, Handle: h}}
		}},
		{"one-pool-many-workers", func(m *dw.Manager, scope dw.Scope, h e2e.Handler) []*e2e.Pool {
			return []*e2e.Pool{{Manager: m, Scope: scope, Workers: 8, Handle: h}}
		}},
		{"many-pools", func(m *dw.Manager, scope dw.Scope, h e2e.Handler) []*e2e.Pool {
			return []*e2e.Pool{
				{Manager: m, Scope: scope, Workers: 3, Handle: h},
				{Manager: m, Scope: scope, Workers: 3, Handle: h},
				{Manager: m, Scope: scope, Workers: 2, Handle: h},
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eachBackend(t, func(t *testing.T, b e2e.Backend) {
				t.Helper()
				m := newManager(t, b)
				ctx := t.Context()
				scope := e2e.UniqueScope(t)
				seedFlat(ctx, t, m, scope, topologyNodes, "work")

				x := newTally()
				runPools(ctx, t, tc.pools(m, scope, func(_ context.Context, node dw.Node) ([]byte, error) {
					x.record(string(node.ID))
					return nil, nil
				}), 60*time.Second)

				x.assertExactlyOnce(t, topologyNodes)
				if done, _ := m.IsComplete(ctx, scope); !done {
					st, _ := m.Stats(ctx, scope)
					t.Fatalf("scope not complete: %+v", st)
				}
			})
		})
	}
}

// Workers in another process, reached over gRPC. The server here is in this
// test binary only so the test is self-contained; from the worker's point of
// view it is a socket and nothing else.
func TestE2E_Topology_ExternalGRPCWorkers(t *testing.T) {
	t.Parallel()
	m, ctx, scope := fixture(t, memoryBackend(t))
	seedFlat(ctx, t, m, scope, topologyNodes, "work")

	addr := serveGRPC(t, m)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	x := newTally()
	runExternal(ctx, t, 4, func(wctx context.Context, id int) {
		w := grpcclient.NewWorker(conn, string(scope),
			grpcclient.WithWorkerID(fmt.Sprintf("remote-%d", id)))
		_ = w.Run(wctx, func(_ context.Context, node *pb.Node) grpcclient.Outcome {
			x.record(node.GetId())
			return grpcclient.Complete(nil)
		})
	}, m, scope)

	x.assertExactlyOnce(t, topologyNodes)
}

// The same, over HTTP.
func TestE2E_Topology_ExternalHTTPWorkers(t *testing.T) {
	t.Parallel()
	m, ctx, scope := fixture(t, memoryBackend(t))
	seedFlat(ctx, t, m, scope, topologyNodes, "work")

	base := serveHTTP(t, m)
	x := newTally()
	runExternal(ctx, t, 4, func(wctx context.Context, id int) {
		if err := httpWorker(wctx, base, string(scope), fmt.Sprintf("http-%d", id), x); err != nil {
			t.Errorf("http worker: %v", err)
		}
	}, m, scope)

	x.assertExactlyOnce(t, topologyNodes)
}

// The arrangement that actually proves the design: in-process workers, gRPC
// workers and HTTP workers all draining the same scope at once, with nothing
// coordinating them but the store's atomic claim.
func TestE2E_Topology_MixedInternalAndExternal(t *testing.T) {
	t.Parallel()
	m, ctx, scope := fixture(t, memoryBackend(t))
	seedFlat(ctx, t, m, scope, topologyNodes, "work")

	grpcAddr := serveGRPC(t, m)
	httpBase := serveHTTP(t, m)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	x := newTally()
	wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// In-process pool.
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := &e2e.Pool{Manager: m, Scope: scope, Workers: 2, Handle: func(_ context.Context, n dw.Node) ([]byte, error) {
			x.record(string(n.ID))
			return nil, nil
		}}
		_ = p.Run(wctx)
	}()

	// Two gRPC workers.
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := grpcclient.NewWorker(conn, string(scope), grpcclient.WithWorkerID(fmt.Sprintf("g-%d", i)))
			_ = w.Run(wctx, func(_ context.Context, node *pb.Node) grpcclient.Outcome {
				x.record(node.GetId())
				return grpcclient.Complete(nil)
			})
		}()
	}

	// Two HTTP workers.
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := httpWorker(wctx, httpBase, string(scope), fmt.Sprintf("h-%d", i), x); err != nil {
				t.Errorf("http worker: %v", err)
			}
		}()
	}

	waitComplete(ctx, t, m, scope, 60*time.Second)
	cancel()
	wg.Wait()

	x.assertExactlyOnce(t, topologyNodes)
}

// ---------------------------------------------------------------- helpers

// httpWorker is one worker living outside this process, as far as the library
// is concerned: it holds a socket and nothing else.
func httpWorker(ctx context.Context, base, scope, id string, x *tally) error {
	c := httpclient.New(base)
	for ctx.Err() == nil {
		leases, err := c.Claim(ctx, scope, httpclient.ClaimOptions{
			WorkerID: id, MaxNodes: 2, Wait: 200 * time.Millisecond,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("claim: %w", err)
		}
		for _, l := range leases {
			x.record(l.Node.ID)
			// A completion drops the poll loop's cancellation but keeps its
			// values: the lease outlives the call that granted it, and
			// stopping the loop must not abandon an acknowledgement mid-flight.
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			_, err := c.Complete(cctx, scope, l.ID, nil)
			cancel()
			if err != nil {
				return fmt.Errorf("complete %s: %w", l.Node.ID, err)
			}
		}
	}
	return nil
}

func memoryBackend(t *testing.T) e2e.Backend {
	t.Helper()
	for _, b := range e2e.Backends() {
		if b.Name == "memory" {
			return b
		}
	}
	t.Fatal("no in-memory backend")
	return e2e.Backend{}
}

func serveGRPC(t *testing.T, m *dw.Manager) string {
	t.Helper()
	srv, err := grpcadapter.New(m)
	if err != nil {
		t.Fatalf("grpc.New: %v", err)
	}
	// The server outlives every request context it serves and is stopped by
	// t.Cleanup, which runs after t.Context() is already cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = srv.Serve(ctx, lis) }()
	t.Cleanup(func() {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer scancel()
		_ = srv.Shutdown(sctx)
		cancel()
		<-done
	})
	return lis.Addr().String()
}

func serveHTTP(t *testing.T, m *dw.Manager) string {
	t.Helper()
	srv, err := httpadapter.New(m)
	if err != nil {
		t.Fatalf("http.New: %v", err)
	}
	// The server outlives every request context it serves and is stopped by
	// t.Cleanup, which runs after t.Context() is already cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = srv.Serve(ctx, lis) }()
	t.Cleanup(func() {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer scancel()
		_ = srv.Shutdown(sctx)
		cancel()
		<-done
	})
	// The client's base URL carries the API version prefix; the server mounts
	// its routes under it.
	return "http://" + lis.Addr().String() + "/v1"
}

// runExternal starts n network workers, waits for the scope to finish, then
// stops them.
func runExternal(ctx context.Context, t *testing.T, n int, run func(context.Context, int), m *dw.Manager, scope dw.Scope) {
	t.Helper()
	wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() { defer wg.Done(); run(wctx, i) }()
	}
	waitComplete(ctx, t, m, scope, 60*time.Second)
	cancel()

	stopped := make(chan struct{})
	go func() { wg.Wait(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(15 * time.Second):
		t.Error("external workers did not stop after the scope completed")
	}
}

func waitComplete(ctx context.Context, t *testing.T, m *dw.Manager, scope dw.Scope, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		done, err := m.IsComplete(ctx, scope)
		if err == nil && done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	st, _ := m.Stats(ctx, scope)
	t.Fatalf("scope did not complete within %s: %+v", within, st)
}

var _ atomic.Int64
