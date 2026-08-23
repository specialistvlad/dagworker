// Package grpcadapter is dagworker's optional gRPC network surface: it lets a
// worker written in any language that has a gRPC/protobuf toolchain
// participate in a DAG whose Manager lives in a different process.
//
// The core dagworker module has zero import edge to this package or to
// google.golang.org/grpc (ADR-0037, enforced by go.mod and by this module's
// own .golangci.yml); this package imports the core, never the reverse. The
// package is named grpcadapter, not grpc, so it never shadows
// google.golang.org/grpc in a file that imports both.
//
// See docs/spec/02-adapter-contract.md for the shape every adapter exports
// and the error-mapping table both adapters share, and
// docs/research/13-grpc-worker-protocol.md for the protocol design this
// package implements.
package grpcadapter

import (
	"context"
	"fmt"
	"net"
	"sync"

	dw "github.com/specialistvlad/dagworker"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Server serves both WorkerService and ControlService over one listener.
// The zero value is not usable; construct one with [New].
type Server struct {
	grpcServer *grpc.Server

	// shutdown is closed exactly once, by Shutdown, and never by anything
	// else. Every long-poll ClaimNode and every open Watch selects on it
	// alongside its own wait condition, which is what lets GracefulStop drain
	// promptly instead of waiting out a poll timeout that can be minutes away
	// (docs/spec/02-adapter-contract.md's "draining is prompt" rule).
	shutdown     chan struct{}
	shutdownOnce sync.Once
}

// New builds a server over m. It does not take ownership of m — closing the
// Server never closes the Manager — and it does not start anything: no
// goroutine runs and no port is bound until [Server.Serve] is called, exactly
// as the adapter contract requires.
func New(m *dw.Manager, opts ...Option) (*Server, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: manager must not be nil", dw.ErrInvalidArgument)
	}
	cfg := defaultServerConfig()
	for _, o := range opts {
		if o == nil {
			return nil, fmt.Errorf("%w: nil option", dw.ErrInvalidArgument)
		}
		o.apply(&cfg)
	}

	s := &Server{shutdown: make(chan struct{})}

	serverOpts := make([]grpc.ServerOption, 0, 5+len(cfg.extraServerOpts))
	serverOpts = append(serverOpts,
		grpc.MaxConcurrentStreams(cfg.maxConcurrentStreams),
		// PermitWithoutStream is not optional here: a worker parked in a
		// ClaimNode long poll or an idle Watch has no other traffic on the
		// connection to piggyback a liveness ping on. Without it a half-dead
		// connection is invisible until the next real call, which for a long
		// poll can be minutes away. See docs/research/13-grpc-worker-protocol.md §8.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     0,
			MaxConnectionAge:      cfg.maxConnectionAge,
			MaxConnectionAgeGrace: cfg.maxConnectionAgeGrace,
			Time:                  defaultKeepaliveTime,
			Timeout:               defaultKeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             defaultKeepaliveMinTime,
			PermitWithoutStream: true,
		}),
		grpc.ChainUnaryInterceptor(errorUnaryInterceptor(cfg.logger)),
		grpc.ChainStreamInterceptor(errorStreamInterceptor(cfg.logger)),
	)
	// Authorization goes inside the error interceptor, so a panicking
	// Authorizer costs one RPC rather than the process, and outside every
	// handler, so no method can be reached without passing it — including
	// methods added to the proto after this line was written.
	if cfg.authorizer != nil {
		serverOpts = append(serverOpts,
			grpc.ChainUnaryInterceptor(authUnaryInterceptor(cfg.authorizer)),
			grpc.ChainStreamInterceptor(authStreamInterceptor(cfg.authorizer)),
		)
	}
	serverOpts = append(serverOpts, cfg.extraServerOpts...)

	gs := grpc.NewServer(serverOpts...)
	pb.RegisterWorkerServiceServer(gs, &workerServer{mgr: m, cfg: cfg, shutdown: s.shutdown})
	pb.RegisterControlServiceServer(gs, &controlServer{mgr: m, cfg: cfg, shutdown: s.shutdown})
	s.grpcServer = gs
	return s, nil
}

// Serve blocks, accepting connections on lis and serving both services until
// lis fails or Shutdown is called from another goroutine, or ctx ends — the
// two are equivalent triggers for the same graceful drain. It returns nil on
// a clean shutdown, matching the adapter contract's rule that "I was asked to
// stop" is not an error the caller must special-case.
func (s *Server) Serve(ctx context.Context, lis net.Listener) error {
	if lis == nil {
		return fmt.Errorf("%w: listener must not be nil", dw.ErrInvalidArgument)
	}

	done := make(chan error, 1)
	go func() { done <- s.grpcServer.Serve(lis) }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// ctx ending is just another shutdown trigger. There is no further
		// deadline to respect beyond "let what's in flight finish", so this
		// hands GracefulStop a background context rather than one that could
		// itself already be expired.
		//nolint:contextcheck // ctx is already Done; a fresh background context is the correct replacement, not an oversight
		_ = s.Shutdown(context.Background())
		return <-done
	}
}

// Shutdown stops accepting new work, lets in-flight requests finish, and
// returns when they have or when ctx expires — whichever comes first. On a
// timeout it falls back to Stop, which hard-cancels whatever did not drain in
// time, so a caller's own deadline is always honored even if a handler is
// stuck.
//
// It never touches the Manager or the leases it has granted: those live in
// storage and outlive this process, so a rolling restart does not revoke a
// single in-flight lease across the fleet (docs/spec/02-adapter-contract.md's
// "in-flight leases survive a restart" rule).
func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() { close(s.shutdown) })

	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.grpcServer.Stop()
		return ctx.Err()
	}
}
