// Package httpadapter — see doc.go for the package-level overview.
package httpadapter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*
	"sync"

	dagworker "github.com/specialistvlad/dagworker"
)

// Server serves dagworker over HTTP/JSON on one listener. The zero value is
// not usable; construct one with [New].
//
// A Server does not own the [dagworker.Manager] it was built over: closing
// the Server never closes the Manager, matching the Manager's own promise
// that it does not own its Store (manager.go).
type Server struct {
	mgr  *dagworker.Manager
	opts options
	mux  *http.ServeMux
	http *http.Server

	// done is closed exactly once, by Shutdown. Every blocking or streaming
	// handler — the claim long-poll, the SSE loop — selects on it in addition
	// to its own deadline and the request's own context, because
	// [http.Server.Shutdown] alone does not cancel in-flight request contexts;
	// it only waits for handlers to return on their own. Without this channel,
	// an open SSE connection or a long-poll claim would keep Shutdown blocked
	// until the client disconnected or the wait elapsed, not until the
	// caller's shutdown deadline (the exact hazard dossier 14 §11 documents).
	done      chan struct{}
	closeOnce sync.Once
}

// New builds a Server over m. It starts nothing — no listener, no goroutine —
// so building one has no observable effect until [Server.Serve] is called,
// per the adapter contract §1.
func New(m *dagworker.Manager, opts ...Option) (*Server, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: manager must not be nil", dagworker.ErrInvalidArgument)
	}
	o := defaultOptions()
	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("%w: nil option", dagworker.ErrInvalidArgument)
		}
		opt.apply(&o)
	}

	s := &Server{
		mgr:  m,
		opts: o,
		done: make(chan struct{}),
	}
	s.mux = http.NewServeMux()
	s.routes()

	// Authorization sits inside recovery and logging — so a rejection is
	// logged and a panicking Authorizer cannot take the process down — and
	// outside everything else, so no handler can be reached without passing
	// it, including handlers added after this line was written.
	handler := chain(s.mux,
		recoveryMiddleware(o.logger),
		loggingMiddleware(o.logger),
		s.authMiddleware(o.authorizer),
		bodyLimitMiddleware(o.maxBodyBytes),
	)

	s.http = &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: o.readHeaderTimeout,
		WriteTimeout:      o.writeTimeout,
		IdleTimeout:       o.idleTimeout,
		ErrorLog:          nil, // net/http's default logger writes to stderr uninvited; every path here logs through o.logger instead.
	}
	return s, nil
}

// Serve blocks, accepting connections on lis until it fails, [Server.Shutdown]
// is called, or ctx is done — whichever happens first. It returns nil on a
// clean shutdown, never [http.ErrServerClosed]: "I was asked to stop" is not
// an error a caller composing this adapter into a larger process should have
// to special-case (adapter contract §1).
//
// ctx becomes the base context for every request's [http.Request.Context]
// (via [http.Server.BaseContext]), so canceling it is also a valid way to
// stop the server — equivalent to calling Shutdown with an already-expired
// grace period. A caller that wants requests to finish first should call
// Shutdown with its own bounded context instead of canceling ctx directly.
func (s *Server) Serve(ctx context.Context, lis net.Listener) error {
	if lis == nil {
		return fmt.Errorf("%w: listener must not be nil", dagworker.ErrInvalidArgument)
	}

	s.http.BaseContext = func(net.Listener) context.Context { return ctx }

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			// Canceling ctx is documented as equivalent to an already-expired
			// shutdown grace period (Serve's doc comment): derived from ctx via
			// WithoutCancel so it still carries whatever values/trace info ctx
			// held, but with an explicit zero timeout rather than ctx's own
			// (already-fired) Done channel, so this reads as "shut down now"
			// rather than as reusing a context that is already dead.
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 0)
			_ = s.Shutdown(shutdownCtx)
			cancel()
		case <-s.done:
		case <-stop:
		}
	}()

	err := s.http.Serve(lis)
	if err != nil && errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown stops accepting new connections, signals every blocking handler to
// unwind (see the [Server.done] field doc), and waits for in-flight requests
// to finish or ctx to expire — whichever comes first. It is idempotent: a
// second call observes the same done-channel close and just waits on
// [http.Server.Shutdown] again, which itself already tolerates repeat calls.
//
// It does not touch any lease a worker currently holds. Those live in
// storage and survive this process exiting entirely — canceling them here
// would revoke every in-flight lease on a routine rolling restart, which the
// adapter contract §2 forbids outright.
func (s *Server) Shutdown(ctx context.Context) error {
	s.closeOnce.Do(func() { close(s.done) })
	return s.http.Shutdown(ctx)
}
