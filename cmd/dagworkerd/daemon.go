package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net" //nolint:depguard // dagworkerd is the composition root; see admin.go's identical justification
	"net/http"
	"sync"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
	grpcadapter "github.com/specialistvlad/dagworker/adapters/grpc"
	httpadapter "github.com/specialistvlad/dagworker/adapters/http"
)

// daemon owns every listener and background resource dagworkerd starts. It
// is the one place that knows the shutdown order
// docs/research/15-daemon-packaging-and-ops.md Part 2 §2.4 specifies: fail
// readiness, stop accepting new claims, let what is already in flight drain,
// then close the store — and the one place that enforces the deviation this
// project's own hard rule makes from that dossier's step 4: never release a
// lease a worker might still be about to acknowledge. A lease lives in
// storage and outlives this process by design (docs/spec/02-adapter-contract.md
// "in-flight leases survive a restart"); a rolling restart that revoked every
// lease it was holding would turn a routine deploy into a fleet-wide retry
// storm.
type daemon struct {
	cfg    Config
	logger *slog.Logger

	handle storeHandle
	mgr    *dagworker.Manager

	grpcSrv *grpcadapter.Server
	httpSrv *httpadapter.Server

	grpcLis  net.Listener
	httpLis  net.Listener
	adminLis net.Listener
	adminSrv *http.Server

	ready     readiness
	startedAt time.Time
}

// newDaemon builds the store, the Manager, every adapter cfg enables, and
// every listener — but starts nothing: no goroutine runs and no connection is
// accepted until [daemon.Run], mirroring the adapter contract's own "New
// starts nothing" rule (docs/spec/02-adapter-contract.md §1) one level up.
func newDaemon(ctx context.Context, cfg Config, logger *slog.Logger) (*daemon, error) {
	handle, err := openStore(ctx, cfg)
	if err != nil {
		return nil, err
	}

	mgr, err := dagworker.New(handle.store, dagworker.WithLogger(logger)) //nolint:contextcheck // dagworker.New takes no context parameter; there is nothing to propagate
	if err != nil {
		_ = handle.Close(ctx)
		return nil, fmt.Errorf("dagworkerd: constructing manager: %w", err)
	}

	d := &daemon{cfg: cfg, logger: logger, handle: handle, mgr: mgr, startedAt: time.Now()}
	if err := d.listen(ctx, cfg); err != nil {
		_ = mgr.Close(ctx)
		_ = handle.Close(ctx)
		return nil, err
	}
	return d, nil
}

// listen binds every configured listener and constructs the corresponding
// adapter over it, split out of newDaemon to keep each function under this
// module's complexity ceiling. Binding itself uses ctx only to bound the
// syscall (net.ListenConfig.Listen's own contract); it has no bearing on how
// long the resulting listener lives, which is governed entirely by
// [daemon.Run] and [daemon.shutdown] instead.
func (d *daemon) listen(ctx context.Context, cfg Config) error {
	var lc net.ListenConfig

	tokens, err := loadAuthTokens(cfg)
	if err != nil {
		return err
	}
	if err := checkAuthPosture(cfg, tokens); err != nil {
		return err
	}
	if len(tokens) == 0 {
		// Said once, at startup, where an operator reading the boot log can
		// see it. The refusal above already covers the dangerous case; this
		// covers the loopback and --insecure ones, which are legitimate but
		// should not be silent.
		d.logger.WarnContext(ctx, "dagworkerd: serving with no authentication",
			"grpc_addr", cfg.GRPCAddr, "http_addr", cfg.HTTPAddr, "insecure", cfg.Insecure)
	}

	if cfg.GRPCAddr != "" {
		lis, err := lc.Listen(ctx, "tcp", cfg.GRPCAddr)
		if err != nil {
			return fmt.Errorf("dagworkerd: listening on grpc addr %s: %w", cfg.GRPCAddr, err)
		}
		grpcOpts := []grpcadapter.Option{grpcadapter.WithLogger(d.logger)}
		if len(tokens) > 0 {
			grpcOpts = append(grpcOpts, grpcadapter.WithAuthorizer(grpcadapter.BearerToken(tokens...)))
		}
		// grpcadapter.New takes no context: it builds a server, it does not
		// start one. contextcheck reaches through it to the stream
		// interceptor's ss.Context(), which is the only context a
		// StreamServerInterceptor is ever given.
		//nolint:contextcheck // New has no ctx parameter; there is nothing to propagate
		srv, err := grpcadapter.New(d.mgr, grpcOpts...)
		if err != nil {
			return fmt.Errorf("dagworkerd: constructing grpc adapter: %w", err)
		}
		d.grpcLis, d.grpcSrv = lis, srv
	}
	if cfg.HTTPAddr != "" {
		lis, err := lc.Listen(ctx, "tcp", cfg.HTTPAddr)
		if err != nil {
			return fmt.Errorf("dagworkerd: listening on http addr %s: %w", cfg.HTTPAddr, err)
		}
		httpOpts := []httpadapter.Option{httpadapter.WithLogger(d.logger)}
		if len(tokens) > 0 {
			httpOpts = append(httpOpts, httpadapter.WithAuthorizer(httpadapter.BearerToken(tokens...)))
		}
		srv, err := httpadapter.New(d.mgr, httpOpts...)
		if err != nil {
			return fmt.Errorf("dagworkerd: constructing http adapter: %w", err)
		}
		d.httpLis, d.httpSrv = lis, srv
	}

	adminLis, listenErr := lc.Listen(ctx, "tcp", cfg.AdminAddr)
	if listenErr != nil {
		return fmt.Errorf("dagworkerd: listening on admin addr %s: %w", cfg.AdminAddr, listenErr)
	}
	d.adminLis = adminLis
	mux := newAdminMux(d.mgr, &d.ready, d.startedAt, cfg.AdminPprof)
	d.adminSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return nil
}

// GRPCAddr returns the gRPC listener's actual bound address, or nil if the
// gRPC adapter is disabled. Tests use it to discover the port the OS picked
// for an ephemeral (":0") listen address.
func (d *daemon) GRPCAddr() net.Addr {
	if d.grpcLis == nil {
		return nil
	}
	return d.grpcLis.Addr()
}

// HTTPAddr returns the HTTP listener's actual bound address, or nil if the
// HTTP adapter is disabled.
func (d *daemon) HTTPAddr() net.Addr {
	if d.httpLis == nil {
		return nil
	}
	return d.httpLis.Addr()
}

// AdminAddr returns the admin listener's actual bound address. Unlike
// GRPCAddr and HTTPAddr this is never nil: the admin listener is not optional.
func (d *daemon) AdminAddr() net.Addr { return d.adminLis.Addr() }

// Run serves every listener until ctx is done, then runs the ordered
// shutdown sequence and returns. It is the daemon's entire lifecycle in one
// call, which is what lets a test drive it in-process: start it on a
// goroutine, exercise it over its listeners, cancel ctx, and assert it
// returns within cfg.ShutdownTimeout.
//
// Each adapter's [grpcadapter.Server.Serve] / [httpadapter.Server.Serve] is
// given context.Background(), never ctx: shutdown here is driven entirely by
// the explicit Shutdown calls in [daemon.shutdown], in the exact order that
// function documents, never by ctx cancellation racing that order — an
// adapter that also watched ctx directly could begin draining before
// readiness had failed.
func (d *daemon) Run(ctx context.Context) error {
	errs := make(chan error, 3)
	var running sync.WaitGroup

	running.Add(1)
	go func() {
		defer running.Done()
		errs <- d.adminSrv.Serve(d.adminLis)
	}()
	if d.grpcSrv != nil {
		running.Add(1)
		//nolint:gosec,contextcheck // deliberately detached from ctx — see this method's own doc comment
		go func() {
			defer running.Done()
			errs <- d.grpcSrv.Serve(context.Background(), d.grpcLis)
		}()
	}
	if d.httpSrv != nil {
		running.Add(1)
		//nolint:gosec,contextcheck // deliberately detached from ctx — see this method's own doc comment
		go func() {
			defer running.Done()
			errs <- d.httpSrv.Serve(context.Background(), d.httpLis)
		}()
	}

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errs:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = err
		}
	}

	d.shutdown() //nolint:contextcheck // shutdown must NOT inherit ctx: by the time it runs, ctx may already be Done, and shutdown needs its own fresh, independently-bounded context — see shutdown's own doc comment
	running.Wait()
	close(errs)
	for err := range errs {
		if runErr == nil && err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = err
		}
	}
	return runErr
}

// shutdown runs the ordered sequence exactly once per Run: fail readiness,
// stop accepting new claims and drain what is in flight, close the store,
// and only then stop answering on the admin listener — so /healthz and
// /readyz keep answering for the orchestrator throughout every step before
// that.
func (d *daemon) shutdown() {
	d.ready.fail()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), d.cfg.ShutdownTimeout)
	defer cancel()
	d.shutdownAdapters(shutdownCtx)

	closeCtx, cancelClose := context.WithTimeout(context.Background(), d.cfg.ShutdownTimeout)
	defer cancelClose()
	if err := d.mgr.Close(closeCtx); err != nil {
		d.logger.WarnContext(closeCtx, "dagworkerd: manager close", "error", err)
	}
	if err := d.handle.Close(closeCtx); err != nil {
		d.logger.WarnContext(closeCtx, "dagworkerd: store close", "error", err)
	}

	_ = d.adminSrv.Shutdown(shutdownCtx)
}

// shutdownAdapters stops accepting new claims and drains in-flight ones on
// both claim-serving adapters concurrently, bounded by ctx. It never touches
// a lease: [grpcadapter.Server.Shutdown] and [httpadapter.Server.Shutdown]
// both document, and the adapter contract requires, that neither cancels or
// releases anything a worker currently holds.
func (d *daemon) shutdownAdapters(ctx context.Context) {
	var wg sync.WaitGroup
	if d.grpcSrv != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.grpcSrv.Shutdown(ctx); err != nil {
				d.logger.WarnContext(ctx, "dagworkerd: grpc adapter shutdown", "error", err)
			}
		}()
	}
	if d.httpSrv != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.httpSrv.Shutdown(ctx); err != nil {
				d.logger.WarnContext(ctx, "dagworkerd: http adapter shutdown", "error", err)
			}
		}()
	}
	wg.Wait()
}
