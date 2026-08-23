package main

import (
	"context"
	"fmt"
	"net/http" //nolint:depguard // dagworkerd is the composition root and constructs its own admin HTTP surface; ADR-0037's ban targets the core module, not this one
	"net/http/pprof"
	"runtime"
	"sync/atomic"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
)

// readinessCheckTimeout bounds the reachability probe /readyz performs on
// every request. Short enough that a wedged backend fails the check quickly
// rather than piling up slow health-check requests.
const readinessCheckTimeout = 2 * time.Second

// readiness is the one flag the shutdown sequence must flip before anything
// else: whether this replica should keep receiving new claim traffic. A
// plain atomic is enough because the only transition that matters is the
// one-way "serving" -> "draining" flip made once, at the start of shutdown
// (docs/research/15-daemon-packaging-and-ops.md Part 2 §2.4, step 1).
type readiness struct {
	draining atomic.Bool
}

// fail flips readiness to draining. Idempotent: called exactly once by the
// real shutdown sequence, but safe if it were not.
func (r *readiness) fail() { r.draining.Store(true) }

// isDraining reports whether shutdown has begun.
func (r *readiness) isDraining() bool { return r.draining.Load() }

// newAdminMux builds the admin listener's handler: liveness, readiness,
// metrics, and — only when enabled — pprof. It is deliberately never reached
// through [http.DefaultServeMux]: net/http/pprof's own doc warns that
// importing it "is typically only imported for the side effect of
// registering its handlers" on that shared mux, which is exactly the trap of
// exposing /debug/pprof/* on whatever port happens to use the default mux.
// Every handler here is registered explicitly on a mux this function owns.
func newAdminMux(mgr *dagworker.Manager, ready *readiness, startedAt time.Time, pprofEnabled bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", handleReadyz(mgr, ready))
	mux.HandleFunc("GET /metrics", handleMetrics(ready, startedAt))
	if pprofEnabled {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}

// handleHealthz answers liveness only: this handler is running, full stop.
// It must return 200 for the entire lifetime of the process, including every
// moment of a graceful drain — an orchestrator that conflates liveness with
// readiness will kill a pod that was merely slow to finish in-flight work,
// which is precisely the failure mode the two-endpoint split exists to
// prevent. It never touches the store or the readiness flag.
func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writePlain(w, http.StatusOK, "ok\n")
}

// handleReadyz answers whether this replica should keep receiving new claim
// traffic: not draining, and the storage backend answers. The reachability
// probe is [dagworker.Manager.Scopes] — every [dagworker.Store] implements
// Scopes, so this one call works identically whether the backend is memory,
// Redis, or PostgreSQL, with no backend-specific plumbing in this package.
func handleReadyz(mgr *dagworker.Manager, ready *readiness) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ready.isDraining() {
			writePlain(w, http.StatusServiceUnavailable, "draining\n")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), readinessCheckTimeout)
		defer cancel()
		if _, err := mgr.Scopes(ctx); err != nil {
			writePlain(w, http.StatusServiceUnavailable, fmt.Sprintf("store unreachable: %v\n", err))
			return
		}
		writePlain(w, http.StatusOK, "ready\n")
	}
}

func writePlain(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

// handleMetrics exposes a small Prometheus text-exposition page.
//
// It cannot report the RED-method request metrics a dispatcher like this one
// ideally would (dagworkerd_claims_total, per-transport claim/ack duration
// histograms) without an instrumentation hook inside the gRPC and HTTP
// adapters, and this module may not modify another one to add such a hook
// (its own hard rule) — so this is an honest subset: process identity,
// uptime, and the same reachability signal /readyz already computes, rather
// than a fuller set this vantage point cannot actually observe.
func handleMetrics(ready *readiness, startedAt time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		draining := 0
		if ready.isDraining() {
			draining = 1
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		body := fmt.Sprintf(
			"# HELP dagworkerd_up 1 if the process is running.\n"+
				"# TYPE dagworkerd_up gauge\n"+
				"dagworkerd_up 1\n"+
				"# HELP dagworkerd_draining 1 once the graceful shutdown sequence has started.\n"+
				"# TYPE dagworkerd_draining gauge\n"+
				"dagworkerd_draining %d\n"+
				"# HELP dagworkerd_uptime_seconds Seconds since process start.\n"+
				"# TYPE dagworkerd_uptime_seconds gauge\n"+
				"dagworkerd_uptime_seconds %f\n"+
				"# HELP dagworkerd_goroutines Current goroutine count.\n"+
				"# TYPE dagworkerd_goroutines gauge\n"+
				"dagworkerd_goroutines %d\n",
			draining, time.Since(startedAt).Seconds(), runtime.NumGoroutine(),
		)
		// A ResponseWriter write failing means the client is already gone;
		// there is nothing left to do about it and nothing worth logging on
		// what is, by construction, a low-traffic scrape endpoint.
		_, _ = w.Write([]byte(body))
	}
}
