package httpadapter

import (
	"log/slog"
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*
	"time"
)

// middleware is the standard func(Handler) Handler shape, chosen specifically
// because it is what lets any custom logging/metrics wrapper stay compatible
// with [net/http.ResponseController]: the controller walks an optional
// Unwrap() method to reach the real ResponseWriter through any number of
// wrappers, and every wrapper in this file implements it (dossier 14 §11).
type middleware func(http.Handler) http.Handler

// chain applies middlewares in the order listed, so the first one named is
// the outermost — the first to see the request and the last to see the
// response.
func chain(h http.Handler, mw ...middleware) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

// statusRecorder captures the status code a handler wrote, for the access
// log, without changing what reaches the client.
//
// It implements Unwrap so http.ResponseController can still reach the
// underlying ResponseWriter's Flusher and deadline-setting methods straight
// through this wrapper — the exact hazard dossier 14 §11 calls out: a logging
// middleware that omits Unwrap silently breaks SSE flushing the moment it is
// added.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController see through this wrapper.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// loggingMiddleware logs one line per request through the server's injected
// slog.Logger — never through fmt.Print* or the standard library's default
// logger, both of which would write to a host's stderr uninvited.
func loggingMiddleware(logger *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()
			next.ServeHTTP(rec, r)
			logger.LogAttrs(r.Context(), slog.LevelInfo, "dagworker/http: request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Duration("duration", time.Since(start)),
			)
		})
	}
}

// recoveryMiddleware turns a panicking handler into a 500 problem+json
// response instead of a crashed connection (or, without http's own recover,
// a crashed process). The panic is logged with the request that triggered it
// so it is diagnosable, and re-panicking is deliberately not done: net/http
// already logs and closes the connection on an unrecovered panic, which would
// duplicate this middleware's own log line for no benefit.
func recoveryMiddleware(logger *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			//nolint:contextcheck // r.Context() is captured from the enclosing handler, not a
			// fresh/background context; defer-wrapped closures are a documented false-positive
			// shape for this linter (kkHAIKE/contextcheck#25-style: it cannot see the capture).
			defer func() {
				if rec := recover(); rec != nil {
					logger.ErrorContext(r.Context(), "dagworker/http: panic in handler",
						"method", r.Method, "path", r.URL.Path, "panic", rec)
					p := problem{
						Type:     defaultProblemBaseURI + "internal",
						Title:    "Internal Server Error",
						Status:   http.StatusInternalServerError,
						Detail:   "the server encountered an unexpected condition",
						Instance: r.URL.Path,
					}
					writeJSON(w, http.StatusInternalServerError, problemContentType, p)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// bodyLimitMiddleware wraps every request body in [http.MaxBytesReader], so
// an oversized body fails fast with a clear read error instead of a handler
// discovering it mid-decode or, worse, a client being allowed to hand the
// server an unbounded stream (dossier 14 §10).
func bodyLimitMiddleware(maxBytes int64) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			}
			next.ServeHTTP(w, r)
		})
	}
}
