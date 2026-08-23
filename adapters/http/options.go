package httpadapter

import (
	"log/slog"
	"math/rand/v2"
	"time"
)

// Conservative defaults. Named and documented individually because each one
// answers a question dossier 14 argues from a primary source, and a bare
// number in an options struct would lose that argument.
const (
	// defaultMaxWait is the ceiling a client's requested claim "wait" is
	// clamped to, regardless of what it asks for. Long enough to collapse
	// effective claim latency to near zero under load; short enough to stay
	// well inside the default idle timeout of load balancers this server does
	// not control (dossier 14 §2.2).
	defaultMaxWait = 60 * time.Second

	// defaultWaitBudget is reserved off the end of the clamped wait for
	// marshaling and flushing the response before the client's own deadline —
	// or an intermediary's idle timeout — fires first.
	defaultWaitBudget = 500 * time.Millisecond

	// defaultHeartbeat is how often the SSE handler writes a ": heartbeat"
	// comment line, which resets idle timers on the path without being visible
	// to EventSource.onmessage (dossier 14 §5.3).
	defaultHeartbeat = 15 * time.Second

	// defaultReadHeaderTimeout bounds how long a client may take to finish
	// sending headers, defending against Slowloris-style clients without
	// touching body-read time.
	defaultReadHeaderTimeout = 5 * time.Second

	// defaultWriteTimeout is the ordinary per-request write budget. The events
	// handler opts itself out via http.ResponseController; every other handler
	// stays covered by it (dossier 14 §10, the WriteTimeout footgun).
	defaultWriteTimeout = 30 * time.Second

	// defaultIdleTimeout closes keep-alive connections nobody is using.
	defaultIdleTimeout = 120 * time.Second

	// defaultMaxBodyBytes caps request bodies. dagworker's own payload cap
	// defaults to 256 KiB (config.go); 1 MiB leaves headroom for the JSON
	// envelope around a base64-inflated payload without inviting a client to
	// upload something the library was going to reject anyway.
	defaultMaxBodyBytes = 1 << 20

	// defaultProblemBaseURI is the namespace RFC 9457 "type" URIs are minted
	// under. It does not have to resolve; it has to be stable and owned.
	defaultProblemBaseURI = "https://dagworker.dev/problems/"
)

// options holds every [Server] setting an [Option] can change. Unexported: the
// functional-option pattern (mirroring ADR-0027's shape in the core module)
// exists precisely so this struct's fields never become part of the public
// API surface.
type options struct {
	logger            *slog.Logger
	authorizer        Authorizer
	maxWait           time.Duration
	waitBudget        time.Duration
	heartbeat         time.Duration
	readHeaderTimeout time.Duration
	writeTimeout      time.Duration
	idleTimeout       time.Duration
	maxBodyBytes      int64
	problemBaseURI    string
	jitter            func(max time.Duration) time.Duration
}

func defaultOptions() options {
	return options{
		logger:            slog.New(slog.DiscardHandler),
		maxWait:           defaultMaxWait,
		waitBudget:        defaultWaitBudget,
		heartbeat:         defaultHeartbeat,
		readHeaderTimeout: defaultReadHeaderTimeout,
		writeTimeout:      defaultWriteTimeout,
		idleTimeout:       defaultIdleTimeout,
		maxBodyBytes:      defaultMaxBodyBytes,
		problemBaseURI:    defaultProblemBaseURI,
		jitter:            defaultJitter,
	}
}

// defaultJitter draws uniformly from [0, max). Not a security decision: it
// only decorrelates a fleet of blocking-claim clients that reconnected at the
// same instant (dossier 14 §2.3), so a cryptographic source would spend
// entropy for no benefit.
func defaultJitter(maxD time.Duration) time.Duration {
	if maxD <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(maxD))) //nolint:gosec // scheduling jitter, not a secret
}

// Option configures a [Server]. The interface is opaque — the same
// functional-option shape ADR-0027 establishes for the core module — so that
// new settings can be added later without the set of legal values becoming
// part of this package's compatibility surface.
type Option interface{ apply(*options) }

type optionFunc func(*options)

func (f optionFunc) apply(o *options) { f(o) }

// WithLogger sets the structured logger used for request logging and for
// warnings about degraded paths (a failed SSE flush, a doorbell that errored).
// The default discards everything: a library that writes to its host's
// stderr uninvited is a defect, not a feature — the same stance core takes
// (config.go).
func WithLogger(l *slog.Logger) Option { //nolint:ireturn // ADR-0027 functional-option pattern (see .golangci.yml's own dagworker.Option/memory.Option allowance); this Option is the identical opaque-interface shape for this module
	return optionFunc(func(o *options) {
		if l != nil {
			o.logger = l
		}
	})
}

// WithMaxWait sets the server-side ceiling a client's requested claim "wait"
// is clamped to. Zero or negative is ignored; the default is 60s.
func WithMaxWait(d time.Duration) Option { //nolint:ireturn // ADR-0027 functional-option pattern (see .golangci.yml's own dagworker.Option/memory.Option allowance); this Option is the identical opaque-interface shape for this module
	return optionFunc(func(o *options) {
		if d > 0 {
			o.maxWait = d
		}
	})
}

// WithHeartbeatInterval sets how often the SSE handler writes a comment line
// to keep idle intermediaries from timing out the connection. Zero or
// negative is ignored.
func WithHeartbeatInterval(d time.Duration) Option { //nolint:ireturn // ADR-0027 functional-option pattern (see .golangci.yml's own dagworker.Option/memory.Option allowance); this Option is the identical opaque-interface shape for this module
	return optionFunc(func(o *options) {
		if d > 0 {
			o.heartbeat = d
		}
	})
}

// WithTimeouts sets the server's ReadHeaderTimeout, WriteTimeout, and
// IdleTimeout. WriteTimeout never applies to the events endpoint, which
// clears its own write deadline (doc.go). Zero or negative leaves the
// corresponding default in place.
func WithTimeouts(readHeader, write, idle time.Duration) Option { //nolint:ireturn // ADR-0027 functional-option pattern (see .golangci.yml's own dagworker.Option/memory.Option allowance); this Option is the identical opaque-interface shape for this module
	return optionFunc(func(o *options) {
		if readHeader > 0 {
			o.readHeaderTimeout = readHeader
		}
		if write > 0 {
			o.writeTimeout = write
		}
		if idle > 0 {
			o.idleTimeout = idle
		}
	})
}

// WithMaxBodyBytes caps request body size. Requests over the limit fail with
// [ErrInvalidArgument]'s HTTP mapping before the handler ever sees the body.
// Zero or negative is ignored.
func WithMaxBodyBytes(n int64) Option { //nolint:ireturn // ADR-0027 functional-option pattern (see .golangci.yml's own dagworker.Option/memory.Option allowance); this Option is the identical opaque-interface shape for this module
	return optionFunc(func(o *options) {
		if n > 0 {
			o.maxBodyBytes = n
		}
	})
}

// WithProblemBaseURI overrides the namespace RFC 9457 problem "type" URIs are
// minted under. Empty is ignored.
func WithProblemBaseURI(uri string) Option { //nolint:ireturn // ADR-0027 functional-option pattern (see .golangci.yml's own dagworker.Option/memory.Option allowance); this Option is the identical opaque-interface shape for this module
	return optionFunc(func(o *options) {
		if uri != "" {
			o.problemBaseURI = uri
		}
	})
}

// withJitter replaces the randomness behind claim-wait jitter. Unexported:
// it exists for this package's own tests to make jitter deterministic, not
// as something a real deployment should ever need to override.
func withJitter(fn func(time.Duration) time.Duration) Option { //nolint:ireturn // ADR-0027 functional-option pattern (see .golangci.yml's own dagworker.Option/memory.Option allowance); this Option is the identical opaque-interface shape for this module
	return optionFunc(func(o *options) {
		if fn != nil {
			o.jitter = fn
		}
	})
}

// WithAuthorizer installs an [Authorizer] that every request must pass before
// it is routed. Nil is ignored, which leaves the server unauthenticated —
// see [Authorizer] for when that is defensible and [BearerToken] for the
// smallest thing that is not.
func WithAuthorizer(a Authorizer) Option { //nolint:ireturn // ADR-0027 functional-option pattern (see .golangci.yml's own dagworker.Option/memory.Option allowance); this Option is the identical opaque-interface shape for this module
	return optionFunc(func(o *options) {
		if a != nil {
			o.authorizer = a
		}
	})
}
