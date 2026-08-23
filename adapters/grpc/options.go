package grpcadapter

import (
	"log/slog"
	"time"

	"google.golang.org/grpc"
)

// Server-side tuning constants that are not exposed as their own [Option]
// because getting them wrong is a footgun, not a legitimate choice — see
// docs/research/13-grpc-worker-protocol.md §8 for the keepalive analysis and
// §15 for MaxConnectionAge as forced client rebalancing.
const (
	defaultKeepaliveTime           = 2 * time.Minute
	defaultKeepaliveTimeout        = 20 * time.Second
	defaultKeepaliveMinTime        = 1 * time.Minute
	defaultMaxConnectionAge        = 30 * time.Minute
	defaultMaxConnectionAgeGrace   = 5 * time.Minute
	defaultMaxConcurrentStreams    = 4096
	defaultMaxPollTimeout          = 600 * time.Second // Nomad's blocking-query ceiling
	defaultDefaultPollTimeout      = 30 * time.Second  // used when a request's poll_timeout is unset
)

// serverConfig collects what every [Option] mutates. It is unexported: a
// caller configures a [Server] only through the functional options below,
// which is what lets this struct grow a field later without becoming part of
// the API's shape (ADR-0027's pattern, applied to this adapter).
type serverConfig struct {
	logger *slog.Logger

	maxConcurrentStreams  uint32
	maxConnectionAge      time.Duration
	maxConnectionAgeGrace time.Duration

	// maxPollTimeout is the hard ceiling ClaimNode's poll_timeout is clamped
	// to. defaultPollTimeout is what an unset (zero) poll_timeout resolves to
	// — proto3 cannot distinguish "the field was omitted" from "explicitly
	// zero" on a plain (non-optional) Duration, so this adapter treats both
	// as "use the server's default long-poll window," matching how Temporal
	// and Nomad both apply a positive default when a caller asks for none.
	maxPollTimeout     time.Duration
	defaultPollTimeout time.Duration

	extraServerOpts []grpc.ServerOption
}

func defaultServerConfig() serverConfig {
	return serverConfig{
		logger:                slog.New(slog.DiscardHandler),
		maxConcurrentStreams:  defaultMaxConcurrentStreams,
		maxConnectionAge:      defaultMaxConnectionAge,
		maxConnectionAgeGrace: defaultMaxConnectionAgeGrace,
		maxPollTimeout:        defaultMaxPollTimeout,
		defaultPollTimeout:    defaultDefaultPollTimeout,
	}
}

// Option configures a [Server]. The interface is opaque, mirroring
// dagworker.Option (ADR-0027), so a future option never becomes part of the
// set of legal values callers can enumerate.
type Option interface{ apply(*serverConfig) }

type optionFunc func(*serverConfig)

func (f optionFunc) apply(c *serverConfig) { f(c) }

// WithLogger sets the structured logger used for the handful of things this
// adapter logs on its own behalf (an unmapped error reaching the interceptor,
// a recovered panic). The default discards everything, matching the core
// library's own default: a dependency that writes to its host's stderr
// uninvited is a defect, not a feature.
func WithLogger(l *slog.Logger) Option {
	return optionFunc(func(c *serverConfig) {
		if l != nil {
			c.logger = l
		}
	})
}

// WithMaxConcurrentStreams sets the HTTP/2 stream limit gRPC enforces per
// connection. This is the credit protocol dagworker's long-poll dispatch
// relies on instead of a bespoke capacity message: point it at the fleet's
// real worker-slot count, and a client that opens exactly that many
// concurrent ClaimNode calls can never over-subscribe a replica (see
// docs/research/13-grpc-worker-protocol.md §4).
func WithMaxConcurrentStreams(n uint32) Option {
	return optionFunc(func(c *serverConfig) { c.maxConcurrentStreams = n })
}

// WithMaxConnectionAge periodically GOAWAYs every connection regardless of
// health, forcing a client using dns:///+round_robin (see client.Dial) to
// re-resolve and rebalance on a bounded cycle — the fix for a connection that
// has been healthy, and therefore never rebalanced, for hours (§15). grace
// is how long an in-flight ClaimNode/Watch gets to finish before the hard
// close.
func WithMaxConnectionAge(age, grace time.Duration) Option {
	return optionFunc(func(c *serverConfig) {
		c.maxConnectionAge = age
		c.maxConnectionAgeGrace = grace
	})
}

// WithMaxPollTimeout sets the hard ceiling a ClaimNodeRequest.poll_timeout is
// clamped to, and WithDefaultPollTimeout sets what an unset one resolves to
// (see serverConfig's doc comment on the proto3 ambiguity this resolves).
func WithMaxPollTimeout(d time.Duration) Option {
	return optionFunc(func(c *serverConfig) {
		if d > 0 {
			c.maxPollTimeout = d
		}
	})
}

// WithDefaultPollTimeout sets what an unset ClaimNodeRequest.poll_timeout
// resolves to.
func WithDefaultPollTimeout(d time.Duration) Option {
	return optionFunc(func(c *serverConfig) {
		if d > 0 {
			c.defaultPollTimeout = d
		}
	})
}

// WithGRPCServerOptions is the escape hatch for anything this package does
// not expose its own [Option] for — transport credentials chief among them.
// Options passed here are applied after this adapter's own defaults
// (keepalive, MaxConcurrentStreams, the error-mapping interceptors), so they
// can override a default but never accidentally get overridden by one.
func WithGRPCServerOptions(opts ...grpc.ServerOption) Option {
	return optionFunc(func(c *serverConfig) { c.extraServerOpts = append(c.extraServerOpts, opts...) })
}
