// Package client is the reference Go SDK for dagworker's gRPC adapter. It
// owns the parts a worker author should never have to re-derive: the
// long-lived connection's load-balancing and keepalive defaults (this file),
// and the claim → heartbeat → complete/fail/skip loop (worker.go) — so a host
// using this package writes only the work function.
//
// The one rule this package exists to make structurally hard to violate: the
// lease's deadline lives in the server's storage, not in any context a
// worker holds. Every RPC this package issues after a successful ClaimNode —
// each heartbeat, and the final Complete/Fail/Skip — gets its own short-lived
// context derived from context.Background(), never the poll's or the work
// handler's. See ClaimAndRun's doc comment and
// docs/research/13-grpc-worker-protocol.md §7 for why reusing one context
// across that whole lifecycle is the single most common mistake in a
// poll-then-work client.
package client

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// Matched to the server defaults in ../options.go: PermitWithoutStream is not
// optional for a client that spends most of its life parked in a ClaimNode
// long poll (docs/research/13-grpc-worker-protocol.md §8), and Time must sit
// at or above the server's KeepaliveEnforcementPolicy.MinTime or the server
// treats legitimate pings as abuse and tears the connection down with
// ENHANCE_YOUR_CALM.
const (
	defaultClientKeepaliveTime    = 1 * time.Minute
	defaultClientKeepaliveTimeout = 20 * time.Second
)

// dialConfig collects what a [DialOption] mutates.
type dialConfig struct {
	extraDialOpts []grpc.DialOption
}

// DialOption configures [Dial]. Opaque for the same reason grpcadapter.Option
// and dagworker.Option are: a future option should never become part of the
// set of legal values a caller can enumerate (ADR-0027's pattern).
type DialOption interface{ apply(*dialConfig) }

type dialOptionFunc func(*dialConfig)

func (f dialOptionFunc) apply(c *dialConfig) { f(c) }

// WithDialOptions is the escape hatch for anything this package does not
// expose its own [DialOption] for. Applied after Dial's own defaults
// (load-balancing policy, keepalive), so a caller can override one of those
// defaults but never be silently overridden by it — for example, passing a
// grpc.WithDefaultServiceConfig here replaces Dial's round_robin choice
// rather than losing to it.
func WithDialOptions(opts ...grpc.DialOption) DialOption {
	return dialOptionFunc(func(c *dialConfig) { c.extraDialOpts = append(c.extraDialOpts, opts...) })
}

// Dial opens a connection suitable for a long-lived worker.
//
// target should be a resolver scheme that can return more than one address —
// "dns:///dagworkerd.namespace.svc.cluster.local:443" for a headless
// Kubernetes Service, for instance — never a bare "host:port" or a
// ClusterIP/VIP. grpc-go's default balancer, pick_first, connects to the
// first resolved address and keeps every RPC on that one connection for its
// whole lifetime; for a worker that holds a handful of very long-lived,
// mostly-idle connections (parked in ClaimNode/Watch), an unlucky initial
// placement never averages out the way short bursty connections eventually
// would, and one dagworkerd replica ends up permanently overloaded. Dial
// sets round_robin instead, which opens one subchannel per resolved address
// and spreads successive RPCs across whichever are ready — but that only
// helps if the target's resolver actually returns more than one address in
// the first place (see docs/research/13-grpc-worker-protocol.md §15).
//
// creds is required rather than defaulted to insecure: a worker SDK that
// silently picked plaintext for you is the wrong kind of convenient. Tests
// and local development pass insecure.NewCredentials() explicitly, which
// keeps that choice visible in the caller's own code.
func Dial(target string, creds credentials.TransportCredentials, opts ...DialOption) (*grpc.ClientConn, error) {
	cfg := dialConfig{}
	for _, o := range opts {
		o.apply(&cfg)
	}

	dialOpts := make([]grpc.DialOption, 0, 3+len(cfg.extraDialOpts))
	dialOpts = append(dialOpts,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                defaultClientKeepaliveTime,
			Timeout:             defaultClientKeepaliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	dialOpts = append(dialOpts, cfg.extraDialOpts...)

	return grpc.NewClient(target, dialOpts...)
}
