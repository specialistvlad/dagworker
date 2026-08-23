// Package redis implements the dagworker storage port on top of Redis,
// using github.com/redis/go-redis/v9. It is the cross-process backend that
// trades PostgreSQL's transactional durability for Redis's throughput:
// CapCrossProcess is set (many independent *Store values may share one
// Redis deployment and race on its atomic claim), CapDurableStorage is not
// (Redis's default async replication can lose up to ~1s of writes on an
// unclean primary failover — see the Durability doc comment below).
//
// # Atomicity
//
// Every mutating Store method — AddNodes, AddEdges, RemoveEdges, RemoveNode,
// Cancel, CancelScope, Claim, Complete, Extend, Sweep — is exactly one Lua
// script (lua_scripts.go), loaded with EVALSHA and falling back to EVAL on
// NOSCRIPT (go-redis's Script.Run does this natively). Redis executes a
// script atomically with respect to every other client, which is the whole
// of the cross-process guarantee this package provides: there is no
// additional locking, and none is needed.
//
// Scripts are deliberately bounded (docs/research/05-redis-backend.md §8):
// every loop either walks a single node's own edge set (bounded by its
// degree, not the graph), or is capped by a caller-supplied limit (Claim's
// Max, Sweep's limit) or an internal constant (PROMOTE_CAP in the Lua
// prelude) rather than the size of the ready set, the lease set, or the DAG.
// CancelScope is the one documented exception: like the in-memory reference
// implementation's own CancelScope, it walks every live node in the scope,
// because cancelling everything is inherently proportional to everything —
// this is not a violation introduced by this backend, it is the same
// property the reference has, and the complexity contract (docs/spec/01
// §10) states no bound for it either.
//
// # Clock authority
//
// Every deadline this package writes is computed inside the Lua script from
// redis.call('TIME'), never from the calling process's clock (ADR-0008).
// Absolute instants are kept in whole milliseconds, not nanoseconds: a
// Redis/Lua number is a float64, exactly representable only up to 2^53, and
// nanoseconds since the Unix epoch already exceed that today while
// milliseconds will not for millennia. See the nowMs doc comment in
// lua_prelude.go for the detail, and the package's returned deviations for
// what this costs in practice (sub-millisecond retry/backoff configuration
// rounds down to zero, which is also what every conformance test that
// deliberately uses a near-zero delay is already relying on).
//
// # Key scheme and Cluster readiness
//
// Every key a scope owns carries the same {scope} hash tag (keys.go), so
// every multi-key Lua script here is legal under Redis Cluster without
// exception, and a scope is consequently the natural unit of horizontal
// scale-out: two scopes can live on two different Cluster shards, but one
// scope's data and CPU cost is permanently bound to whichever single shard
// owns its hash slot. The one key that is deliberately NOT scope-tagged is
// the global scope registry (scopesRegistryKey in keys.go), because it is
// inherently a cross-scope index; it is written by a plain, separate command
// outside of any script, never alongside a {scope}-tagged key in the same
// atomic call, so it never turns a script into a CROSSSLOT error.
//
// # Durability
//
// This package makes no stronger a durability claim than the Redis
// deployment it is pointed at. With Redis's own defaults (asynchronous
// replication, AOF disabled or fsync=everysec), an unclean primary failover
// can lose up to roughly the last second of writes to any structure this
// backend maintains — a claim never reaching a promoted replica reappears as
// "never claimed," a completion's fan-out can vanish and take a whole
// downstream subtree back to an earlier state. A host that needs stronger
// guarantees on a specific call is expected to issue WAIT/WAITAOF itself
// against the underlying client between calls that must survive a failover;
// this package does not do so automatically, because that cost belongs to
// the caller who knows which calls are worth paying it for.
package redis

import (
	"context"
	"fmt"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	dw "github.com/specialistvlad/dagworker"
)

// Store is a Redis-backed implementation of [dagworker.Store].
type Store struct {
	rdb        goredis.UniversalClient
	ownsClient bool
	defaults   dw.ScopeConfig
	scripts    *scriptSet

	closed    chan struct{}
	closeOnce sync.Once
}

// Option configures a [Store].
type Option interface{ apply(*Store) }

type optionFunc func(*Store)

func (f optionFunc) apply(s *Store) { f(s) }

// WithScopeDefaults sets the [dagworker.ScopeConfig] reported by
// [Store.ScopeConfig] for a scope that has never had SetScopeConfig called on
// it — mirroring the in-memory and PostgreSQL backends' option of the same
// name, so a host can swap backends without re-deriving its defaults.
func WithScopeDefaults(cfg dw.ScopeConfig) Option {
	return optionFunc(func(s *Store) { s.defaults = cfg })
}

// New wraps an existing client. It never dials the network itself, so a host
// that already owns a *redis.Client/*redis.ClusterClient's lifecycle can
// construct a Store synchronously; [Store.Close] never closes a client
// supplied this way; the caller retains ownership.
func New(client goredis.UniversalClient, opts ...Option) *Store {
	s := &Store{
		rdb:     client,
		scripts: newScriptSet(),
		closed:  make(chan struct{}),
	}
	for _, o := range opts {
		o.apply(s)
	}
	return s
}

// Open dials a fresh client against addr and returns a Store that owns it:
// [Store.Close] closes the client too.
func Open(ctx context.Context, addr string, opts ...Option) (*Store, error) {
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis: connect to %s: %w", addr, err)
	}
	s := New(client, opts...)
	s.ownsClient = true
	return s, nil
}

// Capabilities implements [dagworker.CapabilityReporter]. Redis is the
// cross-process backend but not (by default configuration) the durable one —
// see the package doc comment's Durability section.
func (s *Store) Capabilities() dw.Capabilities {
	return dw.Capabilities(dw.CapList | dw.CapDurableEvents | dw.CapDoorbell |
		dw.CapCollect | dw.CapCrossProcess)
}

func (s *Store) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// Close implements [dagworker.Store]. It is idempotent and unblocks every
// in-flight Watch and WaitForWork rather than leaving their goroutines
// parked, matching the other backends' shutdown contract.
func (s *Store) Close(context.Context) error {
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.ownsClient {
			_ = s.rdb.Close()
		}
	})
	return nil
}

// registerScope records scope in the global, un-tagged scope registry, as a
// plain command outside any Lua script (see the package doc comment on why
// that key can never share a script call with a {scope}-tagged key). Called
// from every path that can implicitly create a scope, mirroring exactly
// which calls pass create=true to the in-memory reference's scopeFor.
func (s *Store) registerScope(ctx context.Context, scope dw.Scope) {
	_ = s.rdb.SAdd(ctx, scopesRegistryKey, string(scope)).Err()
}

// ScopeConfig implements [dagworker.Store]. A scope that has never had
// SetScopeConfig called on it returns the store's configured defaults (zero
// value unless [WithScopeDefaults] was used) and no error: scopes are
// created implicitly, so asking about one that does not exist yet is
// ordinary, not exceptional.
func (s *Store) ScopeConfig(ctx context.Context, scope dw.Scope) (dw.ScopeConfig, error) {
	if s.isClosed() {
		return dw.ScopeConfig{}, dw.ErrClosed
	}
	flat, err := s.rdb.HGetAll(ctx, keyCfg(scope)).Result()
	if err != nil {
		return dw.ScopeConfig{}, fmt.Errorf("redis: ScopeConfig: %w", err)
	}
	if len(flat) == 0 {
		return s.defaults, nil
	}
	return hashToCfg(flat), nil
}

// SetScopeConfig implements [dagworker.Store].
func (s *Store) SetScopeConfig(ctx context.Context, scope dw.Scope, cfg dw.ScopeConfig) error {
	if s.isClosed() {
		return dw.ErrClosed
	}
	if err := s.rdb.HSet(ctx, keyCfg(scope), cfgToHash(cfg)).Err(); err != nil {
		return fmt.Errorf("redis: SetScopeConfig: %w", err)
	}
	s.registerScope(ctx, scope)
	return nil
}

// Seal implements [dagworker.Store]. Sealing an already-sealed scope is a
// no-op because it is a single idempotent field write, not because this
// method special-cases it.
func (s *Store) Seal(ctx context.Context, scope dw.Scope) error {
	if s.isClosed() {
		return dw.ErrClosed
	}
	if err := s.rdb.HSet(ctx, keyCfg(scope), fSealed, "1").Err(); err != nil {
		return fmt.Errorf("redis: Seal: %w", err)
	}
	s.registerScope(ctx, scope)
	return nil
}

// ScopeStats implements [dagworker.Store]. Every counter is maintained
// incrementally by the Lua script that changes it (adjustBucket in the
// prelude), so this is a handful of O(1) reads, never a scan.
func (s *Store) ScopeStats(ctx context.Context, scope dw.Scope) (dw.ScopeStats, error) {
	if s.isClosed() {
		return dw.ScopeStats{}, dw.ErrClosed
	}
	pipe := s.rdb.Pipeline()
	statsCmd := pipe.HGetAll(ctx, keyStats(scope))
	sealedCmd := pipe.HGet(ctx, keyCfg(scope), fSealed)
	cursorCmd := pipe.Get(ctx, keyCursor(scope))
	_, err := pipe.Exec(ctx)
	if err != nil && err != goredis.Nil {
		return dw.ScopeStats{}, fmt.Errorf("redis: ScopeStats: %w", err)
	}
	stats := statsCmd.Val()
	out := dw.ScopeStats{
		Total:      atou64(stats["Total"]),
		Blocked:    atou64(stats["Blocked"]),
		Scheduled:  atou64(stats["Scheduled"]),
		Ready:      atou64(stats["Ready"]),
		InProgress: atou64(stats["InProgress"]),
		Succeeded:  atou64(stats["Succeeded"]),
		Failed:     atou64(stats["Failed"]),
		Sealed:     sealedCmd.Val() == "1",
	}
	if c, cerr := cursorCmd.Result(); cerr == nil {
		out.Cursor = dw.Cursor(atou64(c))
	}
	out.Complete = out.Sealed && out.NonTerminal() == 0
	return out, nil
}

// Scopes implements [dagworker.Store].
func (s *Store) Scopes(ctx context.Context) ([]dw.Scope, error) {
	if s.isClosed() {
		return nil, dw.ErrClosed
	}
	names, err := s.rdb.SMembers(ctx, scopesRegistryKey).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: Scopes: %w", err)
	}
	out := make([]dw.Scope, len(names))
	for i, n := range names {
		out[i] = dw.Scope(n)
	}
	return out, nil
}

// resolvedConfig reads a scope's raw stored configuration (or the store's
// defaults, if the scope has never been configured) and returns it through
// the real [dagworker.ScopeConfig.Resolved] — never a reimplementation of its
// fallback rules.
func (s *Store) resolvedConfig(ctx context.Context, scope dw.Scope) (dw.ScopeConfig, error) {
	cfg, err := s.ScopeConfig(ctx, scope)
	if err != nil {
		return dw.ScopeConfig{}, err
	}
	return cfg.Resolved(), nil
}

// runScript executes sc against a single routing-anchor key for scope,
// prepends the scope's key prefix as ARGV[1], and decodes the standard
// {header, effects} reply every mutating script returns. A DWERR-shaped
// error reply is translated via mapScriptErr before being returned.
func (s *Store) runScript(ctx context.Context, sc *goredis.Script, scope dw.Scope, argv ...any) ([]any, []dw.Effect, error) {
	full := append([]any{prefix(scope)}, argv...)
	res, err := sc.Run(ctx, s.rdb, []string{keyCfg(scope)}, full...).Result()
	if err != nil {
		return nil, nil, mapScriptErr(err, scope)
	}
	top, ok := res.([]any)
	if !ok || len(top) != 2 {
		return nil, nil, fmt.Errorf("redis: unexpected script reply shape: %#v", res)
	}
	header, _ := top[0].([]any)
	effects := decodeEffects(top[1])
	return header, effects, nil
}

var (
	_ dw.Store              = (*Store)(nil)
	_ dw.Lister             = (*Store)(nil)
	_ dw.Doorbell           = (*Store)(nil)
	_ dw.DurableEventStream = (*Store)(nil)
	_ dw.Collector          = (*Store)(nil)
	_ dw.CapabilityReporter = (*Store)(nil)
)
