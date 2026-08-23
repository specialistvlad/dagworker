package redis

import dw "github.com/specialistvlad/dagworker"

// scopesRegistryKey is the single global registry of scope names. It
// deliberately carries no {scope} hash tag: it is a cross-scope index by
// nature (Scopes enumerates every scope the store knows about), so it cannot
// be co-located with any one scope's slot. Nothing atomic depends on it —
// Scopes carries no atomicity requirement of its own — so a plain,
// un-tagged key is safe here precisely because no Lua script ever touches it
// alongside a {scope}-tagged key in the same call.
const scopesRegistryKey = "dagworker:scopes"

// prefix returns the key prefix every key belonging to scope shares,
// including the {scope} hash tag. Building every other key by string
// concatenation of this prefix, entirely inside Lua scripts, is what keeps
// every multi-key script legal under Redis Cluster (docs/research/05 §9): the
// hash tag guarantees co-location, so CRC16("{scope}") alone decides the slot
// regardless of what follows the closing brace.
//
// s.keyspace, when non-empty, is folded inside the hash tag ahead of the
// scope name. It exists solely for this package's own conformance test,
// which needs many parallel subtests to share one Redis instance without
// colliding: dagstoretest's suite hard-codes the scope name "s" for every
// subtest (it is testing scope-relative behaviour, not scope naming), so
// isolation across subtests has to come from somewhere other than the scope
// string the suite passes in. It has no effect on any value a caller
// observes — dw.Node.Scope, dw.Lease.Scope, and every Effect this package
// returns always carry the scope exactly as the caller wrote it — because it
// is folded in only here, at the point a Redis key's bytes are built, never
// into a Go dagworker value.
func (s *Store) prefix(scope dw.Scope) string { return "{" + s.keyspace + string(scope) + "}:" }

// The suffixes below exist only as documentation of the key scheme; every
// script builds these itself from ARGV[1] (the prefix) by concatenation, per
// Redis's own scripting guidance to never construct a key's *name* outside of
// KEYS/ARGV-derived strings. Go-side helpers that need to address a key
// directly (GetNode, ScopeConfig, ...) use the methods below.
const (
	sufCfg    = "cfg"    // HASH: scope policy + sealed flag
	sufStats  = "stats"  // HASH: O(1) counters
	sufCursor = "cursor" // STRING: scope-wide event cursor counter
	sufIdx    = "idx"    // ZSET, score=0 for every member: lexicographic node-id index
	sufLeases = "leases" // ZSET: member=NodeID, score=deadline (unix ms)
	sufEvents = "events" // STREAM: durable event log, ID = "<cursor>-0"
	sufBell   = "bell"   // Pub/Sub channel: rung whenever a node becomes ready
)

func (s *Store) keyCfg(scope dw.Scope) string    { return s.prefix(scope) + sufCfg }
func (s *Store) keyStats(scope dw.Scope) string  { return s.prefix(scope) + sufStats }
func (s *Store) keyCursor(scope dw.Scope) string { return s.prefix(scope) + sufCursor }
func (s *Store) keyIdx(scope dw.Scope) string    { return s.prefix(scope) + sufIdx }
func (s *Store) keyEvents(scope dw.Scope) string { return s.prefix(scope) + sufEvents }
func (s *Store) keyBell(scope dw.Scope) string   { return s.prefix(scope) + sufBell }
func (s *Store) keyReady(scope dw.Scope, kind string) string {
	return s.prefix(scope) + "r:" + kind
}
