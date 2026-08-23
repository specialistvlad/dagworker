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
func prefix(scope dw.Scope) string { return "{" + string(scope) + "}:" }

// The suffixes below exist only as documentation of the key scheme; every
// script builds these itself from ARGV[1] (the prefix) by concatenation, per
// Redis's own scripting guidance to never construct a key's *name* outside of
// KEYS/ARGV-derived strings. Go-side helpers that need to address a key
// directly (GetNode, ScopeConfig, ...) use the functions below.
const (
	sufCfg     = "cfg"     // HASH: scope policy + sealed flag
	sufStats   = "stats"   // HASH: O(1) counters
	sufCursor  = "cursor"  // STRING: scope-wide event cursor counter
	sufNextOrd = "nextord" // STRING: topological-rank counter
	sufNextFF  = "nextfifo"
	sufIdx     = "idx"    // ZSET, score=0 for every member: lexicographic node-id index
	sufKinds   = "kinds"  // SET: every kind name ever seen ready in this scope
	sufLeases  = "leases" // ZSET: member=NodeID, score=deadline (unix ms)
	sufSched   = "sched"  // ZSET: member=NodeID, score=readyAt (unix ms)
	sufEvents  = "events" // STREAM: durable event log, ID = "<cursor>-0"
	sufBell    = "bell"   // Pub/Sub channel: rung whenever a node becomes ready
)

func keyCfg(scope dw.Scope) string     { return prefix(scope) + sufCfg }
func keyStats(scope dw.Scope) string   { return prefix(scope) + sufStats }
func keyCursor(scope dw.Scope) string  { return prefix(scope) + sufCursor }
func keyIdx(scope dw.Scope) string     { return prefix(scope) + sufIdx }
func keyLeases(scope dw.Scope) string  { return prefix(scope) + sufLeases }
func keyEvents(scope dw.Scope) string  { return prefix(scope) + sufEvents }
func keyBell(scope dw.Scope) string    { return prefix(scope) + sufBell }
func keyNode(scope dw.Scope, id dw.NodeID) string { return prefix(scope) + "n:" + string(id) }
func keyBlob(scope dw.Scope, id dw.NodeID) string { return prefix(scope) + "b:" + string(id) }
func keySucc(scope dw.Scope, id dw.NodeID) string { return prefix(scope) + "s:" + string(id) }
func keyPred(scope dw.Scope, id dw.NodeID) string { return prefix(scope) + "p:" + string(id) }
