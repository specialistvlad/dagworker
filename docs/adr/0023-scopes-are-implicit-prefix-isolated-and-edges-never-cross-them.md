# ADR-0023: Scopes are implicit, prefix-isolated, and edges never cross them

- **Status:** Accepted, amended by ADR-0044 and ADR-0046
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Amended by:** ADR-0044 §9 -- `ErrCrossScopeEdge` is never returned, because `AddEdges` takes a single scope and a cross-scope edge is unrepresentable rather than rejected. The guarantee is stronger than described below, not weaker.
- **Amended by:** ADR-0046 — §3 puts access control outside the library, which holds for the embedded case and not for a `cmd/dagworkerd` deployment, where the daemon *is* the host layer §3 expects to enforce it. Scope authorization is now an optional adapter facet.
- **Backing research:** docs/research/12-dag-semantics-and-state-machine.md §4.1, §4.2, §4.3

## Context

The brief requires a scope to simultaneously be a namespace, an isolation boundary, a concurrency
quota unit, and a GC/retention unit. Kubernetes solves the analogous problem by fragmenting these
four responsibilities across four separate mechanisms — `Namespace` for naming, RBAC for
isolation, `ResourceQuota` for concurrency limits, and no first-class per-namespace TTL at all,
leaving retention to bespoke controllers (12 §4.1). Fragmenting responsibilities this way is
itself a design decision this library should not copy: dag-worker-go's job is narrower than
Kubernetes' (one library, one owned graph store, no multi-tenant RBAC model to integrate with), so
fusing all four responsibilities into one `Scope` concept is both possible and strictly simpler for
callers, provided the fusion doesn't quietly reintroduce the global-coordination costs scoping is
supposed to eliminate.

The performance mandate is the forcing function. Cycle detection (ADR-0004 in the numbered set),
completion detection (ADR-0024), retention/GC (ADR-0034), and concurrency quotas (ADR-0034) are
all specified to be O(1) or O(log n) *per operation*, which is only achievable if each of these
computations can be answered by inspecting one scope's data in isolation. The moment an edge is
allowed to cross a scope boundary, "is this DAG done" and "is this insert a cycle" both have to
reason about a graph that spans key-prefix boundaries — collapsing the general Dijkstra-Scholten/
Safra distributed-termination-detection problem back onto a system that was specifically designed
to avoid it by keeping the graph scope-local (12 §4.3). Key-prefixing across all three mandatory
backends is what makes scope-local reasoning cheap in the first place — Redis SCAN and Lua-script
atomicity boundaries, Postgres's leading composite-key column, and the in-memory store's top-level
shard key all depend on "everything this scope owns lives under one identifiable key range" being
true unconditionally, not just usually.

## Decision

**A `Scope` is an opaque, caller-chosen string. It is created implicitly on first use — there is no
`CreateScope` call.** The first `AddNode`/`AddNodes` call that references a scope ID the backend has
not seen before creates that scope's record (with the library-wide fallback `ScopeConfig`, ADR-0034)
as part of that same atomic call, exactly mirroring "namespaces are created implicitly" from the
brief and Kubernetes' own precedent (12 §4.1).

1. **Physical isolation is key-prefixing, and it is the only isolation mechanism** — there is no
   ACL/ownership system inside the library core. Every backend keys all scope-owned data off the
   scope identity, unconditionally:
   - **Redis:** all keys for a scope carry a `{scope}` hash tag, so `SCAN` restricted to a scope is
     a prefix match and any Lua script enforcing atomicity across multiple keys for that scope
     never needs cross-tag coordination.
   - **PostgreSQL:** `scope` is the leading column of every composite primary key and every index
     the engine issues, so every query is naturally scope-scoped and index-local, and tearing down
     a whole scope is one indexed range delete, never a full-table scan.
   - **In-memory:** scope is the top-level shard key in the struct-of-arrays index — a scope's
     nodes, edges, and ready-set never share a shard with another scope's.
2. **`AddEdge` (and multi-node `AddNodes` batches carrying edges) that reference a node in a
   different scope are rejected outright** with a typed `ErrCrossScopeEdge` — never partially
   accepted, never silently redirected into either scope, never retried into a merged scope by the
   library.
3. **Ownership and access control are explicitly out of the library's scope** (pun unavoidable) —
   same as Kubernetes leaves "who may touch this namespace" to RBAC, a subsystem entirely separate
   from namespace semantics themselves. A host program that needs per-tenant ACLs enforces them in
   its own layer before calling `AddNode`/`Claim`.
4. **A caller needing one causal chain across two logical namespaces has exactly two supported
   options, both explicit:** (a) put both halves in one scope and use `Labels` (12 §5.4) for
   logical sub-partitioning and selective subscription filtering within it, or (b) bridge the two
   scopes at the application layer — a terminal node's `Ack` in scope A triggers a host-program
   call to `AddNode` in scope B, the same "worker fans out" pattern already used for dynamic
   fan-out (12 §2.6), just crossing a scope boundary through caller code instead of through the
   graph itself.
5. Scope completion (`Sealed && notTerminalCount == 0`, ADR-0024) and every `ScopeConfig` field
   (retention, quotas, partition count — ADR-0034) are computed and stored per scope, never
   globally, consistent with this ADR's key-prefix guarantee.

## Consequences

### Positive
- Cycle-check, completion-detection, retention, and GC all stay provably scope-local — the
  precondition the O(1)/O(log n) performance goal depends on for every one of those operations
  (12 §4.3 point 1).
- Tearing down a finished scope is one indexed range delete or prefix scan on every backend, never
  a global "is anyone else still pointing at one of my nodes" liveness check — the exact class of
  problem Kubernetes namespace deletion has to solve for cross-namespace owner references, and
  which this design avoids needing to solve at all (12 §4.3 point 2).
- Per-scope concurrency quotas (`ScopeConfig.MaxInFlight`, ADR-0034) stay meaningful: a quota on
  scope A cannot be bypassed by routing work through a cross-scope edge dependency chain landing
  its accounting on scope B, because no such edge can exist (12 §4.3 point 3).

### Negative
- A genuine cross-namespace causal chain costs an explicit application-layer bridge — an extra
  round trip and extra host-program code the caller must write and maintain themselves, rather
  than the library handling it transparently.
- Implicit scope creation means a typo'd scope ID silently starts a new, empty scope instead of
  surfacing a "scope not found" error — a caller-side bug class this design accepts in exchange for
  never requiring an explicit lifecycle call.

### Neutral
- Most reported "I need edges across namespaces" use cases in prior-art surveys turn out to be "one
  scope with labels" in disguise (12 §4.3 point 4) — this ADR's restriction is expected to be a
  rare, not a common, cost in practice, but it is not zero and should be documented as a known
  design tradeoff rather than an oversight.

## Alternatives considered

- **Explicit `CreateScope`/`DeleteScope` lifecycle calls**: rejected — the brief and 12 §4.1 both
  want namespace-on-first-use semantics; an explicit create call adds a step every caller must
  remember and a new failure mode (a create-race, or a caller that forgets to create) for no
  isolation benefit, since isolation comes entirely from key-prefixing, not from a registry row's
  existence.
- **Allow cross-scope edges, backed by a distributed completion/cycle check**: rejected — this
  reintroduces the general Dijkstra-Scholten/Safra termination-detection problem that keeping the
  graph scope-local was specifically designed to collapse into a cheap local counter check
  (ADR-0024); the performance mandate does not survive this alternative.
- **Enforce ownership/ACLs inside the library core**: rejected — a library embedded across
  arbitrary host programs cannot assume a universal identity or authorization model; Kubernetes'
  own core leaves RBAC to a separate, pluggable subsystem for exactly this reason, and dag-worker-
  go follows the same precedent (12 §4.1).

## References

- [Kubernetes — ResourceQuota](https://kubernetes.io/docs/concepts/policy/resource-quotas/)
- docs/research/12-dag-semantics-and-state-machine.md §4.1-§4.3
- ADR-0004 (cycle rejection, relies on scope-local ordering); ADR-0024 (scope completion, the O(1)
  local counter this ADR's isolation makes possible); ADR-0034 (`ScopeConfig`, stored per scope)
