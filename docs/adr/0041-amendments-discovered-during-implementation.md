# ADR-0041: Amendments discovered during implementation

- **Status:** Accepted
- **Date:** 2026-08-23
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** ADR-0002, ADR-0004, ADR-0016, ADR-0020, ADR-0030, ADR-0031
- **Backing research:** docs/research/00-synthesis.md, docs/spec/01-contract.md

## Context

ADR-0001 through ADR-0040 were written from the research synthesis, before any
code existed. Writing the type foundation and the storage port surfaced five
places where the design as recorded was either internally inconsistent or
strictly improvable. ADRs are immutable once accepted, so rather than editing
them this record amends them.

Each amendment below was forced by an attempt to write the thing down in Go, not
by a change of taste. That is the useful signal: a design that cannot be typed
is not yet a design.

## Decision

### 1. `Phase` collapses from nine values to five — amends ADR-0002

The synthesis proposed `Blocked/Ready/Claimed/Succeeded/Failed/TimedOut/
UpstreamFailed/Skipped/Cancelled`. The last six duplicate `Reason` one for one,
so every terminal node would carry the same fact twice in two fields that could
disagree — and one of them would eventually be updated without the other.

`Phase` is now `Blocked`, `Scheduled`, `Ready`, `Claimed`, `Done`. It answers
only the question `Status` cannot: *why is this node not running yet*. Which
terminal outcome, and why, is `Status` and `Reason`, with exactly one
representation.

`PhaseScheduled` is new and load-bearing: a node awaiting a retry backoff has
satisfied dependencies but is not yet claimable, which is neither `Blocked` nor
`Ready`. Without it, a retrying node has to be misfiled as one or the other.

### 2. `TriggerAlways` means "ignores predecessors" — amends ADR-0030

As specified, `Always` fired when every predecessor was terminal, which is
exactly `AllDone`. Two names for one rule is a bug in the vocabulary.

`TriggerAlways` now makes a node claimable immediately, ignoring predecessor
state entirely, matching Airflow's `always` trigger rule. Edges into such a node
still exist for documentation and still participate in cycle checking; they
simply never gate it.

### 3. `Store` lives in the root package — amends ADR-0016, ADR-0031

The plan put the port in a `dagstore` subpackage. That cannot compile: `Manager`
must reference `Store`, so the root imports `dagstore`; and `Store`'s method
signatures use `Node`, `Status`, `NodeSpec`, `ScopeConfig`, which the root also
exposes — so `dagstore` must import the root. An import cycle.

The three escapes are all worse than the fix. A shared `dagtypes` package splits
the domain vocabulary away from the package users actually import, so `go doc`
on the main package shows almost nothing. Type aliases in the root pointing at
`dagstore` invert the dependency but leave the canonical documentation in a
subpackage nobody reads first. Duplicating the types in both packages guarantees
they drift.

`Store` therefore lives in the root package, in its own `store.go`. This is what
the standard library does with `http.Handler` alongside `http.Client`: an
interface implemented by others and an API consumed by others can share a
package. Backend modules import only `github.com/specialistvlad/dagworker`,
which has no dependencies of its own, so the cost of that import is zero.

The module topology in ADR-0031 is otherwise unchanged.

### 4. `Cursor` is split from `Seq` — amends ADR-0020

ADR-0020 gave `Seq` two jobs: a per-node monotonic counter for staleness
detection, and the resume token for a subscription. Those are incompatible. A
subscription spans nodes, and per-node counters are unrelated numbers — there is
no ordering between node A's `Seq` 7 and node B's `Seq` 7, so "resume after 7"
is meaningless for a stream.

There are now two numbers:

- `Seq` — per node, bumped on every write to that node. Orders that node's own
  events, and detects a stale read.
- `Cursor` — per scope, a position in the scope's event log. Orders the stream
  and is what a reconnecting subscriber resumes from.

This is etcd's split of per-key `ModRevision` from store-wide `Revision`, which
exists for precisely this reason.

The dossiers' warning against a monotonic counter — that it serializes every
writer — was aimed at a *global* counter shared across scopes. A per-scope
counter costs nothing extra, because writes within a scope are already
serialized by the atomicity the storage port requires of every backend.

Because cursors are per scope, a subscription with an empty `Scope` and a
non-zero `From` is rejected: there is no cross-scope position to resume from.

### 5. Full Pearce–Kelly ships in v1 — amends ADR-0004

The plan was to ship the single-node rank bump first and escalate to full
Pearce–Kelly affected-region reordering only if a fast-path-hit-rate metric
showed the approximation degrading.

Writing it out, the full algorithm is roughly a hundred lines: two bounded
depth-first searches from the edge's endpoints, then a merge that reassigns the
union of the two regions' existing rank values in sorted order. The
approximation is not meaningfully simpler, and it carries an obligation the real
algorithm does not — to monitor a hit rate and to be replaced later on evidence.

Shipping the correct algorithm now removes a metric, a migration, and a class of
"why did this graph get slow" investigation. The `topo_fastpath_hit_ratio`
metric is still exported, now purely as an observability signal rather than as
the trigger for a planned rewrite.

## Consequences

### Positive

- One representation of every fact: no field pair that can disagree.
- Subscriptions can actually resume, which ADR-0020 promised but could not deliver.
- Backends import one dependency-free module and implement one interface.
- No planned-but-unbuilt algorithm work remains in the roadmap for cycle checking.

### Negative

- The root package is larger than a purist would like. Mitigated by file
  organisation and by the fact that `Store` is the only interface a backend
  author reads.
- `Cursor` is a second number for callers to understand. Mitigated by `Seq`
  being the only one most callers ever touch; `Cursor` appears only on `Event`
  and on `SubscribeOptions.From`.

### Neutral

- The public API surface grows by one type and one field. Both are additive.

## Alternatives considered

### Keep the nine-value `Phase` and derive `Reason` from it

Rejected: it inverts the dependency the wrong way. `Reason` is public and frozen;
`Phase` is internal and explicitly carries no compatibility promise. Deriving a
stable public value from an unstable internal one makes the internal one stable
by accident.

### A `dagtypes` package imported by both root and `dagstore`

Rejected: it solves the cycle but scatters the domain vocabulary across three
packages, and `go doc github.com/specialistvlad/dagworker` — the first thing any
evaluator runs — would show a `Manager` whose every method signature points
somewhere else.

### One global `Cursor` across all scopes

Rejected for the reason the dossiers give: a single counter serializes every
writer in the backend, turning independent scopes into contenders. Per-scope
keeps the isolation that ADR-0023 exists to provide.

## References

- [etcd API: Revision and ModRevision](https://etcd.io/docs/v3.5/learning/api/)
- [Airflow trigger rules](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules)
- Pearce & Kelly, "A dynamic topological sort algorithm for directed acyclic graphs", ACM JEA 11, 2007
- ADR-0002, ADR-0004, ADR-0016, ADR-0020, ADR-0030, ADR-0031
