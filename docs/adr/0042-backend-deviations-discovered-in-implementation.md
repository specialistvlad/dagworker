# ADR-0042: Backend deviations discovered during implementation

- **Status:** Accepted
- **Date:** 2026-08-23
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** ADR-0004, ADR-0012, ADR-0016, ADR-0020, ADR-0021
- **Backing research:** docs/research/04-postgres-backend.md, docs/research/05-redis-backend.md

## Context

The conformance suite decides whether a backend is correct. It does not, and
cannot, decide whether two correct backends are *identical*: a store is free to
reach the required behaviour by whatever route its engine makes cheap. Where
those routes differ visibly — in a field a caller can read, a guarantee that is
stronger on one side, or a cost that only appears at scale — the difference
belongs in a decision record rather than in a code comment nobody reads.

This record collects every such difference found while implementing the
PostgreSQL and Redis backends, plus the three defects in the core that only
surfaced once a real database was on the other end.

## Decision

### 1. `CycleError.Path` may be empty — amends ADR-0004

The in-memory backend reconstructs the cycle-closing path from its search's
parent pointers and returns it. Redis reports only that a cycle exists.

Accepted as-is. The port already says `Path` "is empty only if the backend
cannot reconstruct it cheaply", and a caller that branches on the path rather
than on `errors.Is(err, ErrCycle)` has misread the contract. Reconstructing it
inside a Lua script would mean carrying parent pointers through a bounded
search whose whole purpose is to stay bounded.

### 2. Redis reimplements the backoff window in Lua — amends ADR-0012

Every backend is required to use `dagworker.Backoff` rather than its own
formula, so a node's retry schedule does not depend on where it is stored.
Redis cannot honour that for one path: `Sweep` discovers which nodes to back
off, reads their attempt counts, and computes their next attempt entirely
inside one atomic script driven by the server's clock. There is no round trip
available mid-script.

The Lua port is formula-identical and sits beside the Go original in the same
repository. Where a round trip *is* available — the lease clamp on `Claim` and
`Extend` — the real `ClampLease` is called in Go at full precision.

### 3. Redis stores durations in milliseconds

A Lua number is a float64, exactly representable only to 2^53; nanoseconds
since the epoch already exceed that. Durations and instants are therefore whole
milliseconds on Redis.

The visible consequence: a sub-millisecond retry delay rounds to zero. That is
harmless — zero means "claimable immediately", which is what anyone asking for
a sub-millisecond backoff wanted — but it is why the conformance suite's
baseline uses a millisecond rather than a nanosecond as its "negligible"
backoff. A nanosecond rounds to zero, zero is how `ScopeConfig` spells *unset*,
and the library default of one second comes back instead.

### 4. PostgreSQL's cursors are store-wide, not per-scope — amends ADR-0020

`Cursor` is specified as a per-scope position. PostgreSQL allocates it from one
identity column shared across scopes, so a scope's cursors are strictly
increasing but not contiguous.

Accepted: strictly increasing within a scope is the entire resume contract, and
a per-scope counter would be a second hot row on the write path — the shape
that section 7 below is about. The contract is amended to require monotonicity
rather than contiguity.

### 5. PostgreSQL retains events forever — amends ADR-0021

There is no compaction job, so `ErrCursorExpired` is unreachable and
`T-WATCH-CURSOR-EXPIRED` skips. Unbounded retention is a stronger guarantee
than the contract asks for, not a weaker one, and the skip is honest rather
than silent. A retention job is future work, and the table is **not** partitioned for it --
`dagw.events` is an ordinary table and the string `PARTITION` appears in no migration. Its key,
`(scope, cursor)`, is partition-compatible, which is a fair thing to say and a smaller one: range
partitioning on `cursor` would need no new column, and would turn retention into `DROP PARTITION`
rather than a `DELETE` that leaves bloat in the fastest-growing table in the schema. Whether to do
it is tracked in issue #20. Claiming it was already done removed the reason anyone would.

### 6. Structural mutation serializes per scope; the hot path does not

PostgreSQL takes a per-scope advisory lock for `AddNodes`, `AddEdges`,
`RemoveEdges`, `RemoveNode`, `Cancel` and `CancelScope`, because incremental
topological reordering cannot safely interleave. `Claim`, `Complete`, `Extend`
and `Sweep` deliberately do **not** take it: they rely on `SKIP LOCKED` and
row-level locks, which is what makes real cross-process throughput possible.
Redis reaches the same split by being single-threaded per script.

The consequence is worth stating plainly: **building a graph is serialized per
scope; running it is not.** A workload that inserts continuously from many
writers into one scope will contend. Use more scopes.

### 7. The scope's aggregate counter row is a contention point

`ScopeStats` is O(1) because every transition maintains a counter, and on
PostgreSQL those counters live in one row per scope. Concurrent mutations in
the same scope serialize briefly on that row.

This was worse than a contention point before it was fixed: the counters were
updated once per node, so a 500-node batch made 500 tuple versions of one row
and paid quadratically in chain traversal. Deltas are now accumulated and
written once per transaction. The remaining contention is inherent to exact,
scan-free statistics; sharded counters with periodic reconciliation is the
escape hatch if it ever matters.

### 8. Three core defects that only a real database exposed

Recorded because the pattern is the point — none of them could have been found
by the in-memory backend:

- **`Manager.Nack` discarded the retry decision.** It computed whether the
  failure became another attempt and threw it away, forcing the adapters into
  a second, unfenced read that can describe a different worker's attempt. Found
  by the gRPC adapter. `Nack` now returns an `AttemptResult`.
- **The conformance suite assumed a controllable clock in four places.** The
  sharpest: it never set `MinLeaseTimeout`, so the library's one-second floor
  silently inflated every 400ms test lease while the suite waited 650ms. A
  backend driving a fake 30-second lease never approaches the floor, so the
  in-memory backend could not have caught it. Both database agents found it
  independently and both correctly refused to work around it.
- **A parameter where a literal was required.** PostgreSQL's partial indexes
  need their predicate provable from the query text. The claim query knew this;
  the sweep that runs before every claim did not, and bound `phase` instead. It
  worked for five executions on a custom plan and then fell to a sequential
  scan for every one after. Only the million-node benchmark saw it.

## Consequences

### Positive

- The differences between backends are written down where a user comparing them
  will find them, rather than discovered in production.
- Two of the three core defects were found by an implementer refusing to work
  around something that looked wrong. That is the behaviour to keep.

### Negative

- The capability matrix is no longer the whole story; this record is part of it.

### Neutral

- Items 2, 3 and 4 are permanent consequences of the storage engines, not debt.
  Items 6 and 7 are tunable. Item 5 is future work.

## Alternatives considered

### Force every backend to be byte-identical in observable behaviour

Rejected. It would mean giving up native atomics — reconstructing a cycle path
inside a bounded Lua search, or minting per-scope sequences on PostgreSQL's
write path — to make backends agree on things no caller depends on. The
conformance suite already pins everything a caller may rely on.

### Leave these in code comments

Rejected. Someone choosing between Redis and PostgreSQL reads the docs, not
`lua_prelude.go`.

## References

- ADR-0004, ADR-0012, ADR-0016, ADR-0020, ADR-0021
- docs/spec/01-contract.md §13 (durability disclosure)
- [PostgreSQL: partial indexes](https://www.postgresql.org/docs/current/indexes-partial.html)
- [PostgreSQL: prepared statements and generic plans](https://www.postgresql.org/docs/current/sql-prepare.html)
