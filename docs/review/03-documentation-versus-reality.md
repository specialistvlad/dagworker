# Documentation vs. Reality

Scope: `README.md`, `docs/spec/01-contract.md`, a sample of 27 of the 42 ADRs (well over the
required 12), `example_test.go`, and code comments across the root package and the `storage/`,
`cmd/dagworkerd/` trees. Everything below was checked against the code as it sits on disk, not
against what the docs say the code does. Commands were run with `go build`, `go test`, `go vet`,
`go list`, `grep`, and `make`; nothing was modified.

The short version: the spec and most ADRs are unusually honest about trade-offs, the zero-dependency
claim is real, and all eight runnable examples pass verbatim. But the normative contract — the one
document in this repo whose whole job is to be checkable — contains at least seven claims that are
simply false against the current code, one ADR whose two headline justifications are both
contradicted by the code that cites it, and one ADR whose entire proposed internal architecture was
not built. None of these are amended by ADR-0041 or ADR-0042. This is not a project where the docs
are slightly stale; it is a project where a reader cannot tell, without doing what this review did,
which parts of 7,180 lines of ADR prose and a 576-line contract actually describe the shipped code.

## Findings, most consequential first

### 1. [MAJOR] The contract's PostgreSQL durability disclosure describes a table that does not exist

`docs/spec/01-contract.md:476` (§13, Durability disclosure — the section an operator reads to decide
what survives a crash):

> PostgreSQL | full WAL durability for nodes, edges, and events. **The `leases` table is
> intentionally `UNLOGGED`** — losing a revocable, deadline-bound grant on crash is correct
> behaviour.

There is no `leases` table. `storage/postgres/migrations/0001_init.sql` has exactly four tables —
`scopes`, `nodes`, `edges`, `events` — and lease state (`epoch`, `deadline`, `worker`) is columns
`91-103` of the ordinary, fully WAL-logged `dagw.nodes` table (migration lines 74-129). Nothing in
either migration file uses `UNLOGGED`; a repo-wide grep for the keyword finds it only in the
research dossiers, never in a `.sql` file.

This isn't a stray sentence — `docs/research/04-postgres-backend.md:491-512` builds an entire
argument for exactly this design (§11, "`UNLOGGED` tables"), gives the reasoning ("removes WAL-write
overhead from the highest-churn insert+delete cycle in the schema"), and even supplies the DDL:
`docs/research/04-postgres-backend.md:615-617` reads `CREATE UNLOGGED TABLE dagw.leases (...)`. The
synthesis document (`docs/research/00-synthesis.md:676,719,765`) and the normative contract both
adopted this as settled design. The implementation folded lease fields into `nodes` instead — a
defensible choice on its own (it avoids a second table and a cross-table join on every claim), but
it is the opposite durability trade-off from the one every design document describes, and it means
the highest-churn write path this optimization targeted is *not* the reduced-WAL path the contract
promises. `ADR-0042` amends four other PostgreSQL behaviors discovered during implementation but
says nothing about this one, even though its whole purpose is to catalog exactly this kind of gap.

### 2. [MAJOR] ADR-0029's two "binding on the implementation" corollaries are both false

`docs/adr/0029-minimum-go-version-is-1-25.md` pins the whole repo to Go 1.25 substantially *because*
of two stdlib features, and is explicit that using them is "binding on the implementation, not just
on `go.mod`":

> 1. All internal scheduler timing goes through the stdlib `time` package … or a thin wrapper …
>    **never a bespoke fake-clock abstraction built to route around `time`**. This is what makes
>    wrapping the lease-timeout state machine in `synctest.Test` sufficient on its own, with no
>    parallel fake-clock plumbing to maintain.
> 2. Every internally-spawned … goroutine … is launched via **`subWG.Go(func() { ... })`**, and
>    `Manager.Close` calls `subWG.Wait()` …

Both are contradicted by the code:

- `testing/synctest` is imported **nowhere** in the repository (`grep -rl synctest **/*.go` — zero
  hits). Instead, the exact thing corollary 1 forbids is what ships: `dagstoretest/clock.go:17-129`
  is a hand-rolled `FakeClock` with its own timer list, and `example_test.go:303-330` duplicates it
  again as a second, separate hand-rolled clock (`exampleClock`). This is not an incidental
  omission — it is the "parallel fake-clock plumbing" the ADR says adopting 1.25 makes unnecessary,
  built anyway, twice.
- `sync.WaitGroup.Go` — the actual Go 1.25 API named in the ADR — is used **nowhere**. Every
  goroutine this project launches under a `WaitGroup` uses the pre-1.25
  `wg.Add(1); go func() { defer wg.Done(); ... }()` shape instead: `manager.go:80`,
  `subscribe.go:190`, `subscribe.go:216`, `subscribe.go:311`, `cmd/dagworkerd/daemon.go:228,237`,
  `storage/postgres/notifier.go:47`, `adapters/grpc/watch.go:183`,
  `dagstoretest/trigger_lease.go:179`. Nine call sites, zero uses of the API the ADR was partly
  written to justify.

The Go-1.25 floor may still be the right call for other reasons, but the two concrete, checkable
reasons this ADR gives for it are demonstrably not how the code was written.

### 3. [MAJOR] The contract's `PartitionAssigner` claim (with MUST-level force) is fiction

`docs/spec/01-contract.md:373` (§8, Work distribution):

> The internal `PartitionAssigner` interface **exists from the first commit** with the trivial `P=1`
> implementation, so the v0.5 upgrade … **MUST NOT** change any public signature (ADR-0014).

`ADR-0014` describes this in detail as an internal `Assigner` interface with `Partition` and `Owner`
methods, living under `internal/distribution`, "from the first commit that touches claim routing."
There is no `Assigner`, no `PartitionAssigner`, and no `internal/distribution` package anywhere in
the tree — `internal/` contains exactly one package, a priority-queue (`internal/pq`). `Claim` in
`claim.go` and `store.go` routes directly to `Store.Claim` with no partition-routing indirection of
any kind. `ScopeConfig.PartitionCount` (`config.go:107`) exists as an accepted-but-inert field, which
is consistent with "v1 is pull-based," but the specific interface the contract asserts already
exists, with normative force, was never written.

### 4. [MAJOR] ADR-0028's entire internal architecture for the in-memory backend was not built

`docs/adr/0028-in-memory-backend-internals-soa-dense-handles-sharding.md` is the most heavily
researched ADR in the set — citations to the Go GC guide, Discord's Rust postmortem, BigCache,
Travis Downs' concurrency-cost hierarchy, an ETH Zürich atomic-ops benchmark — and it commits, as
"Accepted," to six specific mechanisms: generation-counted `int32` handles from a slab allocator
(`Slab[T]`, with `gens []uint32` for stale-handle detection), a single string→`int32` interner, CSR
adjacency (`childOff`/`childIdx []int32`, explicitly **never** "`[]*Node`/`[]Handle`-per-node
slices"), bitset-based ready/blocked membership, and "sharded, cache-line-padded `RWMutex` striping
… never a single global mutex" at up to 256 shards keyed by `8×GOMAXPROCS`.

None of it is in `storage/memory`:

- `storage/memory/scope.go:67-107` (`type scope struct`) has exactly one `sync.RWMutex` (line 68)
  per scope, plus one more on the top-level `Store` (`memory.go:28`) guarding the scope map. That is
  the entire mutex population of the package — a `grep` for `sync.RWMutex\|sync.Mutex` across every
  file in `storage/memory` returns exactly those two lines. There is no striping, no shard count, no
  `GOMAXPROCS` reference anywhere in the package.
- Adjacency is `succ [][]int32` and `pred [][]predEdge` (`scope.go:80-81`) — a slice of per-node
  slices, which is precisely the representation decision #3 names and rejects in favor of CSR.
- Ready/lease/scheduled sets are `*pq.Heap` values (`scope.go:92-94`), not bitsets — a reasonable
  choice given priority ordering is required, but not what the ADR specifies, and popcount-based
  cardinality never appears because there is no bitset to count.
- `nodeRec` (`scope.go:30-51`) has no generation field at all; free-list reuse (`scope.go:152-178`)
  is guarded by an `alive bool`, not the `gens []uint32` staleness check the ADR's own code sample
  gives as the reason a slab allocator is safe to reuse indices from.
- `Handle`, `Slab`, `Interner`, and `Bitset` — the four named types the decision section defines in
  Go — do not exist anywhere in the module.

The shipped design (per-scope mutex, map+slice node table, heap-based ready sets) is simpler,
plausibly easier to get right, and — per `make test` — correct. But a reader who takes ADR-0028 as a
description of the code, as its "Accepted" status invites, will be wrong about the mutex model, the
adjacency representation, the membership structure, and the handle-safety mechanism. None of this is
mentioned in ADR-0041 or ADR-0042.

### 5. [MAJOR] The one runnable daemon command in the README does not run

`README.md:174`:

```
dagworkerd --store=postgres --postgres-dsn=... --grpc-addr=:9090 --http-addr=:8080
```

Reproduced directly:

```
$ go run ./cmd/dagworkerd --store=postgres --postgres-dsn=foo --grpc-addr=:9090 --http-addr=:8080
flag provided but not defined: -postgres-dsn
```

The real flag is `--postgres-dsn-file` (`cmd/dagworkerd/config.go:318-319`) — a path to a file
holding the DSN, not the DSN itself, deliberately, per `cmd/dagworkerd/secrets.go`'s whole design
(secrets as file paths, not flag values, so they never land in `ps`, shell history, or a container's
env dump). That design is good. But it means the single concrete, copy-pasteable command the README
offers for the daemon fails on first contact, and fails specifically in the way that erodes trust
fastest: the flag name looks right and isn't.

### 6. [MAJOR] Two sentinel-error names in the contract don't exist in the code — a pattern, not a typo

- `docs/spec/01-contract.md:35` (§1.1): "Violations return **`ErrInvalidIdentifier`** wrapping the
  offending field name." No such symbol exists anywhere in the module. `identifier.go`'s
  `Scope.validate`/`NodeID.validate`/`validateKind` all return `invalidArg(...)`, which wraps the
  generic `ErrInvalidArgument` (`errors.go:78-80`) — the same sentinel a negative duration or an
  unknown enum value produces. A caller who writes `errors.Is(err, dagworker.ErrInvalidIdentifier)`
  per the contract gets a compile error, and one who assumes identifier errors are distinguishable
  from other argument errors is wrong.
- `docs/spec/01-contract.md:446,451` (§12): "there is no fallback path and no **`ErrCapability`**
  escape for these," "it either declines with **`ErrCapability`**." The shipped name is
  `ErrUnsupported` (`errors.go:89-91`). This isn't just the contract: `docs/research/00-synthesis.md:555`,
  `docs/research/06-memcached-and-storage-abstraction.md:601`, `docs/adr/0016-*.md:66,131,151`, and
  `docs/adr/0017-*.md:140` all use `ErrCapability` consistently. The rename to `ErrUnsupported`
  happened during implementation and was never propagated back into a single one of the four
  documents that name the old identifier.

Two independent sentinel renames that never made it back into the docs, on top of two missing
interfaces (#3, #4) and a missing table (#1), is a pattern: late implementation decisions in this
project routinely do not get reflected back into the design documents that describe them, and
nothing in the process (ADR-0041/0042 exist for exactly this purpose) caught these.

### 7. [MAJOR] The contract's required observability metric doesn't exist

`docs/spec/01-contract.md:291-292` (§6.2): "The library **MUST** export a
`topo_fastpath_hit_ratio` metric as an observability signal." A repo-wide search for that string, or
for any metrics/expvar/Prometheus/OpenTelemetry hook in the root package, finds nothing. The daemon
does expose a `/metrics` endpoint (`cmd/dagworkerd/admin.go:104-127`), but it is explicitly scoped —
its own comment says "an honest subset: process identity, uptime, and the same reachability signal
`/readyz` already computes" — and does not include this metric either. The `topo_fastpath_hit_ratio`
name appears only in `docs/adr/0004-*.md` and `docs/adr/0041-*.md`'s text ("still exported, now
purely as an observability signal"), never in code.

### 8. [MAJOR] `make check` — the contribution bar `CONTRIBUTING.md` states as a standing fact — fails on a clean checkout

`CONTRIBUTING.md:8,17`: "**`make check` must pass.** That is `tidy-check`, `lint`, `race` and
`cover`." … "If `make check` fails on `main`, that is a bug and a report is welcome."

Running it:

```
$ make check
...
storage/postgres/graph.go:33:1: cognitive complexity 22 of func `settleTouched` is high (> 20) (gocognit)
storage/postgres/engine.go:533:18: func (*engine).createNode is unused (unused)
2 issues:
* gocognit: 1
* unused: 1
make: *** [lint] Error 1
```

Both are real: `settleTouched` (`storage/postgres/graph.go:33`) exceeds the project's own
`gocognit` threshold (`ADR-0039`'s coding-standards config), and `createNode`
(`storage/postgres/engine.go:533`) is genuinely dead — it was superseded by the pipelined
`createNodes` (`storage/postgres/pipeline.go:67`, called from `graph.go:137`) and nothing calls the
singular version anymore. `make test` and `make race` do pass cleanly across every module including
`storage/postgres` and `storage/redis` against the live test databases, so the substantive code is
healthy — but the specific, named quality gate this repository asks contributors to hold themselves
to is red right now, most likely a straggler from the same recent PostgreSQL-backend work that left
ADR-0042 and the `leases`-table gap (#1) uncaught.

## Everything else, in brief

### [MINOR] Conformance-test count is stale by ~23%

`README.md:163`: "That is roughly 65 named tests." Running the suite
(`go test -run TestConformance -v ./storage/memory`) and independently deduping every `"T-..."`
literal across `dagstoretest/*.go` both land on **80**, not "roughly 65." Two of the four
`dagstoretest` files (`conformance.go`, `trigger_lease.go`) have a later mtime than `README.md`,
consistent with tests having been added after the count was last written and never re-checked.

### [MINOR] `goleak` is named in the contract but never used

`docs/spec/01-contract.md:389` (§9): "A `goleak` assertion **MUST** cover this on every backend."
`uber-go/goleak` is not a dependency anywhere in the module graph. The property is tested — but by a
hand-rolled `runtime.NumGoroutine()` before/after comparison with a polling grace period
(`manager_test.go:498-530`, `TestCloseLeavesNoGoroutines`) — a reasonable substitute for a single
test on one backend, but not the named tool, and not "every backend."

### [MINOR] `EventCreated` is a real, well-documented event kind the contract never mentions

`event.go:14-38` defines three `EventKind` values — `EventCreated`, `EventTransition`,
`EventReady` — each with a substantial doc comment; `EventCreated`'s explains it exists precisely so
"a subscriber maintaining a live view of the graph does not have to know a trick to spot a new
node." `docs/spec/01-contract.md §7.1`'s table lists only two kinds, `EventTransition` and
`EventReady`. This is not an ADR-0041/0042 amendment either — the contract's event-kind table is
simply short one row for a kind that materially changes what `SubscribeOptions.Kinds` can filter on.

### [MINOR] `Typed[T].decode`'s comment describes behavior the code doesn't have

`typed.go:54-57`: "A payload that fails to decode is a poison node: it would fail identically on
every attempt, so **it is failed immediately rather than retried** … Silently retrying a node that
can never succeed is how a queue fills up with work nobody looks at." The implementation
(`typed.go:76-88`) calls the ordinary `t.m.Nack(ctx, lease, decodeErr)` with `Reason: ReasonWorkerError`
— the identical path an ordinary worker failure takes. With the library's own default
`MaxAttempts = 3` (`config.go:28`), an undecodable payload is retried twice more, each after a
jittered backoff, before terminally failing: exactly the "silently retrying a node that can never
succeed" outcome the comment claims this code avoids. The comment is describing an intent
(special-case a poison payload as non-retryable) that was never implemented; only the caller-visible
symptom — `Claim`/`TryClaim` returning the decode error immediately instead of the caller retrying
its own loop — resembles what the comment describes.

### [NIT] README's ADR count is off by one

`README.md:260`: "the 41 architecture decision records." The repo ships 42 (`0001`–`0042`),
including `ADR-0042` itself, dated the same day as the README's last edit — the same
just-landed-and-not-back-propagated pattern as the test count above. (The neighboring claim, "the 15
primary-source research dossiers," is correct: `docs/research/` has 16 files, and the 16th,
`00-synthesis.md`, is explicitly carved out in the same sentence as "the synthesis that reconciled
them.")

## What holds up

- **"The core module has no dependencies."** Verified: `go.mod` at the repo root has no `require`
  block at all, and `go list -deps .` (filtered for stdlib/`internal`/`unsafe`) resolves to exactly
  `github.com/specialistvlad/dagworker` itself. This is the load-bearing claim in the README's pitch
  and it is exactly true.
- **All eight examples in `example_test.go` pass, verbatim, and demonstrate what their comments
  claim.** `go test -run Example -v .` is 8/8 green. `Example_leaseTimeout` genuinely exercises
  fencing (a stale `Ack` from a superseded lease is refused via `ErrLeaseMismatch`);
  `Example_concurrentWorkers` genuinely races 8 goroutines against the in-memory backend's atomic
  claim and asserts zero double-delivery; `ExampleManager_Inspect`'s output ordering
  (`extract-b` left waiting) is not flaky — it follows directly from the documented FIFO tie-break on
  insertion order, not from goroutine scheduling luck.
- **The blocking-claim wakeup protocol** (`claim.go:101-185`) matches ADR-0033's three-part
  description precisely: immediate attempt, doorbell with a bounded wait, jittered poll fallback —
  code and doc comment agree line for line.
- **The performance-ratio testing methodology is real, not aspirational.** `test/perf/complexity_test.go`
  genuinely measures a cost curve across graph sizes and asserts a ratio bound rather than an
  absolute threshold, exactly as the README's Performance section describes; this is one ADR-adjacent
  claim that was checked against actual test code and held up completely.
- **Full-jitter backoff** (`backoff.go`) matches its own doc comment and ADR-0012's description
  exactly, overflow-safe shift-based windowing included.
- **`AsWorker`'s "no bearing on correctness" framing** (`claim.go:30-35`) is consistent everywhere
  it's repeated — the contract, ADR-0035, and the code comment all agree, and the fencing epoch is
  in fact the only thing any Complete/Extend call checks.

## Is the documentation proportionate?

No. Non-test Go source is ~26,300 lines; Go tests are ~7,900 more. Markdown documentation totals
~26,000 lines — essentially matching the shipped implementation line for line — of which the 16
research dossiers alone are ~16,800 lines (nearly two-thirds of all documentation, almost as long as
the entire implementation) and the 42 ADRs are another ~7,200. The actual normative contract, the
one document a maintainer can be held to, is 576 lines — proportionate on its own, but a sliver next
to the research and decision layers built on top of it.

That volume would be easy to defend if it stayed synchronized with the code, the way the
zero-dependency claim and the examples do. It does not: of the roughly two dozen ADRs and contract
clauses checked directly against source for this review, at least four ADR-level claims (the
`leases` table, the two ADR-0029 corollaries, the `PartitionAssigner` interface, ADR-0028's entire
internal architecture) describe something that plainly was not built, and none of the four is
flagged by ADR-0041 or ADR-0042 — the two amendment records that exist for precisely this purpose.
A research and decision corpus this large is a genuine asset when it is trustworthy, because it
means a maintainer never has to guess why a rule exists. Once a reader has caught it being wrong
about a database table, a stdlib API, an internal interface, and an entire subsystem's data
structures, the remaining 30-odd ADRs stop being free evidence and start being 7,000 lines that each
need the same treatment this review gave twelve of them.

## Verdict

The engineering underneath is real and, on the evidence of `make test`/`make race` and the
conformance suite, correct: 80 named backend-agnostic tests pass on the in-memory backend, the
zero-dependency claim is exactly true, and every runnable example does what its comment says. But
this review was asked to check whether the documentation is true, and on that specific question the
answer is: substantially, but with enough load-bearing exceptions that the documentation cannot be
trusted on its own — it must be checked against source every time, which defeats much of its
purpose. The normative contract, the single document with RFC-2119 force, is wrong about a database
table, two error-sentinel names, an internal interface's existence, and a required metric. One ADR's
two stated justifications for the project's Go-version floor are both contradicted by the code that
cites them. Another ADR's entire proposed data-layout for the highest-performance-sensitive backend
was replaced by something simpler that was never written back into the record. And the project's own
`make check` — the bar `CONTRIBUTING.md` states contributors must clear — currently fails on a clean
checkout of `main`. None of this is disqualifying on its own; together, it says the documentation
process here generates prose faster than it reconciles that prose with what actually got built, and
a team staking a production system on this library should read the contract as a strong hint, verify
every guarantee they actually depend on against the conformance suite and the source, and not assume
that "documented" means "true."
