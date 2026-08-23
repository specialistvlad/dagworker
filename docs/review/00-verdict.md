# Verdict — dagworker, synthesized from six independent reviews

This is a synthesis, not a seventh independent pass. Six reviewers each spent a full pass on one
lens — correctness/concurrency, API/Go idiom, documentation-vs-reality, test quality,
operations/failure-modes, and "does this make sense" — and all six are in
`docs/review/0{1..6}-*.md`. Every claim below traces back to one or more of those six; file:line
citations are theirs, carried forward here without re-verification. Where the six disagree, that
is called out explicitly and resolved with a stated position rather than averaged away.

---

## 1. Is this any good, and would I use it

**Yes, cautiously, for one specific job, not yet for the job the README opens with.**

The core scheduling primitive — a dynamic DAG, fenced leases, incremental readiness, cycle
rejection at insert time — is genuinely well engineered. All six reviewers, working
independently and looking for different things, converge on the same conclusion about the
concurrency core: the phase+epoch fencing check, the `SKIP LOCKED` claim path, the scope-wide
advisory lock for structural edits on Postgres, and the Pearce-Kelly incremental topological
order are implemented correctly and consistently across memory, Redis, and PostgreSQL, and hold
up under `-race` and real multi-instance contention. Six people trying to break the same
mechanism from six different angles and not finding a hole in the mechanism itself is a real
signal, not a coincidence.

But "good mechanism" is not the same question as "good to deploy." Layered on top of that solid
core are two classes of problem that would each, independently, keep me from staking a
production system on this today:

1. **The parts the happy path doesn't exercise are unfinished, not un-thought-of.** A caller-
   controlled `Claim` batch size with no ceiling anywhere in the stack, a background maintenance
   loop with no per-call timeout, a doorbell error handler that spins instead of backing off, and
   a fencing epoch that resets when a node ID is recycled are all real bugs in code that is
   otherwise careful — and every one of them is the kind of gap a load test or a chaos day finds
   in week one of real traffic, not a design flaw that needs rethinking.
2. **The documentation cannot be trusted at face value**, and this project's whole pitch rests on
   documentation: 42 ADRs, a 576-line "normative" contract, 16 research dossiers. Multiple of
   those documents assert things about the shipped code — a table's durability mode, an internal
   interface's existence, a testing architecture, a wakeup algorithm — that are simply not true.
   For a project whose central claim to trustworthiness is "read the contract, it's normative,"
   that is a serious problem independent of whether the underlying code is fine (it mostly is).

I would embed the library today, on Postgres or in-memory, behind my own trusted worker fleet,
for a moderate-scale pipeline workload — after fixing the batch-size and maintenance-loop issues,
which are each a few days of work in code that already knows how to do the equivalent thing
correctly somewhere else in the same repo. I would not run `dagworkerd` on a network segment
shared with anything I didn't fully trust, and I would not point a team at the ADRs/contract as a
reliable substitute for reading the source, until the fix list below is substantially cleared.

The project is eight hours old by git history (review 06, §1), tagged once, with zero known
users. Every claim about "the niche this fills" and "the trigger rules that actually come up" is
a hypothesis, stated with the confidence of a track record it does not yet have.

---

## 2. THE FIX LIST

Ordered by what a maintainer should pick up first. Tier 0 is "would not ship as-is." Tier 1 is
"fix before freezing a 1.0 API/wire surface." Tier 2 is "the documentation actively misleads a
reader and must be reconciled." Tier 3 is "the test/process infrastructure has real gaps that
should close before the next big claim gets made on top of them."

### Tier 0 — would not ship

**1. `Claim`/`ClaimBatch`'s node count has no upper bound anywhere in the stack — a live DoS on Redis, reachable over the unauthenticated HTTP adapter.**
`claim.go:45-103` (`buildClaim`), `storage/memory/lease.go:140-196`, `storage/redis/ops_lease.go:17-32` + `storage/redis/lua_scripts.go:260-317` (`while granted < maxN do`), `storage/postgres/lease.go:97-133`, `adapters/http/claim.go:81` + `adapters/http/wire.go:374` (`MaxNodes int`, no range check).
Every other batch-shaped operation in this codebase (`AddNodes`, `Sweep`, `promoteScheduled`) is
explicitly capped by `MaxBatchSize` or an equivalent constant; `Claim` is the one path nobody
bounded. On Redis this loop runs inside one atomic, single-threaded Lua script, so a caller
asking for a few hundred thousand leases freezes the *entire Redis instance* — every scope, every
tenant — for the duration. The HTTP adapter passes a client-supplied `max_nodes` straight through
with zero validation, and that endpoint has no authentication, making this trivially triggerable
by anyone who can route a packet to the published port. Contradicts the normative complexity
table at `docs/spec/01-contract.md:407` (`Claim | O(log R)`), which the unbounded batch defeats.
Reported independently, as the top BLOCKER, by both review 01 and review 05.
**Effort: small.** Clamp `Max` against `ScopeConfig.MaxBatchSize` (or a dedicated field) once, in
`buildClaim`, and every backend gets the protection for free.

**2. A doorbell that errors turns `Claim` into an unbounded busy loop.**
`claim.go:151-185`, the `default` branch of `waitForWork`'s error switch. Every branch, including
`default`, returns immediately; nothing waits out the poll interval on a doorbell error. Verified
by a standalone repro: **1.87 million `store.Claim` calls in one second** against a broken
doorbell, each also emitting a `WarnContext` log line. Against Redis or Postgres this is not a
CPU curiosity, it is a connection-flooding, log-saturating incident triggered by exactly the
transient fault (a Redis/Postgres blip) the doorbell design exists to degrade gracefully from.
The one test covering this path (`fault_test.go:188`, `TestClaimFallsBackWhenTheDoorbellFails`)
only asserts the claim eventually succeeds, so it is green whether or not the fallback waits.
(Review 02, finding 1.)
**Effort: small.** Make the `default` branch wait out `wait`/select on `wctx.Done()` the same way
the non-doorbell branch already does two lines below it.

**3. Deleting and re-adding a node under the same `NodeID` resets the fencing epoch to zero on all three backends, and a stale lease from the deleted generation can be honored against the new one.**
`storage/memory/graph.go:67-93`, `storage/postgres/migrations/0001_init.sql:92` (`epoch bigint NOT NULL DEFAULT 0`), `storage/redis/lua_prelude.go:605`. The fencing check everywhere is `phase == Claimed AND epoch == presented`, scoped only to the current row — `storage/memory/lease.go:220-224`, `storage/postgres/lease.go:336-349`, `storage/redis/lua_scripts.go:332-336`. `RemoveNode` hard-deletes the row; a later `AddNode` under the same ID is indistinguishable from a fresh node and starts back at `epoch = 0`. Because the most common value any node's epoch ever holds is 1 (its first claim), the very first claim of two generations of the same ID collide by default: an honest, merely-delayed worker's stale `Ack` from a deleted generation is accepted against the new generation's live lease. This breaks the exact "a dead worker cannot lose work" guarantee the README leads with, with no error and no log line, in a sequence (`RemoveNode` then re-`AddNode`) `docs/adr/0036` itself calls ordinary usage.
(Review 01's top finding; reproducible sequence given in full there.)
**Effort: medium.** Needs a monotonically increasing, tombstoned generation counter per `NodeID`
that survives the row's deletion, seeded into a new generation's starting epoch above every value
that ID has ever issued. Postgres has the easiest path (fence on the never-reused `bigserial`
internal id instead of `node_id`); memory and Redis need an explicit persisted counter. At
minimum, document the hazard loudly next to `RemoveNode` until it is fixed — today's doc comment
says nothing about it.

**4. The background maintenance loop has no per-call timeout and no fault isolation between scopes — one slow backend call stalls reclaim and retention for the whole process.**
`manager.go:403-434` (`maintain`), running on `bgCtx` (`manager.go:76`, cancelled only by `Close`,
no deadline). Neither Postgres nor Redis sets a server-side statement timeout anywhere in this
repo. A single hung/slow backend call (DB under load, a half-open TCP session during a partition)
parks the one maintenance goroutine forever: every scope after the stuck one in that tick never
gets swept or collected, no further ticks ever fire, and there is no log line (the `WarnContext`
calls only fire on a *returned* error, never on a call that hangs) and no metric. The damage is
silent and fleet-wide: idle scopes' dead-worker leases linger indefinitely, and
`TerminalRetention`-driven GC silently stops everywhere the moment any one scope's backend call
hangs — during precisely the kind of incident (DB slow, partial partition) where storage pressure
is most likely to already be a live problem. `fault_test.go`'s fault injection covers only
immediate errors, never latency, so this exact failure mode is untested.
(Review 05, finding 2.)
**Effort: small-to-medium.** Wrap each `sweepScope`/`collectScope` call in a bounded
`context.WithTimeout` derived from `bgCtx`, and log/count a per-scope timeout as its own event
rather than letting it stall the loop silently.

**5. No authentication or authorization on the worker-facing network surface, which is exactly where finding #1's DoS is reachable from.**
Grepped across `adapters/grpc` and `adapters/http`: the only "credential"-shaped thing found is
the lease fencing token, which is explicitly documented as carrying no privilege
(`claim.go:31-34`). Nothing in `grpcadapter.New`/`httpadapter.New` accepts an auth interceptor or
middleware hook. The TLS gap is honestly disclosed in `cmd/dagworkerd/README.md`'s "Known
limitations"; the authorization gap is disclosed nowhere. The published (non-loopback) ports —
per the daemon's own `docker-compose.yml` — are exactly the ports that can claim work meant for
someone else, cancel scopes, add poison nodes, or trigger #1.
(Review 05, finding 8; independently the enabling condition review 01 assumes for #1's severity.)
**Effort: small to document, medium to fix properly.** At minimum, state the trust assumption as
loudly as the TLS one already is ("this daemon assumes every caller is already trusted; put it
behind a mesh/gateway that authenticates before proxying"). Properly: an auth hook on both
adapters before the daemon is called production-ready.

### Tier 1 — fix before freezing the 1.0 API or wire surface

**6. Two concurrent `Complete` calls can deadlock in PostgreSQL, and PostgreSQL's `40P01` is not mapped to anything the caller can act on.**
`storage/postgres/engine.go:400-451` (`terminate`'s BFS cascade), `successorsForUpdate` at
`:256-278` locks a node's *direct* successors in ascending-id order, but that ordering is only
local per node, not global across BFS levels; two independent roots whose cascades share
descendants at different depths can lock them in opposite relative order. A concrete crossing
graph is constructed in review 01 (ids 1, 5, 100, 6 vs. 2, 6, 100) that produces a textbook AB-BA
deadlock. Postgres's own deadlock detector aborts one transaction with a plain wrapped `40P01`
error, which is not `ErrLeaseMismatch`, not a context error, and nothing retries it. The
transaction rolls back cleanly (no data loss), but the caller has no principled way to know that.
Needs a wide "several independent roots feeding a shared diamond at different depths" shape to
trigger — consistent with the finding in review 04 that no conformance test exercises
`Complete`/`Ack` under real concurrency at all.
**Effort: medium.** Retry a `40P01` inside `Complete`/`Extend` (safe — the rolled-back write is
idempotent by construction), or map it to a documented, explicitly-retryable sentinel.

**7. `Typed[T]`'s "poison node" guarantee is false under the library's own default configuration.**
`typed.go:52-88`. The doc comment says an undecodable payload "is failed immediately rather than
retried." The implementation calls the ordinary `Nack` path, which is not terminal on the first
call; under the default `MaxAttempts: 3` (`config.go:28`) a payload that can never decode is
retried twice more before dying. Verified by repro: node stays `StatusNew`, `Attempt=1`, across 5
`TryClaim` calls with no scope configuration. The one test covering this (`typed_test.go:69-92`)
passes only because it overrides `MaxAttempts: 1`, which is what actually makes the failure
terminal — masking that `decode()` itself does nothing special.
(Review 02, finding 2; documented independently as a doc/code mismatch by review 03.)
**Effort: small.** Route `decode()`'s failure through a path that is unconditionally terminal, the
way `Skip` already is.

**8. Redis's blocking-`Claim` wakeup opens one dedicated pool connection per blocked caller — a modest idle-worker count can exhaust the pool and starve unrelated Redis operations fleet-wide.**
`storage/redis/watch.go:195` (`s.rdb.Subscribe`, holds a pool connection for the subscription's
whole life), `storage/redis/redis.go:140` (client opened with default `PoolSize`, i.e.
`10*GOMAXPROCS(0)`, never customized). 50 concurrently blocked workers on one idle scope — well
below the "10,000 idle workers" sizing example `docs/adr/0033` itself uses — can exceed a typical
20-40 connection pool and starve claims, `Ack`, and the readiness probe of a connection for
everything else on that client. Postgres and in-memory both correctly share one
connection/broadcast per store instead.
(Review 05, finding 3.)
**Effort: medium.** Multiplex all of a store's `WaitForWork` calls onto one shared subscription
per scope, mirroring the Postgres `notifier` design this codebase already has.

**9. The counting-signal doorbell `docs/adr/0033` specifies does not exist; all three backends broadcast-wake-all — the exact design the ADR's own "Alternatives considered" rejects.**
`storage/memory/watch.go:78` (close-and-replace channel), `storage/postgres/notifier.go:71-76`
(same idiom), Redis pub/sub (protocol-level broadcast). No `doorbell.Register`/`Signal(n)` type
exists anywhere. Correctness is not at risk (a woken loser just re-registers), but the specific
performance property the ADR spends paragraphs justifying — O(readied nodes), not O(blocked
waiters) — is false for the shipped code on every backend, under exactly the large-fan-out-plus-
many-idle-workers load pattern the ADR calls out as the risk case. Neither `docs/adr/0041` nor
`0042` — the two documents that exist to catalogue exactly this kind of gap — mentions it.
(Review 05, finding 4.)
**Effort: large to build the real thing; small to be honest about it.** Recommend rewriting
ADR-0033 now to describe the shipped broadcast design and its actual cost, and treat the counting
signal as a deferred optimization, not a shipped guarantee.

**10. `Subscribe`'s fan-out cost is paid by every write in the process, not just the affected scope.**
`subscribe.go:238-269` (`publish`), `manager.go:31` (`m.subs`, one flat map across every scope).
Every mutating call (`AddNodes`, `Claim`, `Ack`/`Nack`/`Extend`, etc.) iterates *every*
subscription in the `Manager`, regardless of scope, for every effect it produces — synchronous,
inline, on the caller's own goroutine. A deployment with a few thousand small scopes, each with
its own status subscriber (the deployment shape the ADRs repeatedly target as first-class), pays
a few-thousand-iteration bystander tax on every `Ack` in every unrelated scope.
(Review 05, finding 7.)
**Effort: medium.** Index `m.subs` by scope so `publish` only iterates subscribers who could
possibly want the event.

**11. Per-scope `SweepInterval` is stored, validated, and echoed back through every adapter — and never actually consulted.**
`manager.go:406`: `interval := m.cfg.defaults.Resolved().SweepInterval` is computed once from the
`Manager`-wide construction-time default and used for every scope's cadence. A per-scope override
is accepted, persisted, and silently ignored, with no error, log, or validation catching the
mismatch — directly contradicting the "many small scopes with different SLAs" premise multiple
ADRs (0033, 0034) build on.
(Review 05, finding 5.)
**Effort: small-to-medium.** Either derive per-scope sweep timing from the stored config, or
delete the field and document that sweep cadence is process-wide.

**12. `TerminalRetention`'s "off by default" footgun has no observability, despite `docs/adr/0034` explicitly requiring it be made diagnosable.**
`cmd/dagworkerd/admin.go`'s `/metrics` exposes nothing about the graph — no node count, nothing
sourced from `Manager.Stats`/`ScopeStats.Total`, which already computes exactly this number in
O(1). Combined with the daemon's own `--store=memory` default, unbounded node growth in a
long-running deployment ends in an unannounced OOM-kill.
(Review 05, finding 9.)
**Effort: small.** Add per-scope `Total`/`NonTerminal` gauges to `/metrics` — no adapter
instrumentation hook required, the number is already computed.

**13. `AsWorker`/`ClaimRequest.WorkerID` is a documented observability feature with literally no read path in any backend.**
`claim.go:30-35`, `store.go:72-75`. Written on claim, cleared on completion, in both memory and
Postgres; never read back into `Node`, `Inspection`, `Effect`, or `Event` anywhere. No public type
has a field to put it in. A caller who dutifully calls `AsWorker("worker-7")` gets nothing, with
no compiler or test signal telling them so.
(Review 02, finding 4.)
**Effort: small-to-medium, but decide before freezing.** Add `ClaimedBy string` to `Node`/
`Inspection`, or drop `AsWorker` until there's somewhere for it to go — either is cheap now and
expensive after 1.0 freezes the shape.

**14. `ErrLeaseExpired` is a dead sentinel wired into both network adapters but never produced by any backend.**
`errors.go:50-52`, `adapters/grpc/errors.go:42`, `adapters/http/problem.go:76`. It is part of the
frozen public taxonomy (`errors.go:11-13` says changing the taxonomy is a versioned, deliberate
act) but is never constructed anywhere; the doc comment's claimed distinction from
`ErrLeaseMismatch` ("epoch matches but deadline has passed, checked independently") corresponds
to nothing any backend's `Complete`/`Extend` actually checks — both compare only epoch.
(Review 02, finding 5.)
**Effort: small.** Implement the deadline-independent check the doc promises, or remove the
sentinel before it's frozen in alongside two adapters that already reference it.

**15. `Typed[T]`'s encode/decode errors escape the error taxonomy entirely.**
`typed.go:41,80,96,117`. `errors.go:8-9` states every error the package returns wraps exactly one
sentinel; `Typed[T]`'s JSON errors wrap only the raw `encoding/json` error. Verified:
`errors.Is` against every relevant sentinel returns `false`. A caller wanting to branch on "this
payload is corrupt" has no supported way to do it in the convenience layer most likely to be a
new user's first contact with the library.
(Review 02, finding 3.)
**Effort: small.** Wrap through `ErrInvalidArgument` (or a dedicated sentinel) the same as every
other public entry point.

### Tier 2 — documentation actively misleads; reconcile before trusting the paper trail again

These are grouped because the fix for essentially all of them is the same kind of work — a
documentation pass reconciling the contract/ADRs against what shipped — even though the
individual claims are scattered across a dozen files.

**16. The "normative" contract disagrees with the shipped `Store` interface in several checkable ways, none reconciled by ADR-0041/0042.**
- `ErrInvalidIdentifier` (`docs/spec/01-contract.md:35`) doesn't exist; the real mechanism is
  `*InvalidArgumentError`/`ErrInvalidArgument` (`errors.go:118-134`).
- `ErrCapability` (`docs/spec/01-contract.md:446,451`, and `docs/adr/0016`, `0017`, two research
  dossiers) doesn't exist; the shipped name is `ErrUnsupported` (`errors.go:89-91`).
- `ConditionalDeleter`/`BatchClaim` (§12, contract) don't exist; `Doorbell`/`Collector` do exist
  and aren't in the contract's facet list (`store.go:296-413`).
- `EventCreated` (`event.go:13-22`), a first-class `EventKind` with its own semantics, is entirely
  absent from the contract's events table (§7.1).
- `WithPollInterval` shipped as a `Manager`-level `Option` (`config.go:268-274`), the exact
  one-size-fits-all shape `docs/adr/0033` explicitly argues against as a `ClaimOption`.
- The required `topo_fastpath_hit_ratio` metric (§6.2, MUST-level) doesn't exist anywhere —
  no metrics/expvar/Prometheus/OpenTelemetry surface exists in the core package at all.
(Reviews 02 and 03, overlapping findings, consolidated here.)
**Effort: medium.** One focused pass reconciling `store.go`'s facet list, the error sentinel
names, the events table, and the poll-interval option against the contract text; either implement
the metric or downgrade the MUST.

**17. ADR-0040 (Accepted, dated current) describes a testing architecture that does not exist anywhere in the repo.**
`docs/adr/0040-testing-strategy-coverage-and-ci.md` describes, in the present tense: `testing/
synctest`-based determinism, `pgregory.net/rapid` property tests with four named mandatory
properties, a seeded chaos harness at `internal/chaos`, a same-process Porcupine linearizability
checker, a `test/feature/` tier, `GOCOVERDIR`/`covdata` e2e coverage, and a CI complexity gate
that asserts cross-backend throughput ratios via `BenchmarkComplexity_*`. A repo-wide grep for
every one of `synctest`, `rapid`, `porcupine`, `chaos`, `feature`, `covdata` returns zero hits.
`internal/` contains exactly one package (`internal/pq`). The actual CI complexity job runs
`TestComplexity_*` functions with a materially different shape. The ADR's own "Consequences"
section claims, present tense, that these techniques "converge... a bug that slips past one is
unlikely to slip past all three" — false today. What actually backs the concurrency claim is two
real hand-written goroutine races (`T-CLAIM-ATOMIC` and the cross-process Redis/Postgres tests) —
genuinely good, but a narrower bet than the ADR advertises.
(Review 04's top finding, rated BLOCKER there on documentation-integrity grounds; see §3 below for
how this review resolves that rating.)
**Effort: small to fix the document; large to build what it describes.** Rewrite ADR-0040 to
describe what actually ships today, and treat the property-based/chaos/linearizability layer as a
real, worthwhile, *future* project rather than a shipped one.

**18. ADR-0029's two "binding on the implementation" corollaries — no bespoke fake-clock, and every goroutine launched via `subWG.Go(...)` — are both false.**
`testing/synctest` is imported nowhere. Two separate hand-rolled fake clocks exist instead
(`dagstoretest/clock.go:17-129`, `example_test.go:303-330`). `sync.WaitGroup.Go` (the actual Go
1.25 API the ADR cites) is used at zero of the nine goroutine-launch sites; all nine use the
pre-1.25 `wg.Add(1)`/`defer wg.Done()` shape (`manager.go:80`, `subscribe.go:190/216/311`, and six
more listed in review 03).
**Effort: small.** Either adopt the two APIs the ADR claims are load-bearing, or rewrite the ADR
to state the actual (also reasonable) reasons for the 1.25 floor.

**19. ADR-0028's entire proposed in-memory backend architecture — slab-allocated generation-counted handles, CSR adjacency, bitset ready/blocked sets, 256-way sharded mutexes — was never built.**
`storage/memory/scope.go:67-107` has exactly two mutexes in the whole package (one per-scope, one
on `Store`), slice-of-slices adjacency (the exact shape the ADR names and rejects), heap-based
ready sets instead of bitsets, and no generation field on freed handles. The shipped design is
simpler, plausibly easier to get right, and — per `make test` — correct. Neither ADR-0041 nor
0042 mentions the replacement.
**Effort: small.** Rewrite ADR-0028 to describe the simpler design that shipped; no code change
needed, the simpler design is fine.

**20. The PostgreSQL durability disclosure describes a table that does not exist.**
`docs/spec/01-contract.md:476` says the `leases` table is intentionally `UNLOGGED`. There is no
`leases` table; lease fields (`epoch`, `deadline`, `worker`) live in the fully WAL-logged
`dagw.nodes` table (`storage/postgres/migrations/0001_init.sql:74-129`). This is the opposite
durability trade-off from the one every design document (the research dossier, the contract)
describes and adopted as settled.
**Effort: small to fix the disclosure text.** Separately worth a real decision: is the
higher-durability nodes-table design (current) or the lower-write-amplification UNLOGGED-split
design (as documented) actually wanted? Either answer is fine; right now neither is accurately
recorded.

**21. `README.md:174`'s only runnable daemon command fails on first contact.**
`dagworkerd --store=postgres --postgres-dsn=...` — the real flag is `--postgres-dsn-file`
(`cmd/dagworkerd/config.go:318-319`), reproduced live as `flag provided but not defined:
-postgres-dsn`. The file-path-not-value design is good (keeps secrets out of `ps`/shell
history); the copy-pasteable example in the README is simply wrong.
**Effort: trivial.**

**22. `make check` — the bar `CONTRIBUTING.md` states contributors must clear — fails on a clean checkout.**
`storage/postgres/graph.go:33` (`settleTouched`, cognitive complexity 22 > 20) and
`storage/postgres/engine.go:533` (`createNode`, genuinely unused, superseded by
`pipeline.go:67`'s `createNodes`). `make test`/`make race` are green; `lint` is red.
**Effort: trivial.** Delete the dead function, refactor one function under the complexity
threshold.

**23. `cmd/dagworkerd`'s actual shutdown behavior and metrics scope diverge from ADR-0037's stated decision, and neither divergence is recorded in the ADR or the deviation ledger.**
`docs/adr/0037` states the daemon does "graceful shutdown that actively releases the replica's
in-flight leases" and ships "RED/USE metrics." `cmd/dagworkerd/daemon.go:17-27` deliberately does
the opposite (arguably the more correct choice — never releasing a lease a worker might still
complete), and `admin.go:94-131`'s `/metrics` exposes four process gauges, nothing RED/USE-shaped.
Both are honestly disclosed in README/code-comment prose, but neither is recorded in the ADR
itself or the two documents (`0041`, `0042`) that exist specifically to catch this.
**Effort: trivial.** Write the amendment; no code change needed — the shipped behavior is fine.

**24. The README overstates or omits several things the project's own research already knows.**
The network-surface justification (`docs/adr/0037`) cites a cross-language client story and a Buf
Schema Registry publish pipeline "on every merge to main" — neither exists (no non-Go client
anywhere, no `buf push` step in either CI workflow). The README's "no coordinator, no leader
election" pitch (`README.md:63-65`) omits the throughput ceiling the project's own research
already found: pull-based competition means every claim serializes through one hot key/region
(`docs/research/07-work-distribution-across-instances.md:65`), and v1 ships the trivial `P=1`
partition assigner with real partitioning deferred to an unbuilt "v0.5 upgrade"
(`docs/spec/01-contract.md:435`). The trust model (a malicious worker can forge an epoch and steal
a node) is documented only in the spec (`§4.6`), never in the README, which is what a five-minute
evaluator actually reads. PostgreSQL's 21-minute seed time for a million nodes vs. 0.9s
in-memory/34s Redis is disclosed with an unimplemented fix (`pgx.Batch` pipelining, named but not
shipped).
**Effort: small.** A README honesty pass — most of this material already exists correctly in the
research dossiers; it just needs to reach the document people actually read first.

### Tier 3 — test/process infrastructure gaps worth closing before the next big claim

**25. `test/perf`'s Redis/Postgres harness has no isolation and no cleanup — unsafe to run against any shared instance.**
`test/perf/backends_pg_redis.go:24-48` dials plain default addresses with no namespacing;
`seed()` (`backends.go:121-151`) writes into fixed scope names (`"million"`,
`"claim-%d"`) with no `t.Cleanup` ever registered. Contrast with `storage/postgres` and
`storage/redis`'s own conformance tests, which use scratch databases/random keyspaces and clean
up correctly. Review 04 deliberately did not run this suite against the review's own shared
databases for exactly this reason.
**Effort: small.** Mirror the isolation pattern the storage-tier conformance tests already use.

**26. The complexity-ratio CI guard runs a materially weaker check for Redis/Postgres than the README's own math assumes.**
`sizes()` (`complexity_test.go:118-127`) caps networked backends to a 100x span
(`[1_000, 10_000, 100_000]`) unless a human sets `DAGWORKER_PERF_FULL=1`, which the CI-invoked
`make complexity` target never does. Over 100x, an O(√n) regression yields ~10x — comfortably
under the 30x networked bound — where the same regression would correctly fail at the documented
1000x span (~31x). The two backends whose query planners are the plausible regression source get
the weaker check.
**Effort: small.** Set `DAGWORKER_PERF_FULL=1` in the CI job that already runs
`DAGWORKER_INTEGRATION=1`, or relabel the guard's actual bound honestly.

**27. Several conformance-suite gaps mean a backend could diverge from the other two and still pass all 80 tests.**
Multi-kind `Claim` (`ClaimRequest.Kinds` with >1 entry) is never exercised across any of the 14
call sites that use it; `Max <= 0` clamping is never exercised at the store level, despite being
implemented three times independently; `MaxBatchSize` enforcement has conformance coverage in
exactly one backend (memory); no conformance test exercises `Complete`/`Ack` under real
concurrency (the only genuine concurrency test in the whole 80 is `T-CLAIM-ATOMIC`, which covers
claim exclusivity only); payload-aliasing/copy safety is never tested anywhere.
**Effort: medium.** Roughly five to eight targeted tests would close most of this.

**28. A real flake was found and root-caused under the mandated `-race -shuffle=on` run: `TestWorkerRunClaimsAndCompletes` (`adapters/grpc/client/worker_test.go:97-118`).**
The test stores `handled=true` inside the handler before it returns, then races a separate
`GetNode` RPC against the real report path, which only fires later, after the heartbeat goroutine
is closed and joined. This is a test bug (TOCTOU), not a library bug.
**Effort: trivial.** Poll `GetNode` with a bounded retry loop, the way other tests in the same
codebase (`manager_test.go:596-613`) already correctly do.

---

## 3. Disagreements between reviewers, resolved

**Is ADR-0040's fictional testing architecture a BLOCKER or a MAJOR?**
Review 04 rates it BLOCKER, explicitly "on documentation-integrity grounds independent of the
code being otherwise decent." Review 03, surveying a comparable set of ADR-vs-code gaps (the
`leases` table, ADR-0029's corollaries, ADR-0028's architecture, the `PartitionAssigner`), rates
all of its analogous findings MAJOR and states outright "BLOCKERS: none." Both reviewers are
looking at the same class of problem — a normative document asserting something false about the
shipped system — and disagree only on severity label.

**Resolved:** MAJOR, not BLOCKER, but placed at the top of Tier 2 rather than buried in it. A
BLOCKER should mean "would not ship the code as-is"; ADR-0040 being fictional does not change
what actually ships or its correctness — the shipped tests (goroutine races, the conformance
suite) are real and reasonably good, just narrower than advertised. What it does change is
whether a reader can trust the label "Accepted" on any of the other 41 ADRs without independent
verification, which is a trust problem, not a shipped-code problem. It earns its place near the
top of the fix list on the strength of being the single largest gap between claim and reality
anywhere in the repository, not on the strength of putting a production system at risk today.

**Should the network surface (`adapters/grpc`, `adapters/http`, `cmd/dagworkerd`) be cut, or fixed and kept?**
Review 06 recommends cutting it, or deferring it to a validated v2: it's ~43% of the codebase,
justified by a cross-language client story and a Buf Schema Registry pipeline that don't exist,
built before the single-process embedding use case has any adopters. Review 05 evaluates the same
surface from an operations lens and recommends fixing it (auth, the batch-size DoS) rather than
removing it, treating it as a deployable that just needs hardening.

**Resolved: keep the code, demote the claim.** The engineering itself is competent — it builds
clean, the generated artifacts are real, not hand-waved. Deleting ~14,000 lines of working code
over an unvalidated-use-case argument would be a worse trade than the alternative: stop
advertising it as a production-ready, cross-language surface until (a) the unbounded-batch DoS
and missing-auth findings above are actually fixed, and (b) either a non-Go client exists or the
Buf Schema Registry claim is removed from the ADR and README. Ship it, if at all, as clearly
labeled early/optional surface area — not as the second half of the README's headline pitch —
until it has earned the same confidence the core library has.

**A smaller, resolved non-disagreement worth stating plainly:** review 05 independently confirms
that the general poison-node-bounding mechanism (`failAttempt`, the timeout-reclaim path) is
correct — a node that crashes every worker that touches it still exhausts `MaxAttempts` and goes
terminal. This is not in tension with finding #7 above (Typed[T]'s specific "immediate" claim is
false): the general retry-then-terminal mechanism working correctly is exactly *why* a decode
failure eventually terminates — just not "immediately," as `Typed[T]`'s own doc comment claims.
Both things are true at once; it is worth flagging so the two findings aren't misread as
contradicting each other.

---

## 4. What the project genuinely does well

Brief, because the fix list above is already long and the point of this section is not to soften
it — but these are earned:

- **The concurrency core is the real thing.** Six independent reviewers went looking for holes in
  the fenced-CAS/SKIP-LOCKED/advisory-lock/Pearce-Kelly mechanism from six different angles and
  found none in the mechanism itself — only in the parts around it (epoch recycling, unbounded
  batch size) that the happy path never exercises.
- **The conformance suite (`dagstoretest/`, 80 named tests) is thoughtful and mostly non-gameable.**
  It catches the kind of bug a naive single-backend test suite would miss, and the claim "all
  three backends pass the same suite" is a `go test` invocation, not marketing.
- **Zero-dependency core, verified true.** `go list -deps .` resolves to nothing but the module
  itself and the standard library — the one headline claim checked hardest by every reviewer and
  confirmed exactly.
- **The lint configuration is unusually serious**, and reports zero issues on the reviewed
  packages — an allowlist rather than a denylist, exhaustive-switch enforcement, and a `depguard`
  rule enforcing "core has no network import" at lint time.
- **Context/shutdown discipline is real.** `Manager.Close` genuinely waits on every goroutine it
  started; no exported type stores a `context.Context` in a struct field; the daemon's shutdown
  sequencing (`/readyz` fails first, drain both adapters, close the manager, stop admin last) is
  well thought through.
- **The README's non-goals section and performance methodology are unusually honest** for the
  category — three separate places telling a reader "use the other tool instead," and a
  ratio-based performance claim that states its methodology before its numbers.
- **Secrets handling is correct and well explained** — DSNs and passwords are file paths, never
  flag values or env vars that would land in `ps` or shell history.

---

## 5. Two real use cases, crisp enough for a README

Taken from review 06's search for genuine fits (it found two solid ones and one that sounds
plausible but doesn't survive inspection):

1. **Runtime-discovered fan-out pipelines embedded in an existing Go worker fleet.** A processing
   service where an early step determines the fan-out shape at runtime (e.g., a probe step
   discovers N segments, each needing independent processing, with a final step that depends on
   all of them and a cleanup step that must run regardless of outcome). The team is already
   Go-centric, already runs its own workers, and does not want to stand up Airflow or Temporal to
   schedule one subsystem. The differentiator that actually matters here is dynamic `AddNodes`
   mid-scope, not "the graph is dynamic" in the abstract — and fencing genuinely matters, because
   long-running, resource-hungry workers are the ones most likely to be paused-not-dead.

2. **A non-Kubernetes internal CI/CD or release-orchestration tool with a self-hosted runner
   fleet.** Build → test → [lint, security-scan] → publish → [notify, deploy-canary], with a
   lease so a vanished runner doesn't strand the pipeline forever. Real and common — but
   conditional, not unconditional: a team already on Kubernetes should use Argo Workflows instead,
   which already owns this niche with a UI, for free, in-cluster. dagworker wins specifically for
   a bespoke, non-Kubernetes-native deploy tool.

(A third candidate — a Bazel-style distributed build/test scheduler — does not survive
inspection: the actual hard part of that problem, content-addressed caching and incremental
rebuild semantics, is exactly what dagworker doesn't provide, so adopting it there means solving
the hard problem anyway while only getting the dependency-dispatch layer for free.)

---

## 6. What I would cut

- **The claim that this is production-ready over a network.** Not the code — the *claim*. Until
  Tier 0 items #1 and #5 are fixed, `cmd/dagworkerd` should not be presented as something to point
  at an untrusted network segment, full stop.
- **ADR-0040 as written.** A fictional testing architecture actively describing techniques the
  project doesn't have is worse than no ADR at all on this topic; cut the present-tense claims
  now, keep the roadmap framing if the plan is still live.
- **The Buf Schema Registry / non-Go-SDK promise in ADR-0037 and the README**, until at least one
  non-Go client exists. A promised capability with zero supporting evidence is scope that reads as
  delivered and isn't.
- **The `topo_fastpath_hit_ratio` MUST-level requirement in the contract**, until it's built.
  A normative MUST that nothing satisfies undermines every other MUST in the same document.
- **`PartitionAssigner`'s MUST-level "exists from the first commit" claim** (`docs/spec/
  01-contract.md:373`) — downgrade to describe the accepted-but-inert `PartitionCount` field
  that actually ships, or build the interface.
- **`ConditionalDeleter`/`BatchClaim` from the contract's facet list** — replace with the facets
  that actually exist (`Doorbell`, `Collector`).
- **The ADR-0028 in-memory architecture description** — rewrite to describe the simpler design
  that shipped, which is fine on its own merits and doesn't need the more elaborate one to be
  credible.
- **Nothing in the actual concurrency mechanism.** This is the one part of the project that six
  independent adversarial reads could not find a design-level problem with — that's worth stating
  as plainly as everything above states its opposite.
