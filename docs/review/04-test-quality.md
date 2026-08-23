# Test Quality Review

Scope: the test suites under the root module, `dagstoretest/`, `storage/{memory,redis,postgres}`,
`test/perf`, `test/e2e`, and `adapters/*`. I ran the suites (see "Test runs performed" at the
bottom) rather than just reading them, and I read every line of `dagstoretest/` and the root
package's test files.

The short version: the conformance suite and the root package's unit tests are genuinely good —
better than most libraries this size. The problem is not that the tests are theatre; most of them
are not. The problem is that the project's own documentation (ADR-0040, and the README's
performance section) claims a verification architecture considerably stronger than what actually
ships, and two of the mechanisms that *do* ship (the complexity ratio guard, `test/perf`'s database
isolation) are weaker than their own doc comments say. I found one genuine flake under the
mandated `-race -shuffle=on` run and root-caused it.

## 1. BLOCKER — ADR-0040 documents a testing architecture that does not exist

`docs/adr/0040-testing-strategy-coverage-and-ci.md` is `Status: Accepted`, dated 2026-08-22 (i.e.
current). It is not background research — `docs/research/11-testing-verification-and-ci.md` is the
research; this is the normative decision the project claims to have implemented. It describes, in
the present tense, as things the project *does*:

- **`testing/synctest`**-based deterministic tests for "the lease-timeout state machine" (ADR-0040
  lines 60-75, with a worked code example).
- **Property-based testing via `pgregory.net/rapid`**, with four *mandatory* named properties — "No
  double lease," "Eventual readiness," "Antichain invariant," "Acyclicity under interleaving ...
  after every single mutation" (lines 77-89).
- **A seeded chaos harness at `internal/chaos`** injecting latency, transient errors, and torn
  writes into the in-memory store (lines 91-99).
- **A same-process Porcupine linearizability checker** running "on every PR" against
  `Claim`/`Ack`/`Nack`/`Extend` (lines 101-108).
- A **`test/feature/`** directory as one of "four test tiers, with a fixed location each" (line 47
  table).
- **Quarterly mutation testing** over the lease/scheduling core (lines 199-201).
- A CI complexity gate implemented as `go test -bench 'BenchmarkComplexity_.*'` asserting
  cross-backend throughput ratios — "pipelined ≥ 20× unpipelined; in-memory ≥ 100× Redis ≥ 300×
  Postgres — same run" (lines 218, 232-233).
- Coverage instrumentation for the e2e suite via `GOCOVERDIR`/`go tool covdata` against the *built
  `dagworkerd` binary* (lines 178-190).
- A `storage-integration` CI job with a **per-backend matrix**, each leg a separate required status
  check (lines 218-224, 244-246).

I grepped the entire repository for every one of these:

```
$ grep -rln "synctest" --include="*.go" .        # (nothing)
$ grep -rln "pgregory|\"rapid\"|rapid\." --include="*.go" .   # (nothing)
$ grep -rln "porcupine" --include="*.go" .        # (nothing)
$ find . -type d -iname "*chaos*"                 # (nothing)
$ find . -type d -iname "*feature*"                # (nothing)
$ grep -rln "GOCOVERDIR|covdata" --include="*.go" --include="Makefile" --include="*.yml" .  # (nothing)
```

None of it exists. Not a stub, not a partial version, not a `// TODO: not yet implemented` note
anywhere in the ADR or the code. `internal/` contains exactly one package, `internal/pq` (a SQL
helper), not `internal/chaos`. There is no `test/feature/` directory — `find . -type d` lists
`test/e2e` and `test/perf` only. The actual CI job names in `.github/workflows/ci.yml`
(`lint`, `test`, `race`, `coverage`, `integration`, `complexity`, `vuln`, `bench`) don't match the
ADR's matrix (`unit`, `race`, `coverage`, `storage-integration` per-backend, `lint`,
`complexity-ratio-guard`, `benchmark-nightly`), and the actual `complexity` job runs
`go test -run TestComplexity` against hand-timed functions (`test/perf/complexity_test.go`), not
`go test -bench 'BenchmarkComplexity_.*'` against the cross-backend throughput ratios the ADR
specifies (Makefile:92-95, ci.yml:105).

This matters for exactly the reason the ADR itself gives for wanting these techniques: "a green
suite proves nothing about interleavings the Go scheduler simply did not happen to produce on a
given run" (ADR-0040, Context). The ADR's own "Consequences > Positive" section claims, in the
present tense, "The four named rapid properties plus the seeded chaos harness plus Porcupine give
three structurally different techniques converging on the same claim ... a bug that slips past one
is unlikely to slip past all three" (lines 264-267). That sentence is false today. What actually
backs the concurrency claim is two hand-written goroutine races (§6 below) — real, but a much
narrower bet than the ADR advertises, and specifically *not* the three-structurally-different-
techniques redundancy the ADR uses to justify shipping without deterministic simulation or a
heavier Jepsen-style investment.

I do not know whether this is a plan that was written and never built, or a plan that was built and
then quietly reverted without amending the ADR — there's no git history available in this checkout
to distinguish the two ("Is directory a git repo: No"). Either way, a reader who does what the task
brief asked me to do — check the ADRs against the code — walks away with a materially wrong picture
of how this project verifies concurrency correctness. For a library whose entire pitch is
correctness under concurrent, cross-process access, that is the single most damaging finding in
this review, and I rate it a blocker on documentation-integrity grounds independent of the code
being otherwise decent.

## 2. MAJOR — `test/perf`'s Redis/Postgres harness has no isolation and no cleanup

`test/perf/backends_pg_redis.go:24-48` opens the Redis and Postgres backends by dialing the plain
default addresses (`127.0.0.1:16379`, `127.0.0.1:15432/dagworker`) with no namespacing:

```go
New: func(tb testing.TB) dw.Store {
    st, err := redisstore.Open(context.Background(),
        Env("DAGWORKER_REDIS_ADDR", "127.0.0.1:16379"))
    ...
```

Compare this with the same repository's own `storage/redis/conformance_test.go:38-48`, which
generates a random 8-byte hex keyspace per test and deletes exactly that namespace in cleanup, and
`storage/postgres/conformance_test.go:59-90`, which creates a throwaway `CREATE DATABASE
dagworker_test_<pid>_<rand>` per test and `DROP DATABASE ... WITH (FORCE)`s it afterward. Both of
those were clearly written with "this might run against a shared instance" in mind. `test/perf`
was not: `test/perf/backends.go:121-151` (`seed`) writes into fixed, human-readable scope names —
`"million"`, `fmt.Sprintf("claim-%d", n)`, `fmt.Sprintf("chain")`, etc. — and neither `seed` nor
`integrationBackends`'s `New` registers any `t.Cleanup` that removes what it wrote. Only the
connection is closed.

Run `make complexity`, `make million`, or `make integration` (which loops `test/perf` into its
module list, Makefile:9) against any Postgres/Redis instance that is not both empty and exclusively
owned for the run, and you get up to 1,000,000+ rows written under those fixed scope names,
permanently, with no cleanup path — the opposite of the careful isolation the same repository's
storage-tier conformance tests demonstrate is easy to do. Two concurrent runs (two developers, two
CI jobs, or — the exact situation this review's own task brief describes, "another process is using
them") will step on each other's scopes; a scope name collision with pre-existing unrelated data
returns `ErrIDConflict` and fails the run outright, or, if the IDs happen to coincide, silently
no-ops via the idempotent-insert rule (`T-NODE-IDEMPOTENT`) and reports success while measuring
someone else's leftover graph.

I deliberately did **not** run `test/perf` with `DAGWORKER_INTEGRATION=1 -tags=integration` against
the databases provided for this review, precisely because of this gap — the task brief says another
process is using them, and this harness would write real, permanent data at scale into that shared
instance with no way to undo it. `storage/postgres` and `storage/redis`'s own integration test
suites I *did* run, because they demonstrably clean up after themselves (§ "Test runs performed").

## 3. MAJOR — the complexity ratio guard proves less than the README and CI claim

`test/perf/complexity_test.go`'s doc comment and the README's Performance section both describe a
single, simple story: "measured at 1,000,000 nodes on every backend... CI asserts the ratio of
per-operation cost between a thousand nodes and a million" (README.md:216-217), with the specific
numbers `20x` (in-memory) / `30x` (networked) bound chosen because "a linear scan shows up as
~1000x and even a `sqrt(n)` term as ~31x" over that thousand-to-a-million span (README.md:229,
complexity_test.go:26-35). That math is correct — *for the span it describes*. But the span the
CI job actually runs for Redis and PostgreSQL is not that span.

`sizes()` (complexity_test.go:118-127):

```go
func sizes(t *testing.T, b perf.Backend) []int {
    if testing.Short() { return perf.SmallSizes }
    if b.Networked && os.Getenv("DAGWORKER_PERF_FULL") == "" {
        return perf.SmallSizes
    }
    return perf.Sizes
}
```

`perf.SmallSizes = []int{1_000, 10_000, 100_000}` (backends.go:62) — a **100x** span, not 1000x.
`Makefile:92-95`'s `complexity` target, the one `.github/workflows/ci.yml:105` actually invokes on
every push, sets `DAGWORKER_INTEGRATION=1` but never `DAGWORKER_PERF_FULL`. So on every PR, the
ratio guard for Redis and PostgreSQL — the two backends whose query planners are the plausible
source of an accidental O(n) or O(√n) regression — runs over 100x, not 1000x, while the in-memory
backend (`Networked: false`, always gets the full `Sizes`) gets the full, tighter check. Recompute
the same math the doc comment gives, at 100x instead of 1000x: O(√n) gives a ratio of only `√100 ≈
10x`, comfortably under the 30x networked bound, where the doc comment's own reasoning says it
should be flagged (it computes to ~31x and fails a 20x bound at the intended 1000x span). A
genuinely `O(√n)` PostgreSQL or Redis operation — the realistic shape of "someone dropped an
index" — passes the CI-invoked guard silently. Nobody would notice until `make million` is run by
hand (~25 minutes, per Makefile:102, and never invoked from CI — see below), and per §2, running it
by hand against anything but a disposable instance is itself unsafe.

Separately, `assertFlat` (complexity_test.go:74-100) only ever compares `ms[0]` and
`ms[len(ms)-1]` — the smallest and largest size. The intermediate points are measured, logged, and
otherwise ignored, so a curve that is flat at both ends but spikes in the middle (a plausible
signature of e.g. an index that degrades only in a specific cardinality range) cannot fail this
check by construction. Not a fatal flaw given the two-point ratio *is* the documented design, but
it is one more way "proves nothing grows" is doing less work than it sounds like it's doing.

`TestMillionNodes` (complexity_test.go:354-416) — "the headline claim, checked on every backend"
per its own comment — computes `statsCost`, `getCost`, and `claimCost` at n=1,000,000, but only
`statsCost` is ever asserted on (line 411: `if statsCost > 50*time.Millisecond`). `getCost` and
`claimCost` are computed and `t.Logf`'d (line 405) and nothing else — a regression that made
`GetNode` or `Claim` scan at a million nodes would produce a slower test run and a number in the
log, but not a failure. And this test is gated behind `if testing.Short() { t.Skip(...) }` with no
other gate, so it *would* run under `make test`'s plain `go test ./...` (no `-short` passed anywhere
in the Makefile) — but only against the in-memory backend, since `Backends()` (backends.go:38-53)
returns just `memory` unless `DAGWORKER_INTEGRATION=1` is set, and the `test` CI job never sets it.
The only way `TestMillionNodes` ever touches Redis or PostgreSQL is `make integration` (which pulls
in the whole unfiltered `test/perf` suite, with no `-run` filter and no `-timeout` override — Go's
default per-package test timeout is 10 minutes, and the README's own performance table says seeding
a million PostgreSQL rows takes "~21 min," Makefile:103) or the standalone `make million` target,
which is not invoked by any job in `ci.yml`. I could not verify from this checkout whether the
`integration` CI job has actually been timing out on this in practice — there's no CI run history
available here — but the arithmetic (21 minutes of seeding vs. a 10-minute default `go test`
timeout, multiplied across three backends compiled into the same test binary) says it should be.
This is worth the maintainer actually checking against real CI logs rather than taking my word for
it, but it is not a hypothetical built from nothing: it follows directly from the Makefile and the
README's own numbers.

**Net effect:** the specific, falsifiable claim in README.md:67 ("Nothing is O(n)... This is
enforced by tests, not claimed in a README") is true for the in-memory backend on every push. For
the two backends that ship as the whole reason to choose this library over an in-memory queue —
Redis and PostgreSQL, "multiple processes, one graph" (README.md:63-64) — the same claim, as
actually wired into CI, is enforced over a materially weaker span than the one whose math the
project uses to justify the bound.

## 4. MAJOR — a real flake, found and root-caused, under the mandated `-race -shuffle=on` run

Running the exact command the task asked for surfaced a genuine failure:

```
$ cd adapters/grpc && go test -race -count=2 -shuffle=on ./...
-test.shuffle 1787489496997299000
--- FAIL: TestWorkerRunClaimsAndCompletes (0.02s)
    worker_test.go:118: status = NODE_STATUS_IN_PROGRESS, want SUCCESS
FAIL
FAIL    github.com/specialistvlad/dagworker/adapters/grpc/client    0.292s
```

It did not reproduce over ~30 further attempts (isolated re-runs of the test, the whole package,
the whole module, and under artificial CPU load), which is consistent with a narrow but real race
window rather than a one-off infrastructure hiccup — and reading the code shows exactly the window.

`adapters/grpc/client/worker_test.go:74-129` (`TestWorkerRunClaimsAndCompletes`) runs the SDK's
`Worker.Run` loop with a handler that does this, in order:

```go
// worker_test.go:95-100
runErr <- w.Run(runCtx, func(_ context.Context, node *pb.Node) client.Outcome {
    handled.Store(true)                 // <-- signalled here, synchronously, before returning
    ...
    return client.Complete([]byte("done"))
})
```

and the test's main goroutine polls `handled.Load()` and, the instant it flips true, immediately
issues a *separate* `GetNode` RPC expecting to already observe `NODE_STATUS_SUCCESS`
(worker_test.go:106-118). But `handled.Store(true)` fires from inside the handler, before the
handler returns — and the handler's return value is not what updates the node's status.
`worker.go:221-248` (`runLease`) shows what happens after the handler returns:

```go
outcome := handle(workCtx, lease.GetNode())   // worker.go:240 — handled=true fires inside here
close(stop)                                    // tell the heartbeat goroutine to stop
<-done                                         // wait for it to actually stop
w.report(lease.GetTaskToken(), outcome)        // worker.go:248 — THIS issues the CompleteNode RPC
```

`w.report` (worker.go:298-306) is what actually calls `CompleteNode` on the server — the RPC that
flips the node's status to `SUCCESS`. Between the handler storing `handled=true` and `w.report`'s
RPC actually landing, the code must: return from the handler closure, close a channel and block on
`<-done` until the independently-scheduled `heartbeatLoop` goroutine notices and exits (which can
itself be blocked mid-RPC on an in-flight `ExtendLease` call), and only then make and complete the
`CompleteNode` round trip. The test's polling loop has no dependency on any of that — it races the
real, multi-step "report the outcome" path against a boolean it can observe the instant the handler
body starts returning. Under `-shuffle=on` reordering other packages' goroutines onto the same
runtime, or under `-race`'s scheduling overhead, that window is wide enough to lose.

This is a test bug, not a library bug — the fix is for the test to poll `GetNode` with a bounded
retry loop the way `manager_test.go`'s own `TestBackgroundSweeperReclaims` (manager_test.go:596-613)
and `coverage_test.go`'s `TestRetentionCollectsOnlyWhenConfigured` (coverage_test.go:180-195)
correctly do elsewhere in this same codebase, rather than trusting a same-process flag set before
the RPC that the assertion depends on. I'm rating it MAJOR rather than MINOR because it is an
observed, reproducible-in-principle CI flake in the exact configuration the project's own `race`
CI job runs (`go test -race -shuffle=on`, Makefile:44-47), and because it's the one place in this
otherwise careful codebase where a synchronization primitive was used as a proxy for an RPC outcome
instead of for the outcome itself.

## 5. The conformance suite: real, but here is what a backend could still get wrong

`dagstoretest/` is genuinely good work. I read every test in `scope_graph.go`, `trigger_lease.go`,
and `removal_facets.go` (80 test IDs total — the "80 tests" claim in the task brief checks out
exactly: `grep -oE '\{"T-[A-Z0-9-]+"' dagstoretest/*.go | wc -l` → 80, no duplicates, enforced at
`conformance.go:415-419`). The comments explain *why* each assertion exists and what a backend that
got it wrong would look like, which is the right way to write a shared contract suite, and several
tests specifically exist to close gaps a naive implementation would fall into — `T-EDGE-CYCLE-
LEAVES-GRAPH-INTACT` (scope_graph.go:212-229) checking that a *rejected* batch leaves stats
untouched, `T-CLAIM-RECLAIMS-INLINE` (trigger_lease.go:418-441) checking that reclaim doesn't
secretly depend on a background sweeper ever running, `T-REMOVE-NODE-LEAF` (removal_facets.go:97-
109) checking that a removed leaf's predecessor doesn't keep believing it has a successor. These
are the tests of someone who has actually watched an in-memory-only test suite pass while a real
backend does the wrong thing.

But "the suite is the definition of correct backend behaviour" (dagstoretest package doc, `doc.go`
equivalent at the top of `conformance.go`) is a strong claim, and here is where a backend could
diverge from every other backend and still pass all 80 tests:

- **`Kinds` is never exercised with more than one kind.** Every call site across the whole suite
  passes zero or one kind string (`grep -n "tryClaim("` across `dagstoretest/*.go` — 14 call sites,
  max one argument each). `ClaimRequest.Kinds []string` is documented as an OR-filter over kinds
  (store.go), but a backend that only correctly implements the single-kind case — treats a
  multi-kind request as AND, or only honours `Kinds[0]`, or panics on `len(Kinds) > 1` — would pass
  every one of the 80 tests undetected.
- **`Max <= 0` is never exercised at the store level.** `store.go:61`: "Values below one are
  treated as one." All three backends implement this independently and correctly
  (`storage/memory/lease.go:161`, `storage/postgres/lease.go:168`, `storage/redis/ops_lease.go:28-
  30`) — but the conformance suite never calls `Claim` with `Max: 0` or a negative value, so this is
  three separately-maintained, currently-correct, entirely untested-by-the-arbiter implementations
  of the same rule. A future change to any one of the three that broke this contract point would
  ship undetected by the suite whose whole purpose is to catch exactly that class of drift (the
  purpose ADR-0018 states explicitly: "a backend can pass its own hand-written tests while..." the
  contract silently drifts, docs/adr/0018:149).
- **`MaxBatchSize` enforcement is not in the conformance suite at all.** It's implemented three
  times independently (`storage/memory/graph.go:106-108`, `storage/postgres/graph.go:39-41`,
  `storage/redis/ops_graph.go:42-44`, all raising `ErrInvalidArgument`) and tested exactly once, for
  exactly one backend: `storage/memory/memory_test.go:178-220` (`TestBatchAndPayloadLimits`). Redis
  and PostgreSQL have zero test coverage of this rule. It happens to be implemented consistently
  today; the suite gives no signal if it ever isn't.
- **No conformance test exercises `Complete`/`Ack` under real concurrency.** `T-CLAIM-ATOMIC`
  (trigger_lease.go:165-207) is a genuinely strong test — 50 nodes, 8 racing goroutines, asserting
  no node is ever granted twice — and I want to be clear this is not theatre; it would catch a real
  locking bug. But it is the *only* concurrent-goroutine test in the shared 80, plus
  `T-DOORBELL-WAKES`'s single helper goroutine (which isn't a race test, just an async wakeup
  check). Every fencing/epoch test — `T-FENCE-STALE-ACK`, `T-FENCE-DOUBLE-ACK`, `T-EXTEND-FENCED` —
  is sequential: claim, then reclaim, then try the stale operation, one step at a time. That proves
  the fencing *mechanism* exists and rejects an already-superseded lease. It does not prove what
  happens when two goroutines call `Complete` with the *same currently-valid* lease at the same
  instant (exactly-one-wins, fan-out computed exactly once) or when two callers race `AddEdges` to
  close a cycle from opposite directions. Given that fencing-under-genuine-concurrency is the
  library's headline safety property, one concurrency test covering one code path (claim) is a
  real, not cosmetic, gap.
- **No conformance test on payload aliasing.** Nothing checks whether a backend copies the
  `[]byte` payload a caller passes to `AddNodes`, or keeps a reference to it. A backend that stores
  the slice by reference would corrupt already-persisted data the moment a caller reused or zeroed
  their own buffer after the call returned — a classic Go footgun, and specifically the kind of bug
  that's invisible in every test here because every test constructs a fresh `[]byte` literal per
  call and never touches it again.
- **Cross-scope isolation isn't in `dagstoretest` at all** (it's covered instead at the `test/e2e`
  layer — `TestScopesAreIsolated`, `test/e2e/multiinstance_test.go:240-269`, which is a solid test,
  run against memory unconditionally and against Redis/Postgres under
  `DAGWORKER_INTEGRATION=1`). That's real coverage, but it means a *new* backend that only
  satisfies `dagstoretest.RunConformance` — which per ADR-0018 is supposed to be sufficient proof of
  correctness on its own — could leak nodes across scopes and still be declared conformant.

None of this means the three shipped backends are currently broken in these ways — I found no
evidence they are, and I ran the redis and postgres conformance suites myself (§ "Test runs
performed") and they pass. It means the suite's own stated bar — "a backend is finished when
`RunConformance` passes... a disagreement... is a bug in the backend unless the suite is changed
deliberately" (conformance.go doc comment) — is calibrated more loosely than its language implies.

One design point in the suite's favor that's worth calling out explicitly, since it looks like a
gap on first read and isn't: every facet test (`T-LIST-*`, `T-WATCH-*`, `T-DOORBELL-*`,
`T-COLLECT-*`) skips when a backend doesn't report the matching capability bit
(`removal_facets.go:135-363`). That's a real design risk for a *future* backend that under-declares
its capabilities to duck a test family. It is not currently exploited: I checked, and `memory`,
`postgres`, and `redis` all report `CapList|CapDurableEvents|CapDoorbell|CapCollect`
(`storage/memory/memory.go:136-137`, `storage/postgres/postgres.go:156-158`,
`storage/redis/redis.go:152-154`), so all 80 tests genuinely execute, not skip, against all three
shipped backends today.

## 6. Concurrency and property tests: the two that exist are meaningful, not theatre

To be fair about what *does* work: `T-CLAIM-ATOMIC` (trigger_lease.go:165-207) and
`storage/redis/cross_process_test.go`'s `TestCrossProcessClaimIsExclusive` (the whole file, 40
nodes, 4×2 racing goroutines across **two independent `*Store` values on two independent client
connections** — the shape that actually stands in for two separate host processes, not just two
goroutines sharing one connection pool) and its PostgreSQL equivalent
(`storage/postgres/conformance_test.go:139` onward) are real tests that would fail on a broken
locking scheme. I ran all three (root module, plus Redis and PostgreSQL integration) and they
passed cleanly under `-race`. This is good, honest concurrency testing for the one property it
covers (claim exclusivity) — it's just the *only* property covered by an actual multi-goroutine
race in the whole test suite, which is the point of §5's gap above and §1's larger point that
ADR-0040's promised four-property `rapid` suite plus Porcupine plus chaos harness — which would
have covered completion-path and cycle-insertion races too — doesn't exist to pick up the slack.

## 7. Coverage: real, not padding

The task brief asks specifically whether ~96% coverage is real or padding. I measured it myself
(`go test -covermode=atomic -coverpkg=<the three packages the Makefile's `cover` target names> ./...`)
and got **95.8%**, matching the claim and the 95% floor `Makefile:79-90` enforces. More importantly,
I checked whether it's *earned*:

- Assertion density is high and consistent across the root package's test files — `manager_test.go`
  has 16 test functions and 102 `t.Fatalf`/`t.Errorf` calls; `types_test.go` has 15 and 58;
  `coverage_test.go` (a name that would normally make me suspicious) has 9 tests and 60 assertions,
  and every one I read checks a specific, named field or transition, not just `err == nil`.
- The uncovered statements, per `go tool cover -func`, are spread thinly across genuine edge
  branches (e.g. `storage/memory/lease.go:18 clampAttempt` at 66.7%, `claim.go:151 waitForWork` at
  71.4%) rather than concentrated in one large dead zone or hidden behind a pile of trivial
  one-line getters that would inflate the number without meaning anything.
- `example_test.go`'s eight `Example*` functions all carry `// Output:` comments (verified: `grep -c
  "// Output:" example_test.go` → 8, matching 8 `Example` funcs), meaning they are genuinely
  executed and checked by `go test`, not dead documentation that merely compiles.
- `fault_test.go`'s `faultStore` (fault_test.go:17-79) is real fault injection — it wraps the real
  memory store and selectively fails `Scopes`/`Sweep`/`ScopeConfig`/`CollectTerminal`/`Claim`/
  `WaitForWork`, and the tests built on it (`TestMaintenanceSurvivesAFailingBackend`,
  `TestRetentionSurvivesAFailingBackend`, `TestClaimFallsBackWhenTheDoorbellFails`) verify the
  Manager's background maintenance loop actually keeps retrying after each kind of backend failure,
  not just that it doesn't crash. This is the kind of test that's easy to skip and valuable when
  present.
- `TestCloseLeavesNoGoroutines` (manager_test.go:498-533) is a real goroutine-leak check —
  `runtime.NumGoroutine()` before and after a `Manager` with five active subscriptions is created
  and closed, with a bounded polling wait for the scheduler to settle rather than a flat sleep.

I did not find meaningful "executes but asserts nothing" padding anywhere in the root package or
`dagstoretest`. If it exists, it's a small fraction of the suite, not a pattern.

## 8. Determinism inventory

- **Map iteration**: the few places tests range over a `map` (`gaps_test.go:25`
  `TestTriggerRuleNames`, `gaps_test.go:245-268` `TestScopeConfigValidationBranches`,
  `test/e2e/lifecycle_test.go:151`, `test/e2e/scenario_release_test.go:191`) all do so to run
  independent per-key assertions or named subtests, never to build an order-dependent sequence.
  Not a flake source.
- **Real sleeps**: still common as a synchronization mechanism (`time.Sleep(50 * time.Millisecond)`
  then check, e.g. `manager_test.go:222`, `fault_test.go:112`, `storage/memory/memory_test.go:87`).
  Every instance I checked is paired with either a bounded polling loop with a multi-second overall
  deadline, or a generous `select ... case <-time.After(5*time.Second)` fallback, so these are a
  latent CI-flakiness risk on a genuinely overloaded runner but not a live one at any load I could
  produce — I ran the full suite twice under `-race -shuffle=on` (§ below) and none of these fired.
  §4's flake is the one place a same-process signal, rather than a sleep, was the actual problem.
- **`dagstoretest`'s fake clock path is fully deterministic**; the real-clock fallback
  (`realLease = 400ms`, `realSlack = 400ms`, conformance.go:60-63) only engages for backends that
  can't be driven (PostgreSQL, per ADR-0008) and is generous enough that it did not flake across two
  full integration runs.
- **Port collisions**: none found. The gRPC/HTTP adapter tests all use `bufconn` (in-process,
  no real sockets — `adapters/grpc/client/worker_test.go:38`) or `httptest.NewServer` (ephemeral
  OS-assigned ports), never a hardcoded port.
- **Shared state across parallel subtests**: `dagstoretest.RunConformance`'s `Harness.New` doc
  comment explicitly requires "a store isolated from any other, so that subtests can run in
  parallel" (conformance.go:51), and all three backends honor it — memory via a fresh `*Store`,
  Postgres via a fresh scratch database, Redis via a fresh random keyspace. Running the Postgres
  suite creates and drops up to 80 scratch databases concurrently (one per subtest, all
  `t.Parallel()`); it completed in 3.7s in my run with no errors, so this is a NIT-level scalability
  note for whoever adds test #200, not a current problem.

## 9. Test runs performed

All from a clean `go build ./...` and `go vet ./...` (both silent) and `golangci-lint run
./...` → `0 issues.` on the root module.

| Command | Result |
|---|---|
| `go test -race -count=2 -shuffle=on ./...` (root module) | **PASS** |
| same, `storage/postgres` (no integration tag) | PASS |
| same, `storage/redis` (no integration tag) | PASS |
| same, `adapters/grpc` | **FAIL once** — `TestWorkerRunClaimsAndCompletes`, see §4 |
| same, `adapters/http` | PASS |
| same, `cmd/dagworkerd` | PASS |
| same, `test/perf` (memory only, no integration tag) | PASS (96.5s — runs `TestMillionNodes`/`TestChainDrainsInLinearTime` against memory twice) |
| same, `test/e2e` | PASS |
| `DAGWORKER_INTEGRATION=1 go test -race -shuffle=on -tags=integration ./...` in `storage/redis` (random keyspace, self-cleaning) | PASS, 24s |
| `DAGWORKER_INTEGRATION=1 go test -race -shuffle=on -tags=integration ./...` in `storage/postgres` (scratch databases, self-dropping) | PASS, 3.7s |
| `test/perf` with `-tags=integration DAGWORKER_INTEGRATION=1` | **not run** — see §2, this would write unbounded, uncleaned data into the shared instance |
| `make integration` / `make complexity` / `make million` (any target that calls `reset-db`, which `TRUNCATE`s and `FLUSHALL`s) | **not run** — explicitly disallowed by the task brief |

I made ~30 further isolated attempts to reproduce §4's flake (repeated runs of the single test, the
package, and the module, including under artificial background CPU load) and could not — consistent
with a narrow, load-dependent race rather than a deterministic bug, and consistent with the
mechanism I traced in the code.

## Verdict

The parts of this test suite that were actually built are, for the most part, well-built: the
80-test conformance suite is thoughtful and mostly non-gameable, the root package's unit tests have
real assertion density and include genuine fault-injection and goroutine-leak checks, the one
true concurrency test (`T-CLAIM-ATOMIC`) and its cross-process Redis/Postgres siblings are not
theatre, and the ~96% coverage number is earned rather than padded. If the review stopped at "is
each test file doing what it claims," this would be a strong pass. But the review was also asked
to check documentation claims against the code, and on that axis this project fails badly in one
specific, important place: ADR-0040 — an Accepted, current decision record — describes property-
based testing, a chaos harness, and a linearizability checker that back the library's core
correctness-under-concurrency claim, and none of it exists. Layered on top of that, the one
automated mechanism that *does* try to prove the headline "nothing is O(n)" claim runs a
meaningfully weaker check than the README describes for exactly the two backends (Redis, Postgres)
where a regression would matter most, and the performance-test harness itself is not safe to run
against anything but a disposable, exclusively-owned database — a constraint the project doesn't
state anywhere a user would see it before running `make million`. I would trust the shipped
backends' current correctness more than I'd trust this project's own account of how that
correctness is verified, and I would not sign off on the ADR as an accurate description of the
system until either the missing pieces are built or the document is corrected to say what actually
ships.
