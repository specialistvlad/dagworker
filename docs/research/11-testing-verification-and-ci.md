# Testing, Verification, and CI for a Concurrent Go DAG Scheduler

Scope: how to get real concurrency confidence — not just a coverage number — for a library
that leases DAG nodes to external workers across multiple pluggable storage backends and
multiple concurrent process instances. Ordered: (1) Go test tooling mechanics and gotchas,
(2) correctness verification beyond unit tests, (3) coverage mechanics, (4) integration
infrastructure, (5) CI pipeline design, (6) strict linting configuration.

---

## 1. Go test tooling: mechanics and gotchas

### 1.1 `t.Parallel()` and the loop-variable trap

`t.Parallel()` signals that a test should run concurrently with (and only with) other
parallel tests in the same parent; the parent test function returns immediately after all
its `t.Parallel()` subtests have been started, and those subtests then run concurrently
against each other once the parent and its siblings finish their own sequential bodies.
This has three concrete gotchas that matter for a project betting hard on parallel test
suites:

**1. Pre-1.22 loop-variable capture.** Before Go 1.22, a `for` loop declared one variable
per loop, reused every iteration. Capturing it in a closure handed to `t.Run` + `t.Parallel`
without rebinding meant every subtest goroutine could observe the *final* loop value once
the outer loop returned:

```go
// BROKEN prior to Go 1.22
for _, tc := range cases {
    t.Run(tc.name, func(t *testing.T) {
        t.Parallel()          // outer loop has already advanced by the time this runs
        checkNode(t, tc.node) // tc may be the last case's value for every subtest
    })
}
```

The fix before 1.22 was the well-known `tc := tc` rebind inside the loop body before the
closure. **Go 1.22 changed loop semantics so each iteration gets its own copy of the loop
variable**, which "naturally prevents the common bug where all goroutines reference the
same loop variable" — the shadowing idiom becomes unnecessary from 1.22 onward, but any
module whose `go.mod` still pins `go 1.21` or lower does not get the new semantics even on
a newer toolchain, so the version pin in `go.mod` (not just the installed `go` binary)
is what gates this fix — [Go 1.22 release notes](https://go.dev/doc/go1.22). Since this
project targets O(1)/O(log n) hot paths and heavy `t.Parallel()` use, **pin `go 1.22` or
later in `go.mod`** and do not rely on the shadow idiom as a safety net for anything written
against a floor below that.

**2. `t.Cleanup` ordering.** `Cleanup` "registers a function to be called when the test (or
subtest) and all its subtests complete. Cleanup functions will be called in last added,
first called order" (LIFO) — [`pkg.go.dev/testing`](https://pkg.go.dev/testing#T.Cleanup).
For a DAG-store test this matters concretely: if a test opens a Redis client, then opens a
transaction/lease on top of it, cleanups must be registered lease-close *then*
client-close, in that order, so that LIFO unwinds the lease first — registering them in
the wrong order deadlocks or leaks under `-race` when the client is torn down before the
lease that depends on it. `Cleanup` runs even when the test fails or calls `t.Fatal`,
which is precisely why storage backend tests should prefer it over bare `defer` for
resource teardown that must survive assertion failures.

**3. `t.Setenv` is incompatible with `t.Parallel()`.** "Because Setenv affects the whole
process, it cannot be used in parallel tests or tests with parallel ancestors" —
[`pkg.go.dev/testing`](https://pkg.go.dev/testing#T.Setenv). Calling `t.Setenv` in a test
that also calls (or whose parent called) `t.Parallel()` fails the test immediately. For
this project the practical corollary: never gate backend selection (`DAG_STORAGE_BACKEND`,
Redis/Postgres DSNs) with `t.Setenv` inside a parallel table-driven suite — pass
configuration as explicit function/struct parameters into the backend-factory table
(§4.3) instead of environment mutation, which also sidesteps the whole class of "who
resets the env var" ordering bugs between sibling parallel tests.

**4. Interaction with `TestMain`.** Package-level parallel tests are still gated by
`GOMAXPROCS` and the `-parallel` flag (default: matches `-cpu`, itself defaulting to
`GOMAXPROCS`); pool-per-test setups (spinning up N in-memory stores per subtest) should
size worker pools relative to `runtime.GOMAXPROCS(0)`, not a hardcoded constant, or CI
runners with 2 vCPUs will serialize what the test author assumed was parallel.

### 1.2 `-race`: what it proves and what it costs

The race detector instruments memory accesses and flags concurrent unsynchronized
access where at least one is a write. Two properties matter for how much to trust it:

- **It only reports races that actually execute.** Per the official docs: "the race
  detector only finds races that happen at runtime, so it can't find races in code paths
  that are not executed" —
  [Go race detector article](https://go.dev/doc/articles/race_detector). A race-free
  `-race` run over a suite with 60% coverage says nothing about the other 40%. This is
  the single strongest argument in this whole dossier for **combining `-race` with high
  coverage and with `-shuffle`/`-count` (below) rather than treating a green `-race` run
  as a correctness certificate** — it is a *lower bound* on bugs found, not an upper bound
  on bugs present.
- **Overhead is real but bounded and known**: memory usage rises 5–10x and CPU time
  2–20x depending on the program, requires cgo (a C compiler on non-Darwin platforms),
  and needs 8 extra bytes per `defer`/`recover` on the calling goroutine that are not
  reclaimed until the goroutine exits — meaning long-lived worker-pool goroutines that
  `defer`/`recover` in a tight per-task loop can leak memory under `-race` in ways that
  won't show up in `runtime.ReadMemStats` — [same article](https://go.dev/doc/articles/race_detector).
  For this project's target of a 1,000,000-node benchmark, run the *functional*
  concurrency suite under `-race` on every push, but keep the 1M-node performance
  benchmarks **out** of the race build (§5) — the slowdown and memory multiplier would
  make them either too slow to run in CI or misleading as performance numbers.
- Supported platforms are limited to `darwin/{amd64,arm64}`, `freebsd/amd64`,
  `linux/{amd64,arm64(48-bit VMA),ppc64le,riscv64}`, `windows/amd64` — plan the CI matrix
  (§5) around this; there is no race detector for `linux/386` or 32-bit ARM, so those
  targets (if ever supported) get concurrency confidence only from the other layers below.

### 1.3 `-shuffle` for order-dependence bugs

`go test -shuffle=on` (Go 1.17+) randomizes the order tests and subtests within a package
run, and `-shuffle=N` reproduces a specific ordering by seed; when `-shuffle=on` is used
without an explicit seed the tool prints the seed it picked so a failing run can be
replayed exactly. For a DAG library where tests share an in-memory store singleton or a
package-level scope registry, `-shuffle=on` is the cheapest way to catch **tests that
silently depend on execution order** — e.g., a "scope created implicitly on first use"
test that only passes because an earlier test already created that scope. Run it in a
dedicated CI job (not by default locally, to keep default runs deterministic for
bisecting) and log the seed on every CI run so a flake is reproducible from the log alone.

### 1.4 `-count` for flake detection

`-count=N` reruns the test binary's tests N times without using the test cache;
`-count=1` is "the idiomatic way to disable test caching explicitly" —
[`pkg.go.dev/cmd/go`](https://pkg.go.dev/cmd/go). For flake-hunting, `-count=50` (or more)
combined with `-race -shuffle=on` on a specific package under suspicion is the standard
Go idiom for turning a "sometimes fails in CI" report into a locally reproducible
failure; this is also exactly the technique `benchstat` expects for benchmarks — see
§5.5 — where `-count=10` is the documented floor for statistically meaningful
before/after comparisons. Bake a `make flaky-check PKG=./storage/redis COUNT=200` target
into the repo up front; do not build this workflow only after the first CI flake report.

### 1.5 `testing/synctest`: deterministic virtual time (GA in Go 1.25)

`testing/synctest` graduated from experimental (`GOEXPERIMENT=synctest` under Go 1.24) to
general availability in Go 1.25; the old experimental API is still reachable under the
flag through Go 1.25 but is removed in Go 1.26 —
[Go 1.25 release notes](https://go.dev/doc/go1.25). This is the single most relevant new
piece of stdlib tooling for the per-node lease-timeout requirement in this project's
brief.

**Mechanics**, from the [`testing/synctest` package docs](https://pkg.go.dev/testing/synctest):

- `synctest.Test(t, f)` runs `f` inside an isolated **"bubble"**: a group of goroutines
  whose `time` package calls are redirected to a fake clock private to that bubble. The
  bubble's clock starts at midnight UTC on 2000-01-01 and only advances when **every
  goroutine in the bubble is "durably blocked"** — meaning blocked in a way only another
  goroutine in the same bubble can unblock (channel send/receive on a bubble-created
  channel, `sync.Cond.Wait`, `sync.WaitGroup.Wait` after an in-bubble `Add`, `time.Sleep`).
  Network I/O, syscalls, and plain mutex contention do **not** count as durable blocking —
  a goroutine parked on a real socket read will simply never let the bubble's clock move,
  which matters if the lease-timeout code path also does I/O inside the timed section.
- `synctest.Wait()` blocks the calling goroutine until every *other* goroutine in the
  bubble is durably blocked — the tool for asserting "the scheduler has settled" before
  making assertions.
- `synctest.Sleep(d)` is `time.Sleep(d)` immediately followed by `Wait()` — sleep the
  virtual clock forward *and* wait for the rest of the bubble to react to it, which is
  the idiom for "advance past this node's lease deadline and let the timeout-sweeper
  goroutine observe it."
- Restrictions: `Test` panics if called from inside an existing bubble; the `*testing.T`
  passed to the bubble body cannot call `T.Run`, `T.Parallel`, or `T.Deadline`; timers,
  tickers, and channels created inside a bubble panic if operated on from outside it, and
  a `sync.WaitGroup` becomes bound to whichever bubble first calls `Add`/`Go` on it.

**Direct application to this project**: the lease-timeout state machine ("worker did not
ack within the per-node timeout → mark error-with-timeout") is exactly the kind of code
`synctest` exists for. A real test shape:

```go
func TestLeaseTimeout_MarksNodeErrorTimeout(t *testing.T) {
    synctest.Test(t, func(t *testing.T) {
        store := newInMemoryStore()
        sched := NewScheduler(store, WithDefaultLeaseTimeout(30*time.Second))

        nodeID := mustEnqueueReadyNode(t, store)
        lease, err := sched.Lease(context.Background(), WorkerID("w1"))
        if err != nil { t.Fatal(err) }
        if lease.NodeID != nodeID { t.Fatalf("got %v, want %v", lease.NodeID, nodeID) }

        // Advance the bubble's virtual clock exactly past the lease deadline and let
        // the internal timeout-sweeper (a goroutine started by NewScheduler) react.
        synctest.Sleep(31 * time.Second)

        status, err := store.NodeStatus(context.Background(), nodeID)
        if err != nil { t.Fatal(err) }
        if status != StatusErrorTimeout {
            t.Fatalf("status = %v, want StatusErrorTimeout", status)
        }
    })
}
```

This runs in microseconds of wall-clock time regardless of the configured timeout, is
100% deterministic (no `time.Sleep(31*time.Second)` racing a background goroutine's
polling interval), and — critically — **only works if the scheduler's internal
sweeper goroutine is started from inside the bubble** (e.g. inside `NewScheduler` called
within the `synctest.Test` closure) and uses `time.Now`/`time.After`/`context.WithTimeout`
rather than a hand-rolled ticker sourced from `runtime.nanotime` or an externally-injected
non-`time`-package clock. **This is an architectural constraint worth writing into the
ADR now**: the internal scheduler must do all its timing exclusively through the stdlib
`time` package (or a thin wrapper over it) precisely so that wrapping the whole component
under test in `synctest.Test` is sufficient — no separate hand-rolled fake-clock
abstraction is needed for the timeout logic, which is a meaningful simplification over
what pre-1.25 Go projects had to build by hand.

### 1.6 Fuzzing graph mutation sequences

`go test -fuzz=FuzzX` (native since Go 1.18) mutates byte-level inputs against a corpus
seeded by `f.Add()` calls and files under `testdata/fuzz/{FuzzTestName}/`; supported
argument types are limited to `string`, `[]byte`, the integer/float families, and `bool`
— [`go.dev/security/fuzz`](https://go.dev/security/fuzz/). A DAG mutation sequence is not
naturally one of those types, so the standard pattern is **encode a sequence of graph
operations as a byte string and decode it inside the fuzz target**:

```go
type opKind byte

const (
    opAddNode opKind = iota
    opAddEdge
    opRemoveNode
    opComplete
    opFail
)

func FuzzDAGMutationsPreserveAcyclicity(f *testing.F) {
    f.Add([]byte{byte(opAddNode), 1, byte(opAddNode), 2, byte(opAddEdge), 1, 2})
    f.Fuzz(func(t *testing.T, raw []byte) {
        g := newGraph()
        for ops := decodeOps(raw); len(ops) > 0; ops = ops[1:] {
            switch ops[0].kind {
            case opAddNode:
                g.AddNode(ops[0].id)
            case opAddEdge:
                // AddEdge must reject anything that would introduce a cycle;
                // it must never leave the graph in a state with a cycle.
                _ = g.AddEdge(ops[0].from, ops[0].to)
            case opRemoveNode:
                g.RemoveNode(ops[0].id)
            }
            if g.HasCycle() {
                t.Fatalf("cycle introduced by op sequence: %v", ops)
            }
        }
    })
}
```

`decodeOps` must be **total** (never panic on garbage bytes — return an empty/truncated
op list instead), because the fuzzer will throw arbitrary byte garbage at it and a panic
inside decoding, rather than inside the graph logic, produces false-positive crash
reports that waste triage time. Keep the fuzz target itself allocation-light and free of
global state per the docs' requirement that targets be "fast, deterministic, and
state-free" to run safely in parallel. In CI, run `go test -run=FuzzDAGMutations...`
**without** `-fuzz` so the seed corpus replays as ordinary regression tests on every push
(cheap, deterministic); run the actual fuzzer (`-fuzz=... -fuzztime=5m`) as a separate,
scheduled (nightly/weekly) job, and commit any crasher it finds under
`testdata/fuzz/...` so it becomes a permanent regression test — this is the officially
recommended split, since "fuzzing won't report any issues that would already be caught
by an existing test."

### 1.7 Property-based testing: `pgregory.net/rapid` vs `leanovate/gopter`

Both give you generators + shrinking; the meaningful difference for a **stateful DAG
scheduler** is the model-based testing API.

**`pgregory.net/rapid`** — [`pkg.go.dev/pgregory.net/rapid`](https://pkg.go.dev/pgregory.net/rapid):
values are drawn with `rapid.IntRange(min, max).Draw(t, "label")`-style calls directly
against generators (`Int`, `SliceOf`, `MapOf`, `StringOf`, `OneOf`, `Custom`, `Make[V]()`
for reflection-based generation, `Filter`), integrates directly with `*testing.T` via
`rapid.Check(t, prop)`, and — the piece that matters here — **`t.Repeat` implements
stateful/model-based testing directly**:

```go
func TestSchedulerStateMachine(t *testing.T) {
    rapid.Check(t, func(t *rapid.T) {
        real := NewScheduler(newInMemoryStore(), WithDefaultLeaseTimeout(time.Minute))
        model := newReferenceModel() // trivial in-memory map-based reimplementation

        t.Repeat(map[string]func(*rapid.T){
            "addNode": func(t *rapid.T) {
                id := rapid.StringMatching(`[a-z]{3,8}`).Draw(t, "id")
                deps := rapid.SliceOfN(rapid.SampledFrom(model.NodeIDs()), 0, 3).Draw(t, "deps")
                realErr := real.AddNode(id, deps)
                modelErr := model.AddNode(id, deps)
                if (realErr == nil) != (modelErr == nil) {
                    t.Fatalf("AddNode divergence: real=%v model=%v", realErr, modelErr)
                }
            },
            "lease": func(t *rapid.T) {
                l, err := real.TryLease(rapid.StringMatching(`w[0-9]`).Draw(t, "worker"))
                model.RecordLease(l, err)
            },
            "": func(t *rapid.T) { // invariant check, runs before/after every action
                if !real.InFlightSet().IsAntichain(real.Graph()) {
                    t.Fatal("in-flight set is not an antichain")
                }
            },
        })
    })
}
```

The empty-string key in `t.Repeat`'s map is the documented hook for invariant checks that
run around every action — exactly the shape needed for "check the antichain property
after every mutation" without hand-rolling the plumbing.

**`leanovate/gopter`** — [`github.com/leanovate/gopter`](https://github.com/leanovate/gopter):
older (ScalaCheck-lineage), ships a separate `commands` package for model-based/stateful
testing where you define a `Commands` interface (`NewSystemUnderTest`,
`InitialState`, plus a set of `Command`s each with `Run`/`NextState`/`PreCondition`/
`PostCondition`) that the framework then sequences and shrinks — more ceremony than
rapid's `t.Repeat`, but the explicit `PreCondition`/`PostCondition` separation can be
clearer for larger command sets. **Recommendation: `pgregory.net/rapid`.** It is
actively maintained, integrates natively with `*testing.T` (no separate runner, so it
composes with `-run`, `-race`, and coverage instrumentation for free), and its
`t.Repeat` model matches this project's need almost exactly. Reserve `gopter` only if a
contributor already has a large investment in gopter-style commands elsewhere.

**Concrete properties to encode for this scheduler** (each maps to a `t.Repeat` action
set + the empty-string invariant hook above):

| Property | How to check it in the invariant hook |
|---|---|
| No node is ever leased twice concurrently | Maintain a `map[NodeID]WorkerID` of currently-out leases in the *test* (not the model), updated on every successful `Lease`/`Ack`; fail if a `Lease` call returns a node already present in that map. |
| Every node whose predecessors all succeeded eventually becomes ready | After each batch of actions, if a node's `PredecessorsSucceeded()` holds true (checked against the model graph) and it isn't already terminal, assert it is either `Ready` now or reachable to `Ready` within the next `N` reference-model ticks — this is a liveness property, so bound it: assert it holds after quiescence (`synctest.Wait()`-style settling), not at arbitrary interleavings. |
| The in-flight set is always an antichain of the unfinished subgraph | After every action, compute `InFlightSet()`; for every pair `(a,b)` in it, assert neither is a (transitive) predecessor of the other in the current graph. This directly encodes "no node is being worked on while something it depends on, or that depends on it, is also being worked on" — cheap to check with a precomputed reachability index (§ topology reasoning belongs in the graph-algorithms dossier, not here) but here it's simply an O(k²) pairwise check over the small in-flight set `k`, which is fine inside a test. |
| Acyclicity is preserved under any interleaving of inserts | Run `AddEdge`/`AddNode`/`RemoveNode` actions in random order (rapid picks the order) and assert `HasCycle() == false` after every single mutation, not just at the end — a property test that only checks the final state can miss a transient cycle that a concurrent reader could have observed mid-mutation; if edges are added non-atomically (e.g., "check no cycle" then "insert edge" as two separate store calls) that gap is exactly the TOCTOU bug this property is designed to surface once the test is extended to interleave two goroutines each running a `t.Repeat` action stream against the *same* store (see §2.1 below for making that interleaving deterministic rather than best-effort). |

---

## 2. Correctness verification beyond unit tests

Property tests catch violations of properties you thought to state, against
interleavings the Go scheduler happens to produce on your laptop. Neither of those is
enough for a system whose entire value proposition is "correct under concurrent access
from multiple processes against shared storage." Three techniques go further.

### 2.1 Deterministic simulation testing (FoundationDB, TigerBeetle)

**FoundationDB's approach.** FoundationDB runs "a deterministic simulation of an entire
FoundationDB cluster within a single-threaded process," modeling machines, disks (including
the possibility of a disk filling up), and the network, and then deliberately injecting
"failure modes at the network, machine, and datacenter levels, including connection
failures, degradation of machine performance, machine shutdowns or reboots" — running
simulated time at roughly "a 10-1 factor of real-to-simulated time" and, over the
company's history, accumulating what the team estimates as "the equivalent of roughly
one trillion CPU-hours of simulation" —
[Apple's FoundationDB testing docs](https://apple.github.io/foundationdb/testing.html).
Will Wilson's talk "Testing Distributed Systems w/ Deterministic Simulation" is the
canonical narrative source for *why* this works and *how much* it paid off: per a
detailed write-up of the talk, "In the entire history of the company, I think we only
ever had one or two bugs reported by a customer. Ever," and Jepsen's Kyle Kingsbury
reportedly declined to run his usual test suite against FoundationDB because he expected
it to turn up nothing — [Phil Eaton's DST write-up](https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html).
The mechanism: FoundationDB's actor-model language (Flow) compiles cooperative
coroutines that only ever yield at well-defined points, all scheduled by one single
thread, so a seeded PRNG fully determines execution order, timer firing order, and fault
injection (their `BUGGIFY` macro randomly flips designated fault-injection points on/off
per run, weighted by the seed). Wilson's own emphasis, per that write-up: the DST
*harness* is only the enabling infrastructure — "the vast majority of the work" is the
ongoing, iterative tuning of distributions, workloads, and fault-injection points to keep
discovering untested corners of the state space; a DST harness that never gets refined
after the initial build under-delivers badly relative to its cost.

**TigerBeetle's VOPR.** TigerBeetle runs the same idea as a dedicated tool, described as
"a simulated environment where an entire cluster, running real code, is subjected to all
kinds of network, storage and process faults, at 1000x speed," running "24/7 on 1024
cores, fuzzing the latest version of the database," with fault injection across network
(latency/loss/partition), storage (corruption, latency, I/O failure), and process
(crash/hang/timing) — [TigerBeetle safety docs](https://docs.tigerbeetle.com/concepts/safety/).
It is even playable interactively at `sim.tigerbeetle.com`. TigerBeetle's engineering
discipline (Zig, static allocation, "TigerStyle" borrowing from NASA's Power of Ten rules)
exists specifically to keep the system's behavior simulation-friendly: no dynamic
allocation failure modes to model, no hidden OS-thread nondeterminism to fight.

**Why Go makes this structurally harder, and the honest options.** The core requirement
of DST is *single-threaded, cooperatively-scheduled execution driven by one seeded PRNG*
— every source of nondeterminism (goroutine scheduling order, `map` iteration order,
`time.Now`, `crypto/rand`, OS-level I/O timing) must be either eliminated or routed
through that one seed. Go's runtime scheduler is preemptive and multi-threaded by
default and gives you no supported hook to make it single-threaded and seed-driven. Per
the same write-up, real-world attempts confirm this is not a solved problem in Go:
Polar Signals compiled their application to WebAssembly (whose Go runtime target *is*
single-threaded) to get single-threaded execution, then forked the Go runtime itself to
control goroutine-scheduling randomness via an environment variable; the write-up
characterizes the Resonate project's alternative approach as "also looks cumbersome."
**Recommendation for this project**: full FoundationDB/TigerBeetle-grade DST (fork the
runtime or compile to WASM) is disproportionate for a library, not a database kernel.
Instead, build a **narrower, honest approximation** that captures 80% of the value at
a fraction of the cost:

1. **Inject every source of time and randomness.** The scheduler's internal clock is
   `time.Now`/`time.After` exclusively (§1.5) — already required for `synctest`
   compatibility, and it is the same discipline DST needs. Inject a `*rand.Rand` (or
   `math/rand/v2`'s `rand.Rand`) seeded explicitly into anything that makes a
   load-balancing or retry-jitter decision — never call the global `rand` functions from
   library code.
2. **Force single-goroutine execution of the scheduler's *decision* logic** by running
   the core lease/complete/fail state transitions behind a single mutex or a single
   dedicated goroutine reading off a channel (an actor, in effect) — this is a
   correctness-oriented design choice, not just a testing one, and it is cheap because
   the actual I/O to storage backends can still be concurrent; only the in-memory
   decision-making needs to be serialized per scope. This single-actor design is what
   makes a deterministic *replay* test tractable: drive the actor's input channel from a
   test harness that itself is single-threaded and seeded, recording the sequence of
   injected events (worker A requests lease at logical-tick 3, worker B acks node X at
   logical-tick 4, ...) so a failing sequence can be replayed byte-for-byte.
3. **Build a small in-process "chaos harness"** around the in-memory backend
   specifically: a wrapper store that, given a seed, randomly injects latency, a
   transient error, or a torn write (return success but don't actually commit — modeling
   a backend crash mid-operation) on a configurable fraction of calls. This is a much
   smaller engineering lift than simulating a whole network stack and it directly targets
   the property this project's owner cares most about: multiple instances racing against
   shared storage. Wrap the *storage interface*, not the network — for the Redis/
   Postgres/Memcached backends, real fault injection has to happen at the integration
   level (toxiproxy, or killing the container mid-test — see §4) since those clients own
   their own real I/O.
4. **Log the seed on every failure and provide a `-seed` replay flag** on the chaos
   harness's test entry point, mirroring `-shuffle`'s reproducibility contract (§1.3).

This is explicitly *not* full DST — it will not catch a bug that only manifests through
real OS thread-scheduling nondeterminism outside the actor — but it is a proportionate,
buildable middle ground that directly exercises the properties in §1.7's table under
adversarial timing, which a plain `-race`-clean property test does not.

### 2.2 Linearizability checking: Porcupine and the Jepsen/Elle approach

**Porcupine** — [`github.com/anishathalye/porcupine`](https://github.com/anishathalye/porcupine)
— checks whether a recorded concurrent history of operations (each with a call time, a
return time, an input, and an output) is consistent with *some* legal sequential
ordering of those operations that respects real-time non-overlap constraints, i.e., is
linearizable. It is a general-purpose Go library (not tied to any particular system) and
documents itself as "1,000x–10,000x" faster than the earlier Knossos checker via a
P-compositional algorithm. The API is exactly three pieces: `Init` (returns the initial
model state), `Step(state, input, output) (bool, newState)` (the reference sequential
semantics — "is this operation legal from this state, and what state does it produce"),
and an optional `Hash` for faster state deduplication during the search. For this
project's lease protocol, the model is small and mechanical to write:

```go
type leaseModel struct {
    status map[string]string // nodeID -> "new"|"leased"|"success"|"error"
    leasedBy map[string]string // nodeID -> workerID, only present while leased
}

type leaseOp struct {
    kind  string // "lease", "ack-success", "ack-error", "timeout"
    node  string
    worker string
}

var LeaseModel = porcupine.Model{
    Init: func() interface{} {
        return leaseModel{status: map[string]string{}, leasedBy: map[string]string{}}
    },
    Step: func(state, input, output interface{}) (bool, interface{}) {
        st := state.(leaseModel).clone()
        op := input.(leaseOp)
        switch op.kind {
        case "lease":
            if st.status[op.node] != "new" { return false, st } // illegal: double lease
            st.status[op.node] = "leased"
            st.leasedBy[op.node] = op.worker
        case "ack-success":
            if st.status[op.node] != "leased" || st.leasedBy[op.node] != op.worker {
                return false, st // illegal: acking a lease you don't hold
            }
            st.status[op.node] = "success"
        // ... ack-error, timeout similarly
        }
        return true, st
    },
}
```

Recording the history is the harder half in practice: every call to the scheduler's
public API from every goroutine (and, for the multi-instance case, every process) must
be timestamped at call and at return and serialized into Porcupine's `porcupine.Event`
format (`porcupine.CheckEvents(model, events)`), which means the test harness needs a
shared, monotonic, cross-process clock — for a single-process concurrency test that's
just `time.Now()`; for a genuine multi-process test against shared Redis/Postgres it
means either running all instances as goroutines in one test binary (simplest — gets you
the property test on the *protocol*, not on network partitions between real processes)
or accepting clock skew and widening the checker's tolerance, which Porcupine does not
natively support — real cross-process linearizability testing at that level is Jepsen's
territory, not Porcupine's.

**Jepsen / Elle.** Jepsen tests real, deployed clusters (not a model) by running a
workload against a live system while injecting real faults (network partitions via
`iptables`, process kills, clock skew) and then checking the resulting history for
consistency violations; **Elle** (Jepsen's newer checker, distinguishing it from
Knossos/Porcupine-style checkers) detects violations by looking for cycles in an
inferred dependency graph over the history rather than doing exhaustive/branch-and-bound
sequential-ordering search, which lets it scale to and diagnose weaker consistency
models (snapshot isolation, causal consistency) that pure linearizability checkers
aren't built for and gives it a documented advantage: when it finds a violation it can
point at the specific cycle (a concrete "G2-item" or "G-single" anomaly) rather than
just "not linearizable." Jepsen-style testing of this library is realistic and valuable
specifically for the **multi-instance-against-shared-storage** requirement in the
brief — spin up 2+ instances of the host program plus a real Redis/Postgres cluster in
Docker, inject partitions with `pumba` or `toxiproxy`, and record+check the resulting
lease history. This is heavier infrastructure than Porcupine and is realistically a
**post-1.0, backend-specific hardening project** rather than a day-one CI gate; Porcupine
against an in-process multi-goroutine harness is the right day-one investment because it
is cheap, fast, and runs on every PR.

### 2.3 Lightweight formal modelling: is TLA+ worth it here?

**Real opinion: yes, but scoped tightly to the lease state machine and the multi-instance
lease-acquisition protocol — not the whole library.** The case for formal methods on
*exactly this slice* is strong and well-precedented: Amazon's engineers report using
TLA+ specifically on DynamoDB-class replication and lease/lock protocols and found it
caught "subtle bugs in complex concurrent systems" that were "hard to find by testing" —
concurrency and timing-dependent errors in exactly the shape this project has (multiple
racing actors, a timeout, an ack protocol) — and their guidance is to reach for formal
methods for "high-criticality components" with "complex concurrency" where "the cost of
failure is high and bugs are hard to find through testing," treating it as "a testing
complement, not a replacement" — [Newcombe et al., "How AWS Uses Formal Methods" (PDF via lamport.azurewebsites.net)](https://lamport.azurewebsites.net/tla/formal-methods-amazon.pdf).
Lamport's own framing is that TLA+'s core value is "eliminating fundamental design
errors, which are hard to find and expensive to correct in code" —
[Lamport's TLA+ home page](https://lamport.azurewebsites.net/tla/tla.html) — i.e. it pays
off most *before* the implementation exists, exactly the design stage this project is
currently in.

The case *against* going further (modelling the whole storage-abstraction layer, or
every backend's specific transaction semantics in TLA+) is proportionality: this is an
MIT-licensed library, not a payments ledger, and the team asking for it has not asked for
a formal-methods practice — spending real calendar time getting fluent in TLA+ well
enough to model, say, Postgres's actual isolation-level behavior faithfully would be
weeks well spent on the wrong thing. The lease/ack/timeout state machine, by contrast, is
small (four to six states), the interesting behavior is entirely about *interleaving*
(exactly the class of bug testing is worst at and TLC's exhaustive model checking is
best at), and a first spec is genuinely a one-to-two day exercise for someone who has
done a little TLA+ before.

**Sketch of the spec** (illustrative skeleton, not exhaustive — the point is the shape):

```tla
---- MODULE LeaseProtocol ----
EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, Workers, MaxTime

VARIABLES
    status,      \* [Nodes -> {"new", "leased", "success", "error"}]
    leaseOwner,  \* [Nodes -> Workers \union {"none"}]
    leaseDeadline, \* [Nodes -> 0..MaxTime]
    now          \* current logical time, 0..MaxTime

TypeInvariant ==
    /\ status \in [Nodes -> {"new", "leased", "success", "error"}]
    /\ leaseOwner \in [Nodes -> Workers \union {"none"}]

\* Safety property this whole exercise exists to check:
NoDoubleLease ==
    \A n \in Nodes :
        status[n] = "leased" => leaseOwner[n] # "none"

\* The property from the brief, stated as a TLA+ invariant:
NeverLeasedTwiceConcurrently ==
    \A n \in Nodes :
        LET leasers == {w \in Workers : leaseOwner[n] = w} IN
            Cardinality(leasers) <= 1

Lease(n, w) ==
    /\ status[n] = "new"
    /\ status' = [status EXCEPT ![n] = "leased"]
    /\ leaseOwner' = [leaseOwner EXCEPT ![n] = w]
    /\ leaseDeadline' = [leaseDeadline EXCEPT ![n] = now + 30]
    /\ UNCHANGED now

AckSuccess(n, w) ==
    /\ status[n] = "leased"
    /\ leaseOwner[n] = w
    /\ status' = [status EXCEPT ![n] = "success"]
    /\ UNCHANGED <<leaseOwner, leaseDeadline, now>>

Timeout(n) ==
    /\ status[n] = "leased"
    /\ now >= leaseDeadline[n]
    /\ status' = [status EXCEPT ![n] = "error"]
    /\ leaseOwner' = [leaseOwner EXCEPT ![n] = "none"]
    /\ UNCHANGED <<leaseDeadline, now>>

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<status, leaseOwner, leaseDeadline>>

Next ==
    \/ \E n \in Nodes, w \in Workers : Lease(n, w)
    \/ \E n \in Nodes, w \in Workers : AckSuccess(n, w)
    \/ \E n \in Nodes : Timeout(n)
    \/ Tick

Spec == Init /\ [][Next]_<<status, leaseOwner, leaseDeadline, now>>

THEOREM Spec => [](TypeInvariant /\ NeverLeasedTwiceConcurrently)
====
```

Run this under TLC with `Nodes` and `Workers` bounded to small finite sets (e.g. 3 nodes,
2 workers) — TLC's exhaustive state-space search will surface any interleaving that
violates `NeverLeasedTwiceConcurrently`, including ones a human would never think to
write as a unit test (e.g., a lease acquired at `now=T`, timed out and reassigned at
`now=T+30`, and the *original* worker's stale ack arriving after reassignment — the
classic "zombie worker" bug this exact spec shape is built to catch, once an `AckSuccess`
guard for "was this the *current* lease owner" is added). Once the spec is trusted,
**the Go implementation is checked against it manually** (TLA+ model checking does not
verify Go source directly) — the payoff is a machine-checked confidence that the
*protocol* has no double-lease interleaving, which then narrows unit/property testing to
"does the code correctly implement this already-proven-safe protocol," a much easier
question. Treat the TLA+ spec as a living design artifact under `docs/adr/`, not a
one-off: update it whenever the lease state machine gains a state or transition, exactly
the ADR-then-spec discipline already in this project's own working conventions.

**P language, briefly**: [P](https://p-org.github.io/P/) (from the group that built the
formal foundations later reflected in some AWS/Azure verification work) targets exactly
"asynchronous event-driven systems" and — unlike TLA+ — can compile a P model down to
executable C/Java code and generate systematic testing harnesses from the same spec. It
is a heavier toolchain investment than TLA+ for a Go shop with zero existing P exposure
and no polyglot compile target need; **not recommended here** — the payoff-to-onboarding-
cost ratio favors TLA+ for a single, bounded state machine when nobody on the team has
used either tool before.

---

## 3. Coverage mechanics

### 3.1 The basic profile: `-coverprofile` and `-coverpkg`

```bash
go test -coverprofile=cover.out -covermode=atomic ./...
go tool cover -func=cover.out   # per-function %, plus a `total:` line
go tool cover -html=cover.out -o cover.html
```

`-covermode` has three settings, all documented at
[`pkg.go.dev/cmd/go`](https://pkg.go.dev/cmd/go): `set` (did this statement run — a
single boolean per statement, cheapest), `count` (how many times — an int counter, more
information but not safe for racing goroutines to increment), and `atomic` (`count` but
using atomic increments, "significantly more expensive" but correct when the same
statement runs concurrently from multiple goroutines). **Use `-covermode=atomic`
unconditionally for this project** — combining `go test -race -coverprofile` on a
scheduler whose entire purpose is concurrent execution with the default `set` mode would
under-count coverage of exactly the racy code paths that matter most, and the two flags
are commonly combined anyway (the race build already pays a large overhead, so atomic's
extra cost on top is comparatively small).

By default, coverage is computed **per package** — a test in package `scheduler` only
"sees" its own package's statements even if it drives code in package `storage` through
a real call chain, unless `-coverpkg` is given. `-coverpkg=pattern,pattern,...` (or
`-coverpkg=./...` for everything in the module) extends instrumentation to any package
matching the given import-path patterns, which is essential here: an end-to-end test in
`e2e_test` package that drives `scheduler` → `storage/redis` → real Redis should count
toward `storage/redis`'s coverage number, not just `e2e_test`'s own (empty) statement
count. **Recommended default**: `go test -coverpkg=./... -coverprofile=cover.out
-covermode=atomic ./...` from the repo root, so cross-package call chains are credited
to the packages that actually executed.

### 3.2 Integration coverage via `GOCOVERDIR` (Go 1.20+)

`go test -coverprofile` only instruments *test binaries*. A Docker-Compose-driven
end-to-end suite that builds the host program as a real binary, starts it, and drives it
over the network needs a different mechanism, introduced in Go 1.20 — instrument the
**production binary itself**:

```bash
go build -cover -o dagworkerd -coverpkg=./... ./cmd/dagworkerd
mkdir -p covdata
GOCOVERDIR=covdata ./dagworkerd --config e2e.yaml &
# ... run the actual end-to-end test suite against the running process, over the network ...
kill -TERM $!   # graceful shutdown; coverage is flushed on normal os.Exit()/return from main,
                 # NOT on a panic or SIGKILL — a killed-mid-test binary loses its coverage data
```

— [`go.dev/doc/build-cover`](https://go.dev/doc/build-cover). This produces
`covmeta.*`/`covcounters.*` files under `covdata/` for every run (multiple runs
accumulate multiple counter files against the same meta file), which `go tool covdata`
then processes:

```bash
go tool covdata percent -i=covdata                     # quick %, per package
go tool covdata textfmt -i=covdata -o e2e.out           # convert to go tool cover's legacy format
go tool covdata merge -i=unit_covdata,e2e_covdata -o combined_covdata   # merge unit + e2e runs
go tool covdata percent -i=combined_covdata             # the number that should gate CI
```

The critical operational detail from the docs: **coverage is only written out on a clean
exit** — `os.Exit()` or a normal return from `main` — so the end-to-end test harness
must send a graceful shutdown signal the binary handles (flushing `GOCOVERDIR` data)
rather than `SIGKILL`ing the process, or an entire end-to-end run's coverage silently
evaporates. For a service whose brief includes graceful timeout handling anyway, this is
one more reason the shutdown path needs a real, tested code path rather than "the test
harness just kills the process."

### 3.3 Excluding generated code

`go tool cover`/`covdata` has no first-class "exclude generated code" flag; the standard
approach is a post-processing filter over the profile text. Generated files are
conventionally marked with the `// Code generated ... DO NOT EDIT.` header (the exact
convention `go generate`-based tools follow); filter them out of the merged profile
before computing the percentage that gates CI:

```bash
# Strip lines whose source file matches generated-code patterns before scoring.
grep -v -E '(_mock\.go|_gen\.go|\.pb\.go):' combined.out > combined.filtered.out
go tool cover -func=combined.filtered.out | tail -1
```

A more robust variant scans the actual file headers rather than trusting filename
conventions (`grep -l "^// Code generated .* DO NOT EDIT\.$"` across the tree, build an
exclude-list, then filter the profile by that list) — filename-pattern matching is
faster to write but silently stops working the day a generator changes its output
filename convention.

### 3.4 What legitimately cannot be covered, and keeping the number honest

Be explicit about these categories rather than padding the number with tests that
exercise code without asserting anything about it (a well-known way coverage tools get
gamed — a test that calls a function and checks nothing raises the percentage without
raising confidence):

- **Unreachable defensive code.** A `default: panic("unreachable")` branch in a switch
  over an internal-only enum, added for exhaustiveness (§6, `exhaustive` linter) rather
  than because the branch is reachable, is legitimately untestable without contriving an
  invalid enum value via unsafe casts — don't; instead mark it with a `// coverage-ignore:
  reason` comment convention and exclude it from the denominator explicitly rather than
  writing a test that manufactures an invalid state just to tick the line.
- **OS/platform-specific branches** (a build-tagged file for an OS this project's CI
  matrix doesn't run) are only coverable by running CI on that OS — either add it to the
  matrix (§5.1) or explicitly exclude those files from the coverage gate with a
  documented reason, never silently.
- **True "this should never happen" storage-backend error paths** — e.g., a Postgres
  driver returning a wire-protocol error type that the client library's own tests already
  guarantee cannot occur for well-formed queries — are candidates for exclusion, but only
  with a comment citing *why* (linking the upstream guarantee), so the exclusion itself
  is auditable rather than a hiding place.
- **The honest number is coverage of *statements the test suite asserts something
  about*, not statements merely executed.** Since Go's tooling cannot distinguish "ran
  but nothing was asserted" from "ran and was verified," the only real defense is code
  review discipline plus mutation testing as a periodic spot-check: run
  [`go-mutesting`](https://github.com/avito-tech/go-mutesting) or a similar mutation
  tester occasionally (not on every PR — it's slow) against the highest-value packages
  (the lease/scheduling core) and confirm a high fraction of introduced mutants get
  killed by the existing suite; a 95%-statement-coverage suite that lets 40% of mutants
  survive is the textbook symptom of coverage gamed via assertion-free tests.
- **95% is a floor on the whole module, not on every file.** Enforce it via
  `go tool covdata percent` / `go tool cover -func` total line in CI, but do not require
  every single file to individually clear 95% — that pressure is precisely what produces
  padding tests on genuinely hard-to-test glue code; instead spot-check per-package
  numbers in review and let the aggregate gate the merge.

---

## 4. Integration infrastructure

### 4.1 `testcontainers-go` vs a hand-written Docker Compose file — an honest comparison

| | Docker Compose file | `testcontainers-go` |
|---|---|---|
| Fixed, predictable host ports | Trivial — declare `"16379:6379"` etc. directly | Actively works against you: [testcontainers-go docs](https://golang.testcontainers.org/) build around dynamic host-port allocation plus **Ryuk**, a companion "reaper" container that garbage-collects containers after a test run — getting a *fixed* port back out requires opting out of its normal mode |
| Matches the brief's explicit ask ("custom non-default ports") | Yes, directly — this is exactly what Compose's `ports:` mapping is for | No — fighting the tool's default and idiomatic usage |
| Per-test isolation (spin up a fresh, private container per test) | Manual — one shared Compose stack for the whole suite unless you script `docker compose run` per test | This is testcontainers' actual strength: request a fresh container from Go code per test/subtest, with automatic teardown |
| Readiness gating | `depends_on: condition: service_healthy` (§4.2) — coarse, at the Compose-stack level | Rich, composable per-container wait strategies (`wait.ForHealthCheck()`, `wait.ForListeningPort()`, `wait.ForLog()`, `wait.ForSQL()`, `wait.ForHTTP()`, `wait.ForAll(...)`) — [testcontainers-go docs](https://golang.testcontainers.org/) |
| Local developer ergonomics | `docker compose up` — works identically to how any operator would run these same databases; no Go toolchain needed to just bring the stack up for manual poking | Requires running the Go test binary; a developer who wants to `psql` into the test Postgres has no `docker compose up` to reach for unless one is also maintained |
| CI dependency footprint | Docker + Compose only | Docker + Ryuk (extra container) + the testcontainers-go module and its indirect deps |
| Cost of writing "the same suite against every backend" | A single `docker-compose.yml` with one service block per backend, referenced by one CI job matrix entry | A Go-side `map[string]func() Container` factory, functionally similar in spirit to §4.3 below regardless of which approach is used underneath |

**Recommendation: hand-written Docker Compose, per the project owner's explicit
request for custom fixed ports.** The clinching argument isn't just matching the ask —
it's that testcontainers' central value propositions (dynamic ports + Ryuk-managed
per-test container isolation) are specifically the two things this project does *not*
want: it wants one well-known, stable set of ports across local dev, CI, and anyone
debugging by hand with `redis-cli -p <custom-port>`, and it wants one long-lived stack
per CI run (spinning up a fresh Postgres container per test function would be far slower
than reusing one Postgres instance across a table-driven suite that resets state between
cases via `TRUNCATE`/`DEL`). Where testcontainers-go would actually help — an occasional
test that needs a container with faults injected mid-test via its exec/log/network APIs
— consider it as a narrow, additional dependency for *that one test file*, not as the
project's default integration harness.

### 4.2 Fixed ports and readiness in `docker-compose.yml`

```yaml
services:
  redis:
    image: redis:7.4-alpine
    ports: ["16379:6379"]          # custom, non-default host port per the brief
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 2s
      retries: 15
      start_period: 5s

  memcached:
    image: memcached:1.6-alpine
    ports: ["16211:11211"]
    healthcheck:
      test: ["CMD-SHELL", "echo stats | nc -w 1 localhost 11211 | grep -q STAT"]
      interval: 2s
      timeout: 2s
      retries: 15

  postgres:
    image: postgres:17-alpine
    ports: ["15432:5432"]
    environment:
      POSTGRES_PASSWORD: dagworker
      POSTGRES_DB: dagworker_test
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 2s
      retries: 15
      start_period: 5s

  integration-tests:
    build: { context: ., dockerfile: Dockerfile.test }
    depends_on:
      redis: { condition: service_healthy }
      memcached: { condition: service_healthy }
      postgres: { condition: service_healthy }
    environment:
      REDIS_ADDR: redis:6379          # in-network hostname:port, not the mapped host port
      MEMCACHED_ADDR: memcached:11211
      POSTGRES_DSN: postgres://postgres:dagworker@postgres:5432/dagworker_test?sslmode=disable
```

Two details from the [Compose spec](https://docs.docker.com/reference/compose-file/services/)
worth calling out precisely: `healthcheck.test` as a list must start with `NONE`, `CMD`,
or `CMD-SHELL` (the third form runs the rest through a shell, needed for the pipe in the
memcached check above); and `start_period` is a grace window during which failing checks
don't count toward `retries` — set it generously for Postgres, whose first-boot
initialization can briefly report not-ready. `depends_on: <service>: condition:
service_healthy` makes Compose block starting the dependent service until the healthcheck
passes — [Compose spec](https://docs.docker.com/reference/compose-file/services/) — which
is the mechanism that replaces a hand-rolled `wait-for-it.sh` polling loop.

### 4.3 One suite, every backend: a table of factories

```go
type BackendFactory struct {
    Name string
    New  func(tb testing.TB) Store // returns a ready Store; tb.Cleanup handles teardown
}

func Backends() []BackendFactory {
    return []BackendFactory{
        {"inmemory", newInMemoryStoreForTest},
        {"redis",    newRedisStoreForTest},     // dials REDIS_ADDR; t.Skip if unset
        {"memcached", newMemcachedStoreForTest},
        {"postgres", newPostgresStoreForTest},
    }
}

func TestStoreContract(t *testing.T) {
    for _, b := range Backends() {
        b := b // pre-1.22 rebind; harmless no-op at go 1.22+, keep it if go.mod floor is uncertain
        t.Run(b.Name, func(t *testing.T) {
            t.Parallel()
            store := b.New(t)
            testLeaseNeverDoubleGranted(t, store)
            testAntichainInvariant(t, store)
            testScopeImplicitCreation(t, store)
            // ... every contract test runs identically against every backend
        })
    }
}
```

This is the standard Go idiom for "same behavioral contract, N implementations" —
sometimes called a conformance/contract test suite. The factory function for each real
backend should `t.Skip()` (not fail) when its required connection info is absent, so
`go test ./...` without Docker running still passes — pushing the actual gating into the
build-tag/env-var mechanism below rather than into ad hoc skip logic scattered per test.

### 4.4 Keeping integration tests off the default `go test ./...` path

Three real options, compared honestly:

- **Build tags** (`//go:build integration`): explicit, and `go vet`/most editors respect
  build constraints correctly, but it fragments the source tree (every integration test
  file needs the tag, and a `_test.go` file *without* the tag can't share unexported
  helpers with one that has it without careful package layout) and is easy to forget.
- **`testing.Short()` / `-short`**: inverted from what's needed here — `-short` is
  designed to skip *slow* tests within an otherwise-normal run, not to require *extra
  infrastructure* the reader may not have running. Using it for "skip if no Docker" is a
  semantic mismatch that will confuse the next contributor who reads `-short` and expects
  "faster," not "different infra requirement."
- **An environment variable** (e.g. `DAGWORKER_INTEGRATION=1`), checked at the top of each
  integration test via `t.Skip()` if unset, or centralized in a `TestMain`: simplest,
  requires zero build machinery, keeps integration and unit tests in the same package
  (so they can share test helpers freely), and self-documents in the skip message
  (`t.Skip("set DAGWORKER_INTEGRATION=1 and start docker compose to run")`).

**Recommendation: the environment-variable gate**, combined with a package-naming
convention (`storage/redis/redis_integration_test.go`) for human navigability rather than
compiler enforcement. It is the lowest-ceremony option, keeps `go test ./...` fast and
Docker-free by default (matching "unit tests always run in parallel" from the brief —
they can't if `go test ./...` is blocked waiting on containers that aren't running), and
the CI-only nature of the gate is exactly mirrored by CI simply setting that one env var
before invoking the same `go test ./...` command — no separate `go test -tags=integration`
invocation to keep in sync with the default one.

---

## 5. CI pipeline design (GitHub Actions)

### 5.1 Matrix over Go versions and backends

```yaml
name: ci
on: [push, pull_request]

jobs:
  unit:
    strategy:
      matrix:
        go: ["1.24", "1.25"]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version: "${{ matrix.go }}" }   # setup-go caches modules by default
      - run: go build ./...
      - run: go test -shuffle=on ./...

  race:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version: "1.25" }
      - run: go test -race -shuffle=on -count=1 ./...

  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version: "1.25" }
      - run: go test -covermode=atomic -coverpkg=./... -coverprofile=cover.out ./...
      - run: |
          pct=$(go tool cover -func=cover.out | tail -1 | grep -oE '[0-9]+\.[0-9]+')
          echo "coverage: $pct%"
          awk -v p="$pct" 'BEGIN { exit !(p >= 95.0) }'

  integration:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        backend: [redis, memcached, postgres]
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version: "1.25" }
      - run: docker compose -f docker-compose.test.yml up -d ${{ matrix.backend }}
      - run: |
          timeout 60s bash -c \
            'until docker compose -f docker-compose.test.yml ps ${{ matrix.backend }} | grep -q "healthy"; do sleep 1; done'
      - env:
          DAGWORKER_INTEGRATION: "1"
        run: go test -race -run "TestStoreContract/${{ matrix.backend }}" ./storage/...
      - if: always()
        run: docker compose -f docker-compose.test.yml down -v

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version: "1.25" }
      - uses: golangci/golangci-lint-action@v9
        with: { version: v2.12 }
```

Splitting **race**, **coverage**, and plain **unit** into separate jobs (rather than one
job doing `go test -race -cover`) is deliberate: race mode's 2–20x slowdown and 5–10x
memory multiplier (§1.2) would otherwise inflate the coverage job's wall time for no
benefit (coverage doesn't need race instrumentation to be *accurate*, only to also
increment counters correctly under concurrency — hence `-covermode=atomic`, not `-race`,
being coverage's actual concurrency-correctness requirement, §3.1); running them as
separate jobs also means a race failure and a coverage-threshold failure show up as two
distinctly-named, independently-rerunnable check-suite entries rather than one conflated
red job.

### 5.2 Service containers vs Compose in CI

GitHub Actions' native `services:` block runs containers alongside the job with port
mapping via `ports: ["16379:6379"]`, and — for jobs running directly on the runner
(not inside a container job) — those ports are reached over `localhost` —
[GitHub Actions service-containers docs](https://docs.github.com/en/actions/using-containerized-services/about-service-containers).
This works and is simpler to read for a *single* backend, but for this project running
`docker compose -f docker-compose.test.yml up` in the integration job (as above) is the
better choice specifically because **it's the same file used for local development and
end-to-end testing** — a contributor debugging a failing integration test locally runs
the literal same Compose file GitHub Actions runs, whereas GitHub's native `services:`
syntax has no local-development equivalent and would have to be kept manually
synchronized with a separately-maintained Compose file, doubling the surface for the
ports/healthchecks to drift out of sync.

### 5.3 Caching

`actions/setup-go` (v5+) caches Go's module and build cache by default (`cache: true`),
keyed off a hash of `go.mod`/`go.sum` by default and configurable via
`cache-dependency-path` for multi-module repos —
[`actions/setup-go` README](https://github.com/actions/setup-go). This is sufficient for
most projects and the docs make no mention of layering `actions/cache` on top, so don't —
an extra hand-rolled cache step would only risk cache-key drift against the one
`setup-go` already manages. `golangci-lint-action` maintains its own separate cache for
`~/.cache/golangci-lint`, keyed by OS, working directory, a 7-day rotation window, and a
hash of `go.mod`, with automatic invalidation when dependencies change and a
`skip-cache: true` escape hatch — [`golangci-lint-action` README](https://github.com/golangci/golangci-lint-action).

### 5.4 `golangci-lint` in CI

Use the official `golangci/golangci-lint-action` (JS-based, not Docker, for speed) rather
than a hand-rolled `go install golangci-lint && golangci-lint run` step — it manages the
binary version pin, restores/saves its own cache, and renders findings as GitHub
annotations inline on the diff — [`golangci-lint-action` README](https://github.com/golangci/golangci-lint-action).
Pin `version: v2.12` (or whatever the current v2.x line is) explicitly rather than
`latest`, so a new golangci-lint release with newly-enabled-by-default linters cannot
turn a previously-green PR red without a deliberate version bump commit.

### 5.5 Benchmark regression tracking

Two real options: **`benchstat` against the base commit** (run in CI, compare, fail or
warn on regression) or **`benchmark-action/github-action-benchmark`** (persists results
across runs, renders a trend chart, comments on PRs). `benchstat` compares two Go
benchmark-output files, computing the **median** and a **95% confidence interval** per
benchmark and, for A/B comparison, a **Mann–Whitney U-test** p-value — a `~` in its
output means "no statistically significant difference detected"; the tool's own
guidance is to collect **at least `-count=10`** repetitions per side for a meaningful
comparison — [`pkg.go.dev/golang.org/x/perf/cmd/benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat).
For a project whose headline goal is O(1)/O(log n) performance at 1M nodes, wire this in
as a real CI gate, not just an informational report:

```yaml
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
        with: { fetch-depth: 0 }
      - uses: actions/setup-go@v6
        with: { go-version: "1.25" }
      - run: go install golang.org/x/perf/cmd/benchstat@latest
      - run: go test -run '^$' -bench . -benchmem -count=10 ./... | tee new.txt
      - run: |
          git checkout "${{ github.event.pull_request.base.sha }}" -- .
          go test -run '^$' -bench . -benchmem -count=10 ./... | tee old.txt
          git checkout "${{ github.sha }}" -- .
      - run: |
          benchstat old.txt new.txt | tee bench-diff.txt
          # fail the job if any benchmark regressed by more than a threshold with p<0.05 —
          # benchstat's own output format is the input to whatever threshold script gates this
```

`benchmark-action/github-action-benchmark` is the better fit for *tracking a trend over
many commits* (it stores a rolling history and renders a chart, and can auto-comment
regressions on a PR); `benchstat` run ad hoc against the PR's base commit is the better
fit for *gating a single PR* against regression without needing a persisted history
store. Run both is reasonable: `benchstat` as a required, blocking PR check;
`benchmark-action` as a non-blocking dashboard for long-term trend visibility (e.g.,
noticing a slow creeping regression that no single PR's benchstat comparison was large
enough to flag as statistically significant).

### 5.6 Coverage thresholds and required status checks

Compute the aggregate percentage as shown in §5.1's `coverage` job and fail the job
outright below 95% (the `awk`-based gate above), rather than relying on a third-party
coverage-hosting service (Codecov/Coveralls) as the sole enforcement — those add value
for PR-diff coverage annotations and historical trend charts, but a raw `go tool cover`
threshold check in the workflow itself has zero external dependency, zero flakiness from
a third-party service being down, and zero risk of a misconfigured webhook silently not
gating a merge. In branch protection, mark `unit`, `race`, `coverage`, `lint`, and each
`integration (backend)` matrix leg as **required status checks** — GitHub's branch
protection matches job names (including the matrix leg's rendered name, e.g.
`integration (redis)`), so a new backend added to the matrix must also be added to the
required-checks list or it silently becomes non-blocking.

---

## 6. Strict linting: a concrete `golangci-lint` v2 configuration

golangci-lint v2 changed the config file's shape: it now requires a top-level
`version: "2"` key, reorganizes linter settings under `linters:` with `default`/
`enable`/`disable`/`settings` subsections, and — the biggest structural change — **splits
formatters (`gofmt`, `gofumpt`, `goimports`, `gci`) into their own top-level `formatters:`
section**, separate from `linters:`, since formatters *rewrite* code while linters only
*report* on it — [`golangci-lint.run` configuration docs](https://golangci-lint.run/docs/configuration/file/).
A repository reference of every option lives at `.golangci.reference.yml` in the
project's own repo, and configs can be validated against a published JSON Schema.

```yaml
version: "2"

run:
  timeout: 5m
  tests: true

linters:
  default: none   # start from nothing; enable explicitly — see rationale below
  enable:
    # --- correctness / bug-finding, non-negotiable ---
    - errcheck        # every returned error must be handled or explicitly discarded
    - govet           # full vet suite, including shadow, loopclosure, etc.
    - staticcheck     # SA*/ST*/QF* checks — the single highest-value linter in the list
    - ineffassign     # assignments whose value is never used
    - unused          # dead code / unused identifiers

    # --- concurrency-specific, directly relevant to this project ---
    - bodyclose       # http.Response.Body must be closed on every path
    - sqlclosecheck   # sql.Rows / sql.Stmt must be closed
    - rowserrcheck    # sql.Rows.Err() must be checked after the iteration loop
    - contextcheck    # context must be propagated correctly, not silently dropped/replaced
    - containedctx    # a context.Context stored in a struct field is a smell, not a pattern
    - noctx           # HTTP requests / DB calls made without a context

    # --- exhaustiveness on the public status enum (the whole point of this project) ---
    - exhaustive      # every switch on NodeStatus must handle every case explicitly

    # --- style / API-shape enforcement the owner explicitly asked for ---
    - revive          # configurable replacement for the deprecated golint, see settings below
    - gocritic        # broad diagnostic + style + opinionated checks
    - ireturn         # "accept interfaces, return structs" — enforced, not just convention
    - nilnil          # a function must not return (nil, nil) as a valid-looking success value

    # --- test-suite discipline ---
    - paralleltest    # flags missing t.Parallel() and loop-variable reuse in subtests
    - tparallel       # flags t.Parallel() on a parent whose subtests aren't all parallel too
    - thelper         # test helper functions must call t.Helper()
    - testifylint     # correct usage of testify's assert/require (e.g. Equal arg order)

    # --- security ---
    - gosec           # G-series security checks (weak crypto, command injection, etc.)

    # --- performance, matches the project's headline goal ---
    - prealloc        # slices appended-to in a loop with a known bound should be preallocated
    - makezero        # make([]T, n) then append(...) instead of make([]T, 0, n) is a length bug magnet
    - perfsprint       # fmt.Sprintf misuse where string concatenation/strconv would do

    # --- complexity ceilings ---
    - funlen
    - cyclop
    - gocognit

    # --- dependency and API hygiene ---
    - depguard        # ban unapproved dependencies (see settings)
    - forbidigo       # ban fmt.Print*/println in library code (must use structured logging)

  settings:
    revive:
      rules:
        - name: exported                 # exported identifiers must have doc comments
        - name: error-return              # error must be the last return value
        - name: error-strings             # error strings must not be capitalized / end in punctuation
        - name: context-as-argument       # context.Context must be the first parameter
        - name: unused-parameter
        - name: indent-error-flow
        - name: range-val-in-closure      # pre-1.22 loop-capture guard, harmless no-op at 1.22+

    exhaustive:
      default-signifies-exhaustive: false  # a `default:` case must NOT be treated as covering
                                            # every enum value — this is exactly what makes
                                            # exhaustive catch a forgotten NodeStatus case when
                                            # a new status is added later
      check: [switch, map]

    ireturn:
      allow: [error, empty, anon, generic, stdlib]
      # storage.Store, scheduler.Scheduler etc. remain interfaces at the package boundary
      # (accepted as parameters) but constructors must return the concrete *Scheduler,
      # *RedisStore, etc. — ireturn enforces exactly this split.

    gocognit:
      min-complexity: 20

    cyclop:
      max-complexity: 15

    funlen:
      lines: 80
      statements: 50

    depguard:
      rules:
        main:
          deny:
            - pkg: "github.com/pkg/errors"
              desc: "use stdlib errors + fmt.Errorf(\"...: %w\", err) instead"
            - pkg: "io/ioutil"
              desc: "deprecated since Go 1.16; use io or os directly"

    forbidigo:
      forbid:
        - pattern: '^fmt\.Print.*$'
          msg: "use the configured structured logger, not fmt.Print*, in library code"

    gosec:
      excludes: [G104]   # errcheck already covers unchecked errors; avoid double-reporting

  exclusions:
    rules:
      - path: "_test\\.go"
        linters: [errcheck, gosec, funlen, cyclop, gocognit]
        # table-driven test bodies and test helpers legitimately run long and
        # sometimes intentionally ignore errors from test-only setup calls

formatters:
  enable:
    - gofumpt   # stricter superset of gofmt
    - goimports
```

### 6.1 Deliberately **not** enabled, and why

- **`wrapcheck`**: wrapcheck's own stated rationale is legitimate — "errors from external
  packages" should carry call-site context —
  [`tomarrell/wrapcheck` README](https://github.com/tomarrell/wrapcheck) — but for a
  *library* whose own errors are the "external package" from its callers' point of view,
  blanket-enforcing wrap-everything produces two costs that outweigh the benefit here:
  (1) it actively fights **sentinel-error and `errors.Is`/`errors.As` design**, which
  this project needs for its public error taxonomy (a caller checking
  `errors.Is(err, dagworker.ErrNodeNotFound)` needs that sentinel to survive unwrapped or
  `%w`-wrapped consistently, and wrapcheck's default posture nudges every return site
  toward wrapping regardless of whether that call site is a public boundary or an
  internal one three functions deep), and (2) it's high-noise against internal-only
  helper functions where a caller two lines up already has full context. Net effect: use
  wrapping as a deliberate, reviewed API design decision at genuine package boundaries
  (documented in the ADR), not a linter-enforced blanket rule.
- **`err113`** (the enforce-wrapped-static-errors / disallow-`errors.New`-in-conditionals
  linter): actively hostile to the sentinel-error pattern this project's public API
  needs (`ErrCycleDetected`, `ErrNodeNotFound`, etc. as package-level `errors.New`
  values checked via `errors.Is`) — enabling it would mean fighting the linter to build
  the exact error-handling API the brief implies ("nodes have a public status... error").
  Skip it.
- **`exhaustruct`**: forces every struct literal to set every field, which is
  actively wrong for this project's public API shape — `LeaseOptions{Timeout:
  30*time.Second}` relying on zero-value defaults for everything else is the intended,
  idiomatic usage of a functional-options-adjacent config struct, and `exhaustruct`
  would force every call site to spell out every field, defeating the point of having
  sane defaults.
- **`gochecknoglobals`** / **`gochecknoinits`**: overly blunt for a library that
  legitimately wants a small number of well-considered package-level sentinel errors and
  registered default-backend constructors; ban these via code review judgment, not a
  linter that fires on every legitimate case alongside the bad ones.
- **`lll`** (line-length): gofumpt/goimports already produce reasonably wrapped code, and
  a hard line-length linter mostly generates busywork reformatting long but readable
  struct-literal or table-driven-test lines; not worth the friction for the benefit.
- **`wsl`/`wsl_v5`** (whitespace linter, enforcing blank-line placement rules): highly
  opinionated beyond what gofumpt already normalizes; the marginal readability gain
  doesn't justify the churn on every PR.
- **`godox`** (flags TODO/FIXME comments): actively counterproductive during a
  greenfield build where TODOs are a legitimate, temporary way to flag known gaps before
  the corresponding ADR/spec/test/implementation cycle catches up — enable it later,
  post-1.0, as a "no lingering TODOs before release" gate instead of a permanent one.
- **`varnamelen`**: flags short variable names like `n` for a node or `i` for an index;
  for a graph/scheduler codebase where `n *Node`, `e Edge`, `w Worker` are the idiomatic,
  locally-scoped, highly readable names Go style already favors, this linter's
  complaints would be net noise.

### 6.2 The v2 migration point, restated precisely

Anyone hand-writing this config against older tutorials or Stack Overflow answers will
find v1-shaped YAML (`linters: enable: [...]` at the top level without `version: "2"`,
formatter options mixed into `linters-settings`) — that shape is rejected by v2 binaries.
The **required, non-optional top-level key `version: "2"`** is the tell for which format
a given example predates —
[`golangci-lint.run` configuration docs](https://golangci-lint.run/docs/configuration/file/).
Pin the action to a v2.x release (§5.4) and write the config in v2 shape from day one;
there is no reason for a greenfield project in 2026 to start from v1 config and migrate
later.

---

## Recommendations for dag-worker-go

1. **Pin `go 1.22` as the absolute floor in `go.mod`** (prefer `1.25` given the new
   toolchain-directive semantics and `synctest` availability) so the loop-variable fix
   and `testing/synctest` are both guaranteed available without `GOEXPERIMENT` flags.
2. **Architect the scheduler's internal timing exclusively through the stdlib `time`
   package** (no custom clock abstraction beyond a thin wrapper), specifically so
   `testing/synctest.Test` can wrap the lease-timeout state machine end-to-end with zero
   custom fake-clock plumbing — write this constraint into the ADR before implementation,
   not as an afterthought.
3. **Serialize the core lease/complete/fail decision logic behind a single actor (mutex
   or dedicated goroutine) per scope**, both as a correctness simplification and because
   it is the prerequisite for the deterministic-replay chaos harness in §2.1 — this is an
   architectural decision, make it explicit in the ADR.
4. **Build the seeded chaos-wrapper store early** (§2.1, item 3) — wrapping the
   in-memory backend with a seeded fault-injector is cheap, directly exercises the four
   properties in §1.7's table, and gives a `-seed`-reproducible failure mode from day
   one, well before the Redis/Postgres/Memcached backends exist.
5. **Adopt `pgregory.net/rapid` with `t.Repeat` as the property-testing backbone** for
   the four named properties (no double lease, eventual readiness, antichain invariant,
   acyclicity under interleaving) — write these before the scheduler implementation is
   complete, per the working-conventions failing-test-first discipline.
6. **Write the lease/ack/timeout protocol as a small TLA+ spec now**, during design,
   specifically to machine-check the "no node ever leased twice concurrently" and
   "no stale ack after reassignment" properties before writing Go — this is the highest
   leverage-per-hour item in this whole dossier given the project is still greenfield.
7. **Adopt Porcupine for a same-process multi-goroutine linearizability test** of the
   public lease API against the small reference model in §2.2 — cheap, fast, CI-eligible
   on every PR; defer Jepsen/Elle-style cross-process fault injection to a post-1.0
   hardening milestone explicitly scoped to the multi-instance-shared-storage design.
8. **Use `docker-compose.test.yml` with fixed custom ports and `service_healthy` gating**
   (§4.2) as the integration backbone, gated behind a single `DAGWORKER_INTEGRATION=1`
   env var (§4.4), and drive every backend through one `BackendFactory` table (§4.3) so
   the contract suite is written once and runs identically against all four backends.
9. **Split CI into independent `unit` / `race` / `coverage` / `integration (matrix)` /
   `lint` / `benchmark` jobs** rather than one monolithic job, gate `-covermode=atomic`
   (not `-race`) coverage at ≥95% via a plain `awk` check with no third-party coverage
   service as the sole enforcement, and require every matrix leg by name in branch
   protection.
10. **Ship the v2 golangci-lint config from §6 verbatim as a starting point**, explicitly
    documenting in the repo (a `docs/adr/000X-linting.md`) *why* `wrapcheck`/`err113`/
    `exhaustruct` are deliberately excluded — future contributors reaching for "just
    enable the strict error-wrapping linter" need the sentinel-error rationale
    immediately visible, not rediscovered in a PR debate.
11. **Treat 95% coverage as an aggregate module-level gate, never a per-file
    requirement**, and schedule an occasional (not per-PR) mutation-testing spot-check
    on the scheduler core to catch assertion-free padding tests that a raw percentage
    cannot distinguish from real coverage.

## Open questions

- **How far to push the deterministic-simulation harness before it's worth forking the Go
  runtime or targeting WASM** — the §2.1 middle-ground (seeded chaos-wrapper store +
  single-actor decision logic) is proportionate for v1, but if the multi-instance,
  shared-storage guarantees become the project's primary marketed differentiator, is a
  Polar-Signals-style WASM-compiled single-threaded build worth the investment for a true
  FoundationDB-grade simulation, and who maintains that fork long-term?
- **Where exactly does Jepsen/Elle-style real-cluster fault injection belong on the
  roadmap** — is it a pre-1.0 gate for the Redis/Postgres backends specifically (since
  those are the ones multiple instances actually contend over), or a post-1.0 hardening
  project, and does the team have (or want to acquire) the operational Jepsen expertise
  to run and interpret it, versus contracting it out once per backend?
- **Should the TLA+ spec's scope grow to include the multi-instance work-distribution
  protocol** (partition-per-scope / consistent hashing / lease stealing, per the open
  design question in the brief) once that protocol is chosen, given that inter-instance
  coordination is exactly the kind of interleaving-heavy problem TLA+ is best at, or does
  that belong in a second, separate spec written only after the distribution strategy
  itself is settled?
- **What mutation-testing tool and cadence** — `go-mutesting` is unmaintained-looking;
  is there a currently-active Go mutation tester worth standardizing on, and should the
  spot-check in Recommendation 11 be a scheduled quarterly job, a pre-release gate, or
  purely ad hoc when coverage numbers look suspiciously easy?
- **Does the benchmark-regression gate (§5.5) need machine-class pinning** — GitHub-hosted
  runners have documented CPU variability run-to-run; is a self-hosted, dedicated runner
  needed before `benchstat`'s p-value-based regression gate can be trusted as a hard
  blocking check rather than an advisory one, and what noise floor has to be tolerated in
  the meantime?

## Sources

- [Go 1.25 Release Notes](https://go.dev/doc/go1.25) — `testing/synctest` GA, `T.Attr`/`B.Attr`/`F.Attr`, `T.Output`
- [`testing/synctest` package docs](https://pkg.go.dev/testing/synctest) — bubble semantics, `Test`, `Wait`, `Sleep`, durable blocking
- [Go 1.22 Release Notes](https://go.dev/doc/go1.22) — per-iteration loop variables, range-over-int
- [`pkg.go.dev/testing`](https://pkg.go.dev/testing#T.Setenv) — `T.Setenv` parallel restriction, `T.Cleanup` LIFO ordering
- [`pkg.go.dev/cmd/go`](https://pkg.go.dev/cmd/go) — `-covermode`, `-coverpkg`, `-count`, `-race` platform list
- [Go race detector article](https://go.dev/doc/articles/race_detector) — detection scope, overhead, platform support
- [Go fuzzing docs](https://go.dev/security/fuzz/) — corpus mechanics, `f.Add`, supported types, CI guidance
- [Go integration coverage / `build-cover`](https://go.dev/doc/build-cover) — `-cover` binary builds, `GOCOVERDIR`, `go tool covdata`
- [`pgregory.net/rapid` docs](https://pkg.go.dev/pgregory.net/rapid) — generators, `Check`, `T.Repeat` stateful testing
- [`leanovate/gopter` README](https://github.com/leanovate/gopter) — properties, generators, `commands` package
- [Apple's FoundationDB testing docs](https://apple.github.io/foundationdb/testing.html) — simulated cluster, fault injection, CPU-hour estimate
- [Phil Eaton, "Deterministic simulation testing" notes](https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html) — Will Wilson talk summary, cross-language DST implementations
- [TigerBeetle safety / VOPR docs](https://docs.tigerbeetle.com/concepts/safety/) — simulator description, fault domains, scale
- [`anishathalye/porcupine` README](https://github.com/anishathalye/porcupine) — linearizability checking API and performance
- [Lamport's TLA+ home page](https://lamport.azurewebsites.net/tla/tla.html) — purpose, TLC, TLAPS
- [Newcombe et al., "How AWS Uses Formal Methods" (PDF)](https://lamport.azurewebsites.net/tla/formal-methods-amazon.pdf) — bug categories found, cost/effort guidance
- [P language](https://p-org.github.io/P/) — asynchronous event-driven system modelling and code generation
- [`golangci-lint.run` configuration docs](https://golangci-lint.run/docs/configuration/file/) — v2 config shape, `version: "2"`, formatters split
- [`golangci-lint.run` linters list](https://golangci-lint.run/docs/linters/) — linter catalog by category
- [`golangci/golangci-lint-action` README](https://github.com/golangci/golangci-lint-action) — caching, version pinning, workflow example
- [`actions/setup-go` README](https://github.com/actions/setup-go) — built-in module/build caching, version syntax
- [GitHub Actions service containers docs](https://docs.github.com/en/actions/using-containerized-services/about-service-containers) — port mapping, localhost access
- [Docker Compose services reference](https://docs.docker.com/reference/compose-file/services/) — `healthcheck`, `depends_on.condition: service_healthy`
- [`golang.testcontainers.org`](https://golang.testcontainers.org/) — wait strategies, Ryuk reaper, dynamic ports
- [`pkg.go.dev/golang.org/x/perf/cmd/benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) — Mann-Whitney U-test, `-count=10` guidance, filter/table/row/col flags
- [`tomarrell/wrapcheck` README](https://github.com/tomarrell/wrapcheck) — error-wrapping rationale and configuration
- [`butuzov/ireturn` README](https://github.com/butuzov/ireturn) — accept-interfaces-return-structs enforcement, allow/reject config
- [`kunwardeep/paralleltest` README](https://github.com/kunwardeep/paralleltest) — `t.Parallel()` and loop-variable-reuse checks
- [`nishanths/exhaustive` README](https://github.com/nishanths/exhaustive) — exhaustive switch/map checking, `default-signifies-exhaustive`
