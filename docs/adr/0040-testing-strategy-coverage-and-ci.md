# ADR-0040: Testing strategy, coverage policy, and CI topology

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/11-testing-verification-and-ci.md §1-5; docs/research/00-synthesis.md §1, §8-9

## Context

`dagworker`'s entire value proposition is correctness under concurrent access from multiple
processes against shared storage, at a mandated 1,000,000-node scale, across three real backends
(in-memory, Redis, PostgreSQL — memcached is dropped entirely per AMD-5/ADR-0017 and never
appears as a backend under test). A plain `go test ./...` with a coverage number tells you almost
nothing about that specific property: `-race` "only reports races that actually execute" — a
race-free run over a 60%-covered suite says nothing about the other 40% — and a green suite proves
nothing about interleavings the Go scheduler simply did not happen to produce on a given run.
Getting real confidence requires several genuinely different techniques layered on top of each
other, each catching a different class of bug, and each with a defined, permanent home in the repo
rather than a one-off script someone wrote before a release.

Three architectural facts drive where each technique lives. First, ADR-0029 pins Go at exactly
1.25 with no lower floor, which makes `testing/synctest` (GA, not experimental) an unconditional
part of the test architecture for the lease-timeout state machine — this only works because
ADR-0029 already commits the scheduler's internal timing exclusively to the stdlib `time` package.
Second, ADR-0031's module split means a storage backend's own conformance tests must live inside
that backend's own module (`storage/redis`, `storage/postgres`), each calling into the shared
`dagstore/dagstoretest.RunConformance` suite that lives in core — there is no single directory that
can import both backends without re-creating the dependency-weight problem ADR-0031 exists to
avoid. Third, the owner's explicit request for **custom, non-default ports** in the integration
Compose file rules out `testcontainers-go` as the integration harness: its two headline features —
dynamic host-port allocation and Ryuk-managed per-test container teardown — are exactly the two
things this project does not want, since it wants one stable, well-known port a developer can
`redis-cli -p 16379` into by hand, identically in local dev and in CI.

The coverage gate itself needs a specific, honest definition. Go's default coverage instrumentation
is per-package unless `-coverpkg` widens it, `-covermode=set` (the default) silently under-counts
concurrent code because it isn't safe for racing goroutines to increment, and a Docker-Compose-
driven end-to-end suite that drives a *built binary* over the network cannot use `-coverprofile` at
all — it needs the separate `GOCOVERDIR` instrumented-binary mechanism, merged after the fact with
`go tool covdata`. A 95%-of-statements number is also gameable in a way Go's own tooling cannot
detect: "ran but nothing was asserted" and "ran and was verified" produce an identical coverage
report, which is precisely why this ADR treats occasional mutation-testing spot checks and a
same-process linearizability checker as first-class parts of the strategy, not optional extras.

## Decision

### Four test tiers, with a fixed location each

| Tier | Lives at | Backend(s) | Gate |
|---|---|---|---|
| **Unit** | co-located `_test.go` next to every package, in every module | none — no I/O, no goroutscale infra beyond the in-memory store | every `go test ./...`, every push |
| **Feature** | `test/feature/` inside the **core module only** | in-memory (default `Store`) | every push, no external infra |
| **Storage** | inside each backend module (`storage/redis/*_test.go`, `storage/postgres/*_test.go`) plus core's own in-memory backend package, all calling `dagstore/dagstoretest.RunConformance` | in-memory (unconditional), Redis + Postgres (gated) | Redis/Postgres legs gated by `DAGWORKER_INTEGRATION=1` |
| **Perf** | `test/perf/` inside the core module | in-memory, Redis, Postgres in the same run | ratio guards on every push (required); absolute 1M-node benchmarks nightly (advisory + trend) |

The **storage** tier deliberately has no single cross-module directory: `storage/redis`'s
conformance test file lives in `storage/redis` and imports `dagstore/dagstoretest` off the
`require` line it already has on core (ADR-0031) plus its own `go-redis` client; `storage/postgres`
mirrors this with `pgx`. This keeps each backend module's own `go test ./...` fully self-contained
— no module outside `cmd/dagworkerd` ever needs to import more than one backend at a time.

### Unit tests: `t.Parallel()` everywhere, `synctest` for timeouts, `rapid` for the scheduler

Every unit test calls `t.Parallel()` unless a specific, commented reason prevents it (e.g. a test
that legitimately needs `t.Setenv`, which is documented as incompatible with parallel ancestors).
`paralleltest`/`tparallel` (ADR-0039) enforce this mechanically rather than by convention.

The lease-timeout state machine is tested via `testing/synctest.Test`, which requires — and is the
reason ADR-0029 requires — that the internal scheduler's timing goes exclusively through the
stdlib `time` package:

```go
func TestLeaseTimeout_MarksNodeErrorTimeout(t *testing.T) {
    synctest.Test(t, func(t *testing.T) {
        store := newInMemoryStore()
        mgr, _ := dagworker.New(store, dagworker.WithDefaultLeaseTimeout(30*time.Second))
        // ... claim a node, then advance the bubble's virtual clock past the deadline:
        synctest.Sleep(31 * time.Second)
        n, _ := mgr.GetNode(context.Background(), scope, id)
        if n.Status != dagworker.StatusError || n.Outcome.Reason != dagworker.ReasonTimeout {
            t.Fatalf("got %v/%v, want Error/ReasonTimeout", n.Status, n.Outcome.Reason)
        }
    })
}
```

This runs in microseconds of wall-clock time, deterministically, only because the timeout-sweeper
goroutine is started from inside the bubble and uses `time.After`/`context.WithDeadline` — no
hand-rolled ticker, no externally-injected clock (ADR-0029).

**Property tests** use `pgregory.net/rapid` with `t.Repeat`'s stateful/model-based API — a real
scheduler run alongside a trivial reference-model reimplementation, with an empty-string `""` key
in the action map serving as the invariant check that runs around every action. Four named
properties are mandatory, written before the scheduler implementation is complete:

| Property | Check |
|---|---|
| **No double lease** | A `map[NodeID]WorkerID` of currently-out leases, maintained in the *test*, fails immediately if `Claim` returns a node already present in it. |
| **Eventual readiness** | If a node's predecessors have all succeeded (per the reference model) and it isn't terminal, it must be `Ready` — or reachable to `Ready` — after quiescence; checked after settling, never at an arbitrary interleaving (a liveness property, bounded accordingly). |
| **Antichain invariant** | For every pair of nodes in the in-flight set, neither is a transitive predecessor of the other in the current graph — an O(k²) pairwise check over the small in-flight set. |
| **Acyclicity under interleaving** | `HasCycle() == false` after **every single mutation**, not just at the end of a sequence — a property test that only checks final state can miss a transient cycle a concurrent reader could have observed mid-mutation. |

A **seeded chaos harness** (`internal/chaos`, core module — no external dependency) wraps the
in-memory `Store` and, given a seed, injects latency, a transient error, or a torn write (return
success without actually committing, modeling a mid-operation crash) on a configurable fraction of
calls. It logs its seed on every failure and accepts a `-seed` replay flag, mirroring `-shuffle`'s
reproducibility contract, and is exercised by the same rapid property tests above to stress the
four properties under adversarial timing — this is an intentionally narrower substitute for
FoundationDB/TigerBeetle-grade deterministic simulation (full DST requires forking the Go runtime
or a WASM single-threaded target, disproportionate for a library), proportionate because the
scheduler's internal clock and randomness are already injectable (ADR-0029) and its
decision-making is already serialized per scope (ADR-0033).

A same-process **Porcupine** linearizability check runs as a unit-tier test: every call to the
public `Claim`/`Ack`/`Nack`/`Extend` surface, from every goroutine in the test, is timestamped at
call and return and checked against a small reference `porcupine.Model` (state transitions:
`new → leased → success|error`, illegal to lease an already-leased node or ack a lease you don't
hold). This runs on every PR — cheap, fast, no external infra. Cross-process Jepsen/Elle-style
fault injection against real multi-instance Redis/Postgres clusters is explicitly **out of scope**
for this ADR and deferred to a post-1.0 hardening milestone; Porcupine against an in-process
multi-goroutine harness is the correct day-one investment.

### Storage tier: `DAGWORKER_INTEGRATION=1`, fixed ports, `service_healthy`

Integration tests are gated by a single environment variable, checked via `t.Skip()` when unset —
not a build tag (fragments the source tree, easy to forget) and not `testing.Short()` (a semantic
mismatch: `-short` means "skip slow tests," not "requires infrastructure the reader may not have
running"). `go test ./...` with no Docker running still passes everywhere, always.

`docker-compose.test.yml` at the repo root — hand-written, not `testcontainers-go`, per the custom-
port requirement — with **Redis and PostgreSQL only** (no memcached service, AMD-5):

```yaml
services:
  redis:
    image: redis:7.4-alpine
    ports: ["16379:6379"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 2s
      retries: 15
      start_period: 5s

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
```

`depends_on: <service>: condition: service_healthy` gates any dependent service on the healthcheck
passing — this is the mechanism that replaces a hand-rolled polling loop, and it is the same file a
contributor runs by hand (`docker compose -f docker-compose.test.yml up`) as the one CI runs, so a
failing integration test is reproducible identically in both places.

### Coverage: 95% aggregate, per module, never per file

```bash
# inside each module's own directory (ADR-0031's per-module CI loop):
go test -covermode=atomic -coverpkg=./... -coverprofile=cover.out ./...
pct=$(go tool cover -func=cover.out | tail -1 | grep -oE '[0-9]+\.[0-9]+')
awk -v p="$pct" 'BEGIN { exit !(p >= 95.0) }'
```

`-covermode=atomic` unconditionally — `set` (the default) silently under-counts exactly the
concurrent code paths this project cares most about, and the extra cost is marginal on top of a
build already paying for correctness-focused testing. `-coverpkg=./...` unconditionally — a
cross-package call chain (a `test/feature` test driving `internal/engine` → `internal/lease`) must
credit the packages it actually executed, not just the calling test package's own (empty) statement
count.

Coverage from the Docker-Compose-driven end-to-end suite comes from the **built `dagworkerd`
binary itself**, since `-coverprofile` only instruments test binaries:

```bash
go build -cover -o dagworkerd -coverpkg=./... ./cmd/dagworkerd
GOCOVERDIR=covdata ./dagworkerd --config e2e.yaml &
# ... drive the e2e suite over the network against the running process ...
kill -TERM $!   # graceful shutdown — coverage flushes ONLY on clean exit, never on SIGKILL
go tool covdata merge -i=unit_covdata,covdata -o combined
go tool covdata percent -i=combined
```

The 95% gate is **aggregate per module, never per file** — enforced via the single `total:` line
from `go tool cover -func`, not a per-file threshold. Per-file 95% pressure is precisely what
produces assertion-free padding tests on genuinely hard-to-test glue code; spot-checking per-
package numbers is a code-review activity, not a CI gate. Legitimately uncoverable code
(`default: panic("unreachable")` on an internal-only enum switch, OS-specific branches this
project's CI matrix doesn't run) is excluded with a `// coverage-ignore: reason` comment convention
and a documented reason, never silently. An occasional (not per-PR) mutation-testing pass over the
lease/scheduling core is scheduled quarterly to catch the gap Go's own tooling cannot see: a test
that runs a statement but asserts nothing about it.

### CI job matrix

One Go version only — **1.25**, per ADR-0029's single floor; no multi-version hedge matrix.

```yaml
jobs:
  unit:        # per-module loop (ADR-0031), every push
    steps: [go build ./..., go test -shuffle=on ./...]

  race:        # every push
    steps: [go test -race -shuffle=on -count=1 ./...]

  coverage:    # every push, aggregate ≥95% gate per module
    steps: [go test -covermode=atomic -coverpkg=./... -coverprofile=cover.out ./..., awk-threshold-check]

  storage-integration:
    strategy: { matrix: { backend: [redis, postgres] } }   # no memcached — AMD-5
    steps:
      - docker compose -f docker-compose.test.yml up -d ${{ matrix.backend }}
      - wait-for service_healthy
      - env: { DAGWORKER_INTEGRATION: "1" }
        run: go test -race -run "TestStoreContract/${{ matrix.backend }}" ./storage/${{ matrix.backend }}/...
      - docker compose -f docker-compose.test.yml down -v

  lint:        # ADR-0039, pinned golangci-lint v2.x
    steps: [golangci-lint-action]

  complexity-ratio-guard:   # every push, required — dimensionless, same-run ratios
    steps:
      - go test -run '^$' -bench 'BenchmarkComplexity_.*' ./test/perf/...
      - assert: pipelined >= 20x unpipelined; in-memory >= 100x Redis; Redis >= 300x Postgres — same run

  benchmark-nightly:        # scheduled, advisory + trend, NOT a blocking PR check
    steps:
      - go test -run '^$' -bench . -benchmem -count=10 ./test/perf/... | tee new.txt
      - benchstat against the previous nightly baseline; feed benchmark-action for trend charts
```

Splitting `unit`/`race`/`coverage` into separate jobs is deliberate: `-race`'s 2–20x slowdown and
5–10x memory multiplier would otherwise inflate the coverage job's wall time for no accuracy
benefit — `-covermode=atomic`, not `-race`, is coverage's own concurrency-correctness requirement.
Every job name above, including each `storage-integration` matrix leg's rendered name (e.g.
`storage-integration (redis)`), is a required status check in branch protection — a backend added
to the matrix later must be added to that list explicitly or it silently becomes non-blocking.

**Ratio guards are a required, blocking, every-push gate; absolute 1M-node benchmarks are nightly
and advisory.** This split is deliberate, not a demotion of the absolute numbers: dimensionless
ratios computed in the *same CI run* (pipelined ≥ 20× unpipelined; in-memory ≥ 100× Redis ≥ 300×
Postgres) are stable across whatever CPU class a shared runner happens to be that day, while
absolute latency thresholds are not — gating a PR on an absolute number measured on noisy shared
infrastructure produces exactly the flaky-benchmark-blocks-merge failure mode this design avoids
by construction. The nightly job uses `benchstat`'s own documented floor of `-count=10` and tracks
trend via `benchmark-action/github-action-benchmark` so a slow creeping regression too small for
any single PR's ratio check to flag is still visible over weeks.

## Consequences

### Positive
- Four tiers with one fixed location each means a new contributor never has to guess where a test
  belongs, and the per-module CI loop (ADR-0031) means no nested module's tests are silently
  skipped by a root-level `go test ./...`.
- `synctest`-based timeout tests are deterministic and run in microseconds — no `-race`-under-load
  flake from a real 30-second sleep racing a background sweeper's polling interval.
- The four named rapid properties plus the seeded chaos harness plus Porcupine give three
  structurally different techniques converging on the same claim (no double lease, no lost
  readiness, no cycle, no stale ack after reassignment) — a bug that slips past one is unlikely to
  slip past all three.
- The aggregate-not-per-file coverage rule removes the single biggest incentive to write
  assertion-free padding tests.

### Negative
- Four tiers, a chaos harness, a Porcupine model, and a `rapid` property suite is real upfront
  engineering investment before the scheduler's first line of production logic exists — this is a
  deliberate "write the failing test first" front-load, not a cost deferred to later.
- The storage tier's per-backend-module test placement (no shared cross-module integration
  directory) means `dagstore/dagstoretest`'s public surface is a contract every future backend
  module must satisfy exactly — a breaking change to `RunConformance` itself requires touching
  every backend module in the same release window as core, which is the one place this design
  re-introduces a coordination cost ADR-0031 otherwise avoids.
- Jepsen/Elle-style cross-process fault injection is explicitly deferred — real multi-instance
  partition/clock-skew bugs against live Redis/Postgres clusters will not be caught by this ADR's
  day-one gates, only by the post-1.0 hardening milestone.

### Neutral
- Mutation testing is scheduled quarterly, not per-PR, because it is slow; the tool choice
  (`go-mutesting` or a successor) is left open and revisited when the quarterly cadence is first
  exercised in practice.
- The nightly absolute-benchmark job's runner-class variability is a known, accepted limitation —
  self-hosted, pinned-class runners are a future upgrade if `benchstat`'s p-value gate needs to
  become a hard blocking check rather than an advisory trend line.

## Alternatives considered

**`testcontainers-go` as the integration harness.** Rejected per the owner's explicit request for
fixed, custom, non-default ports: testcontainers-go's two headline features (dynamic host-port
allocation, Ryuk-managed per-test container teardown) are precisely the two things this project
does not want — it wants one stable port set identical across local dev, CI, and manual debugging
with `redis-cli -p 16379`.

**Build tags (`//go:build integration`) instead of an env-var gate.** Rejected: fragments the
source tree (every integration file needs the tag; an untagged file can't share unexported helpers
with a tagged one without careful package layout) and is easy for a contributor to simply forget.

**`testing.Short()`/`-short` as the integration gate.** Rejected: a semantic mismatch — `-short` is
documented for skipping *slow* tests within an otherwise-normal run, not for requiring *extra
infrastructure* the reader may not have; using it here would confuse the next contributor who
expects `-short` to mean "faster," not "different infra requirement."

**Full FoundationDB/TigerBeetle-style deterministic simulation testing** (fork the Go runtime for
seeded single-threaded scheduling, or compile to WASM for the same effect). Rejected as
disproportionate for a library rather than a database kernel — the narrower chaos-harness-plus-
injected-clock approximation captures the properties this project's owner cares most about
(multiple instances racing against shared storage) at a fraction of the engineering cost, and
nothing about this design forecloses a heavier investment later if multi-instance guarantees become
the project's primary marketed differentiator.

**Per-file coverage minimums instead of an aggregate gate.** Rejected: this is the textbook way a
coverage number gets gamed rather than earned — a hard-to-test glue file gets a padding test that
executes it without asserting anything, purely to clear a per-file bar, which is worse for
confidence than the same file sitting at a lower, honestly-explained number while the module-wide
aggregate still clears 95%.

**Absolute latency thresholds as the sole performance CI gate.** Rejected per the project's own
"benchmark before publishing tuned numbers" discipline: absolute numbers measured on shared,
variable-class CI runners produce exactly the false-failure flakiness a dimensionless, same-run
ratio gate is immune to by construction.

## References

- [Go race detector article](https://go.dev/doc/articles/race_detector) — detection scope and overhead
- [`testing/synctest` package docs](https://pkg.go.dev/testing/synctest) — bubble semantics, GA in Go 1.25
- [`pgregory.net/rapid` docs](https://pkg.go.dev/pgregory.net/rapid) — `t.Repeat` stateful/model-based testing
- [`anishathalye/porcupine` README](https://github.com/anishathalye/porcupine) — linearizability checking API
- [Apple's FoundationDB testing docs](https://apple.github.io/foundationdb/testing.html) — deterministic simulation precedent
- [TigerBeetle safety / VOPR docs](https://docs.tigerbeetle.com/concepts/safety/) — simulator precedent
- [Go integration coverage / `build-cover`](https://go.dev/doc/build-cover) — `GOCOVERDIR`, `go tool covdata`
- [`pkg.go.dev/golang.org/x/perf/cmd/benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) — Mann–Whitney U-test, `-count=10` floor
- [Docker Compose services reference](https://docs.docker.com/reference/compose-file/services/) — `healthcheck`, `depends_on.condition: service_healthy`
- [`golang.testcontainers.org`](https://golang.testcontainers.org/) — dynamic ports, Ryuk, evaluated and rejected
- docs/research/11-testing-verification-and-ci.md §1 (tooling mechanics), §2 (verification techniques), §3 (coverage mechanics), §4 (integration infra), §5 (CI design)
- docs/research/00-synthesis.md §1 (dimensionless ratio-gate rationale), §9 (phased plan backend availability)
