# ADR-0018: Every backend must pass one shared conformance suite

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/06-memcached-and-storage-abstraction.md §B.4, §B.5; docs/research/11-testing-verification-and-ci.md §4.3, §4.4

## Context

Three real backends (in-memory, Redis, PostgreSQL — memcached is dropped entirely per ADR-0017)
must honor identical semantics for the mandatory `Store` core and the fenced mutation primitives
(ADR-0016, ADR-0038), or "pluggable storage" is fiction the instant a second backend ships. The
capability matrix in docs/research/00 §7 is only true if something enforces it continuously; a
capability table that is "documentation that can silently drift" is exactly the failure mode
dossier 06 names and rejects.

The precedent is unambiguous and well-trodden in Go: `testing/fstest.TestFS(fsys, expected...)`
"walks the entire tree of files in fsys, opening and checking that each file behaves correctly,"
and checks additional behavior only if the `fs.FS` under test also implements a richer interface
like `fs.ReadLinkFS` ([testing/fstest docs](https://pkg.go.dev/testing/fstest)). gocloud.dev's
`blob/drivertest.RunConformanceTests(t, newHarness, asTests)` runs the identical idea at far larger
scale — roughly 21 test groups against whatever `driver.Bucket` a harness constructs, so every
provider package (`s3blob`, `gcsblob`, `fileblob`, `memblob`) gets the same battery for free
([blob/drivertest.go](https://github.com/google/go-cloud/blob/master/blob/drivertest/drivertest.go)).
A new provider proves itself by passing the shared suite, not by writing its own from scratch.

The phased plan (docs/research/00 §9) puts PostgreSQL at v0.2, immediately after the v0.1
in-memory-only correctness kernel — the conformance suite has to exist **before** that second
backend lands, or the first backend's behavior was never actually specified as a contract, only
observed as an implementation detail. Dossier 11 §4.3–§4.4 separately settles how integration-
requiring backends stay out of the default `go test ./...` path: a `BackendFactory` table plus an
environment-variable gate (`DAGWORKER_INTEGRATION=1`), not a build tag and not `-short`, because
build tags fragment the source tree and `-short`'s semantics ("skip slow," not "skip missing
infra") would mislead the next contributor.

## Decision

**Package `dagstore/dagstoretest`**, exported (not `_test.go`-only), modeled directly on
`blob/drivertest` and `testing/fstest.TestFS`:

```go
package dagstoretest

// Harness is implemented once per backend package's own _test.go file —
// modeled on blob/drivertest.HarnessMaker.
type Harness interface {
	MakeStore(ctx context.Context) (dagstore.Store, error)
	Close()
}

// RunConformance is the single call every backend's own TestConformance
// function makes. It runs the mandatory-core suite unconditionally, then
// runs each optional-facet sub-suite only if the returned Store also
// satisfies that facet's interface.
func RunConformance(t *testing.T, mk func(context.Context) (Harness, error))

// runIfCapable Skips (never silently passes, never fails) when the backend
// under test does not implement C — mirroring testing/fstest.TestFS's own
// "only check ReadLinkFS behavior if the fs.FS implements it" discipline.
func runIfCapable[C any](t *testing.T, mk func(context.Context) (Harness, error), fn func(*testing.T, C)) {
	ctx := context.Background()
	h, err := mk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	s, err := h.MakeStore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c, ok := s.(C)
	if !ok {
		t.Skipf("backend does not implement %T; skipping facet suite", new(C))
		return
	}
	fn(t, c)
}
```

**Named test IDs**, run unconditionally as part of the mandatory-core suite (no `Skip` branch —
every backend that compiles as a `Store` must pass all of these, since `Claim`/`Complete`/
`Extend`/`Sweep` are mandatory per ADR-0016/ADR-0038):

| Test ID | What it proves |
|---|---|
| `T-CRUD-ROUNDTRIP` | `Create`/`Get`/`Put` round-trip a `Node` byte-for-byte |
| `T-VERSION-ROUNDTRIP` | a `Version` serialized to string and back via `String()` still CAS-gates correctly (Vitess `topo.Version` contract) |
| `T-EDGE-BATCH-ATOMICITY` | a concurrent observer sampled mid-`AddEdges` sees zero or all edges of one call, never a partial set |
| `T-CLAIM-EXCLUSIVITY` | N competing goroutines calling `Claim` against the same ready node: exactly one grant per epoch, never two simultaneous un-expired claims |
| `T-CLAIM-DEADLINE-IS-BACKEND-CLOCK` | the returned `Deadline` tracks the backend's own clock, not a client-supplied value (ADR-0008) |
| `T-COMPLETE-FENCED-CAS` | `Complete` with a stale/superseded token affects zero rows and returns `ErrLeaseMismatch`, never a silent accept |
| `T-COMPLETE-FANOUT-ATOMIC` | a concurrent observer sees a successor's decremented pending-count and its presence in `readied` together or not at all |
| `T-COMPLETE-FANOUT-TRIGGER-RULES` | an `Error` outcome still readies a successor whose trigger rule is `all_done`/`none_failed_min_one_success` |
| `T-EXTEND-FENCED` | `Extend` after the epoch has moved on fails outright, never silently no-ops as if it succeeded |
| `T-SWEEP-NO-DOUBLE-FIRE` | two goroutines sweeping the same overdue claim concurrently: exactly one transition, the loser observes zero newly-swept, not an error |
| `T-SWEEP-READIES-SUCCESSORS` | a timed-out node's permissively-triggered successor appears in `Sweep`'s `readied`, not left stranded until an unrelated future write |

**Per-capability sub-suites**, run only via `runIfCapable`, `Skip` on absence:

| Test ID | Facet |
|---|---|
| `T-LIST-PAGINATION` | `Lister` — cursor-based pagination is stable under concurrent inserts |
| `T-EVENT-ORDERING` | `EventStream` — events for one node arrive in `Seq` order; additionally asserts at-least-once delivery only when `CapEventStreamDurable` is set |
| `T-COND-DELETE` | `ConditionalDeleter` — delete-if-version-matches rejects on a stale `Version` |
| `T-BATCH-CLAIM-EXCLUSIVITY` | `BatchClaim` — the same exclusivity guarantee as `T-CLAIM-EXCLUSIVITY`, generalized to N-at-a-time grants |

**Concurrency discipline.** `T-CLAIM-EXCLUSIVITY`, `T-COMPLETE-FANOUT-ATOMIC`, and
`T-SWEEP-NO-DOUBLE-FIRE` must run under `-race` with goroutine counts well above the backend's
shard count (docs/research/09 Part 2) — a single-threaded happy-path pass proves nothing about the
atomicity claim these tests exist to check.

**Integration harness plumbing**, per dossier 11 §4.3–§4.4: a `BackendFactory` table
(`{Name, New func(tb) Store}`) drives `RunConformance` for in-memory (always runs, no external
dependency), Redis, and PostgreSQL. Redis/Postgres factories `t.Skip()` when their connection
env var is unset, gated centrally by `DAGWORKER_INTEGRATION=1` so `go test ./...` stays fast and
Docker-free by default. `docker-compose.test.yml` runs exactly two services — `redis`, `postgres`
— health-checked via `depends_on: condition: service_healthy`; **no memcached service**, per
ADR-0017.

## Consequences

### Positive
- The capability matrix (docs/research/00 §7) becomes a live, continuously-tested contract instead
  of documentation that can silently drift — the exact benefit gocloud.dev's own providers get from
  `blob/drivertest`.
- A new backend proves itself by passing the shared suite rather than writing bespoke tests a
  reviewer has to trust are equivalently strict.
- Named test IDs make a CI failure immediately traceable to which specific guarantee broke, on
  which backend, without reading the test body first.

### Negative
- The Redis and Postgres harnesses genuinely require a running instance — `miniredis` cannot
  exercise the Lua/Streams-heavy claim path faithfully, and there is no in-process Postgres
  substitute for `SKIP LOCKED` semantics — so this is real, unavoidable CI cost (containers,
  health-check wait loops), paid once here rather than deferred until it is much more expensive to
  retrofit.

### Neutral
- Because `Claim`/`Complete`/`Extend`/`Sweep` are mandatory (ADR-0016), what the synthesis draft
  originally sketched as two of five capability sub-suites (`ReadyQueueExclusivity`,
  `TimeoutSweepIdempotent`) collapse into the unconditional mandatory-core suite; fewer `Skip`
  branches remain than the original draft, more assertion surface now runs on every backend
  unconditionally.

## Alternatives considered

**Per-backend bespoke test files, no shared suite.** Rejected: exactly the "documentation that can
silently drift" failure mode 06 names — a backend can pass its own hand-written tests while
violating a guarantee a sibling backend's author never thought to encode, because nothing forces
the two test files to agree on what "correct" means.

**Build-tag gating (`//go:build integration`) instead of an environment variable.** Rejected per
dossier 11 §4.4: fragments the source tree (a tagged file cannot share unexported helpers with an
untagged one without careful package layout) and is easy to forget on a new test file.

**Fail, not `Skip`, a capability sub-suite when a backend lacks the facet.** Rejected: this is
precisely the trap `testing/fstest.TestFS` and `blob/drivertest` both avoid — failing would leave
an honestly-scoped backend (one that never claims `ConditionalDeleter`) permanently red in CI for a
facet it never offered, indistinguishable from an actual regression.

**Mock/fake backends only, no real Redis/Postgres in CI.** Rejected: the entire point of the
mandatory core's atomicity claims (ADR-0038) is that they hold under real concurrent access to the
real engine (Lua scripts, `SKIP LOCKED`); a fake that does not implement the same concurrency
primitives cannot validate the property it exists to prove.

## References

- `testing/fstest.TestFS` — https://pkg.go.dev/testing/fstest
- gocloud.dev `blob/drivertest` — https://github.com/google/go-cloud/blob/master/blob/drivertest/drivertest.go
- gocloud.dev `docstore/drivertest` — https://pkg.go.dev/gocloud.dev/docstore/drivertest
- Docker Compose healthcheck / `depends_on` spec — https://docs.docker.com/reference/compose-file/services/
- Sibling ADRs: ADR-0006 (fencing token), ADR-0008 (clock authority), ADR-0016 (storage port
  shape), ADR-0017 (memcached rejected), ADR-0019 (event bus shape), ADR-0030 (trigger rules),
  ADR-0038 (fenced mutation primitives)
