# ADR-0029: Minimum Go version is 1.25

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/08-go-api-and-concurrency-design.md §2.5, §9.4, §11.3; docs/research/11-testing-verification-and-ci.md §1.1, §1.5, "Recommendations" item 1; docs/research/00-synthesis.md §3 (ADR-29 seed), §10.4

## Context

Every module in this monorepo needs a single, non-negotiable floor for the `go` directive in its
`go.mod`. Two pieces of stdlib tooling that the rest of the design leans on hard did not exist,
or existed only behind a flag, before Go 1.25:

- **`testing/synctest`** graduated from experimental (`GOEXPERIMENT=synctest`, Go 1.24) to general
  availability in Go 1.25, and the old experimental gate is removed entirely in Go 1.26. This
  package is the load-bearing mechanism for testing the per-node lease-timeout state machine
  deterministically — `synctest.Test` runs a goroutine group inside a fake-clock "bubble" that
  only advances when every goroutine in it is durably blocked, so a 30-second lease timeout test
  runs in microseconds of wall-clock time with zero flakiness. It only works if the scheduler's
  internal timing goes exclusively through the stdlib `time` package (no hand-rolled ticker, no
  externally-injected non-`time` clock) — that constraint is worth committing to now precisely
  because a 1.25 floor makes `synctest` a permanent, unconditional part of the test architecture
  rather than a `GOEXPERIMENT`-gated hedge that has to be maintained in parallel with a fallback.
- **`sync.WaitGroup.Go`** (Go 1.25) collapses the historically error-prone `wg.Add(1); go func() {
  defer wg.Done(); f() }()` triplet — a frequent source of leaked or miscounted goroutines when
  `Add` and the `go` statement are accidentally separated by a conditional — into one call. This
  project's `Manager.Close` contract (ADR-0027) is a hard promise that every goroutine the
  `Manager` started has exited before `Close` returns; every per-subscription pump goroutine
  (ADR-0022) is launched and tracked via `WaitGroup.Go` for exactly this reason.
- **Per-iteration loop variables** (Go 1.22) are already the toolchain default by the time this
  project starts, so they are not by themselves a reason to require anything newer than 1.22 — but
  they are folded into this same floor because nothing in this decision is served by pinning an
  intermediate version just to get *some* of the benefit.

This project is greenfield: `github.com/specialistvlad/dagworker` has zero existing users and
therefore zero compatibility cost to protect by supporting an older toolchain. The counter-
argument — that a brand-new open-source library should skew conservative on its Go floor because
"real-world Go shops lag current stable by one or two versions" — was raised explicitly in one of
the source dossiers and is taken seriously in the alternatives below, but it does not survive
contact with the calendar: Go 1.25 shipped in August 2025, roughly a year before this ADR's
acceptance date, which is a full release cycle for the "too aggressive for enterprise adoption"
concern to resolve itself in practice. A team that has not moved to 1.25 within a year of its
release is not a team `dagworker` needs to design its test architecture around from day one; if
adoption data after v1.0 says otherwise, that is a `go.mod` bump decision to make with real
numbers, not a speculative hedge to build now.

The `go` directive also changed in behavior, not just in language surface, as of Go 1.21+: it is a
hard minimum toolchain requirement, not merely a target language-version hint — `go build`
automatically downloads and uses a newer toolchain if the installed one is older than the `go.mod`
directive (subject to `GOTOOLCHAIN`). Pinning `go 1.25` is therefore an enforceable floor, not
documentation.

## Decision

Every `go.mod` in the repository — the core module and every nested module created under
ADR-0031 (`storage/redis`, `storage/postgres`, `adapters/grpc`, `adapters/http`,
`cmd/dagworkerd`, `examples`) — declares:

```go
go 1.25
```

with no lower floor anywhere in the module graph, no `GOEXPERIMENT` flags in any build script or
CI job, and no conditional-compilation fallback path for an older toolchain. This is a single
constant enforced identically across the monorepo's per-module CI loop (ADR-0031).

Two architectural corollaries follow directly from why 1.25 was chosen, and are binding on the
implementation, not just on `go.mod`:

1. **All internal scheduler timing goes through the stdlib `time` package** (`time.Now`,
   `time.After`, `context.WithDeadline`/`context.AfterFunc`), or a thin wrapper over it satisfying
   the `Clock` interface (ADR-0027, `08 §11`) for dependency injection — never a hand-rolled
   ticker sourced from `runtime.nanotime` and never a bespoke fake-clock abstraction built to
   route around `time`. This is what makes wrapping the lease-timeout state machine in
   `synctest.Test` sufficient on its own, with no parallel fake-clock plumbing to maintain.
2. **Every internally-spawned, non-error-propagating goroutine whose lifetime the `Manager` owns**
   (per-subscription event pumps, ADR-0022) is launched via `subWG.Go(func() { ... })`, and
   `Manager.Close` calls `subWG.Wait()` after canceling every subscription's context, so `Close`'s
   "every owned goroutine has exited" promise (ADR-0027) is enforced by the stdlib primitive
   itself rather than by hand-counted `Add`/`Done` pairs.

## Consequences

### Positive
- `testing/synctest` is available unconditionally, in every module, with no experimental flag and
  no removal deadline to track (the Go 1.24 experimental gate disappears in Go 1.26 regardless).
- `sync.WaitGroup.Go` removes an entire class of `Add`/`Done`-pairing bugs from the goroutine-
  lifecycle code that `Manager.Close` (ADR-0027) depends on for correctness.
- One version number, one CI toolchain install, no matrix cell for "the old-Go compatibility
  build" to keep green or to quietly bit-rot.
- The `go 1.25` directive is a real, toolchain-enforced minimum (Go 1.21+ semantics), not just a
  comment — a contributor cannot silently build against an older, unsupported toolchain and get a
  passing local build that CI would reject.

### Negative
- Forecloses adoption by any embedding team pinned to a Go toolchain older than 1.25 as of this
  writing — a real cost, explicitly flagged as an open question for the owner in the design
  synthesis (docs/research/00-synthesis.md §11 item 3) and accepted here as a deliberate trade,
  not an oversight.
- No backport story: a security fix needed by a hypothetical user stuck on an older toolchain
  would require them to upgrade Go first: this project makes no LTS-style commitment to support
  older compilers.

### Neutral
- Per-iteration loop-variable semantics (Go 1.22) ride along with this floor but are not
  independently load-bearing; nothing in this codebase should rely on them as the *reason* the
  floor is 1.25 — `paralleltest`'s loop-capture check (ADR-0039) still runs as a second line of
  defense in test code regardless.
- Raising the floor further in a later release (e.g., to pick up a future stdlib feature) is an
  ordinary `go.mod` bump across the monorepo's modules, not a breaking API change — it does not
  interact with the independent-semver policy in ADR-0031.

## Alternatives considered

**Go 1.22 floor**, per one source dossier's initial framing (11 §1.1, §10.4-as-contradiction).
Rejected: 1.22 gives per-iteration loop variables but nothing else this ADR needs — `synctest` is
not even experimentally available until 1.24 and not GA until 1.25, so a 1.22 floor would force
either a hand-rolled fake-clock abstraction for the lease-timeout tests (defeating the
architectural simplification `synctest` exists to provide) or a later, disruptive floor bump mid-
project once the timeout test suite already exists.

**Go 1.23+ floor with a `GOEXPERIMENT=synctest`-gated hedge**, the compromise the same dossier
raised as a fallback if 1.25 proved "too aggressive." Rejected on a hard technical fact, not just
a preference: the experimental gate is removed in Go 1.26 (per the Go 1.25 release notes), so any
1.23/1.24-based hedge is already a dead end within roughly one release cycle of being built —
paying the cost of maintaining two code paths (flagged and unflagged) for a bridge that collapses
on its own schedule is strictly worse than committing to 1.25 now.

**No pinned floor / "track whatever Go version CI happens to run."** Rejected: an embedded
library with a multi-year expected lifetime (per the working-conventions discipline this project
already commits to for API stability) needs an explicit, documented compatibility promise; "current
CI runner version" is not a promise a downstream embedder can build a support policy against, and
it silently changes underneath users whenever the CI image updates.

## References

- [Go 1.25 Release Notes](https://go.dev/doc/go1.25) — `testing/synctest` GA, toolchain semantics
- [`testing/synctest` package docs](https://pkg.go.dev/testing/synctest) — bubble semantics, `Test`, `Wait`, `Sleep`
- [`sync.WaitGroup.Go` docs](https://pkg.go.dev/sync#WaitGroup.Go)
- [Go 1.22 Release Notes](https://go.dev/doc/go1.22) — per-iteration loop variables
- [Go modules reference — `go` directive as minimum toolchain](https://go.dev/ref/mod#go-mod-file-go)
- docs/research/08-go-api-and-concurrency-design.md §9.4 (`WaitGroup.Go` usage), §11.3 (`synctest` for lease timeouts)
- docs/research/11-testing-verification-and-ci.md §1.5 (`synctest` mechanics), §10.4 (contradiction resolution)
