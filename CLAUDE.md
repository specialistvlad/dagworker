# Working rules for this repository

Short list. Where a rule has reasoning worth reading, it points at the ADR that
holds it.

## Branching — never commit to `main`

**Direct commits and pushes to `main` are prohibited.** Every change, however
small, goes on a branch and reaches `main` through a pull request.

Branch names take a prefix matching the commit type they carry:

```
feat/      fix/      docs/      test/
perf/      refactor/ ci/        chore/
```

e.g. `feat/metrics-facet`, `fix/redis-doorbell-leak`, `ci/pin-lint-action`.

Push the branch. **Never open the pull request unasked** — say one is owed and
let the user open it.

## Before anything merges

`make check` must pass: tidy, lint, race, coverage. `make all` adds the
integration and complexity suites against real databases. Nothing merges red,
and a coverage number below 95% fails the build rather than warning.

## The change order

Feedback becomes an ADR, then the spec, then a failing test, then code. Skipping
straight to code is how the documentation and the implementation drift apart —
see [ADR-0044](docs/adr/0044-design-decisions-that-were-not-implemented.md) for
what that cost the first time.

**A doc comment is a claim, and a false one is worse than none.** If you change
behaviour a comment describes, change the comment in the same commit. If you
find a claim the code does not honour, either make it true or record that it
is not — never leave it standing.

## The core module has no dependencies

Not "few" — none. `go.mod` at the root lists only the standard library, and a
`depguard` rule makes it a lint error for it to import `net/http` or gRPC
([ADR-0037](docs/adr/0037-network-surface-in-scope.md)). The backends,
adapters and daemon are separate modules. Adding a module means adding it to
`go.work` **and** to `MODULES` in the `Makefile`, or every target silently
skips it.

## Behaviour lives in the conformance suite

A change to what a `Store` does goes in `dagstoretest/` first, where all three
backends run it, and only then in the backend you were thinking about. A test
that exists in one backend's package proves nothing about the port
([ADR-0018](docs/adr/0018-every-backend-must-pass-one-shared-conformance-suite.md)).

## The frozen surface

`Status` is exactly four values and `Reason` exactly seven. Adding one is an
ADR and a change to three backends and two wire protocols, not an edit
([ADR-0001](docs/adr/0001-public-status-vocabulary-is-exactly-four-values.md),
[ADR-0002](docs/adr/0002-internal-scheduling-phase-is-not-part-of-the-public-api.md)).

## Measure round trips, not the wall clock

The same code timed 341µs and 825µs ten minutes apart on a busy laptop. Perf
claims are asserted as round-trip counts through a `pgx` tracer, or as *ratios*
across a thousandfold size increase — never as absolute durations, which are a
promise about hardware.

## The test databases are shared

`make up` starts PostgreSQL and Redis on fixed ports (15432, 16379). Every
worktree and every terminal talks to those same two containers, and
`make integration` and `make complexity` both begin by truncating them. Run
database-backed targets one at a time; `make check` and `make complexity-quick`
need no databases and are safe to run concurrently.
