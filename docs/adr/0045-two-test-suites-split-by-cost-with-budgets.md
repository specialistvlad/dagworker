# ADR-0045: Two test suites, split by cost, with budgets

- **Status:** Accepted
- **Date:** 2026-08-23
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** ADR-0040 (its tier table, its gate assignments, and its CI matrix)
- **Backing research:** none new — this ADR records a decision forced by measurement

## Context

The merge gate took **fourteen minutes**. That is not a gate. It is something
that happens to you after you have already moved on, and its effect is that
people push and wait rather than run anything locally — which is the opposite
of what a test suite is for.

Profiling the whole of CI, per job and per step, found that almost none of it
was the tests:

| job | wall | of which |
|---|---|---|
| integration | 835s | **722s was `test/perf`** |
| complexity | 443s | 555,000 seeded nodes per backend |
| race | 128s | |
| lint | 80s | 64s linting |
| test | 68s | **10s of tests, 42s of compilation** |

Three findings drove this decision:

1. **`test/perf` ran in three targets.** It holds measurements, not assertions,
   and it was executing from `make test`, `make race` *and* `make integration` —
   where it was 87% of the job — before `make complexity` ran the same guards
   again. The complexity work ran twice per CI run for no extra information.
2. **The build cache had never once been warm.** Every job logged `Cache is not
   found`, which is why compilation was four fifths of the "test" job.
3. **The complexity guards spent most of their time building graphs to throw
   away.** Five guards each seeded their own copy of an identical `SeedWide`
   fixture at every size: 5 × (1,000 + 10,000 + 100,000) = 555,000 nodes per
   backend, which on PostgreSQL at a measured 454µs per node is four minutes of
   setup before a single measurement.

ADR-0040 assigned gates per tier — "ratio guards on every push (required)" — on
the assumption that the ratio guards were cheap. They are not, and no amount of
tuning makes a real-PostgreSQL sweep cheap. The gate assignment has to be made
on measured cost, not on how important the tier feels.

## Decision

**There are exactly two suites, and the split is by cost rather than by kind.**

| | contents | databases | budget | measured |
|---|---|---|---|---|
| `make check` | tidy, lint, race, coverage | **none** | **10s** | 6.7s |
| `make benchmark` | integration, e2e, complexity, throughput | yes | **5 min** | 3m31s |

`make benchmark` is a sequence of `make integration`, `make complexity` and
`make throughput`, each of which remains a target in its own right. It is a
sequence rather than a prerequisite list because all three truncate and rewrite
the same two database containers, and prerequisites may run in any order.

### The budgets are the design constraint

Not aspirations. Anything added to `check` that pushes it past ten seconds
belongs in `benchmark`; anything that pushes `benchmark` past five minutes needs
cost taken out of it somewhere else first. The budget is what makes the rule
decidable, and a rule that is not decidable does not survive contact with the
next contributor in a hurry.

**`check` touches no database, starts no container, and measures nothing.** That
is testable rather than aspirational: run it with `docker compose down` and it
must produce the same result in the same time.

### What this costs

The real-backend conformance suite and the complexity guards leave the fast
gate. A backend regression that only reproduces against real PostgreSQL or
Redis — and this repository has had several — is now caught by `make benchmark`
rather than by `make check`. `benchmark` therefore has to actually run: in CI
on every push, and locally before anything that touches a backend, a hot path,
or the storage port.

That is a genuine reduction in what the ten-second gate proves, accepted
deliberately. The alternative on offer was a fourteen-minute gate that proved
more and was, in practice, run by nobody.

### Fidelity was not traded

The five complexity guards now share their fixtures: two per size — one for the
three read-only operations, one for the claim path, which consumes what it
measures — instead of five. `Claim` and `Claim+Complete` come out of a single
pass, timing the claim and the completion separately, so both curves cost one
graph.

The sizes, the iteration counts, the reps and the 30× bound are **unchanged**.
This is 60% less graph building, not a weaker measurement. A one-minute budget
would have forced cutting the top size or the sample count, and both of those
*are* the guard's detection power; five minutes bought the honest version.

## Consequences

- **The gate is 6.7 seconds and the full suite is 3m31s**, from 14 minutes.
- **`make check` is trustworthy offline**, which is what makes it a habit rather
  than a ceremony.
- **A real-backend regression reaches `main` if `benchmark` is not run.** CI
  runs it; a contributor who only runs `check` locally is relying on that.
- **`make bench` is now `make throughput`**, and `make benchmark` is the
  umbrella. `make check-all` and `make performance` are gone.

### Three measured facts that must not be undone

Each is commented where it lives, because each is the opposite of what looks
obvious:

- **Parallelism across modules is negative on a cold build cache.** Measured
  27.9s parallel against 20.3s serial: `go build` already saturates every core,
  and eight concurrent copies thrash. It pays only once the cache is warm.
- **`golangci-lint` must run serially.** It locks its own analysis cache and
  refuses to run beside another copy of itself. A parallel sweep is a race that
  usually wins and sometimes reports a lint pass it never performed — a worse
  failure than being slow.
- **Sharing the complexity fixtures without restoring the concurrency was
  slower than the five separate tests it replaced** — 399s against 275s. The
  original was five parallel test functions across three backends: fifteen
  workers overlapping their round trips against one PostgreSQL. Collapsing to
  one test left three. Forty percent less work with a third of the overlap is a
  net loss. Both halves are load-bearing.

## What this supersedes in ADR-0040

- Its **tier table's Gate column**. "Ratio guards on every push (required)" is
  no longer true and could not be made true: they are in `benchmark`.
- Its **Feature tier at `test/feature/`**, which was never built. Feature-level
  tests live beside the core module's own packages, and the whole-stack ones in
  `test/e2e`.
- Its **CI job matrix**, which specifies one Go version ("no multi-version hedge
  matrix"). CI tests `1.25` and `stable`: 1.25 is the declared floor from
  ADR-0029 and `stable` catches what the next release will break.

Everything else in ADR-0040 — coverage at 95% aggregate, `t.Parallel()`
everywhere, `-race -shuffle`, the storage tier living inside each backend module
— stands and is what the code does.
