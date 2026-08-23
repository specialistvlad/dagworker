# Contributing to dagworker

Thank you for considering it. This document is short because the rules are few
and mostly mechanical.

## Before you write code

**Never commit to `main`.** Every change goes on a branch prefixed with the
commit type it carries — `feat/`, `fix/`, `docs/`, `test/`, `perf/`,
`refactor/`, `ci/`, `chore/` — and reaches `main` through a pull request.

**`make check` must pass.** That is `tidy-check`, `lint`, `race` and `cover`.
Nothing merges red.

```
make check      # the gate: ~7s, no databases, run it constantly
make benchmark  # real databases, e2e, complexity, throughput: ~3m30s
make down       # stop the containers when you are done
```

`make` with no target lists every target and what it does, and an unknown
target prints the same list rather than a bare "No rule to make target".

| | what it runs | needs databases | budget | measured |
|---|---|---|---|---|
| `make check` | tidy, lint, race, coverage | **no** | **10s** | 6.7s |
| `make benchmark` | integration, e2e, complexity, throughput | yes | **5 min** | 3m31s |
| `make million` | the 1,000,000-node measurement | yes | — | ~10 min |

Inside `benchmark` the parts are also targets, for when you want just one:
`make integration`, `make complexity`, `make throughput`.

The budgets are enforced by review, not by a timer, and they are the reason the
split exists: the old gate was fourteen minutes, so in practice people pushed
and waited. If a change pushes `check` past ten seconds it belongs in
`benchmark`.

If `make check` fails on `main`, that is a bug and a report is welcome.

## The rules that are not mechanical

**A design change needs an ADR.** If your change alters a guarantee, a public
type, the storage contract, or the semantics of an existing operation, add a
file to [`docs/adr/`](docs/adr/) following the shape of the existing ones:
context, decision, consequences, and the alternatives you rejected *with the
reason each lost*. "It was more complex" is not a reason; name the system or
paper that informed the choice.

ADRs are immutable once accepted. To change a decision, write a new ADR that
amends the old one — see
[ADR-0041](docs/adr/0041-amendments-discovered-during-implementation.md) for
what that looks like.

**New behaviour needs a test that fails without it.** Write it first and watch
it fail for the right reason. A test that passes before your change is testing
something else.

**Backend behaviour belongs in the conformance suite.** If you find a case where
two backends disagree, the fix is a test in
[`dagstoretest/`](dagstoretest/) plus a fix in whichever backend is wrong —
not a special case in the Manager. The suite is the definition of correct; prose
in the docs merely describes it.

**Comments explain why, not what.** A comment that restates the line below it is
noise. A comment that records why the obvious approach was rejected is the most
valuable thing in the file.

## Adding a storage backend

1. New module under `storage/`, with its own `go.mod`. Add it to `go.work` and
   to `MODULES` in the `Makefile`.
2. Implement `dagworker.Store` — all of it. There is no partial
   implementation: a store that cannot atomically claim is not a backend this
   library can drive, and there is no fallback path.
3. Implement whichever optional facets you can do *natively*. Never emulate one
   you cannot; report the capability honestly and let the caller find out from
   `Capabilities()` rather than from a surprise.
4. Pass `dagstoretest.RunConformance`. All of it. A skip is only acceptable for
   a facet you do not claim.
5. Read [`docs/spec/01-contract.md`](docs/spec/01-contract.md) first. It is
   normative, and it is shorter than the code you are about to write.

The thing that catches everyone: **the server owns the clock**. Every deadline
is computed and every expiry compared using the storage's own time, never a
value computed in Go and sent over. Two parties reading two clocks cannot agree
on a boundary.

## Performance changes

The library promises that no operation's cost grows with the size of the graph,
and [`test/perf`](test/perf/) enforces it. If you change a hot path, run:

```
make complexity   # the ratio guards, at a million nodes
make bench        # absolute throughput, for a benchstat comparison
```

A performance claim in a commit message needs a `benchstat` comparison behind
it. `-count=10` at minimum; a single run is noise.

## Commit messages

Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `chore:`, `perf:`).
The body should say *why*, and if the change was forced by something you
discovered, say what you discovered — that is the part a reader six months from
now needs.

## Reporting a bug

Include the backend, the Go version, and the smallest graph that reproduces it.
For anything involving leases or timing, `Manager.Inspect` on the stuck node
answers most of the first round of questions before it is asked.

## Security

Do not open a public issue for a vulnerability. See [SECURITY.md](SECURITY.md).
