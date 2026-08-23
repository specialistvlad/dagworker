# ADR-0039: Coding standards and the strict lint configuration

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/11-testing-verification-and-ci.md §5.4, §6, §6.1, §6.2, "Recommendations" item 10

## Context

`golangci-lint` v2 changed the configuration file's shape in a way every future contributor will
trip over at least once: it requires a top-level `version: "2"` key, reorganizes linter settings
under `linters:` with `default`/`enable`/`disable`/`settings` subsections, and — the largest
structural change — splits **formatters** (`gofmt`, `gofumpt`, `goimports`, `gci`) into their own
top-level `formatters:` section, separate from `linters:`, on the grounds that formatters rewrite
code while linters only report on it. Any config copied from a pre-v2 tutorial or Stack Overflow
answer is silently rejected by a v2 binary. This project starts from v2 shape on day one; there is
no migration to perform because there is no v1 config to migrate from.

The more consequential decision is not the linter *list* — most of it is uncontroversial,
correctness-oriented tooling (`errcheck`, `govet`, `staticcheck`, `unused`) that any serious Go
project enables — it is the **deliberate, reasoned exclusion list**. Three commonly-recommended
"strict" linters are excluded here, and each exclusion is a real design decision this project's
public API shape depends on, not an oversight:

- **`wrapcheck`** enforces that every error returned from a call to an external package gets
  wrapped with call-site context. That rationale is legitimate for an application calling into
  libraries it doesn't control — but `dagworker` **is** the library, and its own errors are the
  "external package" from every caller's point of view. This project's public error taxonomy
  (ADR-0027: `ErrNotFound`, `ErrLeaseMismatch`, `ErrCycle`, etc., checked via `errors.Is`) depends
  on sentinels surviving unwrapped, or consistently `%w`-wrapped, all the way to the caller.
  `wrapcheck`'s default posture nudges every return site toward wrapping regardless of whether
  that site is a genuine public boundary or an internal helper three calls deep — high noise
  against exactly the code this project writes the most of.
- **`err113`** (the enforce-wrapped-static-errors / disallow-`errors.New`-in-conditionals linter)
  is actively hostile to the sentinel-error pattern the public API is built on. Package-level
  `errors.New` values checked with `errors.Is` are the intended design (ADR-0027), not a smell
  this linter should be fighting on every PR.
- **`exhaustruct`** forces every struct literal to set every field. This is actively wrong for a
  functional-options-adjacent config shape: `ClaimOptions{LeaseTimeout: 30 * time.Second}` relying
  on the zero value for every other field is the intended, idiomatic call site (ADR-0027 `config`
  struct, ADR-0034 `ScopeConfig`) — `exhaustruct` would force every call site in the codebase to
  spell out fields whose entire purpose is having a sane default.

A second cluster of exclusions (`gochecknoglobals`, `gochecknoinits`, `lll`, `wsl`/`wsl_v5`,
`godox`, `varnamelen`) is excluded for a shared reason: each is a blunt instrument that fires on
legitimate, idiomatic Go in this specific codebase (a small number of well-considered package-level
sentinel errors; short, locally-scoped names like `n *Node`/`e Edge`/`w Worker` in graph code;
temporary `TODO`s during a still-greenfield build) as often as it fires on real problems, and the
false-positive rate is high enough that enforcing it as a hard gate produces busywork rather than
better code. These stay a code-review judgment call, not a CI gate.

The complexity-ceiling linters (`funlen`, `cyclop`, `gocognit`) are enabled, but scoped down in
test files via an explicit exclusion rule — table-driven test bodies and test helpers legitimately
run long and intentionally discard errors from test-only setup calls, and fighting that with the
same ceiling used for production code produces exactly the kind of padding this project's coverage
discipline (ADR-0040) already argues against.

## Decision

`golangci-lint` v2 is pinned via `golangci-lint-action` at a specific `v2.x` release (never
`latest`, so a new release's newly-enabled-by-default linters cannot turn a previously-green PR
red without a deliberate version-bump commit) and runs as its own required CI job (per ADR-0031's
per-module CI loop) against every module in the monorepo. The configuration, committed at
`.golangci.yml` in the repo root and copied verbatim into every nested module's directory that
needs its own invocation:

```yaml
version: "2"

run:
  timeout: 5m
  tests: true

linters:
  default: none   # start from nothing; enable explicitly — an audited allowlist, not a denylist
  enable:
    # --- correctness / bug-finding, non-negotiable ---
    - errcheck        # every returned error must be handled or explicitly discarded
    - govet           # full vet suite, including shadow, loopclosure, etc.
    - staticcheck     # SA*/ST*/QF* checks — the single highest-value linter in this list
    - ineffassign     # assignments whose value is never used
    - unused          # dead code / unused identifiers

    # --- concurrency-specific, directly relevant to a lease/claim protocol ---
    - bodyclose       # http.Response.Body must be closed on every path (adapters/http, ADR-0037)
    - sqlclosecheck   # sql.Rows / sql.Stmt must be closed (storage/postgres)
    - rowserrcheck    # sql.Rows.Err() must be checked after the iteration loop
    - contextcheck    # context must be propagated correctly, not silently dropped/replaced
    - containedctx    # a context.Context stored in a struct field is a smell, not a pattern
    - noctx           # HTTP requests / DB calls made without a context

    # --- exhaustiveness on the public status enum — the whole point of this project ---
    - exhaustive      # every switch on Status/NodeStatus/Reason must handle every case explicitly

    # --- style / API-shape enforcement ---
    - revive          # configurable golint replacement, see settings below
    - gocritic        # broad diagnostic + style + opinionated checks
    - ireturn         # "accept interfaces, return structs" — enforced, not just convention
    - nilnil          # a function must not return (nil, nil) as a valid-looking success value

    # --- test-suite discipline (ADR-0040: t.Parallel everywhere) ---
    - paralleltest    # flags missing t.Parallel() and loop-variable reuse in subtests
    - tparallel       # flags t.Parallel() on a parent whose subtests aren't all parallel too
    - thelper         # test helper functions must call t.Helper()
    - testifylint     # correct usage of testify's assert/require (e.g. Equal arg order)

    # --- security ---
    - gosec           # G-series security checks (weak crypto, command injection, etc.)

    # --- performance, matches the project's 1M-node headline goal ---
    - prealloc        # slices appended-to in a loop with a known bound should be preallocated
    - makezero        # make([]T, n) then append(...) instead of make([]T, 0, n) is a length bug magnet
    - perfsprint      # fmt.Sprintf misuse where string concatenation/strconv would do

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
        - name: range-val-in-closure      # pre-1.22 loop-capture guard; harmless no-op at ADR-0029's 1.25 floor

    exhaustive:
      default-signifies-exhaustive: false  # a `default:` case must NOT count as covering every
                                            # value — this is exactly what makes exhaustive catch
                                            # a forgotten Status/Reason case when one is added later
      check: [switch, map]

    ireturn:
      allow: [error, empty, anon, generic, stdlib]
      # dagstore.Store, dagworker.Clock etc. remain interfaces at the package boundary (accepted
      # as parameters) but constructors return the concrete *Manager, *RedisStore, etc. — ireturn
      # enforces exactly this split, matching ADR-0027's Go API shape.

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
        core-no-network:
          # ADR-0037's hard rule, restated as a lint-level tripwire in ADDITION to the
          # go.mod-level enforcement (`go mod tidy -diff`) — belt and suspenders, not the
          # primary mechanism.
          files: ["!$test"]
          deny:
            - pkg: "google.golang.org/grpc"
              desc: "core module must have zero import edge to the gRPC adapter (ADR-0037)"
            - pkg: "net/http"
              desc: "core module must have zero import edge to the HTTP adapter (ADR-0037)"

    forbidigo:
      forbid:
        - pattern: '^fmt\.Print.*$'
          msg: "use the configured slog.Logger, not fmt.Print*, in library code"

    gosec:
      excludes: [G104]   # errcheck already covers unchecked errors; avoid double-reporting

  exclusions:
    rules:
      - path: "_test\\.go"
        linters: [errcheck, gosec, funlen, cyclop, gocognit]
        # table-driven test bodies and test helpers legitimately run long and sometimes
        # intentionally ignore errors from test-only setup calls (ADR-0040)

formatters:
  enable:
    - gofumpt   # stricter superset of gofmt
    - goimports
```

The `core-no-network` `depguard` rule applies only to the root module's own directory invocation
(it is meaningless, and not applied, inside `adapters/grpc`/`adapters/http`/`cmd/dagworkerd`,
which legitimately import those packages) — the per-module CI loop from ADR-0031 passes a
module-specific config override for this one rule.

## Consequences

### Positive
- The exclusion rationale for `wrapcheck`/`err113`/`exhaustruct` is now a permanent, citable
  document — a future contributor proposing "just enable the strict error-wrapping linter" hits
  this ADR immediately instead of relitigating the sentinel-error design in a PR thread.
- `exhaustive` with `default-signifies-exhaustive: false` gives the four-value `Status` enum
  (ADR-0001) and the closed `Reason` enum a compiler-adjacent guarantee that a newly added value is
  never silently swallowed by an existing `default:` case anywhere in the codebase.
- `paralleltest`/`tparallel`/`thelper` mechanically enforce the "unit tests are parallel by
  default" discipline ADR-0040 depends on, rather than relying on every author remembering
  `t.Parallel()`.
- Pinning the action version (not `latest`) means a linter-set change is always a visible,
  reviewable commit, never a surprise red build from an upstream release.

### Negative
- `ireturn`, `revive`'s `exported`-doc-comment rule, and the complexity ceilings together impose
  real, ongoing authoring friction — every exported type needs a doc comment, every constructor
  needs to return a concrete type even where an interface felt more "flexible," and a function
  that organically grows past 80 lines/50 statements must be refactored before merge, not just
  flagged.
- The exclusion list is itself a maintenance surface: a new golangci-lint release may ship a
  linter whose default behavior overlaps with one of the excluded ones under a new name, requiring
  this ADR to be revisited rather than the exclusion silently going stale.
- `depguard`'s `core-no-network` rule duplicates part of what ADR-0037's `go mod tidy -diff` check
  already guarantees structurally; keeping both is a deliberate belt-and-suspenders redundancy,
  not a sign the module boundary alone was considered insufficient.

### Neutral
- `gosec`'s `G104` exclusion is not a security-posture weakening — `errcheck` already covers
  unchecked-error-return findings; the exclusion only prevents the same finding being reported
  twice under two different linter names.
- `wsl`/`wsl_v5`, `godox`, `varnamelen`, `gochecknoglobals`, `gochecknoinits`, and `lll` are not
  banned from ever being enabled — they are simply not CI gates today; `godox` in particular is a
  reasonable **post-1.0** addition once "no lingering TODOs before release" becomes a meaningful
  gate rather than a greenfield-phase false economy.

## Alternatives considered

**Enable `wrapcheck` project-wide.** Rejected: fights the sentinel-error/`errors.Is` design
ADR-0027 commits the public API to; wrapping every internal call site regardless of whether it is
a real package boundary is noise, not signal, in a codebase whose internal call graph is deep by
design (engine → lease → storage port).

**Enable `err113`.** Rejected outright: it flags the exact `errors.New`-as-package-level-sentinel
pattern (`ErrNotFound`, `ErrCycle`, ...) that this project's public error taxonomy requires;
enabling it means either suppressing it everywhere those sentinels are declared (defeating the
point of a lint gate) or redesigning the error taxonomy to satisfy a linter instead of the API's
actual callers.

**Enable `exhaustruct`.** Rejected: the functional-options-adjacent config-struct pattern
(`ClaimOptions`, `ScopeConfig`) relies on the zero value being a deliberate, documented default for
every field a caller doesn't set; `exhaustruct` would force every call site to restate every
field, which is the opposite of what the API is designed to let a caller avoid.

**Start from `golangci-lint`'s own `default: standard` or `default: all` preset instead of
`default: none` plus an explicit allowlist.** Rejected: both presets change contents across
`golangci-lint` releases without this project's own review, meaning the effective lint gate could
shift underneath a green CI history with no commit in this repo to point to. `default: none` plus
an audited `enable:` list means every linter that runs is a line someone in this project chose and
can be traced to this ADR.

**Adopt `wsl`/`wsl_v5` for blank-line/whitespace discipline.** Rejected: `gofumpt` (already
enabled as a formatter) already normalizes the overwhelming majority of whitespace inconsistencies
a reader would notice; `wsl`'s additional opinions produce PR churn disproportionate to any
readability gain, per the same judgment call dossier 11 §6.1 makes.

## References

- [`golangci-lint.run` configuration docs](https://golangci-lint.run/docs/configuration/file/) — v2 config shape, `version: "2"`, formatters split
- [`golangci-lint.run` linters list](https://golangci-lint.run/docs/linters/) — linter catalog by category
- [`golangci/golangci-lint-action` README](https://github.com/golangci/golangci-lint-action) — caching, version pinning
- [`tomarrell/wrapcheck` README](https://github.com/tomarrell/wrapcheck) — error-wrapping rationale, cited and then argued against for this project's shape
- [`butuzov/ireturn` README](https://github.com/butuzov/ireturn) — accept-interfaces-return-structs enforcement
- [`kunwardeep/paralleltest` README](https://github.com/kunwardeep/paralleltest) — `t.Parallel()`/loop-variable checks
- docs/research/11-testing-verification-and-ci.md §6 (full config derivation), §6.1 (exclusion rationale), §6.2 (v2 migration notes)
