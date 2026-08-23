# ADR-0031: Repository and module topology is a multi-module monorepo

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/15-daemon-packaging-and-ops.md Part 1 (§1.1-§1.7); docs/research/06-memcached-and-storage-abstraction.md Part A; docs/research/00-synthesis.md §3 (ADR-31 seed), §8

## Context

The library must serve a user who wants nothing but the in-process API (no network client, no
SQL driver, no Redis client in `go.sum` at all) equally well as it serves the `dagworkerd`
operator who wants every backend and both network adapters wired into one binary. Go gives
exactly three ways to arrange this, and Go's own documentation states a clear default: "In
general, we recommend that each repository contain only one module, located at the root," with
multi-module treated as the exception, reserved for code that constitutes "multiple, separately
releasable versioned components." `dagworker` meets that bar on its own terms: a core-only user
must *never* resolve `github.com/redis/go-redis/v9`, `github.com/jackc/pgx/v5`,
`google.golang.org/grpc`, or `net/http`-adjacent adapter code, because build tags gate which
files the compiler visits but do nothing to the module dependency graph that `go mod tidy` and
every dependency scanner see — a single-module-with-build-tags design (`gocloud.dev`'s own
shape, inspected directly: over fifty direct requires spanning all three cloud providers
simultaneously, imposed on every importer regardless of which one they use) is the negative case
study this project must not reproduce.

Four real-world topologies were inspected directly (go.mod fetched, not recalled): OpenTelemetry-
Go's forced version lockstep across `otel`/`otel-contrib` (a blunt fix for internal-API coupling
this project does not have); `aws-sdk-go-v2`'s module-per-service with independent versioning
(the closest structural analogue to `storage/redis`/`storage/postgres`); `grpc-go`'s single core
module with heavy-dependency subtrees — `examples/`, `gcp/observability/` — isolated into their
own modules; and `testcontainers-go`'s module-per-integration pattern (`modules/postgres`,
`modules/redis`, each independently tagged). The lesson, consistent across all four: split
*exactly* where an optional feature drags in a dependency tree the median user does not want, and
nowhere else.

**AMD-5 changes the backend list from the original synthesis.** Memcached is dropped entirely —
there is no `storage/memcached` module. ADR-0017 (memcached rejected as a `Store` backend) still
documents the technical reasoning; this ADR's module list simply has one fewer entry than the
synthesis's original §8 repo layout.

A question the source research explicitly leaves open (15, "Open questions": "should
`storage/redis`, `storage/postgres`... share one `storage/testsuite` module... or should each
backend vendor its own copy") must be settled here, because the conformance suite
(`dagstoretest.RunConformance`, ADR-0018) is imported by every backend module's test files and its
placement is a real module-boundary decision, not an implementation detail.

## Decision

The repository is a multi-module monorepo with exactly these modules:

```
dag-worker-go/                                   (git repo root)
├── go.mod            module github.com/specialistvlad/dagworker         — core, near-zero deps
├── go.work           dev-only; never consumed by external `go get`
├── go.work.sum
├── dagworker.go, options.go, status.go, event.go, ...   (public API, §4 of the synthesis)
├── internal/          engine, topo, lease, clock, ready, interner, slab — core-only, unexported
├── dagstore/          storage port: Store core + optional facets (ADR-0016) — no backend code
│   └── dagstoretest/  RunConformance suite (ADR-0018) — SAME MODULE as core, see below
│
├── storage/
│   ├── redis/
│   │   └── go.mod    module .../dagworker/storage/redis
│   └── postgres/
│       └── go.mod    module .../dagworker/storage/postgres
│                      (NO storage/memcached — AMD-5; memcached is rejected outright, ADR-0017)
│
├── adapters/
│   ├── grpc/
│   │   └── go.mod    module .../dagworker/adapters/grpc   (imports core; never net/http)
│   └── http/
│       └── go.mod    module .../dagworker/adapters/http   (imports core; never grpc)
│
├── cmd/
│   └── dagworkerd/
│       └── go.mod    module .../dagworker/cmd/dagworkerd
│                      (the ONLY module allowed to import everything above)
│
└── examples/
    └── go.mod        own module, kept out of every real module's dependency graph
```

Seven `go.mod` files total: root/core, `storage/redis`, `storage/postgres`, `adapters/grpc`,
`adapters/http`, `cmd/dagworkerd`, `examples`.

**The conformance suite (`dagstore/dagstoretest`) lives inside the core module — it is not its
own module.** The deciding constraint, per this ADR's mandate, is that it must be importable by
every backend module without dragging in dependencies those backends don't already carry, and
core wins that test decisively:

1. Every backend module (`storage/redis`, `storage/postgres`) already `require`s core in order to
   implement `dagstore.Store` in the first place. Placing `dagstoretest` as a core subpackage adds
   **zero** new module edges — a backend's test files import `dagworker/dagstore/dagstoretest`
   off the exact same `require` line that already pulls in `dagworker/dagstore` for the interface
   types.
2. A conformance suite is inherently version-locked 1:1 to the `Store` interface it exercises —
   `RunConformance` written against `Store` v0.6 cannot meaningfully test an implementation of
   `Store` v0.5. Extracting it into its own module (an eighth `go.mod`) would recreate, for a
   single-package artifact, exactly the forced-lockstep coupling this ADR rejects wholesale for
   backends and adapters (§ Alternatives, OpenTelemetry-Go) — except with no compensating benefit,
   since nothing outside the test suite ever needs to depend on it independently of core.
3. It resolves the open question dossier 15 itself raises (share one `storage/testsuite` module
   vs. risk drift between per-backend copies) by eliminating both horns: there is exactly one
   copy, inside core, and every backend's test build links the same one by construction — drift
   is structurally impossible, and no eighth `go.mod` is needed to prevent it.
4. It costs nothing in production weight: `dagstoretest` is imported only from `_test.go` files.
   Go's package-level import resolution means a production consumer who imports only
   `dagworker`/`dagworker/dagstore` never compiles `dagstoretest` or resolves any dependency it
   alone requires (e.g. `pgregory.net/rapid` for property-style contract checks, ADR-0040) —
   those stay confined to test binaries that actually import the package.

**Tag format.** Each nested module's git tag is prefixed with its subdirectory path, per Go's own
nested-module convention:

| Module | Tag example |
|---|---|
| core (root) | `v0.6.0` |
| `storage/redis` | `storage/redis/v0.2.0` |
| `storage/postgres` | `storage/postgres/v0.2.0` |
| `adapters/grpc` | `adapters/grpc/v0.3.0` |
| `adapters/http` | `adapters/http/v0.3.0` |
| `cmd/dagworkerd` | `cmd/dagworkerd/v0.3.0` |

`cmd/dagworkerd` is tagged `cmd/dagworkerd/vX.Y.Z` — the module-path-prefixed form, not a bare
`dagworkerd/vX.Y.Z` — because that is the tag Go's module resolver actually requires for
`go get`ing it as a library dependency of a downstream test harness; any GoReleaser binary-release
workflow is configured to trigger off that same tag pattern rather than inventing a second,
non-module-compliant tag namespace for the same commit.

**`go.work` story.** `go.work` and `go.work.sum` are committed at the repo root for contributor
convenience only:

```
go 1.25

use (
	.
	./storage/redis
	./storage/postgres
	./adapters/grpc
	./adapters/http
	./cmd/dagworkerd
	./examples
)
```

`go.work` is never consumed by `go get`/module resolution for anyone depending on a module from
outside the workspace. **Zero `replace` directives are committed in any released `go.mod`** —
`go.work` fully replaces the older `replace ../.. `pattern (grpc-go's and testcontainers-go's own
modules still carry it; both predate `go.work`, introduced in Go 1.18). `go work sync` runs before
every tag, reconciling each module's own `go.mod`/`go.sum` against the workspace's resolved build
list, catching "depended on an unreleased core change" mistakes before they are pushed as a tag.

**Versioning policy: independent per module, never lockstepped** (rejecting OpenTelemetry-Go's
model, §Alternatives). **Release ordering constraint**: a breaking change to core's public
`Store`/`Manager` surface must be tagged on core *first*; only after that tag is resolvable
through the module proxy may a dependent module (`storage/*`, `adapters/*`, `cmd/dagworkerd`) bump
its `require` line for core and cut its own next release, on its own schedule — there is no
requirement that dependents move the same day. This ordering is enforced procedurally, not just
by convention: each dependent module's release workflow includes a pre-tag check
(`go list -m github.com/specialistvlad/dagworker@<pinned-version>` against the proxy) that fails
closed if the core version it is about to depend on has not yet been published, and a required-
reviewer rule on release PRs that bump a core `require` line to a not-yet-tagged version.

**Per-module CI loop, not a root-level `go test ./...`.** Root-level `go test ./...` silently
skips every nested module (each is a separate main module the tool does not descend into), so CI
runs:

```bash
for d in . storage/redis storage/postgres adapters/grpc adapters/http cmd/dagworkerd examples; do
  (cd "$d" && go build ./... && go vet ./... && go test ./... && go mod tidy -diff)
done
```

`go mod tidy -diff` failing on any module is a required check — this is also the mechanism ADR-0037
relies on to make "core has zero import edge to grpc/http" a build-enforced guarantee rather than
a lint rule.

## Consequences

### Positive
- A core-only embedder's `go.sum` never contains a SQL driver, a Redis client, gRPC, or an HTTP
  router — the entire reason for splitting, satisfied structurally.
- Independent release cadence per backend/adapter means a Redis-specific bug fix ships without
  forcing a version bump — or a CI run — of Postgres, gRPC, or core.
- The conformance-suite placement decision removes an entire class of "did the three backends'
  contract tests drift apart" risk by construction, at zero added module count.
- `cmd/dagworkerd` remains the single, clearly-documented "imports everything" exception, so
  reviewers know exactly where an all-of-the-above dependency graph is expected to appear.

### Negative
- `go get github.com/specialistvlad/dagworker` fetches core *only* — a first-time user expecting
  "the library" as one `go get` is surprised; the README must show three copy-pasteable `go get`
  lines up front (core, the storage backend in use, the adapter in use) to head this off.
- `go get -u`/`go mod tidy` must be run per module directory, not once at the repo root; a
  contributor who forgets this gets a stale `go.sum` in exactly one nested module with no root-
  level signal.
- Seven `go.mod` files is seven CI matrix legs, seven `CHANGELOG.md` files (per-module, Part 3 of
  dossier 15), and seven surfaces to keep individually `go vet`-clean — real, ongoing maintenance
  overhead versus a single-module repo.

### Neutral
- `examples/` follows `grpc-go`'s own pattern (a separate module purely to keep demo dependencies
  out of every real module's graph) and carries no versioning discipline of its own beyond "keeps
  building."
- Raising core's Go version floor (ADR-0029) is applied identically across every module's `go.mod`
  in the same per-module CI loop; it is a coordinated bump, not a topology change.

## Alternatives considered

**Single module, single `go.mod`, accept the dependency weight.** Rejected on `gocloud.dev`'s own
directly-inspected `go.mod`: importing one storage sub-package still resolves the entire module
graph, including SDKs for cloud providers the importer never touches. This is precisely the
outcome a "vendor-neutral generic API" cannot avoid in a single-module design once any one backend
has a non-trivial SDK — Redis's client and `pgx` both qualify.

**Single module, build tags gate the backend code.** Rejected for the same root cause as above:
Go build constraints are documented as a *file-selection* mechanism, not a dependency-graph
mechanism — `go.sum` still lists every tagged-out backend's dependencies, and CI must build the
full tag-combination matrix to catch breakage a compiler flag silently introduced.

**OpenTelemetry-Go's lockstep versioning** (every stable module bumps together, blocked on a
matching contrib release). Rejected: lockstep exists there because contrib packages call
*unstable internal* APIs across a repo boundary, so a core-internal change can silently break
every contrib package — forcing simultaneous release is a blunt fix for a coupling problem this
project does not have, since backends and adapters here only ever call the public, versioned
`Store`/`Transport` contracts (ADR-0016).

**A shared `storage/testsuite` module**, the option dossier 15 itself raises without resolving.
Rejected per the Decision above: it adds an eighth `go.mod` whose version must track core's
`Store` interface 1:1 anyway, buying nothing a core-resident package doesn't already buy for
free.

**Committed `replace ../.. ` directives** (the `grpc-go`/`testcontainers-go` pattern). Rejected as
a legacy pattern predating `go.work` (2022, Go 1.18); a greenfield 2026 repository has no reason
to carry the "accidentally-committed local-path replace breaks `go get` for everyone" risk that
`go.work` exists specifically to remove.

## References

- [go.dev/doc/modules/managing-source](https://go.dev/doc/modules/managing-source) — single-module default, nested-module tag prefix rule, import-compatibility rule
- [go.dev/doc/tutorial/workspaces](https://go.dev/doc/tutorial/workspaces) — `go.work`, `go work sync`
- [Go build constraints docs](https://pkg.go.dev/go/build#hdr-Build_Constraints) — build tags as file selection, not dependency-graph control
- [OpenTelemetry-Go `VERSIONING.md`](https://github.com/open-telemetry/opentelemetry-go/blob/main/VERSIONING.md) — lockstep policy, cited as the rejected model
- [`aws-sdk-go-v2` repository](https://github.com/aws/aws-sdk-go-v2) — module-per-service precedent
- [`grpc-go` `examples/go.mod`](https://github.com/grpc-go/grpc-go/blob/master/examples/go.mod), [`gcp/observability/go.mod`](https://github.com/grpc/grpc-go/blob/master/gcp/observability/go.mod)
- [`gocloud.dev` `go.mod`](https://raw.githubusercontent.com/google/go-cloud/master/go.mod) — the negative example, directly inspected
- [`testcontainers-go` `modules/postgres/go.mod`](https://raw.githubusercontent.com/testcontainers/testcontainers-go/main/modules/postgres/go.mod) — module-per-integration precedent
- docs/research/15-daemon-packaging-and-ops.md §1.1-§1.7 — full case-study comparison and recommendations
- docs/research/06-memcached-and-storage-abstraction.md Part A — memcached rejection (ADR-0017), reflected here only as an absent module
