# 15 — `dagworkerd`: optional daemon, module topology, and OSS project mechanics

Scope: (1) how to split the repository into Go modules so that `gRPC`, `net/http`, `redis`,
`pgx`, and `memcached` never appear in the transitive dependency graph of a user who only wants
the in-process API; (2) the design of the optional `dagworkerd` binary that hosts the two network
adapters — config, logging, health, shutdown, signals, metrics, pprof, container image, multi-
replica deployment; (3) the file-and-process furniture that makes an MIT Go library look and
behave like a project worth depending on.

Everything here is either a primary source (Go docs, RFCs/specs, vendor docs) or a live
inspection of a real repository's `go.mod`/`README` at the URL cited. Where a fetch could not be
completed (e.g. a dead blog link) the claim is dropped rather than asserted from memory.

---

## Part 1 — Module topology

### 1.1 The three options, stated plainly

For a repo that needs core (in-memory storage, engine, public API), three optional storage
backends (Redis, PostgreSQL, memcached), two optional network adapters (gRPC, HTTP), and one
optional daemon binary, there are exactly three ways to arrange `go.mod`:

1. **Single module, single `go.mod`, accept the dependency weight.** Every user's `go.sum`
   contains `google.golang.org/grpc`, `github.com/jackc/pgx/v5`, `github.com/redis/go-redis/v9`,
   and a memcached client, whether or not they use them. Build tags can *compile out* the code
   but cannot remove the entries from `go.sum` or the module graph that `go mod tidy` /
   dependency-scanners see, because `go.mod` requirements are resolved per module, not per build
   tag — the tag only guards which files the compiler visits ([Go build constraints
   docs](https://pkg.go.dev/go/build#hdr-Build_Constraints) describe tags as a *file selection*
   mechanism, not a dependency-graph mechanism).
2. **Single module, build tags gate the code.** Same dependency-graph problem as (1), plus the
   ergonomic cost that users must remember `-tags redis,postgres` at build time or get silent
   no-op backends; CI must build the full matrix of tag combinations to catch breakage.
3. **Multi-module monorepo: one `go.mod` per optional dependency edge.** Each backend/adapter is
   its own Go module living in a subdirectory of the same repository, with its own `go.mod`,
   `go.sum`, version tags, and (optionally) release cadence. A user's `go.sum` only grows when
   they literally `import` that module.

Go's own documentation is unambiguous about the default: **"Multiple modules in a repository" is
explicitly the exception, not the goal** — the guidance is "In general, we recommend that each
repository contain only one module, located at the root" and to reach for multiple modules only
"if you have code in the repository that constitutes multiple, separately releasable versioned
components" ([go.dev/doc/modules/managing-source](https://go.dev/doc/modules/managing-source)).
That single sentence is the entire test dagworker needs to apply: **is `storage/redis`
separately releasable and separately dependency-loaded from core?** Yes — its whole reason to
exist is that a core-only user must never resolve `github.com/redis/go-redis/v9`. That satisfies
Go's own bar for splitting.

### 1.2 What Go's own doc says about the mechanics

From the same page ([go.dev/doc/modules/managing-source](https://go.dev/doc/modules/managing-source)):

- Each nested module gets its own `go.mod` in its own subdirectory root.
- **The version tag for a nested module must be prefixed with the subdirectory path**: a module
  at `example.com/mymodules/module1` released as `v1.2.3` is tagged `module1/v1.2.3` in git, not
  bare `v1.2.3`. Git tags are global to the repo, so the prefix is what disambiguates which
  module a tag belongs to.
- Each module's initial commit is expected to carry its own `LICENSE`, `go.mod`, `go.sum`.
- Major version ≥2 additionally requires a `/v2` (etc.) suffix baked into the **module path
  itself** — this is Go's *import compatibility rule*: "If an old package and a new package have
  the same import path, the new package must be backwards compatible with the old package,"
  so an incompatible v2 needs a distinct import path
  ([go.dev/ref/mod#go-mod-file-go](https://go.dev/ref/mod#go-mod-file-go)). For a nested module
  this composes with the subdirectory prefix: `golang.org/x/tools/gopls` at v2 would tag
  `gopls/v2.0.0` and either live in a `gopls/v2/` directory or stay in `gopls/` — Go's tooling
  reads the version from the `go.mod` `module` line, not the directory name, so the subdirectory
  move is optional (same source).

### 1.3 `go.work`: the local-dev answer that replaces `replace`

Historically, developing module `adapters/grpc` against an unreleased change in `core` required
a `replace github.com/specialistvlad/dagworker => ../..` line in `adapters/grpc/go.mod` — which
is exactly the kind of line that gets accidentally committed and breaks `go get` for downstream
consumers (a `replace` with a local filesystem path is not resolvable outside the checkout).
Go 1.18 added **workspaces** (`go.work`) precisely to kill that pattern for local development:

> `go.work` "allows you to work with multiple Go modules in a single directory tree
> simultaneously... without requiring replace directives in individual go.mod files."
> ([go.dev/doc/tutorial/workspaces](https://go.dev/doc/tutorial/workspaces))

Setup is two commands from the repo root:

```bash
go work init
go work use . ./storage/redis ./storage/postgres ./storage/memcached ./adapters/grpc ./adapters/http ./cmd/dagworkerd
```

produces:

```
go 1.23

use (
	.
	./storage/redis
	./storage/postgres
	./storage/memcached
	./adapters/grpc
	./adapters/http
	./cmd/dagworkerd
)
```

`go.work` is resolved by the `go` command whenever it is present in the current directory or a
parent, and it is **never** consumed by `go get`/module resolution for people depending on the
module from outside the workspace — so it is safe (and idiomatic) to commit `go.work` and
`go.work.sum` at the repo root for contributor convenience, while each module's own `go.mod`
stays free of `replace` lines. `go work sync` pushes the workspace's resolved build list back
into each member module's `go.mod`/`go.sum` before a release, which is the moment to run in CI
before tagging (same tutorial page).

### 1.4 Case studies

**opentelemetry-go / opentelemetry-go-contrib — many modules, forced version lockstep.**
The versioning policy is explicit and instructive as a cautionary tale of what *not* to copy
wholesale:

> "All stable modules that use the same major version number will use the same entire version
> number." … "All stable contrib modules of the same major version with this project will use
> the same entire version as this project." … **"No additional stable release in this project
> can be made until the contrib repository has a matching stable release."**
> ([open-telemetry/opentelemetry-go VERSIONING.md](https://github.com/open-telemetry/opentelemetry-go/blob/main/VERSIONING.md))

This buys API consistency across `otel`, `otel/trace`, `otel/metric`, and every contrib
instrumentation package, but it couples two *separate repositories'* release trains together —
a stable `opentelemetry-go` release is blocked on a matching contrib release landing, with no
guaranteed turnaround. For a project with one core and independently-evolving storage backends,
this lockstep model is the wrong template: dagworker's Redis backend changing does not need to
force a version bump in core, and vice versa.

**aws-sdk-go-v2 — module-per-service, deliberately not in lockstep.**
The repo root module (`github.com/aws/aws-sdk-go-v2`) carries only shared plumbing (config,
credentials, retry, transport); every AWS service (`service/dynamodb`, `service/s3`, …) is its
own nested module, each independently `go get`-able and independently versioned, so a consumer
who only calls DynamoDB never resolves the S3 or SES client's dependency footprint. The published
CHANGELOG is per-repository and release notes call out per-module version bumps
([github.com/aws/aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2)) — this is the "storage
backend as a module" shape dagworker should copy for `storage/redis`, `storage/postgres`,
`storage/memcached`.

**grpc-go — single core module, but isolates *heavy-dependency* subtrees into their own
modules.** The main library is one module, but `examples/` and `gcp/observability/` are each
separate modules with their own `go.mod` — confirmed directly:

- `examples/go.mod` declares `module google.golang.org/grpc/examples`
  ([source](https://github.com/grpc/grpc-go/blob/master/examples/go.mod)).
- `gcp/observability/go.mod` declares `module google.golang.org/grpc/gcp/observability` and pulls
  in `cloud.google.com/go/logging`, `contrib.go.opencensus.io/exporter/stackdriver`,
  `go.opencensus.io`, `google.golang.org/api` — none of which appear in the root module's
  `go.mod` — with a local-dev `replace google.golang.org/grpc => ../..` committed in the
  submodule (grpc-go predates `go.work`; this is exactly the pattern `go.work` was built to make
  unnecessary going forward)
  ([source](https://github.com/grpc/grpc-go/blob/master/gcp/observability/go.mod)).

The lesson: you do not have to split *everything*; split precisely at the edges where an optional
feature drags in a dependency tree the median user does not want. `examples/` and `interop/`
exist purely so `go mod graph` on the *core* module stays clean of test/demo scaffolding.

**gocloud.dev (Go CDK) — the negative example: one module, all providers, no escape hatch.**
Direct inspection of `go.mod` at the module root confirms `module gocloud.dev` with over fifty
direct requires spanning **all three** cloud providers simultaneously — Firestore, Pub/Sub,
Secret Manager and Cloud Storage clients for GCP; DynamoDB, S3, KMS, SNS/SQS, SSM clients for
AWS; Service Bus, Blob Storage, Key Vault clients for Azure — plus OpenTelemetry exporters for
each ([raw go.mod](https://raw.githubusercontent.com/google/go-cloud/master/go.mod)). Importing
`gocloud.dev/blob/s3blob` alone still resolves the whole module graph, which means `go.sum`
carries GCP and Azure SDKs for a user who never touches those clouds. This is precisely the
outcome dagworker must not reproduce for its storage backends: "vendor-neutral generic API" and
"one module, pay for everything" are not compatible goals once one of the vendors has a large
SDK.

**testcontainers-go — module-per-integration, the closest structural analogue to dagworker's
storage backends.** Confirmed by direct fetch: `modules/postgres/go.mod` declares
`module github.com/testcontainers/testcontainers-go/modules/postgres`, requires the core
`testcontainers-go` module plus only its own driver deps (`jackc/pgx/v5`, `lib/pq`), and carries
a `replace github.com/testcontainers/testcontainers-go => ../..` for the monorepo checkout
([raw go.mod](https://raw.githubusercontent.com/testcontainers/testcontainers-go/main/modules/postgres/go.mod)).
There is one `go.mod` per database/service integration (`modules/redis`, `modules/kafka`,
`modules/localstack`, …), each independently tagged `modules/postgres/vX.Y.Z`, each with its own
package name matching the directory (`package postgres`,
[confirmed](https://raw.githubusercontent.com/testcontainers/testcontainers-go/main/modules/postgres/postgres.go)).
The project additionally ships a code-generation tool (`modulegen/`) whose job is scaffolding a
new module's boilerplate (`go.mod`, README, CI matrix entry) so the marginal cost of adding
module #40 stays low — worth copying for dagworker once there are 3+ storage backends.

**sigs.k8s.io/controller-runtime — single module, version tied to an external release train.**
One `go.mod` for the whole project; semver stays at `0.x` indefinitely, with minor versions
released in lockstep with each Kubernetes minor version, and the compatibility matrix (which
`k8s.io/client-go` version each controller-runtime minor was tested against) published in the
README rather than encoded as separate modules. Breaking changes are permitted between *minor*
versions while `<1.0.0` (consistent with semver's v0 clause, discussed in Part 3), and PRs are
labeled 🐛/✨/⚠️ to flag patch/feature/breaking intent for changelog generation. This is the right
shape for a project with **one** audience and **one** natural upstream cadence — it is not
dagworker's shape, because dagworker has *several* independent optional dependency edges
(storage backends × adapters), each with its own natural release cadence.

### 1.5 Firm recommendation: multi-module monorepo, deliberately not lockstepped

Given the four case studies, dagworker should be a **multi-module monorepo** with these exact
`go.mod` files:

```
dag-worker-go/                              (git repo root)
├── go.mod                 module github.com/specialistvlad/dagworker
├── go.work                (dev convenience only; not required by consumers)
├── go.work.sum
├── engine/, node/, lease/, event/, ...     (core packages, no adapters/redis/pgx/grpc/http)
│
├── storage/
│   ├── redis/
│   │   └── go.mod         module github.com/specialistvlad/dagworker/storage/redis
│   ├── postgres/
│   │   └── go.mod         module github.com/specialistvlad/dagworker/storage/postgres
│   └── memcached/
│       └── go.mod         module github.com/specialistvlad/dagworker/storage/memcached
│
├── adapters/
│   ├── grpc/
│   │   └── go.mod         module github.com/specialistvlad/dagworker/adapters/grpc
│   │                      (imports core; net/http never appears here)
│   └── http/
│       └── go.mod         module github.com/specialistvlad/dagworker/adapters/http
│                          (imports core; grpc never appears here)
│
├── cmd/
│   └── dagworkerd/
│       └── go.mod         module github.com/specialistvlad/dagworker/cmd/dagworkerd
│                          (imports core + one storage backend of choice, at build time,
│                           + both adapters — this is the ONLY module allowed to import
│                           everything, and it is the only module most users never `go get`
│                           as a library — they download a binary release instead)
│
├── examples/               (own go.mod, mirrors grpc-go's pattern — keeps demo deps
│   └── go.mod              out of every real module's graph)
│
└── internal/testutil/       (shared by core module's own tests only; not a module)
```

Each `storage/*` and `adapters/*` module's `go.mod` should carry a `replace` back to the sibling
core module **only** guarded to development via `go.work` — i.e. no `replace` line committed in
any `go.mod` at all; `go.work` supplies the local wiring, and released modules resolve each other
through the module proxy by version, exactly as external consumers do. This is the one place
dagworker should *not* imitate the older grpc-go/testcontainers-go pattern of a committed
`replace ... => ../..`: that pattern predates `go.work` (2022, Go 1.18) and both of those
examples are older codebases carrying legacy pre-`go.work` conventions forward. A greenfield
2026 repo has no reason to.

**Root `go.mod` (core)** — deliberately minimal, this is the file every "just give me the
in-process API" user resolves:

```go
module github.com/specialistvlad/dagworker

go 1.23

require (
	// only stdlib-adjacent, zero-dependency, or near-zero-dependency packages here.
	// e.g. a UUID/ULID lib, golang.org/x/sync for singleflight/errgroup — audited short list.
)
```

**`storage/redis/go.mod`:**

```go
module github.com/specialistvlad/dagworker/storage/redis

go 1.23

require (
	github.com/specialistvlad/dagworker v0.4.0
	github.com/redis/go-redis/v9 v9.7.0
)
```

**`adapters/grpc/go.mod`:**

```go
module github.com/specialistvlad/dagworker/adapters/grpc

go 1.23

require (
	github.com/specialistvlad/dagworker v0.4.0
	google.golang.org/grpc v1.68.0
	google.golang.org/protobuf v1.35.0
)
```

**`cmd/dagworkerd/go.mod`** is the composition root and the only module allowed to `require`
everything above, plus whichever storage backends it ships pre-wired:

```go
module github.com/specialistvlad/dagworker/cmd/dagworkerd

go 1.23

require (
	github.com/specialistvlad/dagworker v0.4.0
	github.com/specialistvlad/dagworker/storage/redis v0.2.0
	github.com/specialistvlad/dagworker/storage/postgres v0.2.0
	github.com/specialistvlad/dagworker/storage/memcached v0.1.0
	github.com/specialistvlad/dagworker/adapters/grpc v0.3.0
	github.com/specialistvlad/dagworker/adapters/http v0.3.0
)
```

### 1.6 Versioning policy: independent, not lockstepped — and why

Reject the opentelemetry-go lockstep model. Core, each storage backend, and each adapter get
**independent semver counters**. Rationale: lockstep exists in OTel because contrib packages
call *unstable internal APIs* of the core SDK across a repo boundary, so a core-internal change
can silently break every contrib package — forcing simultaneous release is a blunt fix for a
tight coupling problem. dagworker's storage backends and adapters, by design, only call the
**public** `Storage`/`Transport` interface contracts documented elsewhere in this research series
— that is the whole point of the interface boundary. A public, versioned interface is exactly
what makes independent versioning safe: `storage/redis` only needs a new major version when
*dagworker core's `Storage` interface* makes a breaking change, and can otherwise release patch
fixes for Redis-specific bugs on its own clock, same as `aws-sdk-go-v2`'s per-service modules.

**Exact tag format** (per [go.dev/doc/modules/managing-source](https://go.dev/doc/modules/managing-source)):

| Module | Tag example |
|---|---|
| core (root) | `v0.5.0` |
| `storage/redis` | `storage/redis/v0.2.0` |
| `storage/postgres` | `storage/postgres/v0.2.0` |
| `storage/memcached` | `storage/memcached/v0.1.0` |
| `adapters/grpc` | `adapters/grpc/v0.3.0` |
| `adapters/http` | `adapters/http/v0.3.0` |
| `cmd/dagworkerd` (binary releases, GoReleaser-driven, see Part 2) | `dagworkerd/v0.3.0` |

**Exact release procedure** for a change confined to `storage/redis`:

1. Land the PR on `main` with a Conventional Commit (`fix(storage/redis): ...` or
   `feat(storage/redis): ...`, see Part 3 §3.5) scoped to that module.
2. `cd storage/redis && go test ./... && go vet ./...` — CI already gated this on the PR.
3. `go work sync` at the repo root to make sure the workspace's resolved versions match what
   will be tagged (catches "I depended on an unreleased core change" mistakes before tagging).
4. Tag: `git tag storage/redis/v0.2.1 && git push origin storage/redis/v0.2.1`.
5. The module proxy (`proxy.golang.org`) picks up the new tag automatically the first time
   anyone requests it; no separate publish step exists for Go modules (unlike npm/PyPI) —
   `go get github.com/specialistvlad/dagworker/storage/redis@v0.2.1` is the "publish."
6. Update that module's own `CHANGELOG.md` (Part 3) in the same commit that gets tagged.

For a **core** change that is a breaking change to the `Storage` interface, the sequence adds a
compatibility step before any dependent module can move: tag core first (`v0.6.0` or `v1.0.0` if
crossing the v0→v1 boundary, requiring the `/v2`-style path bump only from v2 onward per §1.2),
then bump each storage/adapter module's `require` line for core and release *those* as their own
next minor/patch, on their own schedule — there is no requirement that they all move the same
day, unlike OTel.

### 1.7 The `go get` UX pain, named explicitly

Because each nested module has an independent version, `go get github.com/specialistvlad/dagworker`
**only ever fetches the root/core module** — it does not pull in storage or adapter modules,
which is the intended effect but surprises first-time users who expect "the library" to be one
`go get`. The README (Part 3) must show three copy-pasteable `go get` lines up front:

```bash
go get github.com/specialistvlad/dagworker                       # core, always
go get github.com/specialistvlad/dagworker/storage/redis          # only if using Redis storage
go get github.com/specialistvlad/dagworker/adapters/grpc           # only if embedding the gRPC adapter yourself
```

A second, sharper pain: `go get ./...`-style "upgrade everything" workflows do not span module
boundaries — a contributor inside the monorepo runs `go get -u` per module directory (or lets
`go work sync` reconcile), and `go mod tidy` must be run **inside each module directory**
separately in CI, not once at the repo root. The CI matrix should therefore be
`for d in . storage/redis storage/postgres storage/memcached adapters/grpc adapters/http cmd/dagworkerd examples; do (cd $d && go build ./... && go vet ./... && go test ./...); done`
rather than a single root-level invocation — root-level `go test ./...` silently skips every
nested module because each one is a separate main module Go's tool does not descend into.

---

## Part 2 — The daemon

`dagworkerd` is a thin composition-root binary: parse config → construct a core `Engine` bound
to one configured storage backend → attach the gRPC adapter and/or the HTTP adapter → serve →
handle signals → shut down in order. Nothing adapter-specific belongs in core; nothing
daemon-specific (flags, env parsing, `log/slog` handler wiring) belongs in the adapters, which
should accept an already-constructed `*slog.Logger` and already-parsed config struct so they stay
usable when embedded directly into someone else's binary without `dagworkerd` at all.

### 2.1 Configuration: flags, env, or file — recommend layered, env-primary with flag override

**The 12-factor argument for env vars**, stated by the source itself:

> "Env vars are a language- and OS-agnostic standard" … "Unlike config files, there is little
> chance of them being checked into the code repo accidentally" ([12factor.net/config](https://12factor.net/config))

This is a strong argument specifically for *secrets and per-deployment values* (DB DSN, Redis
address, log level) because it decouples the artifact (one container image) from the environment
it runs in — the same image is promoted staging→prod by changing env, not by rebuilding with a
different baked-in file.

**The critique, sourced.** The 12-factor argument is about deployment portability, not about
security or structure, and both are real costs:

- **Structural**: env vars are flat key=string; there is no native way to express "three
  configured storage backends with per-backend TTLs" without inventing a delimiter-encoded
  mini-language or numbered `BACKEND_0_KIND`, `BACKEND_1_KIND` env vars — exactly the kind of
  config dagworker needs (multiple named scopes, per-scope retry policy). A structured file
  (YAML/TOML) or repeated flags express this natively.
- **Security exposure, sourced from OWASP's Secrets Management Cheat Sheet**: "environment
  variables are generally accessible to all processes and may be included in logs or system
  dumps. Using environment variables is therefore not recommended unless the other methods are
  not possible," and Docker-specific: "secrets themselves should never be hardcoded using docker
  ENV or docker ARG commands, as these can easily leak with the container definitions"
  ([OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)).
  A process's full environment is legible to any co-located process with the right privilege
  (`/proc/<pid>/environ` on Linux), to `docker inspect`, and to crash-reporting tooling that
  dumps environment blocks by default — none of which is true of a value passed only as a CLI
  flag scoped to the process's own argv, and none of which is true of a file with restrictive
  filesystem permissions mounted read-only.

**Recommendation for `dagworkerd`: three-layer precedence, flag > env > file > built-in
default**, mirroring how Prometheus, etcd, and most well-regarded Go infra binaries actually
ship (all three accept a `--config.file` plus env overrides plus flags, with flags winning):

1. **File** (`--config /etc/dagworkerd/config.yaml`, optional) carries the shape that has
   structure: named scopes, per-scope storage selection, retry policy, adapter enable/disable,
   TLS material paths. Loaded first, lowest precedence.
2. **Environment variables** (`DAGWORKERD_STORAGE_KIND=redis`, `DAGWORKERD_REDIS_ADDR=...`)
   override file values — this is what makes one built image portable across environments
   without a config-file diff per environment, honoring the 12-factor promotion argument.
3. **Flags** (`--storage-kind=redis`, `--grpc-addr=:9090`) override both — flags win because
   they are the most explicit, most visible-in-`ps`/systemd-unit-file expression of intent, and
   are what an operator types by hand when debugging ("just this once, use verbose logging":
   `--log-level=debug` should not require editing a file or exporting an env var).

Concretely, use `flag.FlagSet` plus `os.LookupEnv` fallback per flag (no reflection-based env
binding library needed at this scope — a ~15-flag surface does not justify a dependency), and a
single `Config` struct with a `LoadFile(path string) (Config, error)` that a plain YAML decode
populates before flags/env are applied on top. Secrets (DSNs, TLS keys) should be **file-path
flags/env**, not value flags/env — `--redis-password-file=/run/secrets/redis_password` — so the
secret itself never appears in `ps`, shell history, or the process environment block, addressing
the OWASP concern directly rather than arguing it away.

### 2.2 Structured logging with `log/slog`

`dagworkerd` should construct exactly one root `*slog.Logger` at startup from `--log-format`
(`text`|`json`, default `json` for anything that isn't an interactive TTY) and `--log-level`
(`debug`|`info`|`warn`|`error`), then pass it down explicitly — never `slog.SetDefault` inside a
library package, only in `main()`. Every request/claim/lease-expiry log line should carry
structured fields (`node_id`, `scope`, `fencing_token`, `worker_id`) via `slog.Group` or
attached `slog.Attr`s so log aggregators can filter without regexing message strings — this is
the entire reason `log/slog` exists over `log.Printf`. A per-request/per-claim
`logger.With("trace_id", ...)` child logger threaded through `context.Context` (via a private
context key, exposed by a `logctx.From(ctx)` helper) keeps call sites from re-stating fields.

### 2.3 Health vs readiness — why they are different endpoints, sourced

Kubernetes (and any sane LB health-check config) treats these as answering different questions
with different consequences on failure:

> Liveness: "detect and remedy" a container stuck in "broken states [that] cannot recover except
> by being restarted" — **failure restarts the container.**
> Readiness: determines "if the container is ready to accept traffic" — **failure removes the
> Pod from Service endpoints without restarting it.**
> ([kubernetes.io — configure liveness/readiness/startup probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/))

Concretely for `dagworkerd`:

- `GET /healthz` (liveness) should check only "is the process's own event loop alive" —
  essentially a no-op 200 unless a goroutine deadlock/panic-recovery counter has tripped a
  circuit. It must **not** depend on the storage backend being reachable — if Redis is briefly
  down, restarting `dagworkerd` fixes nothing and just causes a crash-loop; the process itself is
  fine.
- `GET /readyz` (readiness) **should** depend on storage reachability (a cheap `PING`/roundtrip
  against the configured backend) plus "has this replica finished its startup subscribe-to-event-
  bus handshake." Failing `/readyz` pulls the replica out of the load-balancer rotation for gRPC/
  HTTP claim traffic without killing the process, so it stops being handed new claims while
  letting existing leases it holds continue to be tracked.
- For the gRPC adapter specifically, implement the **standard gRPC health checking protocol**
  (`grpc.health.v1.Health`) rather than a bespoke health RPC, because that is what
  gRPC-aware load balancers and service meshes (Envoy, Linkerd, gRPC's own client-side LB) know
  how to call natively: a unary `Check(service string) ServingStatus` plus a streaming `Watch`
  that "immediately send[s] back a message indicating the current serving status" and pushes
  updates thereafter, with `ServingStatus` ∈ `{UNKNOWN, SERVING, NOT_SERVING, SERVICE_UNKNOWN}`
  ([grpc/grpc health-checking.md](https://github.com/grpc/grpc/blob/master/doc/health-checking.md)).
  Register the dagworker gRPC service name against this and flip it to `NOT_SERVING` in lockstep
  with `/readyz` failing on the HTTP side, from one shared internal readiness flag.
- A concrete real precedent for keeping liveness/readiness as separate named checker sets inside
  one process is `sigs.k8s.io/controller-runtime`'s manager: a `Checker` is just
  `func(req *http.Request) error`, and the manager mounts distinct `Handler`s — "often referred
  to as healthz and readyz" — each aggregating its own named checkers
  ([pkg.go.dev/.../pkg/healthz](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/healthz)).
  Copy that shape: `dagworkerd` registers named checkers (`"storage"`, `"eventbus"`) into a
  readiness aggregator, and a trivial always-true (or panic-recovery-gated) checker into liveness.

### 2.4 Graceful shutdown ordering — and the lease-specific argument

`net/http.Server.Shutdown` defines the textbook order for the HTTP side: "first closing all open
listeners, then closing all idle connections, and then waiting indefinitely for connections to
return to idle" ([pkg.go.dev/net/http#Server.Shutdown](https://pkg.go.dev/net/http#Server.Shutdown)) —
i.e. **stop accepting new work before draining existing work**, never the reverse (draining first
would let new connections race in while old ones finish, and shutdown never converges). gRPC's
`GracefulStop` follows the identical shape. dagworkerd's shutdown sequence should be, in order:

1. **Flip `/readyz` to failing immediately** on receipt of the shutdown signal — this is a
   cheap, instant "stop routing new traffic to me" signal to the load balancer that happens
   *before* anything else, because LB health-check polling intervals (typically 2-10s) mean
   there is a window between "readiness fails" and "LB actually stops sending," so starting this
   clock first is free latency-hiding for every later step.
2. **Stop accepting new CLAIM RPCs/HTTP requests** — call `grpcServer.GracefulStop()` and
   `httpServer.Shutdown(ctx)` for the claim-issuing endpoints specifically (not the health
   endpoint, which must keep answering `/healthz`=alive/`/readyz`=not-ready during the drain so
   the orchestrator doesn't kill the process mid-drain thinking it's dead).
3. **Let in-flight claim/ack/heartbeat RPCs already accepted finish naturally** — these are
   short (single round-trip), bounded by the adapter's own request timeout, and forcibly cutting
   them mid-flight would turn a clean ack into a lost ack, needlessly triggering the lease-
   timeout failure path for a worker that in fact succeeded.
4. **Explicitly release (not merely stop tracking) any leases this replica currently holds
   ownership bookkeeping for, within the shutdown grace window — do not just let them ride out
   to their natural lease-timeout expiry.** This is the one dagworker-specific decision this
   dossier takes a firm position on, and the reasoning is:
   - A lease's timeout is sized for *worker* stalls (seconds to minutes, chosen by the workload),
     not for *dagworkerd replica* restarts, which orchestrators budget in a much shorter, fixed
     window: Kubernetes' default `terminationGracePeriodSeconds` is 30 seconds, after which
     SIGKILL is sent regardless of process state ([kubernetes.io — pod lifecycle: pod
     termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)).
     If lease timeouts are typically longer than 30s (a very plausible worst case for e.g. a
     lease sized for a slow batch job), passively waiting for lease-timeout expiry means the node
     sits **claimed-but-orphaned** for the full lease duration after the replica is already gone
     — that's dead time no other replica can claim the node in, directly hurting the "work is
     available" latency the whole subscriber design exists to minimize (see event-bus research).
   - The daemon already knows, in memory, exactly which fencing tokens/nodes it is the lease-
     holder of record for (or can ask the storage backend "leases fenced by instance ID X").
     Actively transitioning those nodes back to claimable — the same code path the lease-timeout
     background sweep already uses — during the shutdown grace window converts an unbounded
     "wait for the passive timeout" cost into a bounded "wait up to
     `min(shutdownGracePeriod, remaining-lease-time)` cost, which is strictly better for cluster
     availability and costs nothing extra: it is calling code that already exists.
   - The one caveat this recommendation must carry: only release a lease if the replica can
     prove the worker's in-flight ack is *not* about to land — i.e. run step 3 (drain in-flight
     RPCs) to completion **before** step 4 (release remaining leases), so a worker's ack that was
     already accepted and is in flight gets to complete and mark the node success/error normally,
     and only the leases with **no** in-flight ack at drain-completion get force-released. Get
     the ordering (3 then 4) wrong and you'll release a lease out from under a worker's ack that
     was about to land, handing the fencing token to a second claimant while the first worker's
     ack is still traveling — that's the exact double-claim scenario the fencing token exists to
     prevent (see the leases/fencing research file for the full argument).
5. **Close the storage client / event-bus subscription** last, after 1-4, so the release calls
   in step 4 can actually reach storage.
6. **Call `stop()`** on the `signal.NotifyContext` cancel func to restore default OS signal
   handling before exit, releasing the resource (idiomatic per the stdlib doc, "should be called
   as soon as [it is] no longer needed").

### 2.5 Signal handling with `signal.NotifyContext`

Exact stdlib signature and behavior:

```go
func NotifyContext(parent context.Context, signals ...os.Signal) (ctx context.Context, stop context.CancelFunc)
```

"returns a copy of the parent context that is marked done ... when one of the listed signals
arrives, when the returned stop function is called, or when the parent context's Done channel is
closed, whichever happens first" ([pkg.go.dev/os/signal#NotifyContext](https://pkg.go.dev/os/signal#NotifyContext)).
Wire it as the top of `main()`:

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

srv := newServer(cfg, logger)
go func() {
    if err := srv.Run(ctx); err != nil {
        logger.Error("server exited with error", "err", err)
    }
}()

<-ctx.Done()
stop() // restore default signal behavior so a second SIGTERM/SIGINT force-kills
shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
defer cancel()
srv.Shutdown(shutdownCtx) // runs the ordered sequence in §2.4
```

Note the `stop()` call the moment `ctx.Done()` fires, before starting the bounded shutdown
timer — this deliberately restores default signal disposition so that an operator's *second*
Ctrl-C/SIGTERM (impatience, or a stuck shutdown) terminates the process immediately instead of
being silently absorbed by a context that's already cancelled — a well-known rough edge of naive
"cancel on signal" code that only registers the handler once.

### 2.6 Metrics: RED + USE, and the exact set for a work dispatcher

**RED method** (for anything that serves requests — dagworkerd's claim/ack RPCs): **R**ate,
**E**rrors, **D**uration — per Tom Wilkie's formulation, tracking these three per request type
gives you the leading indicator of user-facing service health.

**USE method** (for the resources dagworkerd's request handling depends on — CPU, the ready-set,
lease bookkeeping): **U**tilization ("the average time that the resource was busy"),
**S**aturation ("the degree to which a resource has ... work it cannot immediately service,"
typically a queue length), **E**rrors ("the count of error events")
([brendangregg.com/usemethod.html](https://www.brendangregg.com/usemethod.html)). USE is the
right lens specifically for the ready-set and the lease-expiry sweep because both are
queue-shaped resources, not request-shaped ones.

**Exact metric set to expose** (Prometheus naming rules applied throughout: base unit `seconds`
not `ms`, `_total` suffix on every monotonic counter, ratios in `[0,1]` not percent, no label
baked into the metric name — "Do not put the label names in the metric name... use labels" per
[prometheus.io/docs/practices/naming](https://prometheus.io/docs/practices/naming/)):

| Metric | Type | Labels | Method |
|---|---|---|---|
| `dagworkerd_claims_total` | Counter | `scope`, `outcome={claimed,empty,error}` | RED-Rate/Errors |
| `dagworkerd_acks_total` | Counter | `scope`, `outcome={success,error,timeout}` | RED-Rate/Errors |
| `dagworkerd_claim_duration_seconds` | Histogram | `scope`, `transport={grpc,http}` | RED-Duration |
| `dagworkerd_ack_duration_seconds` | Histogram | `scope`, `transport` | RED-Duration |
| `dagworkerd_rpc_requests_total` | Counter | `rpc`, `code` (gRPC status / HTTP status class) | RED (transport-level) |
| `dagworkerd_ready_set_size` | Gauge | `scope` | USE-Saturation (ready-set depth) |
| `dagworkerd_queue_depth` | Gauge | `scope`, `state={pending,blocked-by-deps}` | USE-Saturation |
| `dagworkerd_lease_expiries_total` | Counter | `scope`, `reason={timeout,released-on-shutdown}` | USE-Errors (a lease expiry is a failure mode) |
| `dagworkerd_oldest_ready_node_age_seconds` | Gauge | `scope` | USE-Saturation, leading indicator of worker starvation |
| `dagworkerd_storage_op_duration_seconds` | Histogram | `op`, `backend` | USE-Utilization proxy for the storage dependency |
| `dagworkerd_active_leases` | Gauge | `scope` | USE-Utilization (how much of the "in-flight capacity" is occupied) |
| `dagworkerd_up` | Gauge (1/0) | — | trivial liveness-as-metric for dashboards, mirrors `/healthz` |

A **Histogram**, not a Summary, for durations: Prometheus histograms expose
`<name>_bucket{le="..."}`, `<name>_sum`, `<name>_count` and support `histogram_quantile()`
aggregation *across replicas* server-side at query time, whereas a Summary's φ-quantiles are
computed client-side per-process and **cannot be meaningfully averaged/aggregated across
instances** ([prometheus.io/docs/concepts/metric_types](https://prometheus.io/docs/concepts/metric_types/))
— with N replicas of `dagworkerd` behind a load balancer (the deployment model this whole daemon
targets, §2.9), histogram is the only correct choice for latency SLO dashboards.

Register via `prometheus/client_golang`'s `promauto`, expose on `GET /metrics` on the **same**
internal-only admin port as `/healthz`/`/readyz`/`pprof` (§2.7) — never the public claim-serving
port, both to keep the public wire surface minimal and because metrics scraping should not
compete with worker traffic for the same listener's accept queue.

OpenTelemetry's own metric-naming guidance generalizes the same principle across vendors:
dot-namespaced hierarchical names, units carried in instrument metadata rather than jammed into
the name when the framework already tracks units, and counters/histograms/gauges chosen by what
the value semantically is, not by what's convenient to compute
([opentelemetry.io semantic conventions — general metrics](https://opentelemetry.io/docs/specs/semconv/general/metrics/)).
If dagworkerd ships an OTel exporter, that's naming discipline is `dagworker.claims`
(counter, unit `{claim}`) / `dagworker.claim.duration` (histogram, unit `s`) etc., but Prometheus
exposition format remains the pragmatic default given the operator ecosystem this daemon targets.

### 2.7 pprof behind a flag, on a separate listener

`net/http/pprof`'s own doc states it "is typically only imported for the side effect of
registering its HTTP handlers" on `http.DefaultServeMux`
([pkg.go.dev/net/http/pprof](https://pkg.go.dev/net/http/pprof)) — which is exactly the trap to
avoid: importing it for side effects anywhere near the binary's main package silently exposes
`/debug/pprof/*` (including a 30-second blocking CPU profile trigger, and full heap dumps that
can leak sensitive data resident in memory) on whatever port `DefaultServeMux` happens to be
served on. Correct pattern for dagworkerd:

```go
// admin.go — never imports net/http/pprof at package scope unconditionally
func newAdminMux(cfg Config) *http.ServeMux {
    mux := http.NewServeMux()
    mux.HandleFunc("/healthz", healthzHandler)
    mux.HandleFunc("/readyz", readyzHandler)
    mux.Handle("/metrics", promhttp.Handler())
    if cfg.PprofEnabled { // --admin-pprof=true, default false
        mux.HandleFunc("/debug/pprof/", pprof.Index)
        mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
        mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
        mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
        mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
    }
    return mux
}
```

served on a distinct `--admin-addr` (default `127.0.0.1:9091`, deliberately loopback-only by
default so a misconfigured `0.0.0.0` bind requires an explicit operator choice) — separate from
`--grpc-addr`/`--http-addr`, which is the whole point: the admin surface's threat model and
audience (operators, Prometheus scraper, the orchestrator's health checker) is different from the
claim-serving surface's (arbitrary worker processes, potentially cross-network, cross-org for the
"casual HTTP consumers" the assignment explicitly names).

### 2.8 Container image: distroless, static, multi-arch

Build fully static (no libc dependency at all, so `scratch`/`distroless/static` both work):

```dockerfile
# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.23 AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY . .
RUN cd cmd/dagworkerd && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/dagworkerd .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/dagworkerd /dagworkerd
USER nonroot:nonroot
ENTRYPOINT ["/dagworkerd"]
```

- `CGO_ENABLED=0` guarantees a statically-linked binary with no glibc/musl dynamic dependency —
  required for both `scratch` and `distroless/static` to work at all, since neither image ships a
  dynamic linker.
- `-trimpath`: "remove all file system paths from the resulting executable... the recorded file
  names will begin [with] a module path@version... instead of absolute file system paths" —
  both a reproducibility win (build path no longer leaks into the binary, so two builds on two
  different checkout directories produce identical bytes) and a minor information-disclosure
  closure (no local username/directory structure embedded in stack traces)
  ([pkg.go.dev/cmd/go](https://pkg.go.dev/cmd/go)).
- `distroless/static-debian12` over bare `scratch`: distroless additionally ships CA certificates
  and `/etc/passwd` with a `nonroot` (UID 65532) entry pre-defined, needed the moment dagworkerd
  dials out over TLS (to a managed Redis/Postgres) or the operator wants to run as non-root by
  name rather than raw UID — `scratch` has neither, at roughly 2MB vs distroless's similarly tiny
  footprint (documented as "very small... roughly 50% of alpine's size"). Only drop to bare
  `scratch` if the deployment never needs outbound TLS and never needs a shell for `kubectl exec`
  debugging (distroless intentionally ships no shell either, but has a `:debug` variant with
  busybox for exactly that need — use `distroless/static-debian12:debug` in a
  troubleshooting-tagged image if this bites operators)
  ([GoogleContainerTools/distroless](https://github.com/GoogleContainerTools/distroless)).
- Multi-arch via `docker buildx build --platform linux/amd64,linux/arm64 -t ... --push .`, or
  equivalently via goreleaser's Docker pipeline, which "creates a manifest that intelligently
  serves the correct image based on the client's platform" through `docker_manifests`, and can
  drive `buildx` directly (`backend: buildx`) rather than the classic `docker build`
  ([goreleaser.com/customization/docker](https://goreleaser.com/customization/docker/)).
- `-buildvcs=auto` (the default) stamps VCS revision info into the binary
  automatically whenever the build directory is a clean checkout of the module's own repo
  ([pkg.go.dev/cmd/go](https://pkg.go.dev/cmd/go)) — leave this on for release builds (it's what
  `dagworkerd --version` should print), but note it is orthogonal to `-trimpath` (paths are
  still trimmed; only the VCS *revision string*, not filesystem paths, is embedded).

### 2.9 Multiple replicas behind a load balancer

Because leases and the ready-set live in shared storage (not in-process), `dagworkerd` replicas
are stateless from the load balancer's point of view and horizontally scale trivially:

- **gRPC**: prefer client-side or proxy-based load balancing (Envoy/Linkerd/gRPC's own
  `round_robin` resolver) over an L4/TCP load balancer, since a single long-lived HTTP/2
  connection multiplexes many RPCs and an L4 LB would pin all of one worker's claims to whichever
  replica it first connected to — defeating the point of running N replicas. The gRPC health
  protocol from §2.3 is what a gRPC-aware LB polls to pull a draining replica out of rotation.
- **HTTP/JSON**: a conventional L7 LB (ALB/NLB/Envoy/nginx) with `/readyz` as the health-check
  path works directly — HTTP/1.1 or short-lived HTTP/2 requests don't have gRPC's connection-
  pinning problem.
- Both transports' replicas should share one `--admin-addr` convention but each replica's
  `/metrics` needs distinct instance identity (a `instance` label, typically pod name/hostname)
  in the scrape config so Prometheus can distinguish replicas — every gauge in §2.6's table
  should be read per-instance and summed/maxed at query time (`sum by (scope) (...)` for
  `dagworkerd_ready_set_size` since it reflects shared storage state, not per-instance state —
  actually most of these gauges reflect **shared** storage-backed truth, so scraping N replicas
  will show the *same* value N times for scope-level gauges like ready-set size; only
  `dagworkerd_active_leases` and RPC-rate counters are genuinely per-instance. Document this
  distinction in the metrics doc so nobody double-counts a shared gauge across replicas.)

---

## Part 3 — OSS project mechanics

### 3.1 The files that must exist, and why

| File | Purpose | Notes |
|---|---|---|
| `LICENSE` | MIT text, unmodified | Copyright line `Copyright (c) 2026 <name>` |
| `README.md` | Converts a skimmer to a user (§3.8) | Root, always rendered first on GitHub/pkg.go.dev |
| `CONTRIBUTING.md` | How to build/test/PR | Link from README, don't duplicate |
| `CODE_OF_CONDUCT.md` | Contributor Covenant v2.1, unmodified except contact email | Sections: Our Pledge, Our Standards, Enforcement Responsibilities, Scope, Enforcement, Enforcement Guidelines (4-tier: Correction/Warning/Temporary Ban/Permanent Ban), Attribution ([contributor-covenant.org/version/2/1](https://www.contributor-covenant.org/version/2/1/code_of_conduct/)) |
| `SECURITY.md` | Private vulnerability-reporting channel | GitHub surfaces this under the repo's Security tab → "Security policy," and can auto-populate a stub via the UI ([docs.github.com — adding a security policy](https://docs.github.com/en/code-security/getting-started/adding-a-security-policy-to-your-repository)); enable GitHub's **private vulnerability reporting** feature so reporters don't have to email anything |
| `CHANGELOG.md` (one per module, per §1.6) | Human-readable release history | Keep a Changelog format exactly (§3.4) |
| `.editorconfig` | Cross-editor whitespace consistency | See exact contents below |
| `CODEOWNERS` (in `.github/`) | Auto-request reviewers per path | gitignore-style patterns, last match wins, no negation/char-ranges support ([docs.github.com — about code owners](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners)) |
| `.github/ISSUE_TEMPLATE/bug_report.yml`, `feature_request.yml` | Structured intake | YAML form templates render as a real form, not free-text markdown |
| `.github/PULL_REQUEST_TEMPLATE.md` | Checklist: tests added, CHANGELOG updated, ADR updated if behavior changed | |
| `.github/dependabot.yml` | Automated dependency PRs | One entry per module directory (§3.6) |
| `.github/workflows/{ci,codeql,scorecard,release}.yml` | CI, static analysis, supply-chain scoring, release automation | §3.6-3.9 |
| `doc.go` (per package) | Package-level godoc | §3.7 |

**Exact `.editorconfig`:**

```ini
root = true

[*]
charset = utf-8
end_of_line = lf
trim_trailing_whitespace = true
insert_final_newline = true

[*.go]
indent_style = tab

[*.{yml,yaml,json,md}]
indent_style = space
indent_size = 2
```
([editorconfig.org](https://editorconfig.org/) for the format; tab indentation for `.go` matches
`gofmt`'s own output so the file never fights the formatter.)

**Example `CODEOWNERS`:**

```
# Default owners for everything
*                          @specialistvlad

# Storage backends can be owned by different people as the project grows
/storage/redis/            @specialistvlad
/storage/postgres/         @specialistvlad
/adapters/grpc/            @specialistvlad
/adapters/http/            @specialistvlad

# Release/CI plumbing needs the maintainer's eyes always
/.github/                  @specialistvlad
```

### 3.2 Semantic versioning and the v0 escape hatch

Ship `v0.x.y` from day one and say so loudly in the README: SemVer's own spec sanctions this
explicitly — **"Major version zero (0.y.z) is for initial development. Anything MAY change at
any time. The public API SHOULD NOT be considered stable"** ([semver.org](https://semver.org/)).
This is not a cop-out; it is the correct signal for a library whose `Storage`/`Transport`
interface boundaries (the whole point of Part 1's module split) are still being validated against
real Redis/Postgres/memcached backends and two network protocols. Commit to `v1.0.0` only once
those interfaces have shipped at least one full backend + one full adapter each without a
breaking change for, as a rule of thumb, two consecutive minor releases — that's the empirical
signal the interface has stopped moving, not a calendar date.

### 3.3 Conventional Commits, and whether to automate releases

**Format**, per spec: `<type>[optional scope]: <description>`, body, footer(s); `feat` → MINOR,
`fix` → PATCH, a `BREAKING CHANGE:` footer or a `!` immediately before the colon (`feat!:`,
`feat(storage/redis)!:`) → MAJOR regardless of type
([conventionalcommits.org/en/v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/)). Given
Part 1's per-module tagging, **every commit that touches a nested module's directory should carry
that module as its scope** (`fix(storage/redis): ...`) — this is what makes automated per-module
changelog generation possible at all; an unscoped `fix: ...` touching `storage/redis/client.go`
is a lint failure the CI commit-lint step should catch (`commitlint` config restricting scopes to
the known module list, or `.golangci.yml`-adjacent tooling; this is one of few places a Node
dev-dependency, `@commitlint/cli`, is defensible purely as a CI-time lint, never a runtime or
build dependency of any Go module).

**Automation recommendation: `release-please`, one config entry per module, over `goreleaser`
for versioning and over fully manual tags.** Reasoning:

- `release-please` "parses commits for prefixes like fix:, feat:, and breaking changes...opens
  a release PR that automatically updates CHANGELOG.md [and] bumps the version"; merging that PR
  makes it "finalize changelog and version file updates, tag the commit... and create a GitHub
  Release" ([googleapis/release-please](https://github.com/googleapis/release-please)) — this
  directly automates the exact per-module tag-with-prefix workflow Part 1 specifies, because
  `release-please`'s manifest mode supports **multiple release units in one repo**, each with its
  own `component` (map cleanly to `storage/redis`, `adapters/grpc`, etc.) and its own tag prefix.
- **`goreleaser` still has a job — just a different one**: it does not decide *what version
  number* to cut (that's a human/`release-please` decision informed by commit history); it
  consumes an already-created tag and turns it into build artifacts — cross-compiled binaries,
  the multi-arch container image (§2.8), checksums, and (§3.9) signed/provenanced release
  assets. Use both: `release-please` owns versioning + CHANGELOG + tagging for **every** module;
  a `goreleaser`-driven workflow triggers **only** on `dagworkerd/v*` tags to build/publish the
  binary + image, since the library modules (`storage/*`, `adapters/*`, core) have no build
  artifact beyond the tagged source itself — Go's module proxy *is* their distribution mechanism.
- **Fully manual tags** are the wrong default for a multi-module repo specifically because the
  tag-prefix discipline in §1.6's table is easy to typo by hand (`storage-redis/v0.2.0` vs the
  required `storage/redis/v0.2.0`) and manual CHANGELOG editing drifts from actual commit history
  within a few releases — automation earns its keep exactly where the process has this many
  small, repeatable, easy-to-typo steps.

### 3.4 `CHANGELOG.md` — exact Keep a Changelog template, per module

```markdown
# Changelog
All notable changes to this module are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-08-22
### Fixed
- Reconnect loop no longer busy-spins when Redis returns CLUSTERDOWN.

## [0.2.0] - 2026-07-30
### Added
- Support for Redis Cluster topology discovery.
### Changed
- Default dial timeout lowered from 5s to 2s.

[Unreleased]: https://github.com/specialistvlad/dagworker/compare/storage/redis/v0.2.1...HEAD
[0.2.1]: https://github.com/specialistvlad/dagworker/compare/storage/redis/v0.2.0...storage/redis/v0.2.1
[0.2.0]: https://github.com/specialistvlad/dagworker/releases/tag/storage/redis/v0.2.0
```

Section vocabulary is fixed: `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`;
keep an `Unreleased` section permanently at the top so `release-please` (or a human) has
somewhere to append entries between releases; compare-links at the bottom make every version
range one click away on GitHub ([keepachangelog.com/en/1.1.0](https://keepachangelog.com/en/1.1.0/)).

### 3.5 pkg.go.dev documentation quality: `doc.go` and runnable Examples

Every module's root package needs a `doc.go` (no other declarations, just the package comment)
so `go doc` and pkg.go.dev render a real overview rather than the top comment of whatever file
happens to sort first alphabetically:

```go
// Package dagworker provides a dynamic-DAG work scheduler: nodes are claimed
// under a fenced lease by external workers, acknowledged success or error,
// and automatically retried or timed out according to a per-scope retry
// policy. See the top-level README for the network-adapter daemon
// (dagworkerd) if you need non-Go workers; this package alone is a
// zero-network-dependency, in-process API.
package dagworker
```

**Runnable Examples** are the highest-leverage pkg.go.dev investment available: an
`ExampleEngine_Claim` function in `example_test.go` renders **inline, right next to the `Claim`
method's own doc** on pkg.go.dev, and — critically — is compiled and executed by `go test` if it
carries an `// Output:` comment, so it can never silently rot out of sync with the real API the
way a markdown code fence in a README can:

> "the testing framework runs an example, it captures data written to standard output and then
> compares the output against the example's 'Output:' comment. The test passes if the test's
> output matches" ([go.dev/blog/examples](https://go.dev/blog/examples))

Naming: `ExampleEngine_Claim` documents the `Claim` method on type `Engine`; a bare `Example()`
documents the whole package (goes on the package's own doc page); multiple examples for one
identifier get an `_lowercase` suffix (`ExampleEngine_Claim_withTimeout`). For a full end-to-end
flow needing more scaffolding than a doc comment wants, use a "whole file example" — a
`_test.go` file containing exactly one Example function plus supporting declarations, which
pkg.go.dev renders as one collapsible block including the supporting code (same source). At
minimum, dagworker's core module needs runnable examples for: constructing an `Engine`, claiming
a node, acking success, acking error, and subscribing to status transitions — these five double
as the smoke-test suite for the public API's ergonomics.

**Badge set** for the README header (each links to a live, re-checked status, not a static
image): CI status (GitHub Actions), `pkg.go.dev` reference link/badge, `goreportcard.com` grade,
license (MIT, static but conventional), OpenSSF Scorecard score (§3.6), and Go version support
(`go.mod`'s `go` directive, shown as a badge generated from that same file so it can't drift).

### 3.6 Supply-chain hygiene: Scorecard, best-practices badge, Dependabot, CodeQL, govulncheck

**OpenSSF Scorecard** — 18 automated checks across "holistic security practices, source code risk
assessment, and build process risk assessment," each scored 0-10
([securityscorecards.dev](https://securityscorecards.dev/)); wire it as a scheduled GitHub Action
(the [official `ossf/scorecard-action`](https://github.com/marketplace/actions/ossf-scorecard-action))
publishing results to the repo's code-scanning tab and a badge:

```yaml
# .github/workflows/scorecard.yml
name: Scorecard
on:
  branch_protection_rule:
  schedule: [{cron: '30 1 * * 6'}]
  push: {branches: [main]}
permissions: read-all
jobs:
  analysis:
    runs-on: ubuntu-latest
    permissions:
      security-events: write
      id-token: write
    steps:
      - uses: actions/checkout@v4
      - uses: ossf/scorecard-action@v2
        with:
          results_file: results.sarif
          results_format: sarif
          publish_results: true
      - uses: github/codeql-action/upload-sarif@v3
        with: {sarif_file: results.sarif}
```

**OpenSSF Best Practices badge** is a *self-certification* form (BadgeApp) against six criteria
categories — Basics, Change Control, Reporting, Quality, Security, Analysis
([bestpractices.dev/en/criteria/0](https://www.bestpractices.dev/en/criteria/0)) — cross-checked
by the community; most of the "passing" tier criteria (public git repo, unique version IDs per
release, documented bug/vulnerability reporting process, at least one automated test suite,
static analysis before releases) are already satisfied by everything else in this section, so
filling out the form is close to a checklist exercise once CI/SECURITY.md/CHANGELOG exist.

**Dependabot** (`.github/dependabot.yml`), one entry **per module directory** because Dependabot
resolves each `go.mod` independently and won't discover nested modules from a single root entry:

```yaml
version: 2
updates:
  - package-ecosystem: gomod
    directory: "/"
    schedule: {interval: weekly}
  - package-ecosystem: gomod
    directory: "/storage/redis"
    schedule: {interval: weekly}
  - package-ecosystem: gomod
    directory: "/storage/postgres"
    schedule: {interval: weekly}
  - package-ecosystem: gomod
    directory: "/storage/memcached"
    schedule: {interval: weekly}
  - package-ecosystem: gomod
    directory: "/adapters/grpc"
    schedule: {interval: weekly}
  - package-ecosystem: gomod
    directory: "/adapters/http"
    schedule: {interval: weekly}
  - package-ecosystem: gomod
    directory: "/cmd/dagworkerd"
    schedule: {interval: weekly}
  - package-ecosystem: github-actions
    directory: "/"
    schedule: {interval: weekly}
```
(config options and the `github-actions` ecosystem's own workflow-directory scanning behavior per
[docs.github.com — dependabot.yml options](https://docs.github.com/en/code-security/dependabot/dependabot-version-updates/configuration-options-for-the-dependabot.yml-file)).
Renovate is a legitimate alternative with better cross-ecosystem grouping and a `postUpdateOptions:
[gomodTidy]` that runs `go mod tidy` automatically after a bump (Dependabot cannot do this for Go
without an extra workflow step) — recommend Renovate over Dependabot specifically **because** of
the multi-module layout, where a bare version bump without `go mod tidy` across 7 `go.mod` files
is a recurring paper-cut Dependabot doesn't solve natively.

**CodeQL**: `github/codeql-action` with `languages: go`; Go's own build is fast/deterministic
enough that `build-mode: autobuild` is fine unless it fails, in which case fall back to explicit
`go build ./...` per module directory in a manual build step
([docs.github.com — CodeQL for compiled languages](https://docs.github.com/en/code-security/code-scanning/creating-an-advanced-setup-for-code-scanning/codeql-code-scanning-for-compiled-languages)).

**`govulncheck`** in CI, one invocation per module directory (its call-graph analysis is
module-scoped): "analyzes your codebase and only surfaces vulnerabilities that actually affect
you, based on which functions in your code are transitively calling vulnerable functions" —
meaningfully more precise than `go list -m all | grep CVE`-style dependency-only scanning because
a vulnerable function that's present in a dependency but never called by your code does not fire
([go.dev/blog/vuln](https://go.dev/blog/vuln)):

```bash
go install golang.org/x/vuln/cmd/govulncheck@latest
for d in . storage/redis storage/postgres storage/memcached adapters/grpc adapters/http cmd/dagworkerd; do
  (cd "$d" && govulncheck ./...)
done
```

### 3.7 SLSA provenance, cosign, and reproducible builds — for `dagworkerd` releases only

This applies to `cmd/dagworkerd`'s binary/image releases, not to the library modules (which have
no build artifact beyond source, per §3.3).

- **SLSA build levels**: L0 no guarantees; **L1** "provenance showing how it was built" but
  unsigned/untamperproofed; **L2** the same provenance **signed** by a hosted build platform;
  **L3** additionally hardens the build platform itself so "secret material used to sign the
  provenance" is inaccessible even to the build's own user-defined steps
  ([slsa.dev/spec/v1.0/levels](https://slsa.dev/spec/v1.0/levels)). **Recommendation: target
  SLSA Build L3 via the official reusable workflow** rather than hand-rolling provenance
  generation — `slsa-framework/slsa-github-generator` ships a **Go Builder** reusable workflow
  referenced as `slsa-framework/slsa-github-generator/.github/workflows/builder_go_slsa3.yml@vX.Y.Z`
  (pinned to a full semver tag) that "builds and generates provenance for Go projects" meeting
  L3's isolation requirements out of the box, verifiable downstream with `slsa-verifier`
  ([slsa-framework/slsa-github-generator](https://github.com/slsa-framework/slsa-github-generator)) —
  this is strictly less work and more trustworthy than a bespoke workflow claiming L3, since L3's
  hard part is exactly the build-isolation guarantee a hand-rolled Action runner cannot credibly
  self-attest.
- **`cosign` keyless signing**: sign the release binaries/images with GitHub Actions' own OIDC
  token via Sigstore's Fulcio, no long-lived signing key to rotate or leak — "an ephemeral
  public/private keypair is created in memory," Fulcio "issues a short-lived certificate binding
  [it] to an OpenID Connect identity," the private key is destroyed immediately after signing, and
  the event is recorded in the Rekor transparency log for anyone to audit later
  ([docs.sigstore.dev/cosign/signing/overview](https://docs.sigstore.dev/cosign/signing/overview/)).
  In CI: `cosign sign-blob --yes dagworkerd_linux_amd64` for binaries and
  `cosign sign --yes $IMAGE_DIGEST` for the container image, both inside the same
  `id-token: write`-permissioned job as the SLSA generator step. Document the verify command in
  the README's install section so a downstream user can actually exercise the trust chain:
  `cosign verify-blob --certificate-identity-regexp '...' --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' ...`.
- **Reproducible builds**: `-trimpath` (removes filesystem paths, §2.8) plus a pinned
  `go` toolchain version (the `go` and `toolchain` directives in each `go.mod`, respected by
  Go 1.21+'s toolchain-management feature) are the two levers that make two independent builds of
  the same tag byte-identical; goreleaser's default build config already sets `-trimpath` and a
  deterministic `mod_timestamp` from the tag's commit time rather than wall-clock build time,
  which is the other common source of build irreproducibility (embedded build timestamps).

### 3.8 Writing a README that converts a skimmer into a user in 30 seconds

Four widely-used Go library READMEs, live-fetched and structurally compared:

- **cobra**: opens with "Cobra is a library for creating powerful modern CLI applications" plus
  an immediate name-drop of who trusts it (Kubernetes, Hugo, GitHub CLI) *before* any
  installation instructions — social proof precedes the pitch
  ([spf13/cobra README](https://raw.githubusercontent.com/spf13/cobra/main/README.md)).
- **zap**: opens with a single-sentence tagline, "Blazing fast, structured, leveled logging in
  Go," then goes straight to **two copy-pasteable code blocks** (Logger and SugaredLogger usage)
  before any prose explanation, then backs the "blazing fast" claim with actual benchmark tables
  comparing itself against alternatives — the claim in the tagline is proven with numbers, not
  asserted ([uber-go/zap README](https://raw.githubusercontent.com/uber-go/zap/master/README.md)).
- **chi**: opens with what it is and, in the same paragraph, *why*: "especially good at helping
  you write large REST API services that are kept maintainable as your project grows" — the pitch
  targets a specific pain (maintainability at scale) rather than a generic feature list
  ([go-chi/chi README](https://raw.githubusercontent.com/go-chi/chi/master/README.md)).
- **testify**: (well-known structure, not separately re-fetched here beyond general knowledge)
  leads with an assertion-usage code block within the first screen, mirroring zap's
  "show, don't tell" ordering.

**The extracted structure, all four share this skeleton, in this order:**

1. One-sentence tagline that states *what* the library is and, ideally, *for what pain*.
2. Badges (build status, godoc, report card, license) — proof-of-diligence signals, small, no
   prose.
3. A **runnable code block** within the first screen — before "Features," before "Installation"
   prose paragraphs. The reader should see real, working code using the actual public API before
   anything else.
4. `go get` line.
5. Feature list / concept explanation, only *after* the reader has already seen it work.
6. Anything comparative (benchmarks, "why not X") — earns its place only once the reader is
   already convinced enough to be evaluating alternatives.
7. Contributing/license footer.

**Recommended README skeleton for dagworker's root module**, applying that structure to this
specific project's shape (a library with an optional daemon, which the skimmer must understand is
optional in the very first screen or they'll wrongly assume gRPC/HTTP are required):

```markdown
# dagworker

Dynamic-DAG work scheduling for Go: claim ready nodes under a fenced lease, ack success
or error, and let dagworker handle retries, timeouts, and "work is available" notifications
across every process sharing your storage backend.

[![CI](badge)](link) [![Go Reference](badge)](link) [![Go Report Card](badge)](link)
[![OpenSSF Scorecard](badge)](link) [![License: MIT](badge)](link)

```go
eng, _ := dagworker.New(ctx, memstore.New())
eng.AddNode(ctx, "scope", dagworker.Node{ID: "build", Deps: nil})

claim, _ := eng.Claim(ctx, "scope")           // blocks/polls until a node is ready
defer claim.Release(ctx)                       // safety net if we forget to Ack

// ... do the work ...
claim.Ack(ctx, dagworker.Success)
```

## Install

    go get github.com/specialistvlad/dagworker

Need a storage backend other than the built-in in-memory one? Each backend is its own module,
so you only pull in what you use:

    go get github.com/specialistvlad/dagworker/storage/redis

Need non-Go workers (Python, Node, Rust, Java)? Run the optional `dagworkerd` daemon, which
hosts a gRPC and an HTTP/JSON adapter over the same core engine — see [cmd/dagworkerd](cmd/dagworkerd/).
**The core library above has zero dependency on gRPC or net/http; you only pay for the
daemon if you run it.**

## Features
...

## How it works
...

## Benchmarks
...

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md). Please review our [Code of Conduct](CODE_OF_CONDUCT.md).

## License
MIT — see [LICENSE](LICENSE).
```

The single bolded sentence after the `dagworkerd` mention is the load-bearing one: it's the exact
place a skimmer decides whether Part 1's whole module-split investment mattered to them.

---

## Recommendations for dagworker

1. **Multi-module monorepo**, split exactly at dependency edges: core (root), `storage/redis`,
   `storage/postgres`, `storage/memcached`, `adapters/grpc`, `adapters/http`, `cmd/dagworkerd`,
   `examples` — seven-plus `go.mod` files, per §1.5, following the aws-sdk-go-v2/testcontainers-go
   pattern, explicitly rejecting gocloud.dev's one-module-for-everything shape.
2. **Independent semver per module**, not opentelemetry-go's lockstep — core and each backend/
   adapter release on their own clock, coupled only by the public interface contract's own
   version, per §1.6.
3. Commit `go.work`/`go.work.sum` at the repo root for contributor convenience; commit **zero**
   `replace` directives in any released `go.mod` — `go.work` fully replaces that older pattern.
4. Tag format `\{module-subdir\}/vX.Y.Z`; per-module CI loop (`for d in ...; do (cd $d && go
   test ./...); done`), since root-level `go test ./...` silently skips nested modules.
5. Config precedence **flag > env > file > default**; secrets as file-path flags/env, never
   value flags/env, per the OWASP-documented `/proc`/`docker inspect`/log-leakage exposure of
   plain env vars.
6. `/healthz` = process-alive only; `/readyz` = storage+eventbus reachable, gated on the gRPC
   standard health protocol too; both on a separate admin listener with `/metrics` and
   flag-gated `pprof`, never on the claim-serving port.
7. Shutdown order: fail readiness → stop accepting new claims → drain in-flight acks → **actively
   release this replica's remaining leases** (don't passively wait out the lease timeout) → close
   storage → restore signal handling. The active-release step is the one dagworker-specific
   design decision this dossier argues for over the more common "let it ride" default.
8. RED metrics for claim/ack RPCs, USE metrics for the ready-set/queue/lease-expiry sweep;
   histograms (not summaries) for every duration, since replicas run behind a load balancer and
   only histograms aggregate correctly across instances at query time.
9. Container: `CGO_ENABLED=0`, `-trimpath`, `distroless/static-debian12:nonroot` base,
   `buildx`/goreleaser for multi-arch, `-buildvcs=auto` for a real `--version` string.
10. Release automation: `release-please` in manifest mode (one component per module) for
    versioning/CHANGELOG/tagging everywhere; `goreleaser` triggered only on `dagworkerd/v*` tags
    for binary/image builds; both feeding a `cosign`-signed, SLSA L3-provenanced release via the
    official `slsa-github-generator` Go builder.
11. README structure: tagline → badges → runnable code block **before** any prose → `go get` →
    the one bolded "core has zero gRPC/HTTP dependency" sentence → features → benchmarks →
    contributing/license, mirroring cobra/zap/chi's shared skeleton.

## Open questions

- Should `storage/redis`, `storage/postgres`, `storage/memcached` share one `storage/testsuite`
  module of conformance tests (a la testcontainers-go's own test helpers), or should each backend
  vendor its own copy of the same test table? A shared module adds an eighth `go.mod` and its own
  versioning burden; not sharing risks the three backends' conformance tests drifting apart.
- Is `release-please`'s manifest mode's per-component config expressive enough to encode "core's
  breaking change must land and be tagged before any dependent module's next release," or does
  that ordering constraint need a manual gate (a required-reviewers rule on the storage/adapter
  release PRs) layered on top?
- Does the gRPC adapter's health-protocol `ServingStatus` need a third state beyond
  SERVING/NOT_SERVING for "draining — finishing in-flight but not accepting new," or is
  NOT_SERVING plus the ordering in §2.4 sufficient signal for LBs in practice?
- For the admin listener (§2.7-2.9), should `--admin-addr` default to loopback-only even inside a
  container (where loopback-only means "unreachable from outside the pod network namespace,"
  effectively disabling Prometheus scraping unless the operator explicitly rebinds) — or should
  the container image's default flip to `0.0.0.0:9091` with the loopback default reserved for
  bare-metal/systemd deployments? The two deployment targets want opposite defaults.
- Should the SLSA L3 + cosign signing pipeline (§3.7) also cover the **library modules'** tagged
  source (e.g. signing the git tag itself via `git tag -s` and a `gitsign`-based commit signing
  policy), given that most consumers of `storage/redis` etc. never download a build artifact at
  all — only `go get` the source through the module proxy, which already carries its own checksum
  database (`sum.golang.org`) guarantee?
