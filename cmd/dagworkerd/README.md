# dagworkerd

`dagworkerd` is dagworker's optional standalone daemon: it wires one
configured storage backend to the gRPC and/or HTTP network adapters and
serves them until it is asked to stop. It is the composition root — the only
module in this repository allowed to import the core library, both adapters,
and all three storage backends — so that nothing else in the repository has
to.

If every worker in your deployment lives in the same Go process as the
`Manager`, you do not need this binary at all: import
`github.com/specialistvlad/dagworker` directly. Reach for `dagworkerd` when
workers are separate processes, written in a language other than Go, or you
simply want dagworker running as its own service.

## Quick start

```console
$ go run ./cmd/dagworkerd --store=memory --http-addr=:8080
```

This starts the HTTP/JSON adapter on `:8080` and the admin listener
(`/healthz`, `/readyz`, `/metrics`) on its default, loopback-only address.
Nothing is durable: the in-memory backend's data lives only as long as the
process does.

## Configuration

Every setting has three ways in, resolved with one precedence order:

```
flag  >  environment variable  >  config file  >  built-in default
```

A later layer in that list overrides only the settings it actually
mentions — a config file can set every field and a single `--log-level=debug`
flag on top overrides just that one, leaving the rest as the file set them.

The config file's own location follows the identical rule: `--config` beats
`DAGWORKERD_CONFIG`. The file is YAML; every key below is optional.

**Why this order, and why flags win:** environment variables are what makes
one built container image portable across environments without a rebuild
(the 12-factor argument), while a config file is the only place that
naturally expresses genuinely structured settings. Flags win over both
because they are the most explicit, most visible-in-`ps`-or-a-systemd-unit
expression of intent — the thing an operator types by hand when debugging
("just this once, verbose logging") should not require exporting an
environment variable or editing a file first.

| Flag | Env var | Config file key | Default | Meaning |
|---|---|---|---|---|
| `--config` | `DAGWORKERD_CONFIG` | — | none | Path to a YAML config file. |
| `--store` | `DAGWORKERD_STORE` | `store` | `memory` | `memory`, `redis`, or `postgres`. |
| `--redis-addr` | `DAGWORKERD_REDIS_ADDR` | `redis_addr` | none | `host:port`. Required when `--store=redis`. |
| `--redis-password-file` | `DAGWORKERD_REDIS_PASSWORD_FILE` | `redis_password_file` | none | Path to a file holding the Redis AUTH password. |
| `--postgres-dsn-file` | `DAGWORKERD_POSTGRES_DSN_FILE` | `postgres_dsn_file` | none | Path to a file holding the full PostgreSQL DSN. Required when `--store=postgres`. |
| `--grpc-addr` | `DAGWORKERD_GRPC_ADDR` | `grpc_addr` | disabled | Listen address for the gRPC adapter, e.g. `:9443`. Empty disables it. |
| `--http-addr` | `DAGWORKERD_HTTP_ADDR` | `http_addr` | disabled | Listen address for the HTTP/JSON adapter, e.g. `:8080`. Empty disables it. |
| `--admin-addr` | `DAGWORKERD_ADMIN_ADDR` | `admin_addr` | `127.0.0.1:9090` | Listen address for `/healthz`, `/readyz`, `/metrics`, and pprof. |
| `--admin-pprof` | `DAGWORKERD_ADMIN_PPROF` | `admin_pprof` | `false` | Expose `/debug/pprof/*` on the admin listener. |
| `--log-level` | `DAGWORKERD_LOG_LEVEL` | `log_level` | `info` | `debug`, `info`, `warn`, or `error`. |
| `--log-format` | `DAGWORKERD_LOG_FORMAT` | `log_format` | `json` | `json` or `text`. |
| `--shutdown-timeout` | `DAGWORKERD_SHUTDOWN_TIMEOUT` | `shutdown_timeout` | `30s` | Bound on the whole graceful-shutdown sequence. |
| `--version` | — | — | — | Print version information and exit. |

At least one of `--grpc-addr` / `--http-addr` must be set — a daemon with
neither enabled would serve nothing but its own admin endpoints.

### Secrets are file paths, never values

`--redis-password-file` and `--postgres-dsn-file` name a **file**, never the
secret itself. This is deliberate, not a style preference: a value passed as
a plain flag or environment variable is legible to `docker inspect`, to
`/proc/<pid>/environ` on any co-located process with the right privilege,
and to crash-reporting tooling that dumps environment blocks by default —
none of which is true of a file with its own restrictive permissions, mounted
read-only (a Kubernetes Secret volume, a Docker secret, or just `chmod 600`
on a local box).

`dagworkerd`'s `Config` type never holds a secret's *value* at all, only the
path — the value exists as a local variable for exactly as long as the
call that dials Redis or opens the PostgreSQL pool needs it, and the startup
log line that echoes the effective configuration logs the path, never the
content behind it.

Example:

```console
$ echo -n 'hunter2' > /run/secrets/redis_password
$ dagworkerd --store=redis --redis-addr=redis:6379 \
    --redis-password-file=/run/secrets/redis_password \
    --http-addr=:8080
```

### Example config file

```yaml
# /etc/dagworkerd/config.yaml
store: postgres
postgres_dsn_file: /run/secrets/postgres_dsn
grpc_addr: ":9443"
http_addr: ":8080"
admin_addr: "0.0.0.0:9090"
log_level: info
log_format: json
shutdown_timeout: 30s
```

```console
$ dagworkerd --config=/etc/dagworkerd/config.yaml --log-level=debug
```

The `--log-level=debug` flag above overrides just that one field; everything
else comes from the file.

## The admin listener

`--admin-addr` (default `127.0.0.1:9090`, loopback-only until an operator
explicitly opts into something else) serves three endpoints, **never** on
the same port as `--grpc-addr`/`--http-addr`: the claim-serving surface's
audience is arbitrary worker processes, potentially cross-network; the admin
surface's audience is the orchestrator's health checker and a metrics
scraper. Mixing the two would put an unauthenticated worker on the same
listener as a heap-dump trigger.

- **`GET /healthz`** — liveness only: "is this process's own HTTP goroutine
  scheduled and answering." It never touches the store, and it returns 200
  for the *entire* lifetime of the process, including every moment of a
  graceful shutdown drain. An orchestrator that restarts a container because
  `/healthz` failed should never see that happen merely because the process
  was busy finishing in-flight work.
- **`GET /readyz`** — whether this replica should keep receiving new claim
  traffic: not draining, **and** the storage backend answers a cheap
  reachability probe. It fails immediately once shutdown begins (before
  anything else happens — see below) and fails whenever the store is
  unreachable, so a rolling restart or a database blip pulls the replica out
  of a load balancer's rotation without killing it.
- **`GET /metrics`** — a small Prometheus text-exposition page: process
  liveness, whether shutdown has begun, uptime, and goroutine count. It
  cannot report the fuller RED-method set (`claims_total`,
  per-transport claim/ack duration histograms) that a request dispatcher
  ideally would, because that requires an instrumentation hook inside the
  gRPC and HTTP adapters, and this module does not modify another one to add
  it — see "Known limitations" below.
- **`/debug/pprof/*`** — only when `--admin-pprof` is set. Off by default: a
  30-second blocking CPU-profile trigger and a full heap dump are not
  something every deployment should expose merely by starting the process.

## Graceful shutdown

`dagworkerd` listens for `SIGINT` and `SIGTERM` via
[`signal.NotifyContext`](https://pkg.go.dev/os/signal#NotifyContext). The
instant either arrives, default signal handling is restored (so a second
Ctrl-C/SIGTERM force-kills the process rather than being silently absorbed),
and the following sequence runs, bounded overall by `--shutdown-timeout`:

1. **Fail `/readyz` immediately.** This is the cheapest, fastest "stop
   routing new traffic to me" signal available, and it happens before
   anything else specifically because a load balancer's health-check polling
   interval (typically several seconds) means there is an unavoidable window
   between "readiness fails" and "the load balancer actually stops sending" —
   starting that clock first is free latency-hiding for every step after it.
2. **Stop accepting new claims and drain in-flight ones**, on both enabled
   adapters concurrently. Each adapter's own `Shutdown` already does both
   halves of this — refuses new work and lets an accepted request finish —
   and, critically, unblocks a parked long-poll claim or streaming watch
   almost immediately rather than waiting out its own poll timeout, which is
   what keeps this step from taking anywhere near `--shutdown-timeout` in
   practice even though a client may have asked to wait far longer.
3. **Close the Manager, then the store.** Only after every claim-serving
   adapter has finished draining does `dagworkerd` stop the `Manager`'s own
   background maintenance loop and close the storage connection — closing
   storage while an adapter might still be mid-request would turn "finish
   this request cleanly" into "fail this request because the database
   connection just vanished."
4. **Stop the admin listener last**, so `/healthz` and `/readyz` keep
   answering the orchestrator throughout every step above. An orchestrator
   that cannot observe "alive, but draining" during this window has no way
   to distinguish a healthy drain from a hang.

**`dagworkerd` deliberately does not release any lease a worker currently
holds.** A lease lives in storage and is designed to outlive the process
that granted it — that is the entire point of a fenced, storage-resident
lease. Actively revoking every lease this replica issued on every restart
would turn a routine rolling deploy into a fleet-wide retry storm, with every
in-flight job re-attempted at once. If a worker's acknowledgement is already
in flight when shutdown begins, step 2 above lets it land normally; any
lease with no acknowledgement in flight simply rides out its own timeout and
is reclaimed by whichever replica asks for work next, exactly as it would be
if this replica had crashed outright. This is a deliberate, narrower choice
than actively reassigning still-live leases on shutdown — see the daemon's
package doc comment for the full reasoning.

## Docker

Build from the **repository root** (the module depends on its siblings by
local path via `go.work`, so the build context must include the whole
repository, not just this directory):

```console
$ docker build -f cmd/dagworkerd/Dockerfile -t dagworkerd .
```

The image is `CGO_ENABLED=0`, built with `-trimpath`, and based on
`gcr.io/distroless/static-debian12:nonroot` — fully static (no dynamic
linker is available in either `scratch` or `distroless/static`, so
`CGO_ENABLED=0` is not optional), running as the pre-defined `nonroot`
(UID 65532) user, with CA certificates already present for the moment
dagworkerd dials out to a TLS-terminated Redis or PostgreSQL. Multi-arch
images build the same way any other Go binary does:

```console
$ docker buildx build --platform linux/amd64,linux/arm64 \
    -f cmd/dagworkerd/Dockerfile -t dagworkerd --push .
```

See [`docker-compose.yml`](./docker-compose.yml) for a runnable example: the
HTTP and gRPC ports are published, the admin port deliberately is not (its
audience is the orchestrator's own network, not the public internet), and the
Redis password is mounted as a Compose secret rather than passed as an
environment value — the same rule this README's "Secrets are file paths"
section states for a bare `docker run`.

```console
$ docker compose -f cmd/dagworkerd/docker-compose.yml up --build
```

## `--version`

```console
$ dagworkerd --version
dagworkerd v1.2.3 (a1b2c3d4e5f6, go1.25.0)
```

Sourced from `runtime/debug.ReadBuildInfo`: the module version and VCS
revision Go's own toolchain stamps into the binary via `-buildvcs=auto`
(the default), so a plain `go build`/`go install` of a clean checkout already
reports something identifiable with no separate release-time ldflags step.

## Known limitations

- **`/metrics` cannot report per-request RED metrics** (claim/ack rate,
  error rate, latency histograms broken out by transport). Producing those
  needs an instrumentation hook inside the gRPC and HTTP adapter packages,
  and this daemon is not permitted to modify another module to add one; the
  metrics it does expose are what is genuinely observable from outside both
  adapters.
- **No TLS support yet** on the gRPC or HTTP listeners. In most deployments
  of this shape, TLS termination belongs to a service mesh or an L7 load
  balancer sitting in front of the daemon (see the multi-replica discussion
  in `docs/research/15-daemon-packaging-and-ops.md` Part 2 §2.9); a future
  version may add `--tls-cert-file`/`--tls-key-file` for deployments that
  need it terminated here instead.
- **No gRPC-native health-checking protocol** (`grpc.health.v1.Health`).
  The gRPC adapter's `New` does not expose a hook to register an additional
  service on its internal `*grpc.Server`, so this daemon reports readiness
  only over the admin listener's plain HTTP `/readyz`, not over the gRPC
  connection itself.
