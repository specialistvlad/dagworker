---
title: Running it in production
description: Deploying dagworkerd, health versus readiness, shutdown ordering, retention, what to alert on, and how to debug a stuck graph with Manager.Inspect.
---

This page is about running dagworker as a service — either your own program
embedding the `Manager` directly, or `dagworkerd`, the optional standalone
daemon that puts the same protocol on a socket for workers that live
elsewhere.

## Deploying dagworkerd

`dagworkerd` is the composition root: the one place in the repository
allowed to import the core library, both network adapters, and all three
storage backends, so nothing else has to. If every worker in your
deployment is a goroutine in the same process as your `Manager`, you don't
need it at all — this section is for the case where workers are separate
processes, another language, or you simply want dagworker running as its
own service.

```console
$ dagworkerd --store=postgres --postgres-dsn-file=/run/secrets/postgres_dsn \
    --grpc-addr=:9443 --http-addr=:8080
```

At least one of `--grpc-addr`/`--http-addr` must be set — a daemon with
neither enabled would serve nothing but its own admin endpoints.

**Configuration resolves through one precedence order:** flag beats
environment variable beats config file beats built-in default, and a
higher-precedence layer overrides only the settings it actually mentions —
a config file can set every field, and a single `--log-level=debug` flag on
top of it changes just that one setting.

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

**Secrets are file paths, never values.** `--redis-password-file` and
`--postgres-dsn-file` each name a file, never the secret itself — a value
passed as a plain flag or environment variable is legible to `docker
inspect`, to `/proc/<pid>/environ` from any co-located process with the
right privilege, and to crash-reporting tooling that dumps environment
blocks by default. `dagworkerd`'s own config type never holds a secret's
value at all, only the path; even the startup log line that echoes the
effective configuration logs the path, never what's behind it.

## Health versus readiness

`--admin-addr` (default `127.0.0.1:9090`, loopback-only) serves
`/healthz`, `/readyz`, and `/metrics` — deliberately **never** on the same
port as the claim-serving adapters, because the two surfaces have different
audiences: workers, potentially cross-network, versus your orchestrator's
health checker and metrics scraper.

- **`/healthz` is liveness only.** "Is this process's own HTTP goroutine
  scheduled and answering." It never touches the store, and it returns 200
  for the entire lifetime of the process, including every moment of a
  graceful shutdown drain. An orchestrator that kills a container because
  `/healthz` failed should never see that happen merely because the process
  was busy finishing in-flight work — that's exactly the failure mode
  splitting liveness from readiness exists to prevent.
- **`/readyz` is whether this replica should keep receiving new claim
  traffic.** Not draining, *and* the storage backend answers a cheap
  reachability probe. It fails immediately once shutdown begins — before
  anything else happens — and fails whenever the store is unreachable, so a
  rolling restart or a database blip pulls the replica out of a load
  balancer's rotation without killing it outright.
- **`/metrics`** is a small Prometheus text page: `dagworkerd_up`,
  `dagworkerd_draining`, `dagworkerd_uptime_seconds`,
  `dagworkerd_goroutines`. It cannot report per-request rate/error/duration
  metrics broken out by transport — that needs an instrumentation hook
  inside the gRPC and HTTP adapter packages, which this daemon does not
  modify another module to add.

## Shutdown ordering

`dagworkerd` listens for `SIGINT`/`SIGTERM`. The instant either arrives, a
bounded sequence runs (bounded overall by `--shutdown-timeout`, default
30s):

1. **`/readyz` fails immediately** — the cheapest, fastest "stop routing new
   traffic here" signal available, started first specifically because a
   load balancer's own health-check polling interval means there's an
   unavoidable window between "readiness fails" and "the load balancer
   actually stops sending" — starting that clock earliest is free
   latency-hiding for every step after it.
2. **Both enabled adapters stop accepting new claims and drain in-flight
   ones concurrently**, unblocking a parked long-poll or streaming watch
   almost immediately rather than waiting out its own poll timeout.
3. **The `Manager`'s background maintenance loop stops, then the store
   closes** — only after every adapter has finished draining, because
   closing storage while a request might still be mid-flight turns "finish
   this cleanly" into "fail because the database connection just vanished."
4. **The admin listener stops last**, so `/healthz` and `/readyz` keep
   answering the orchestrator throughout every step above.

**`dagworkerd` deliberately does not release any lease a worker currently
holds.** A lease is designed to outlive the process that granted it — that's
the entire point of a fenced, storage-resident lease (see
[Concepts](/dagworker/guide/concepts/)). Actively revoking every lease on
every restart would turn a routine rolling deploy into a fleet-wide retry
storm. Any lease with an acknowledgement already in flight lands normally
during step 2; any other lease simply rides out its own deadline and gets
reclaimed by whichever replica asks for work next, exactly as it would if
this replica had crashed outright.

## Retention

Nothing is deleted by default. `TerminalRetention` (how long a terminal node
is kept before GC) and `MaxSubscriberLag` (how far a durable subscriber may
fall behind before retention advances past it anyway) both default to zero,
and zero means *disabled*, not *immediate* — a library that deletes a
caller's data by default is treated as a defect in this design, not a
convenience. If you want terminal nodes garbage-collected, set
`ScopeConfig.TerminalRetention` explicitly; until you do, every finished
node stays queryable indefinitely.

## What to alert on

- **`/readyz` failing** on a replica that isn't mid-deploy — the store is
  unreachable, and that replica should already be out of rotation.
- **A rising rate of `ErrLeaseMismatch`** from your own worker code. Every
  occurrence means a worker's lease was superseded before it finished — the
  work may already have been redone by someone else. A low background rate
  is expected (workers do occasionally stall past a lease deadline); a
  climbing one usually means lease timeouts are set too tight for the work,
  or a worker pool is starved and falling behind its own claims.
- **The doorbell-degraded log line.** A blocking `Claim` falls back to
  jittered polling automatically when a backend's doorbell (Redis pub/sub, a
  Postgres `LISTEN` connection) fails, and logs a warning when it does —
  correctness is unaffected, but it's a real signal that a connection is
  flapping and claim latency for idle workers has degraded to the poll
  interval.
- **A scope that never reports complete.** Usually means `Seal` was never
  called — see the footgun called out in [Dynamic
  graphs](/dagworker/guide/dynamic-graphs/) — rather than a graph that's
  actually stuck. Distinguish the two with `Manager.Stats`, next.

## Debugging a stuck graph

Start with `Manager.Stats` for the scope-wide picture — sealed, and how many
nodes sit in each bucket:

```go
stats, _ := m.Stats(ctx, scope)
stats.Sealed, stats.Complete
stats.Blocked, stats.Scheduled, stats.Ready, stats.InProgress
stats.Succeeded, stats.Failed
```

If `Blocked` is nonzero and staying that way, `Manager.Inspect` on a
specific node answers exactly why:

```go
insp, _ := m.Inspect(ctx, scope, id)
insp.Phase         // "blocked", "scheduled", "ready", "claimed", or "done"
insp.Waiting       // the specific predecessors still not terminal
insp.LeaseDeadline // when the current lease expires, zero if unclaimed
insp.ReadyAt       // when a scheduled retry becomes claimable, zero if not waiting on backoff
```

`insp.Waiting` is usually the answer: a node stuck `Blocked` is waiting on
exactly the predecessors that field names, and the next question is why
*those* haven't resolved — walk the same `Inspect` call on each of them.
`Inspection` carries no compatibility promise across versions and never
appears on the event stream; it exists purely for this diagnostic path, not
as something your program should build ordinary logic against.
