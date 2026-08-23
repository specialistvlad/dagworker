# grpcadapter

The gRPC network surface for [dagworker](../../README.md): it lets a worker
written in any language with a gRPC/protobuf toolchain claim and complete
nodes from a `dagworker.Manager` running in a different process.

Read `docs/spec/02-adapter-contract.md` (normative: the shape every adapter
exports and the error-mapping table) and
`docs/research/13-grpc-worker-protocol.md` (the protocol design this module
implements) before changing anything here.

## Module boundary

This is its own Go module (`github.com/specialistvlad/dagworker/adapters/grpc`)
so that a `go.mod`-level guarantee — not just a lint convention — keeps
`google.golang.org/grpc` out of the core `dagworker` module. This module
depends on the core; the core has zero import edge back (ADR-0037). The
package is named `grpcadapter`, not `grpc`, so a file that imports both this
package and `google.golang.org/grpc` never has to disambiguate an import
alias for one of them.

## Layout

```
proto/dagworker/v1/       the wire contract: node.proto, worker_service.proto,
                          control_service.proto, watch.proto
gen/dagworker/v1/         generated Go, committed (see "Why commit codegen" below)
client/                   the reference worker SDK (Dial + Worker)
*.go                      the server: Server, the two service implementations,
                          the error-mapping interceptor, wire<->domain conversions
*_test.go                 tests over a real in-process listener (bufconn)
```

## Protocol summary

- **Dispatch is a unary long poll** (`WorkerService.ClaimNode`), exactly
  Temporal's `PollActivityTaskQueue` shape: one outstanding call per worker
  execution slot. A worker's capacity is however many concurrent `ClaimNode`
  calls it keeps open — HTTP/2's own `SETTINGS_MAX_CONCURRENT_STREAMS`
  (`grpc.MaxConcurrentStreams`, set server-side) is the credit protocol, not a
  bespoke message.
- **Heartbeat/Complete/Fail/Skip are unary RPCs** keyed by an opaque
  `task_token`, which this adapter mints from exactly the three fields a
  fenced write needs — scope, node ID, epoch (see `tasktoken.go`) — never
  from the lease's deadline or node snapshot, both of which would go stale
  the instant they were copied into a token a worker might hold for minutes.
- **`Watch` is a separate, independent bidirectional stream**, modeled on
  etcd's `Watch`: a client-assigned `watch_id` multiplexes several
  independent watches on one connection, `start_revision`/`replay` choose
  where to resume, and `compacted_revision` replaces a silently missed gap
  with an explicit resync instruction. It never carries a lease.

Both service surfaces are separate gRPC services (`WorkerService`,
`ControlService`) so that, when this is fronted by mTLS or per-RPC bearer
tokens, a worker's credential can be scoped so it structurally cannot reach a
DAG-mutation RPC — that wiring is left to the deployer (see "Not in scope"
below); this module's job is to keep the two services separable enough for
it.

## Error mapping

Every RPC funnels its error through `mapError` (`errors.go`), which is the
literal `docs/spec/02-adapter-contract.md` §3 table: `errcheck`, `staticcheck`
etc. can verify the *shape* of the code, but only `TestMapErrorTable` pins
that every core sentinel actually lands on its contracted `codes.Code`. Every
mapped status carries a `google.rpc.ErrorInfo{reason, domain: "dagworker.v1"}`
detail (AIP-193) so a client can branch on a stable machine-readable reason
instead of parsing the message string.

`ErrLeaseMismatch` maps to `ABORTED`, per the contract, and is never
special-cased as retryable: retrying it is exactly wrong, since the work may
already have been redone by whoever holds the lease now.

## The one mistake this module is built to make structurally hard

**The lease deadline lives in the server's storage. It is not this RPC's
deadline, not the connection's lifetime, and not the server process's
lifetime.** Concretely:

- The server never derives a lease's `lease_expires_at` from `ClaimNode`'s own
  context — `w.pollTimeout` only bounds how long that one call may block
  (`worker_service.go`).
- Every handler after `ClaimNode` (`ExtendLease`, `CompleteNode`, `FailNode`,
  `SkipNode`) receives its own fresh RPC context from gRPC; nothing in this
  package threads one call's context into another.
- The reference client SDK (`client/`) goes one step further: its heartbeat
  loop and its final completion report are **structurally** rooted at
  `context.Background()` with their own short timeouts, never at the
  long-poll's context or the work handler's — see `client/worker.go`'s
  `Run`, `heartbeatLoop`, and `report` doc comments for exactly why, with the
  dossier's own "WRONG"/"RIGHT" example reproduced in `client/dial.go`'s
  package doc comment.

## Server usage

```go
mgr, _ := dagworker.New(store)
srv, err := grpcadapter.New(mgr,
    grpcadapter.WithMaxConcurrentStreams(4096), // size to the fleet's real worker-slot count
)
lis, _ := net.Listen("tcp", ":9443")
if err := srv.Serve(ctx, lis); err != nil {
    log.Fatal(err)
}
```

`New` never starts anything — no goroutine, no bound port — until `Serve` is
called, per the adapter contract. `Shutdown(ctx)` drains in-flight RPCs
(including a long-poll `ClaimNode` and an open `Watch`, both of which select
on the same shutdown signal so they return promptly instead of waiting out a
poll timeout that can be minutes away) and falls back to a hard `Stop` if
`ctx` expires first.

## Client (worker) usage

```go
conn, err := client.Dial("dns:///dagworkerd.namespace.svc.cluster.local:9443", creds)
w := client.NewWorker(conn, "my-scope", client.WithKinds("gpu"))
err = w.Run(ctx, func(ctx context.Context, node *dagworkerv1.Node) client.Outcome {
    result, err := doWork(ctx, node.GetPayload())
    if err != nil {
        return client.Fail(err.Error())
    }
    return client.Complete(result)
})
```

One `Worker` == one execution slot == one outstanding `ClaimNode` call; run N
of them for N-way concurrency, matching the "capacity is concurrent
ClaimNode calls, not a field" design.

`client.Dial` defaults to `round_robin` load balancing rather than grpc-go's
`pick_first` default. `target` must therefore be a resolver scheme that can
return more than one address (`dns:///host:port` against a headless
Kubernetes `Service`, for instance) — a bare `host:port`, or a ClusterIP/VIP
that always resolves to one address, makes `round_robin` degenerate back to
`pick_first` silently. See `client/dial.go`'s doc comment and
`docs/research/13-grpc-worker-protocol.md` §15 for why a long-lived,
mostly-idle connection makes this worse than it is for ordinary short unary
RPCs: an unlucky initial placement never averages out.

## Generating the protobuf/gRPC Go code

```
make generate
```

installs `protoc-gen-go` and `protoc-gen-go-grpc` with plain `go install`
(pinned versions, matching `go.mod`), then runs `buf lint` and `buf generate`.
`buf.gen.yaml` uses `local` plugins, not BSR `remote` ones, specifically so
this works with no BSR account and no network access beyond what `go install`
already needs — a CI runner or a contributor's laptop with neither is not a
blocker.

**Why the generated Go is committed, not gitignored:** this package is a
public, cross-language wire contract, and
`go get .../adapters/grpc/gen/dagworker/v1` has to work for every downstream
Go consumer — a worker SDK author, a test harness — with zero local
`buf`/`protoc` toolchain and zero build-time codegen step to cache correctly
in CI. A proto field-numbering mistake or an accidental breaking rename is
then visible in the same PR diff as the generated Go it produces, rather than
requiring a reviewer to mentally regenerate code to see the blast radius. The
tree stays fully reproducible (`make generate` from the committed `proto/`
reproduces it byte-for-byte); committing removes the *requirement* to
regenerate before every build, not the *ability* to.

## Linting

```
make lint
```

This module carries its own `.golangci.yml` rather than inheriting
`../../.golangci.yml`. golangci-lint resolves configuration by walking up
from the working directory and stopping at the **first** file it finds — it
does not merge a child config with a parent one — so `golangci-lint run
./...` run from inside this directory uses this file. That file's own header
comment explains why one rule specifically (`depguard`'s "core-no-network")
had to be dropped rather than inherited: it bans importing
`google.golang.org/grpc`, which is this module's entire reason to exist.
Every other linter, setting, and threshold is copied over unchanged — same
strictness, minus the one rule that does not apply here.

## Testing

```
make test
```

runs the suite under `-race -shuffle=on -count=2`, per the project's testing
ADR. `server_test.go` stands up a real `grpcadapter.Server` over the
in-memory backend behind an in-process `bufconn` listener (no real port) and
covers:

- the claim → complete round trip, and a heartbeat moving the lease deadline
  forward from the current time rather than the original grant;
- the fenced stale-ack rejection: a task_token from a reclaimed lease
  surfaces as `ABORTED` on the next `CompleteNode`, never silently accepted —
  made deterministic with `dagstoretest.FakeClock` rather than racing a real
  sleep against a real lease timeout;
- a long poll on an empty scope returning an empty, successful response once
  its `poll_timeout` elapses, never an error;
- `Watch` delivering both a node-creation and a status-transition event on
  one multiplexed stream;
- graceful shutdown draining a 30-second long poll in well under a second,
  rather than waiting it out.

`client/worker_test.go` exercises the reference SDK end to end (claim →
handler → complete, and a failure path) against the same kind of harness,
independently of the server's own tests.

## Not in scope for this deliverable

The design dossier (`docs/research/13-grpc-worker-protocol.md`) also covers
mTLS/bearer-token authn+authz, OpenTelemetry stats-handler wiring, and
`protovalidate`-based request validation as field options in the `.proto`
itself. None of the three is implemented here:

- **AuthN/authZ** is a deployment-time concern (`grpcadapter.WithGRPCServerOptions`
  is the escape hatch for `grpc.Creds`/interceptors a deployer wants to add).
- **Observability** likewise composes via `WithGRPCServerOptions`
  (`grpc.StatsHandler(...)`) rather than being baked in, so a host's own
  OpenTelemetry setup is what's wired, not this package's opinion of one.
- **`protovalidate`** is skipped in favor of the validation the core
  `dagworker.Manager` already does on every argument (empty scope, malformed
  node ID, unknown enum value, negative duration all already return
  `ErrInvalidArgument`, which `mapError` turns into `INVALID_ARGUMENT`) —
  adding a second validation layer in front of one that already exists would
  duplicate it, at the cost of a `buf.build/bufbuild/protovalidate` schema
  dependency, for constraints this adapter would otherwise have to keep in
  sync with the Go-side validation by hand.

## A known limitation in the core API this module cannot fix

`FailNodeResponse.will_retry`/`next_attempt_at` are best-effort:
`dagworker.Manager.Nack` commits its retry-or-terminate decision atomically
but does not return that decision to its caller (`claim.go`'s
`Manager.complete` discards the store's `CompleteResult`). `FailNode`
(`worker_service.go`) works around this with one further, unfenced `GetNode`
read after `Nack` returns — which is not atomic with the completion, so a
client that needs the authoritative answer should treat these two fields as
an optimization and confirm via `GetNode` or `Watch` when it matters. The fix
belongs in `dagworker.Manager.Nack`'s signature, which is outside this
module.
