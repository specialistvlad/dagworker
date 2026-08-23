# ADR-0037: The gRPC, HTTP, and daemon network surface is in scope and optional

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/13-grpc-worker-protocol.md §1-6, §11, §14-16; docs/research/14-http-json-worker-protocol.md §0-6, §9.2; docs/research/15-daemon-packaging-and-ops.md Part 2; docs/research/00-synthesis.md §9 (v0.7), §11 item 4

## Context

The design synthesis's own Open Question #4 asked the owner to confirm, before the module
boundary in ADR-0031 was finalized, whether `dagworkerd` — a standalone daemon plus gRPC and
HTTP/JSON adapters over the core library — belongs in this project's scope at all, since the
original brief only asked for "a library embedded into a host program." **The owner's decision is
that it does: the network surface is in scope.** This is not a hedge or a "maybe later" — it
changes what ADR-0031's module list must contain (`adapters/grpc`, `adapters/http`,
`cmd/dagworkerd`) and it is the reason two entire dossiers (13, 14) exist as load-bearing design
documents rather than speculative appendices.

The reason a remote worker protocol is worth designing carefully, rather than bolted on, is
concrete: the framing goal is that a worker written in Python, Node, Rust, or Java can participate
in the DAG without embedding Go at all, which is impossible without a wire protocol, and the wire
protocol has real failure modes if designed carelessly. Three RPC dispatch shapes exist in
production systems that solve "tell a remote worker here is a node, you have N seconds": unary
long-poll (Temporal's `PollActivityTaskQueue`), server-streaming push (which hits a flow-control
trap — a fast producer can outrun a slow consumer with no backpressure signal short of the TCP
window itself), and bidirectional streaming with an explicit credit protocol (Envoy xDS's
nonce/ack, Kubernetes CRI's explicit rejection of a push model for exec/attach). Dossier 13's
central argument, worked through with real `.proto` and real failure modes, is that unary long-
poll plus HTTP/2's own per-connection stream-concurrency limit (`MaxConcurrentStreams`) already
*is* dagworker's credit protocol for free — a bespoke credit/ack layer on top would solve a
flow-control problem this workload does not have, at real implementation cost for every worker-
language SDK.

The HTTP surface exists for a different, non-overlapping audience: "clients that just want `curl`
and a JSON parser," per dossier 14's design stance, not a mechanically-derived transcoding of the
gRPC surface. `grpc-gateway` was evaluated directly against this requirement and rejected on three
concrete frictions (§ Decision below) — chiefly that it cannot produce real Server-Sent Events for
the event stream and that it drags `google.golang.org/grpc` into the HTTP adapter's dependency
graph for a user who wants only HTTP, defeating the entire point of `adapters/http` being its own
module (ADR-0031).

Both adapters and the daemon are, and must remain, strictly optional: nothing about their
existence may touch what a core-only embedder resolves, builds, or tests. ADR-0031 already
establishes this as a module-boundary fact (core, `adapters/grpc`, and `adapters/http` are three
separate `go.mod` files); this ADR states the corresponding hard rule explicitly and specifies
*how* it is enforced, because "core has zero import edge to either" is a claim that must be
checked by tooling, not left as a promise in a README.

## Decision

**1. The network surface ships as part of this project's scope**, per the owner's decision on
Open Question #4: `adapters/grpc`, `adapters/http`, and `cmd/dagworkerd` (ADR-0031) are real,
maintained modules, phased at v0.7 in the synthesis's delivery plan (§9), not a deferred or
optional-in-the-roadmap-sense initiative.

**2. The gRPC protocol is Temporal-style unary long-poll plus an etcd-shaped `Watch` stream** —
exactly the hybrid dossier 13 specifies, and no more than that hybrid:

- `WorkerService` exposes `ClaimNode` (long-polls server-side until a ready node exists or
  `poll_timeout` elapses — an unset `lease` in the response means "no work, re-call immediately,"
  exactly Temporal's empty-`task_token` convention, never an error), `ExtendLease` (heartbeat),
  `CompleteNode`, and `FailNode` — all four keyed by an opaque, server-issued `task_token` that
  also carries a plain `fencing_token int64` in the clear for storage backends that CAS on a
  bare integer (ADR-0006). A worker's capacity is expressed by how many concurrent `ClaimNode`
  calls it keeps in flight, never by a credit field.
- `ControlService` exposes DAG mutation/inspection (`AddNodes`, `AddEdges`, `GetNode`,
  `CancelNode`) plus `Watch(stream WatchRequest) returns (stream WatchResponse)`: client-assigned
  `watch_id` multiplexes multiple independent watches over one connection, `start_revision`
  resumes from a prior cursor, and a `compacted_revision` reject path is implemented from day one
  — etcd's own `Watch` resume contract, not a bespoke one.
- `WorkerService` and `ControlService` are separate gRPC services specifically so a worker's mTLS
  identity never needs `AddNodes`/`CancelNode` privileges by default (dossier 13 §11).
- No third, generic push-dispatch RPC exists. The proto lives at
  `proto/dagworker/v1/{node,worker_service,control_service,watch}.proto`, generated Go is
  committed to the repo (a public cross-language wire contract, not a build artifact — `go get`
  must work with zero local `buf`/`protoc` toolchain), and non-Go SDKs are produced from the same
  schema published to the Buf Schema Registry on every merge to `main`.

**3. The HTTP protocol is a Consul-style blocking query plus SSE with `Last-Event-ID` as the
resume cursor, errors as RFC 9457 `application/problem+json`:**

- CRUD-shaped resources (`PUT`/`GET`/`PATCH`+`If-Match`/`DELETE` on
  `/v1/scopes/{scope}/nodes/{node}` and `.../edges/{from}..{to}`) use standard HTTP semantics;
  ETag/`If-Match` is the optimistic-concurrency mechanism (RFC 9110 §13), mandatory on `PATCH`
  (missing header ⇒ 428, never silent last-writer-wins).
- The one genuinely RPC-shaped operation, `POST /v1/scopes/{scope}/nodes:claim`, is a collection-
  level custom method (AIP-136), not a pretend resource creation — a client cannot address the
  lease it is about to receive. It blocks server-side on a client-supplied `wait` (Consul's
  blocking-query shape), clamped to `[0s, 60s]` server-side with `rand(0, wait/16)` jitter against
  thundering-herd reconnects, and returns **204 No Content** (never `200 {"leases": []}`) when no
  work turns up before `wait` elapses.
- Ack (`:complete`/`:fail`/`:renew`) is keyed by the same fencing token as gRPC (`X-Fencing-Token`
  header), not a bolted-on `Idempotency-Key` — the fencing token already is the idempotency key
  for these calls, and a stale token returns `409 Conflict` with `application/problem+json`.
- The event stream (`GET /v1/scopes/{scope}/events`) is **SSE primary**
  (`text/event-stream`, `event: node.status` / `event: work.available`, one monotonic `id:`
  sequence, `X-Accel-Buffering: no`, a `: heartbeat` comment every 15s), because the browser
  `EventSource` API's automatic `Last-Event-ID` reconnect header **is** the resume-cursor design
  with zero client code. **NDJSON long-poll is the documented fallback**
  (`?mode=poll&cursor=...&wait=...`, reusing the same blocking-query mechanics) for intermediaries
  that fully buffer `text/event-stream` regardless of headers. WebSocket is rejected as the
  primary channel: the event stream is one-directional server→client, and WebSocket buys nothing
  while giving up `EventSource`'s free reconnect-with-cursor semantics.
- Every error is `application/problem+json` per RFC 9457, against a fixed per-API problem-type
  registry (`lease-expired` → 409, `cycle-detected` → 422, `node-exists` → 409, `scope-unknown` →
  404, `cursor-too-old` → 410, `precondition-failed` → 412, `precondition-required` → 428,
  `rate-limited` → 429) — the 409/422/412 split is deliberate and fixed: 412 only for ETag/
  If-Match failures, 409 for conflicts a client resolves by re-reading and retrying, 422 for
  requests that can never succeed against any state.
- The HTTP surface is **hand-written against a spec-first OpenAPI 3.1 document**, generated with
  `oapi-codegen`'s strict-server mode — **not** derived from the `.proto` via `grpc-gateway**.
  `grpc-gateway` was evaluated and rejected on three concrete grounds: it degrades streaming to
  plain NDJSON with no `Last-Event-ID`/heartbeat/`EventSource` support at all; its wire shape is
  protobuf's naming and path-collision conventions wearing a REST costume, not what a `curl` user
  expects; and it would pull `google.golang.org/grpc` into `adapters/http`'s dependency graph for
  a user who wants only HTTP — directly violating this ADR's own zero-import-edge principle one
  layer down.

**4. `cmd/dagworkerd`** is the composition root that hosts both adapters (ADR-0031) — config
layering, `/healthz`/`/readyz` split, graceful shutdown that actively releases the replica's
in-flight leases rather than passively waiting out their timeout, RED/USE metrics. It is the only
module permitted to import both adapter modules and a storage backend simultaneously.

**5. Hard rule, enforced structurally: the core module has zero import edge to either adapter.**
This is not a lint convention or a code-review checklist item — it is a build-time guarantee that
follows directly from ADR-0031's module split, checked two ways:

```bash
# Run inside the core module's own directory, in CI, on every PR touching core:
go mod tidy -diff        # must produce NO diff — a stray import of grpc/net/http-adapter
                          # code anywhere in core would force a `require` line `tidy` adds,
                          # failing this check in the same PR that introduced it
go list -deps ./...   | grep -E 'google\.golang\.org/grpc|dagworker/adapters/(grpc|http)' \
                       && exit 1 || exit 0
```

Because core is its own Go module (ADR-0031), it is structurally impossible for it to compile
against `adapters/grpc` or `adapters/http` without an explicit `require` line appearing in core's
own `go.mod` — a change that is visible in the diff of any PR that introduces it and that
`go mod tidy` will flag if the import is removed without cleaning up the requirement. This is a
categorically stronger guarantee than "the linter didn't complain," which is what "not enforced
by convention" means here.

## Consequences

### Positive
- A worker in any language can participate via generated stubs off the published Buf Schema
  Registry module, or via plain `curl`/any HTTP client against the OpenAPI-documented surface —
  the framing goal ("Python/Node/Rust/Java workers can participate") is actually deliverable, not
  merely theoretically possible from a `.proto` file nobody outside the Go build can consume.
- Every protocol choice reuses proven, cited precedent (Temporal, etcd, Consul, Nomad, Stripe,
  AIP-136, RFC 9457) rather than inventing new wire semantics — this is a direct reduction in
  protocol-design risk versus a bespoke design.
- HTTP/2's own flow control being the credit protocol means the worker SDK needs no bespoke
  backpressure logic to implement correctly — a real simplification for every third-party SDK
  author.
- The zero-import-edge guarantee is checkable in under a second in CI (`go mod tidy -diff`), so it
  cannot silently regress between reviews.

### Negative
- The project now maintains three independently-versioned public contracts instead of one: the Go
  API (ADR-0027), the gRPC wire schema (`buf breaking`, FILE category), and the OpenAPI/HTTP
  contract — each with its own backward-compatibility discipline and its own place a breaking
  change can leak from.
- Real operational surface is added that a pure-library project would not carry: mTLS/token
  issuance split between `WorkerService` and `ControlService`, gRPC's well-known L4-load-balancer
  connection-pinning trap (requiring the worker SDK to default to `dns:///` + `round_robin` and
  the server to set `MaxConnectionAge`), and SSE-specific reverse-proxy configuration
  (`X-Accel-Buffering: no`) that must be documented for every deployer.
- Two adapter conformance/contract test suites, two sets of API documentation, and two additional
  CI jobs are now permanent maintenance surface, on top of the storage-backend conformance suite
  (ADR-0018).

### Neutral
- The v0.7 phased-plan placement (synthesis §9) is unaffected by this decision being made now
  rather than later — the module boundary was already going to be `adapters/grpc`/`adapters/http`/
  `cmd/dagworkerd` regardless of exactly when in the roadmap they ship real functionality.
- Both adapters remain fully removable — delete a module — without touching core or each other,
  since neither imports the other (ADR-0031); this decision does not foreclose descoping the
  daemon later if the owner's priorities change.

## Alternatives considered

**Descope the network surface entirely, ship library-only.** This was the synthesis's own live
open question (§11 item 4). Rejected: the owner made an explicit decision to include it, on the
grounds that a standalone daemon is what actually makes "workers in any language" true rather than
aspirational.

**A third, generic bidirectional push-dispatch RPC** (Envoy xDS ADS's nonce/ack shape, or
Kubernetes CRI's exec/attach streaming shape) as the primary claim mechanism. Rejected per dossier
13 §2-4: both precedents solve a *convergence* or *interactive-session* problem, not a work-
distribution-with-backpressure problem; HTTP/2's stream-concurrency limit already gives dagworker
a credit protocol for free once `MaxConcurrentStreams` is set deliberately, so a bespoke
credit/ack layer on top adds implementation cost across every worker-language SDK for a guarantee
the transport already provides.

**`grpc-gateway`-derived HTTP surface**, sharing one `.proto` source of truth with gRPC. Rejected
per dossier 14 §9.2 on three concrete, sourced grounds (streaming degrades to plain NDJSON with no
`Last-Event-ID`, wire shape carries protobuf-generator naming conventions rather than idiomatic
REST JSON, and it pulls the entire gRPC dependency tree into the HTTP adapter) — each of which
directly contradicts a requirement this ADR states elsewhere (real SSE, a `curl`-first surface,
zero forced gRPC dependency for HTTP-only consumers).

**WebSocket as the primary event transport.** Rejected per dossier 14 §5.4: the stream is
one-directional server→client by design; WebSocket's bidirectionality is unused cost, it breaks
under more intermediaries than SSE, and it forfeits `EventSource`'s automatic
reconnect-with-`Last-Event-ID` behavior entirely, requiring every client to hand-roll what SSE
gives for free.

**A single shared credential/mTLS surface for `WorkerService` and `ControlService`.** Rejected per
dossier 13 §11 recommendation 7: collapsing them means a worker's identity would carry
`AddNodes`/`CancelNode` privileges from the very first deployment, cheap to avoid now and
expensive to retrofit once real worker fleets hold long-lived certificates issued under the
combined scope.

## References

- docs/research/13-grpc-worker-protocol.md §1-4 (dispatch-shape argument), §5 (full `.proto`), §6 (Watch/etcd), §11 (AuthN/Z), §14-16 (repo layout, recommendations)
- docs/research/14-http-json-worker-protocol.md §1-2 (resource design, blocking query), §3-4 (fencing token, ETag), §5 (SSE), §6 (RFC 9457), §9.2 (grpc-gateway rejection)
- docs/research/15-daemon-packaging-and-ops.md Part 2 (daemon config, health/readiness, shutdown ordering)
- [Temporal `WorkflowService` — `PollActivityTaskQueue`](https://docs.temporal.io/) — long-poll/task-token precedent
- [etcd `Watch` RPC](https://etcd.io/docs/v3.5/learning/api/#watch-api) — resume-by-revision precedent
- [Consul blocking queries](https://developer.hashicorp.com/consul/api-docs/features/blocking) — `wait`/index/jitter precedent
- [RFC 9457 — Problem Details for HTTP APIs](https://www.rfc-editor.org/rfc/rfc9457)
- [RFC 9110 §13 — HTTP Conditional Requests](https://www.rfc-editor.org/rfc/rfc9110.html#name-conditional-requests) — ETag/If-Match
- [Google AIP-136 — Custom Methods](https://google.aip.dev/136) — `:claim` verb justification
- [WHATWG Server-Sent Events spec](https://html.spec.whatwg.org/multipage/server-sent-events.html) — `Last-Event-ID`, `id:`, `retry:`
- [grpc-ecosystem/grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway) — evaluated and rejected, §9.2
- [Kubernetes blog — gRPC Load Balancing on Kubernetes without Tears](https://kubernetes.io/blog/2018/11/07/grpc-load-balancing-on-kubernetes-without-tears/) — L4-pinning trap
