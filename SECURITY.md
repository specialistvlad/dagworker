# Security Policy

## Reporting a vulnerability

Please report security issues through GitHub's private vulnerability reporting:
**[Report a vulnerability](https://github.com/specialistvlad/dagworker/security/advisories/new)**.

Do not open a public issue. You should get an acknowledgement within a few days.

## Supported versions

The most recent minor release. This project is pre-1.0; there is no long-term
support branch yet.

## What is in scope

The library's own behaviour: a way to corrupt the graph, to bypass the fencing
check, to escape a scope boundary, to exceed a configured limit, or to cause
unbounded memory growth from bounded input.

## What is out of scope, by design

**Workers are trusted.** dagworker assumes workers are operated by the same
people as the Manager instances. The lease fencing token is a plain integer, so
a malicious worker can forge one and complete a node it does not hold, or replay
an old acknowledgement. It cannot corrupt the graph's structure, cross a scope
boundary, or exceed the payload cap.

This is a documented limitation rather than an oversight — see
[ADR-0035](docs/adr/0035-worker-trust-model-cooperative-workers-and-a-plain-integer-fencing-token.md),
which also records exactly what would change if untrusted workers became a
target: an HMAC over scope, node, epoch and deadline, plus key rotation. The
`ClaimToken` type is deliberately opaque so that change stays a backend concern
rather than a wire break.

If you are exposing the network adapters to workers you do not control, put an
authenticating proxy in front of them and treat the trust boundary as being
there, not here.

**Payloads are opaque bytes.** The library never interprets them. If your
workers deserialise a payload into something dangerous, that is your
deserialiser's threat model, not this library's.

**The daemon's admin listener** binds loopback by default and serves `pprof`
only behind an explicit flag. It carries no authorizer of its own: `/healthz`,
`/readyz` and `/metrics` are an operator surface, and the protection is that the
listener is separate and loopback-bound. Exposing it is a deployment decision,
and one that should be made with a proxy in front.

**Transport security is the deployment's.** Neither adapter terminates TLS;
both serve whatever `net.Listener` they are handed, so a TLS listener, a unix
socket, or a service mesh sidecar all work without this library modelling any
of them.

## Authentication on the network adapters

"Workers are trusted" is a statement about what an *authenticated* worker may
do.
It is not a licence to let anyone reach the port. Both adapters therefore take
an authorizer, and the daemon refuses to serve without one on a reachable
address.

- **`grpcadapter.WithAuthorizer` / `httpadapter.WithAuthorizer`** run before any
  handler, so every method and every route is covered — including ones added
  later. A rejection is a `401`/`403` (`Unauthenticated`/`PermissionDenied`),
  and an authorizer that fails for an unanticipated reason denies rather than
  allows. The authorizer sees the whole request, so a policy can key on a token,
  an mTLS peer certificate, the scope being addressed, or the method being
  called.
- **`BearerToken(...)`** is the smallest useful implementation: a shared secret,
  compared in constant time, with every holder being the same principal. It
  establishes *that a caller is one of ours*, which is what stops an
  unauthenticated peer on the same network from claiming and completing other
  people's work. It is not an authorization model, and a bearer token belongs
  behind TLS.
- **A `Server` built with no authorizer is unauthenticated**, which is
  appropriate for a loopback listener inside a trusted process and nothing else.
- **`dagworkerd` fails to start** if `--grpc-addr` or `--http-addr` names a
  non-loopback address and no `--auth-token-file` is configured. An operator who
  means it passes `--insecure`; there is no way to arrive there by accident. A
  token file with no tokens in it is also a startup failure, because an
  authorizer that accepts nothing looks exactly like a working one in the logs.

A wildcard bind (`:8080`, `0.0.0.0:8080`) counts as reachable, as does any
address the daemon cannot prove is loopback.

## A scope is not a security boundary

**A scope isolates data and cost. It does not, by itself, isolate access.** That
is unsurprising when the library is embedded — the host program decides who may
call `Claim` before it calls it (ADR-0023 §3) — and a surprise to anyone
exposing a shared `dagworkerd` to semi-trusted workers, because there the daemon
*is* that host layer.

With `BearerToken`, every holder of a token is one principal with access to
every scope; its own doc comment says so. To make a scope an access boundary,
supply a policy:

- **HTTP** — `Authorize` receives the whole request, and `RequestScope(r)`
  reads the scope from the path.
- **gRPC** — implement `ScopeAuthorizer` alongside `Authorizer`. The adapter
  derives the scope from the request (or from the task token, for the calls that
  carry one) and passes it in. Every `Watch` create is checked, not just the
  first.

Two things that remain true even with a scope policy in place:

- **`GET /v1/scopes` returns every scope name in the store** to any
  authenticated caller. Scope names can be meaningful — a tenant, a customer, a
  build — so treat them as disclosed. An authorizer can deny the endpoint
  outright; filtering it per principal is a different question (ADR-0046) and is
  not implemented. gRPC has no equivalent RPC, so this asymmetry is HTTP-only.
- **Kinds are not an access boundary either**, and deliberately so: a kind
  routes work to a pool and is chosen by the caller, so authorizing on it would
  authorize on a scheduling hint.
