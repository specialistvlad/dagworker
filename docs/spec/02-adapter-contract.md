# Adapter contract

**Status:** normative. **Date:** 2026-08-23.

The optional network adapters expose dagworker to workers that are not written
in Go, or not in the same process. Each is its own Go module, and the core
module has **zero** import edge to any of them — enforced by `go.mod`, not by
convention (ADR-0037).

## 1. The shape every adapter implements

So that `cmd/dagworkerd` can compose adapters without knowing anything about
them, each adapter package exports exactly this:

```go
// Server serves one protocol over one listener.
type Server struct{ /* unexported */ }

// New builds a server over a Manager. It must not start anything.
func New(m *dagworker.Manager, opts ...Option) (*Server, error)

// Serve blocks, serving until the listener fails or Shutdown is called. It
// returns nil on a clean shutdown, never http.ErrServerClosed or its gRPC
// equivalent: "I was asked to stop" is not an error the caller must special-case.
func (s *Server) Serve(ctx context.Context, lis net.Listener) error

// Shutdown stops accepting new work, lets in-flight requests finish, and
// returns when they have or when ctx expires — whichever comes first.
func (s *Server) Shutdown(ctx context.Context) error
```

`Option` is the same opaque functional-option interface the core uses
(ADR-0027).

## 2. Rules both adapters MUST follow

**The lease deadline is not the request deadline.** A claim's lease lives in
storage and outlives the RPC that granted it, the connection it arrived on, and
the daemon process itself. An adapter **MUST NOT** derive one from the other,
and **MUST NOT** pass the claim RPC's context to the subsequent
acknowledgement — reusing it silently expires the call mid-job.

**Long-polling is bounded server-side.** A claim that waits **MUST** clamp the
client's requested wait to a server maximum, and **MUST** return an empty
result rather than an error when that elapses. Having no work is ordinary.

**Every wait is jittered.** A fleet of workers that reconnects together and
polls on a fixed interval stays synchronised forever.

**Draining is prompt.** A shutdown **MUST NOT** wait out an in-flight long poll.
Every waiting handler selects on a shutdown signal as well as its own deadline.

**In-flight leases survive a restart.** They live in storage. An adapter
**MUST NOT** cancel or release them on shutdown unless explicitly configured
to — a rolling restart would otherwise revoke every lease in the fleet.

**Errors carry a machine-readable identity.** A client must be able to
distinguish "your lease was superseded" from "the database is down" without
parsing prose.

**Authorization is decided before the handler.** An adapter **MUST** expose a
`WithAuthorizer` option and **MUST** apply it ahead of every handler, so that a
method or route added later is covered without anyone remembering to add a
check to it.

The *method* check happens before routing. The optional *scope* check
(`ScopeAuthorizer`, §6) cannot: naming a scope requires the request decoded far
enough to read one. It still happens before the handler, which is the property
that matters — "before routing" was the stronger claim and it was not true of
scope-level policy. It **MUST** deny on any error the authorizer returns, including
ones outside the taxonomy — an authorizer that fails for an unanticipated
reason fails closed. It **MUST NOT** return the authorizer's own error text to
the caller, which would make every custom authorizer an oracle for whatever it
consults. An adapter built with no authorizer serves anonymously; that is
defensible only for a listener no untrusted peer can reach, and `cmd/dagworkerd`
refuses to start in any other case.

## 3. Error mapping

Both adapters map the core taxonomy identically. This table is the contract:

| Core error | gRPC code | HTTP status | Problem type slug |
|---|---|---|---|
| `ErrNotFound` | `NOT_FOUND` | 404 | `not-found` |
| `ErrIDConflict` | `ALREADY_EXISTS` | 409 | `id-conflict` |
| `ErrCycle` | `FAILED_PRECONDITION` | 409 | `cycle` |
| `ErrCrossScopeEdge` | `INVALID_ARGUMENT` | 400 | `cross-scope-edge` |
| `ErrAlreadyTerminal` | `FAILED_PRECONDITION` | 409 | `already-terminal` |
| `ErrNodeInFlight` | `FAILED_PRECONDITION` | 409 | `node-in-flight` |
| `ErrHasSuccessors` | `FAILED_PRECONDITION` | 409 | `has-successors` |
| `ErrLeaseMismatch` | `ABORTED` | 409 | `lease-superseded` |
| `ErrLeaseExpired` | `ABORTED` | 409 | `lease-expired` |
| `ErrScopeSealed` | `FAILED_PRECONDITION` | 409 | `scope-sealed` |
| `ErrPayloadTooLarge` | `INVALID_ARGUMENT` | 413 | `payload-too-large` |
| `ErrSubscriberLagged` | `ABORTED` | 409 | `subscriber-lagged` |
| `ErrCursorExpired` | `OUT_OF_RANGE` | 410 | `cursor-expired` |
| `ErrInvalidArgument` | `INVALID_ARGUMENT` | 400 | `invalid-argument` |
| `ErrUnsupported` | `UNIMPLEMENTED` | 501 | `unsupported` |
| `ErrClosed` | `UNAVAILABLE` | 503 | `shutting-down` |
| `ErrNoWork` | *not an error* — empty result | 204 | — |

`ErrLeaseMismatch` maps to `ABORTED`/409 and **MUST NOT** be presented as
retryable. Retrying it is exactly the wrong response: the work may already have
been redone by whoever holds the lease now.

## 4. Idempotency

The lease's fencing epoch is the idempotency key. An acknowledgement retried
with the same lease either lands once or is refused; there is no separate
idempotency-key header, because one would duplicate a mechanism the fencing
design already needs for correctness.

## 5. Payloads on the wire

gRPC carries `bytes`. HTTP/JSON carries base64 with an explicit
`payload_encoding` field, so a future out-of-band blob reference is an added
encoding rather than a breaking change.

## 6. Authentication

| | gRPC | HTTP/JSON |
|---|---|---|
| hook | `grpcadapter.WithAuthorizer(Authorizer)` | `httpadapter.WithAuthorizer(Authorizer)` |
| signature | `Authorize(ctx, fullMethod) error` | `Authorize(*http.Request) error` |
| where it runs | chained unary + stream interceptor, inside the error/recovery interceptor | middleware, inside recovery and logging |
| no credential | `codes.Unauthenticated` | `401` + `WWW-Authenticate: Bearer` |
| bad credential | `codes.PermissionDenied` | `403`, deliberately with no challenge |
| any other error | `codes.PermissionDenied` | `403` |
| shared-secret helper | `grpcadapter.BearerToken(...)` | `httpadapter.BearerToken(...)` |
| client side | `client.WithBearerToken` (requires transport security) | `client.WithBearerToken` |

`BearerToken` compares in constant time over a SHA-256 digest and rejects
every request when it is given no tokens: a credential set that degrades to
"allow everything" is the one outcome it must never have.

Neither adapter terminates TLS. Both serve whatever `net.Listener` they are
handed, so TLS, a unix socket, or a mesh sidecar are all deployment choices
rather than adapter features.

### 6.1 Per-scope policy

A scope is an isolation boundary for data and cost. Making it one for *access*
is optional and per adapter:

| | how a policy names the scope |
|---|---|
| HTTP | `Authorize` receives the whole `*http.Request`; `RequestScope(r)` reads it from the path |
| gRPC | implement `ScopeAuthorizer` alongside `Authorizer`; the adapter derives the scope and passes it in |

The gRPC adapter derives the scope from the request rather than from a table of
method names: a `scope` field where the request has one, and the task token for
the four worker calls that identify a node by lease. **A unary request of
neither shape is refused**, so an RPC added later that names its scope some
third way fails loudly instead of skipping the check.

For the streaming `Watch`, **every** `WatchCreateRequest` is checked, not only
the first — one stream multiplexes many watches and each names its own scope.
A message naming no scope passes through. Adding a streaming RPC therefore
means extending the adapter's `scopeOfStreamMessage`; there is no way to fail
closed there without breaking the control messages a stream legitimately
carries.

See ADR-0046.
