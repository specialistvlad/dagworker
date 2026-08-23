# ADR-0046: Scope authorization is an optional Authorizer facet

- **Status:** Accepted
- **Date:** 2026-08-23
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** ADR-0023 §3 (clarifies it for the daemon case); `docs/spec/02-adapter-contract.md` §2
- **Backing research:** none new — this closes a gap found by reading the shipped adapters (issue #18)

## Context

A scope is an isolation boundary for **data and cost**. It was not one for
**access**, and over the network it could not be made one on one of the two
adapters.

ADR-0023 §3 puts access control outside the library:

> A host program that needs per-tenant ACLs enforces them in its own layer
> before calling `AddNode`/`Claim`.

That reasoning is sound while the host program is the thing calling `Claim`. It
stops holding the moment `cmd/dagworkerd` is the deployment, because then **the
daemon is the host layer** the ADR expects to do the enforcing — and it had
nothing to enforce with.

The two adapters were asymmetric:

| | signature | can a policy see the scope? |
|---|---|---|
| HTTP | `Authorize(r *http.Request)` | **yes** — the route is `/v1/scopes/{scope}/…`, and `RequestScope` reads it |
| gRPC | `Authorize(ctx, fullMethod)` | **no** — `fullMethod` is `/dagworker.v1.WorkerService/ClaimNode` and nothing more |

So "this fleet may claim from `tenant-a` and nowhere else" was expressible on
HTTP and **impossible on gRPC** without abandoning `WithAuthorizer` and writing
a bespoke interceptor — which means abandoning the guarantee the hook exists to
provide, that a method added to the proto later is covered without anyone
remembering.

## Decision

**`ScopeAuthorizer` is an optional facet of `Authorizer`, discovered by type
assertion**, matching how the storage port discovers `Lister`, `Doorbell` and
`Collector`.

```go
type ScopeAuthorizer interface {
	AuthorizeScope(ctx context.Context, fullMethod, scope string) error
}
```

Non-breaking: an `Authorizer` that does not implement it behaves exactly as
before.

### The scope is derived, never declared

There is no table of method names to keep in step with the proto. The scope is
taken from the request itself, in the two shapes this API actually uses:

- a **`scope` field**, which 14 of the 18 unary requests have;
- a **task token**, for the four worker calls that identify a node by lease
  rather than by name. The token is a marshalled `TaskToken` and carries the
  scope of the lease it names.

An RPC of either shape is therefore covered the day it is added, with no
registration step to forget.

**A request of neither shape is refused**, with `Internal`. This is the load-
bearing part: an RPC added later that names its scope some third way must fail
loudly rather than skip the check, because a scope authorizer that silently does
not run on one method is worse than none at all. `TestScopeOfRequest` covers
that path directly, since it is unreachable through the public surface today.

### Every `Watch` create, not the first

`Watch` is a bidirectional stream and it **multiplexes**: one stream carries
many `WatchCreateRequest`s, each naming its own scope. Authorizing the stream
once — the obvious reading, and the one the original sketch proposed — would
authorize the first scope and let every subsequent create through on its
coat-tails.

So the stream is wrapped and **each inbound message is checked as it arrives**.
A message that names no scope — `WatchCancelRequest`, which refers to a watch id
authorized when it was created — passes through unchecked.

Unlike the unary path this cannot fail closed on an unrecognised shape: a stream
legitimately carries control messages that name no scope, and refusing those
would break cancel. Adding a streaming RPC therefore means extending
`scopeOfStreamMessage`, which the adapter contract now says.

## The open questions, answered

**Does this belong in the adapters or only in `dagworkerd`?** The adapters. They
are the thing embedded by a host that has its own identities, and the daemon can
then use the same hook. Putting policy only in the daemon would leave the
embedding case with the gap this ADR closes.

**Does ADR-0023 §3 need amending or clarifying?** Clarifying. Its reasoning is
right for the embedded case and unchanged. What it did not anticipate is a
deployment where the daemon *is* the host layer, and that distinction is now
recorded here.

**Is per-scope the right granularity?** Yes, and per-kind is rejected
explicitly. A kind routes work to a pool; it is not an ownership boundary, and
a claim already carries kinds that the caller chose. Authorizing on it would
mean authorizing on a value the caller supplies for scheduling, which is a
different thing wearing the same shape.

**Should `GET /v1/scopes` be filtered?** Not here. It returns every scope name
in the store to any authenticated caller, which is a real disclosure and is now
stated in SECURITY.md. Filtering it requires the authorizer to answer "which
scopes may this principal *see*", which is a different and larger question than
"may this call proceed" — an enumeration API rather than a decision one. An
authorizer can deny the endpoint wholesale today; that is the available answer.

**Should `--auth-token-file` grow a per-scope mapping?** No. It is a shared-
secret floor, and a floor that grows a policy language stops being a floor. A
deployment that needs per-scope rules embeds the adapters and supplies a
`ScopeAuthorizer`.

## Consequences

- **A scope can now be an access boundary on both adapters**, if the deployment
  supplies a policy. It still is not one by default, and `BearerToken` remains
  what it says it is: one principal with access to everything.
- **The sentence worth having written down**, and now written down in
  SECURITY.md: *a scope is an isolation boundary for data and cost, not a
  security boundary* — unsurprising when embedded, a surprise to anyone exposing
  a shared daemon to semi-trusted workers.
- **`docs/spec/02-adapter-contract.md` §2 is weakened truthfully.**
  "Authorization is decided before routing" is right for the method check and
  wrong for the scope check, which is necessarily decided after the request is
  decoded far enough to name a scope — still before the handler, which is the
  property that matters.
- **The HTTP adapter gains nothing and needs nothing.** Its `Authorize` already
  receives the whole request; `RequestScope` is the equivalent. The two
  signatures already differ, so no single type could implement both regardless.
