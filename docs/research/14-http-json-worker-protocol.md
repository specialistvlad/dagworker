# 14 — HTTP/JSON Worker Protocol for dagworker

Status: research dossier. Companion to the gRPC adapter dossier; same capability surface
(claim → lease → ack, node CRUD, edges, event subscription), reachable with `curl` and a JSON
parser. Hosted by the optional `dagworkerd` daemon; the core library imports neither
`net/http` nor gRPC.

## 0. Design stance up front

Resource-ish nouns for everything that is CRUD-shaped (scopes, nodes, edges), one deliberate
RPC-ish verb for the one operation that truly is an RPC (`claim`), ETag/If-Match for optimistic
concurrency, the lease fencing token doubles as the idempotency key for acks, RFC 9457 Problem
Details for every error, keyset pagination only, and SSE as the primary event transport with
NDJSON long-poll as the fallback for intermediaries that mangle streams. Every one of these is
argued below with a primary source, not asserted.

---

## 1. Resource design

### 1.1 Google AIP baseline

The Google API design guide (now published as the AIP series after the `cloud.google.com/apis/design`
docs redirect there) models an API as a resource hierarchy: collections contain resources of
*the same type*, and "if the request to or response from a standard method is or contains the
resource, the resource schema for that resource across all methods must be the same"
[AIP-121](https://google.aip.dev/121). Standard methods (`List`, `Get`, `Create`, `Update`,
`Delete`) are preferred; custom methods exist for "functionality that doesn't cleanly map to
standard operations" and are spelled with a colon: `POST /v1/{name=publishers/*/books/*}:archive`
[AIP-136](https://google.aip.dev/136). AIP-136 is explicit that custom-method HTTP verb choice
follows behavior, not habit: **GET** for pure retrieval, **POST** for anything with a side
effect — which settles `:claim` as POST before we even get to the argument below.

dagworker's resources map cleanly onto that model:

| Resource | Collection URL | Item URL |
|---|---|---|
| Scope | `/v1/scopes` | `/v1/scopes/{scope}` |
| Node | `/v1/scopes/{scope}/nodes` | `/v1/scopes/{scope}/nodes/{node}` |
| Edge | `/v1/scopes/{scope}/edges` | `/v1/scopes/{scope}/edges/{edge}` |
| Lease | `/v1/scopes/{scope}/leases` | `/v1/scopes/{scope}/leases/{lease}` |
| Event stream | — | `/v1/scopes/{scope}/events` (custom, stateless) |

Scopes are created implicitly by the core library (per the project's own design), so
`POST /v1/scopes` is optional sugar for pre-provisioning quota/auth, not a required step — `PUT
/v1/scopes/{scope}/nodes/{node}` with a fresh scope name just works, matching "scopes are
created implicitly" from the brief.

### 1.2 Why claim is RPC-ish, not resource-ish

Two designs were on the table:

**A — resource-ish**: `POST /v1/scopes/{scope}/leases` with a body describing worker capacity
(`{"worker_id": "...", "capabilities": ["gpu"], "max_nodes": 1}`), treating the lease as a
resource being *created*. Fully AIP-compliant `Create`.

**B — RPC-ish**: `POST /v1/scopes/{scope}/nodes:claim` (a collection-level custom method per
AIP-136's "collection-based custom method" pattern), returning zero-or-more claimed nodes with
their leases embedded.

Design B wins, and the resource framing in A is a category error once you look at what the
lease actually *is*. A lease is not a client-authored resource — the client does not choose
which node it gets, does not choose the fencing token, and cannot construct the URL of the
lease it is about to receive (that's the entire point: it's asking the server to pick one for
it). AIP-136 flags exactly this shape — "when the operation does not fit Create/Get/List/etc."
— as the custom-method case; the resource being "created" as a side effect of a claim is a
result, not an input, and `Create` semantics (client PUTs/POSTs a representation, server
echoes roughly the same shape back) don't hold. Framing B also lets one call claim *multiple*
nodes in one round trip (a batch claim is a single array-returning RPC, not N pretend resource
creations), which matters for the "1,000,000 nodes, O(1)/O(log n)" performance goal — one HTTP
round trip amortizes TLS/HTTP overhead across a batch instead of paying it per node. Nomad's
own HTTP API takes the identical position for its analogous "dequeue an evaluation" operation:
it is a `POST` to a verb-shaped path, not a resource creation, for the same reason (the caller
doesn't know what it's going to get back).

The lease *itself*, once granted, **is** addressable as a resource — `GET
/v1/scopes/{scope}/leases/{lease_id}` to inspect it, and the ack calls target
`/v1/scopes/{scope}/leases/{lease_id}:complete` / `:fail` (custom methods again, because
"completing a lease" mutates node state as a side effect, it doesn't replace the lease
representation). So the resource graph is resource-ish everywhere data has an addressable
identity the client picked or can enumerate, and RPC-ish exactly at the two verbs that hand out
or resolve unpredictable results: claim and event-stream. That split — not "REST vs RPC" as a
global choice — is the actual decision worth defending.

### 1.3 Full endpoint table

| Method & Path | Purpose | Idempotent? |
|---|---|---|
| `PUT /v1/scopes/{scope}/nodes/{node}` | Create node (client-chosen ID) | Yes (PUT is idempotent per [RFC 9110 §9.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2)) |
| `GET /v1/scopes/{scope}/nodes/{node}` | Fetch one node, returns `ETag` | Yes |
| `GET /v1/scopes/{scope}/nodes` | List/filter nodes, cursor-paginated | Yes |
| `PATCH /v1/scopes/{scope}/nodes/{node}` | Update node fields (payload, priority); requires `If-Match` | Yes with same `If-Match` |
| `DELETE /v1/scopes/{scope}/nodes/{node}` | Remove node (only if terminal) | Yes |
| `PUT /v1/scopes/{scope}/edges/{from}..{to}` | Create dependency edge | Yes |
| `DELETE /v1/scopes/{scope}/edges/{from}..{to}` | Remove edge | Yes |
| `POST /v1/scopes/{scope}/nodes:claim` | Claim up to N ready nodes (long-poll) | **No** — each call may hand out a different node; see §2 |
| `GET /v1/scopes/{scope}/leases/{lease_id}` | Inspect a held lease | Yes |
| `POST /v1/scopes/{scope}/leases/{lease_id}:complete` | Ack success | Yes, via fencing token — see §3 |
| `POST /v1/scopes/{scope}/leases/{lease_id}:fail` | Ack error | Yes, via fencing token |
| `POST /v1/scopes/{scope}/leases/{lease_id}:renew` | Extend lease before deadline | Yes, via fencing token |
| `GET /v1/scopes/{scope}/events` | Subscribe: transitions + work-available signals (SSE, see §5) | Yes (read-only) |
| `GET /v1/scopes/{scope}/events?mode=poll&cursor=...` | Fallback long-poll cursor read of the same event log | Yes |

Path segment note: `{from}..{to}` for edges is deliberately compound-but-flat rather than a
nested `/nodes/{from}/edges/{to}` — an edge is not *owned* by one endpoint (it's a many-to-many
join), and nesting under one side would make deleting from the "wrong" side awkward. This
mirrors how AIP-122 treats join/relationship resources when there's no natural parent.

### 1.4 Wire examples

**Create a node**

```http
PUT /v1/scopes/prod/nodes/render-frame-042 HTTP/1.1
Content-Type: application/json
Idempotency-Key: not-needed-PUT-is-idempotent

{
  "payload_encoding": "base64",
  "payload": "eyJmcmFtZSI6IDQyLCAic2NlbmUiOiAibG9iYnkifQ==",
  "priority": 5,
  "max_attempts": 3,
  "dependencies": ["render-frame-041"]
}
```

```http
HTTP/1.1 201 Created
ETag: "v1-8f3a9c2"
Location: /v1/scopes/prod/nodes/render-frame-042
Content-Type: application/json

{
  "name": "scopes/prod/nodes/render-frame-042",
  "status": "new",
  "priority": 5,
  "max_attempts": 3,
  "attempt": 0,
  "created_at": "2026-08-22T10:00:00Z",
  "payload_encoding": "base64",
  "payload": "eyJmcmFtZSI6IDQyLCAic2NlbmUiOiAibG9iYnkifQ=="
}
```

**Get a node**

```http
GET /v1/scopes/prod/nodes/render-frame-042 HTTP/1.1
```
```http
HTTP/1.1 200 OK
ETag: "v1-8f3a9c2"
Content-Type: application/json

{ "name": "scopes/prod/nodes/render-frame-042", "status": "new", ... }
```

---

## 2. The claim call: long-poll with a client-supplied wait

### 2.1 Why long-poll, and the canonical reference

Consul's blocking queries are the reference design: a client passes `index` (the last value it
observed) and `wait` (a client-chosen upper bound on how long the server may hold the
connection open before answering, formatted like `10s` or `5m`), the server returns as soon as
the value changes *or* the wait elapses, and the response always carries a fresh index header
to feed into the next call [Consul blocking queries](https://developer.hashicorp.com/consul/api-docs/features/blocking).
Nomad's HTTP API reuses the identical mechanism verbatim down to the header name convention
(`X-Nomad-Index` vs Consul's `X-Consul-Index`) [Nomad blocking queries](https://developer.hashicorp.com/nomad/api-docs#blocking-queries) —
which is exactly the precedent for a second system adopting the pattern without inventing a new
one, which is what we do here too.

Applied to `claim`: instead of an opaque KV index, the "index" dagworker's claim call blocks on
is "is there a ready node." The request:

```http
POST /v1/scopes/prod/nodes:claim HTTP/1.1
Content-Type: application/json

{
  "worker_id": "worker-7f2e",
  "max_nodes": 1,
  "lease_seconds": 30,
  "wait": "30s",
  "labels": ["gpu"]
}
```

Success, work was immediately available:

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "leases": [
    {
      "lease_id": "lease_01J8Z3F7QK9M2X",
      "fencing_token": "7f2e-000000000042",
      "node": { "name": "scopes/prod/nodes/render-frame-042", "status": "in_progress",
                 "attempt": 1, "payload_encoding": "base64", "payload": "eyJmcmFtZSI6NDJ9" },
      "lease_deadline": "2026-08-22T10:00:30Z"
    }
  ]
}
```

No work turned up before `wait` elapsed — this is the 204-vs-200-empty-list question, decided
below:

```http
HTTP/1.1 204 No Content
```

**204 vs 200 with `{"leases": []}`**: use 204. Consul's own doc is blunt that "the return of a
blocking request is no guarantee of a change" — timeout-with-nothing-new is a first-class,
expected outcome, not a degenerate empty case
[Consul blocking queries](https://developer.hashicorp.com/consul/api-docs/features/blocking).
A 204 lets a client's retry loop branch on status code alone without touching the body (no JSON
parse, no allocation, no risk of a client bug that treats `{"leases": null}` differently from
`{"leases": []}`), and it makes the "nothing happened, just poll again" case free to distinguish
from a real error at the transport layer, which a 200 empty array cannot do as cheaply. The one
place 200-empty-array would be defensible is if the response needed to carry metadata (e.g. a
fresh index/cursor for a client that tracks it out of the claim call itself) — but here the
resume state for "what's ready" is server-side and re-derived per call, not client-tracked, so
there is nothing to attach.

### 2.2 Server-side timeout budget

The client's `wait` is a ceiling, not a promise — the server must reserve headroom below it for
its own response-writing and any intermediary's idle-timeout, exactly as Consul caps `wait` at
10 minutes server-side regardless of what the client asks for
[Consul blocking queries](https://developer.hashicorp.com/consul/api-docs/features/blocking).
dagworkerd should:

- Clamp `wait` to `[0s, 60s]` — long-poll windows in the multi-minute range mostly buy protection
  against unusually chatty polling, but this API has SSE for that use case (§5); 60s is enough
  to collapse effective claim latency close to zero under load while staying well inside any
  default load-balancer idle timeout (many default to 60s–120s and are outside dagworkerd's
  control).
- Actually return at `wait − budget`, where `budget` (default ~500ms) is reserved for
  marshaling the response and flushing it before the *client's* deadline or an intermediary's
  timeout fires — a response that starts 50ms before a proxy kills the connection is worse than
  one that returned early.
- Never hold the connection past `context.Deadline()` from the request context, so
  `Server.Shutdown` (§9) or a client disconnect (context canceled) unblocks the handler
  immediately rather than leaking a goroutine until the full `wait` expires.

### 2.3 Jitter against thundering herd

Consul: "a small random amount of additional wait time is added to the supplied maximum `wait`
time to spread out the wake up time of any concurrent requests. This adds up to `wait / 16`
additional time"
[Consul blocking queries](https://developer.hashicorp.com/consul/api-docs/features/blocking).
The mechanism this protects against is real and specific: N workers all called `claim` with
`wait=30s` at roughly the same moment (e.g. they all started up together), all time out at
exactly T+30s with no work found, and all reconnect at the same instant — a self-inflicted
retry storm at a fixed cadence. Adding jitter *on the server's timeout*, not just the client's
backoff, decorrelates that without the client having to implement anything: dagworkerd should
add `rand(0, wait/16)` to its internal deadline the same way, so two workers that dial in
within the same millisecond time out at visibly different instants.

### 2.4 Response headers for the "index" analog

Rather than a numeric KV index, dagworker's claim readiness state is exposed as a monotonic
`work-generation` counter, echoed as a response header so a *future* v2 could support
`If-Ready-Since: <generation>` as an optional optimization (skip even asking if the client
already knows nothing changed since its last poll) — noted as an open question in §14, not
built now, since the wait-based design already gets a request answered as soon as work exists
without it.

---

## 3. Idempotency: fencing token beats a bolted-on header

### 3.1 The generic pattern

Two references converge on the same shape. Stripe's docs: "Stripe's idempotency works by
saving the resulting status code and body of the first request... Subsequent requests with the
same key return the same result, including 500 errors... We can remove keys from the system
automatically after they're at least 24 hours old... All POST requests accept idempotency
keys" [Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests). The IETF
draft generalizes this into a header, `Idempotency-Key`, syntactically an RFC 8941 Structured
Field String, where "uniqueness of the key MUST be defined by the resource owner," a retry
against a still-in-flight original returns **409 Conflict**, and reusing a key against a
*different* payload is a **422 Unprocessable Content**
[draft-ietf-httpapi-idempotency-key-header-07](https://www.ietf.org/archive/id/draft-ietf-httpapi-idempotency-key-header-07.html).
Both designs exist to solve the same problem: a client that doesn't know whether its POST
landed (timeout, dropped connection) needs to retry without double-effect, and the server needs
a client-supplied correlation key because it has no other way to recognize "this is the same
logical request" across two separate TCP connections.

### 3.2 Why the fencing token is strictly better here, argued

dagworker already hands out a fencing token with every lease — the whole point of a fencing
token in a lease-based system is that it's a value the *server* mints, monotonically
increasing (or otherwise ordered) per node, that lets any downstream (storage, in this case)
reject a stale writer even if that writer doesn't know it's stale. That's precisely what
`Idempotency-Key` is trying to bolt onto POST from the outside — a caller-recognizable handle
on "this exact logical operation" — except:

1. **It's already unique per operation by construction, with zero client cooperation
   required.** An `Idempotency-Key` is only as good as the client's discipline in generating and
   reusing it correctly across retries (Stripe recommends "V4 UUIDs, or another random string
   with enough entropy" — but a buggy client that generates a *new* key on every retry gets zero
   idempotency protection and the server can't detect the bug). The fencing token is generated
   once, server-side, at claim time, and is mechanically the *only* credential the worker has
   for acking that lease — there is no way to "forget" to reuse it because there's nothing else
   to send.
2. **It's already required for correctness, not just for retries.** Fencing tokens exist in
   this design specifically to prevent a *second* claimant of the same node (after a timeout
   reclaim) from having its late ack accidentally accepted — the classic "pause-the-worker"
   distributed-lock hazard. That mechanism (compare-and-reject on a stale token) is *exactly*
   idempotent-replay detection applied to the ack call: "have I already accepted a completion
   for this fencing token" and "is this fencing token stale relative to the current lease" are
   the same lookup. Adding a separate `Idempotency-Key` header would mean maintaining two
   independent dedup tables (one keyed by fencing token for correctness, one keyed by
   idempotency key for retry-safety) that must agree, for no additional guarantee.
3. **It has a natural, bounded lifetime already.** Stripe has to invent a policy ("purge after
   24 hours") for how long to remember idempotency keys, because a generic POST has no inherent
   expiry. A lease already has a `lease_deadline`, and once a node has moved past that lease
   (timed out and reclaimed, or completed and archived), the fencing token is dead — remembering
   "have I seen this exact fencing token complete/fail before" only needs to survive as long as
   the node's terminal-state record does, which the system already retains for status queries.
   No separate TTL policy to design or drift out of sync.
4. **It composes with the "stale writer" case for free**, which a bare idempotency key does
   not: if worker A's lease times out, node gets reclaimed by worker B, and A's *original*
   in-flight ack for the *first* fencing token finally lands — that must be rejected outright
   (410 Gone / 409, not silently replayed), not answered with a cached success. A generic
   idempotency key scoped to "the ack endpoint + worker" cannot distinguish "A is retrying its
   own still-valid claim" from "A is retrying a claim that's since been superseded by B" without
   *also* consulting the fencing token — so the token is doing the real work regardless; a
   bolted-on `Idempotency-Key` would be pure redundancy.

The one thing a generic `Idempotency-Key` buys that the fencing token doesn't is protecting
*non-lease* mutations — `PUT` node creation, `PATCH` updates — from double-submission. Those
are handled instead by PUT's native idempotency (RFC 9110 — repeating an identical PUT is
side-effect-free by definition) and by `If-Match` (§4) turning a PATCH retry into a safe no-op
or an explicit 412 the client can inspect. So the decision is: **no generic `Idempotency-Key`
header anywhere in this API** — the fencing token is the idempotency key for `:complete` /
`:fail` / `:renew`, and ETag/If-Match is the idempotency mechanism for node mutation. Adding
the IETF header on top would be two mechanisms solving the one problem this API actually has.

### 3.3 The ack call, concretely

```http
POST /v1/scopes/prod/leases/lease_01J8Z3F7QK9M2X:complete HTTP/1.1
Content-Type: application/json
X-Fencing-Token: 7f2e-000000000042

{
  "result_encoding": "base64",
  "result": "eyJvdXRwdXQiOiAib2sifQ=="
}
```

First delivery:
```http
HTTP/1.1 200 OK
Content-Type: application/json

{ "node": "scopes/prod/nodes/render-frame-042", "status": "success", "completed_at": "2026-08-22T10:00:12Z" }
```

Retried (client never saw the first response — dropped connection): the server recognizes the
fencing token has already been consumed for a `:complete` and replays the exact same
`200 OK` body byte-for-byte, per the Stripe precedent of returning "the same result, including
errors" on replay
[Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests) — this is the
one case where dagworker explicitly borrows Stripe's *replay-the-original-response* behavior,
just keyed by fencing token instead of a bespoke header.

Token stale (node already reclaimed by a different worker after this lease's deadline passed):
```http
HTTP/1.1 409 Conflict
Content-Type: application/problem+json

{
  "type": "https://dagworker.dev/problems/lease-expired",
  "title": "Lease fencing token is stale",
  "status": 409,
  "detail": "Lease lease_01J8Z3F7QK9M2X expired at 2026-08-22T10:00:30Z and node render-frame-042 was reclaimed under a newer lease.",
  "instance": "/v1/scopes/prod/leases/lease_01J8Z3F7QK9M2X:complete",
  "current_fencing_token": "8a1f-000000000043"
}
```

---

## 4. Optimistic concurrency: ETag + If-Match

RFC 9110 defines ETag as an opaque validator and specifies that `If-Match` "succeeds only when
the resource's ETag matches" a client-supplied value (or `*` for existence), used to "prevent
operations on stale resources," with precedence `If-Match` → `If-None-Match` → date-based
conditions, and a **412 Precondition Failed** when the check fails
[RFC 9110 §13](https://www.rfc-editor.org/rfc/rfc9110.html#name-conditional-requests). This is
the HTTP-native compare-and-swap primitive, and it is the correct tool specifically for
`PATCH /nodes/{node}` — a client that read a node's `ETag: "v1-8f3a9c2"`, wants to bump its
`priority`, and sends:

```http
PATCH /v1/scopes/prod/nodes/render-frame-042 HTTP/1.1
If-Match: "v1-8f3a9c2"
Content-Type: application/json

{ "priority": 9 }
```

If another writer (or the worker's own status-transition machinery) mutated the node in the
meantime, the stored ETag is now `"v2-b71c004"` and the server rejects rather than silently
overwriting:

```http
HTTP/1.1 412 Precondition Failed
Content-Type: application/problem+json

{
  "type": "https://dagworker.dev/problems/precondition-failed",
  "title": "ETag mismatch",
  "status": 412,
  "detail": "Node render-frame-042 has been modified; current ETag is \"v2-b71c004\".",
  "instance": "/v1/scopes/prod/nodes/render-frame-042"
}
```

`If-Match` is mandatory on `PATCH` (no header ⇒ 428 Precondition Required, not a silent
last-writer-wins) and optional-but-recommended on `PUT` re-creation of an existing node ID and
on `DELETE`. `GET` never needs it. The ETag value itself should be the node's internal
monotonic version counter (already required internally for status-transition ordering), not a
content hash — cheaper to compute, and it composes naturally with the same version field used
for the in-process API's compare-and-swap, so the HTTP adapter adds no new source of truth.

---

## 5. The event stream

### 5.1 Four options, compared on the actual constraints

| | Delivers server→client push | Survives HTTP/1.1 intermediaries | Resumability | Browser-native | Extra deps |
|---|---|---|---|---|---|
| **SSE** | Yes | Needs care (buffering, 6-conn limit) | Native (`Last-Event-ID`) | Yes (`EventSource`) | None — plain HTTP |
| WebSocket | Yes, bidirectional | Needs upgrade support, breaks some proxies | Must build your own | Yes | None, but a different protocol entirely |
| HTTP long-poll + cursor | Yes (polled) | Best — it's just repeated GETs | Native (cursor is the API) | No (manual fetch loop) | None |
| Chunked NDJSON | Yes | Same buffering risk as SSE, no auto-reconnect | Must build your own (no `id:` convention) | No | None |

### 5.2 SSE mechanics, from the spec

The WHATWG spec: content type `text/event-stream`, UTF-8, fields `event:` (defaults to
`"message"`), `data:` (newline-joined across repeated `data:` lines), `id:` (sets the "last
event ID," which the browser remembers), and `retry:` (reconnection delay in milliseconds);
lines starting with `:` are comments, useful for keepalive
[WHATWG SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html). On disconnect,
the `EventSource` sets `readyState = CONNECTING`, waits the reconnection time (implementation
default "a few seconds"), and **automatically sends the last remembered `id:` value back as a
`Last-Event-ID` request header** on reconnect
[WHATWG SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html) — this is *exactly*
the library's resume-cursor concept with zero application code: the server's `id:` field IS the
opaque cursor dagworker already needs for "resume where I left off," and `Last-Event-ID` IS the
resume request, for free, on every browser and every SSE client library.

```http
GET /v1/scopes/prod/events HTTP/1.1
Accept: text/event-stream
Last-Event-ID: 000000000000041
```

```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
X-Accel-Buffering: no

: heartbeat
id: 000000000000042
event: node.status
data: {"node":"scopes/prod/nodes/render-frame-042","status":"in_progress","attempt":1}

id: 000000000000043
event: work.available
data: {"scope":"prod","reason":"node_ready"}

: heartbeat
```

Two event *types* share the one stream, matching the brief's "every node status transition and
work-available signals": `event: node.status` and `event: work.available` are named SSE event
types (`EventSource.addEventListener('node.status', ...)` on the client), both sharing one
monotonic `id:` sequence so a single `Last-Event-ID` resumes both.

### 5.3 Real-world pitfalls, and mitigations

- **Proxy/CDN buffering**: nginx and many CDNs buffer upstream responses by default and will
  hold the whole SSE stream until it closes or a buffer fills, defeating the point; the
  conventional escape hatch is `X-Accel-Buffering: no` (nginx-specific) plus `Cache-Control:
  no-cache, no-transform` and `Content-Encoding` left unset (compression forces buffering).
  dagworkerd sets these unconditionally on the events endpoint and documents that any reverse
  proxy in front of it must have buffering disabled for this path.
- **HTTP/1.1's six-connections-per-origin browser limit**: "SSE suffers from a limitation to
  the maximum number of open connections... per browser... set to a very low number (6)," marked
  Won't Fix by both Chrome and Firefox, but "when using HTTP/2, the maximum number of
  simultaneous HTTP streams is negotiated... (defaults to 100)"
  [MDN, Using server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events).
  This is the strongest single argument for requiring HTTP/2 on `dagworkerd`'s public listener
  when SSE is exposed to browser-based dashboards (curl/server clients are unaffected — the
  limit is a browser connection-pool policy, not a protocol one).
- **No custom headers from browser `EventSource`**: confirmed by the same spec section — the
  browser API has no header-setting hook, so auth must travel as a query parameter or cookie for
  browser clients (`?access_token=...` or a session cookie), while server-side/CLI worker
  clients using a raw HTTP client can use `Authorization: Bearer` normally. dagworkerd supports
  both on the same endpoint (bearer header preferred; short-lived signed query token accepted as
  a fallback specifically for `EventSource`).
- **Idle intermediaries timing out the stream**: any load balancer or NAT device with an idle
  timeout (commonly 60s) will silently drop a quiet SSE connection. The `: heartbeat` comment
  line above is the standard mitigation — a comment is invisible to `EventSource`'s `onmessage`
  but resets every idle timer on the path. dagworkerd sends one every 15s.
- **`Server.WriteTimeout` silently killing the stream** — this is a Go-specific footgun covered
  in §9, not an HTTP-protocol issue, but it's the single most common reason an SSE
  implementation "works in dev, dies in prod exactly N seconds in."

### 5.4 Recommendation: SSE primary, NDJSON long-poll fallback

**Primary: SSE** at `GET /v1/scopes/{scope}/events`, `Accept: text/event-stream`. It is the only
option in the table with both native resumability *and* zero client-side protocol work — every
language has an SSE client (or `curl --no-buffer` for scripts), and the resume story maps
exactly onto the library's cursor concept as shown above.

**Fallback: NDJSON long-poll**, same underlying event log, `GET
/v1/scopes/{scope}/events?mode=poll&cursor=000000000000043&wait=25s`, reusing the exact
blocking-query mechanics from §2 (client-supplied `wait`, server clamps and jitters, 204 when
nothing new). This exists for the minority of environments where SSE genuinely cannot survive
transit — a corporate proxy that fully buffers `text/event-stream` regardless of headers, or a
worker language/runtime with no SSE client and a policy against long-lived streaming sockets.
Response body is NDJSON (`application/x-ndjson`, one JSON object per line, `\n`-terminated, no
enclosing array) per the NDJSON spec's newline-delimiting rule
[ndjson-spec](https://github.com/ndjson/ndjson-spec) — chosen over a JSON array specifically so
a client can start processing line 1 before line 50 arrives, without needing a streaming JSON
parser, which a top-level array would require.

**WebSocket is explicitly rejected as the primary channel**: dagworker's event stream is
one-directional (server → client); WebSocket's bidirectionality buys nothing here, costs an
upgrade handshake that breaks under more proxies than SSE does, and gives up SSE's automatic
reconnect-with-cursor entirely, requiring the application to hand-roll it. **Bare chunked
NDJSON as the primary is rejected too**: it has the exact same buffering exposure as SSE, but
without SSE's `id:`/`Last-Event-ID`/`retry:` conventions or `EventSource`'s free reconnect
logic — all downside, no upside, unless you're specifically avoiding `text/event-stream`
parsing quirks in a language with poor SSE library support, which is what the poll-mode fallback
is *for* instead of making it the default.

---

## 6. Errors: RFC 9457 Problem Details

RFC 9457 obsoletes RFC 7807, standardizing `application/problem+json` with members `type`
(URI, defaults `about:blank`), `title`, `status`, `detail`, `instance`, plus free-form extension
members, and establishes an IANA "HTTP Problem Types" registry under Specification-Required
policy [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457); the registry itself currently holds
only a handful of generic entries (digest-mismatch, date-not-acceptable, etc.) — no
domain-specific ones — confirming that per-API problem types are expected to live under the
API's own domain rather than in IANA's registry, which is exactly what `type:
"https://dagworker.dev/problems/lease-expired"` above does
[IANA HTTP Problem Types](https://www.iana.org/assignments/http-problem-types/http-problem-types.xhtml).

### 6.1 dagworker's problem-type registry

| `type` suffix | Status | When | Why this status, not another |
|---|---|---|---|
| `lease-expired` | 409 Conflict | Ack against a fencing token no longer current | 409, not 410: the *lease* is gone but the *node* still exists and is claimable again — 410 Gone would wrongly imply the resource itself is permanently gone. |
| `cycle-detected` | 422 Unprocessable Content | `PUT` on an edge that would create a dependency cycle | 422, not 400: the request is syntactically valid JSON with valid field values — it's semantically invalid against current graph state, which is precisely 422's carve-out from RFC 9110's more syntax-focused 400 (echoing the idempotency draft's own choice of 422 for "valid syntax, wrong meaning against server state" [draft-ietf-httpapi-idempotency-key-header-07](https://www.ietf.org/archive/id/draft-ietf-httpapi-idempotency-key-header-07.html)). Not 409 either: 409 in this table is reserved for concurrent-mutation conflicts, not static-graph-validity failures — a cycle is wrong regardless of timing or concurrent writers. |
| `node-exists` | 409 Conflict | `PUT` create on a node ID that already exists with a *different* payload | 409, not 422: this is squarely RFC 9110's own example of 409 — a conflict with the current state of the target resource, not a semantic defect in the request body. A `PUT` that exactly repeats an existing node's current representation is *not* an error at all (PUT is idempotent) and returns 200/204 instead. |
| `scope-unknown` | 404 Not Found | Any operation against a scope explicitly configured to reject implicit creation (e.g. read-only/audit scopes) | Standard 404 — no dedicated status exists for "collection doesn't exist," and inventing one would violate HTTP's own semantics for GET on a missing resource. Kept in the registry mainly so `type` gives client code a stable machine-readable branch distinct from "node not found within a scope that does exist." |
| `cursor-too-old` | 410 Gone | Pagination or event-stream cursor references a compacted/expired position | 410, not 400 or 404: the cursor *was* valid once and pointed at something real — it's specifically gone now, which is 410's textbook use case (as opposed to 404's "never existed here"), and tells the client explicitly "restart from scratch," which a generic 400 would not convey. |
| `precondition-failed` | 412 Precondition Failed | `If-Match` mismatch on PATCH/PUT/DELETE | Directly RFC 9110 §13.1.1's defined semantics — no argument needed, this is what 412 is for. |
| `precondition-required` | 428 Precondition Required | PATCH without `If-Match` | Distinguishes "you didn't even send a precondition" from "you sent one and it failed" — collapsing both into 412 would make client retry logic (re-GET and retry vs. just add a header) ambiguous. |
| `rate-limited` | 429 Too Many Requests | Claim or write rate exceeds per-scope/per-key quota | Standard; `Retry-After` header included per RFC 9110. |

422 vs 409 vs 412, argued as a set: 412 is reserved *exclusively* for the ETag/If-Match
mechanism (§4) — it fires only when a client explicitly declared an expectation via a
conditional header and that expectation was false, which is a narrower and more mechanical
check than "is this write valid." 409 is for conflicts against *current stored state* that
would be resolved simply by the client re-reading and retrying with fresh data (another node
already has this ID; this fencing token has been superseded). 422 is for requests that are
*never* going to succeed no matter how many times you retry them with the same body against any
server state — a cycle-creating edge is invalid on every scope, at every point in time, given
that exact pair of nodes; re-fetching and retrying identically changes nothing. That
distinction is what should drive a client's retry policy: retry-after-refresh for 409, do not
retry unmodified for 422, retry with the header for 412.

---

## 7. Pagination

`GET /v1/scopes/{scope}/nodes` never accepts `offset`. OFFSET pagination requires the database
"to fetch and discard N rows before returning results" with "no context about which rows were
already seen" — cost scales with page depth, O(n) per page, and concurrent inserts produce
duplicate or skipped rows across pages
[Use The Index, Luke — No Offset](https://use-the-index-luke.com/no-offset). That is a direct
violation of the project's own O(1)/O(log n) mandate at 1M nodes: an offset-1,000,000 request
against a backing store would cost proportionally to the offset on every storage backend in
scope (in-memory, Redis, Postgres, memcached), not just Postgres.

Keyset (seek) pagination instead filters `WHERE (priority, created_at, id) < (last_seen...)
ORDER BY ... LIMIT n` — the database jumps straight to the right index position, O(log n) to
seek plus O(page size) to read, independent of how deep the client has paged
[Use The Index, Luke — No Offset](https://use-the-index-luke.com/no-offset).

```http
GET /v1/scopes/prod/nodes?status=new&page_size=100 HTTP/1.1
```
```json
{
  "nodes": [ { "name": "scopes/prod/nodes/render-frame-001", "status": "new", ... }, "...98 more..." ],
  "next_page_token": "eyJwIjo1LCJjIjoiMjAyNi0wOC0yMlQxMDowMDowMFoiLCJpIjoicmVuZGVyLWZyYW1lLTEwMCJ9"
}
```

`next_page_token` is opaque base64url of a small JSON tuple (`{sort key columns..., id}`) —
opaque *by contract* (clients must not decode or construct it) even though it's not encrypted,
so the encoding scheme underneath can change between releases without breaking the API
contract, and so a client can't be tempted to hand-construct a token that skips validation. The
last page returns no `next_page_token` field at all (its absence is the end-of-list signal, not
an empty string, which is more defensible than a magic sentinel value under JSON's optional-key
semantics). Filtering (`status=`, `priority_gte=`) and sort order are fixed to whatever backs
the compound index for that filter combination — the API does not accept an arbitrary
client-chosen `order_by`, because an unindexed sort silently degrades every backend to a full
scan, which is precisely the trap this section exists to close off.

---

## 8. Node payloads: opaque bytes in JSON

Two options: base64-inline with a declared encoding field, or a separate binary sub-resource
(`GET /nodes/{id}/payload` returning `application/octet-stream`). **Decision: base64-inline**,
with an explicit `payload_encoding` field (currently only `"base64"`, versioned so a future
`"base64+zstd"` or similar can be added without a breaking change) — as shown in every example
above (`"payload_encoding": "base64", "payload": "eyJmcmFtZSI6..."`). Reasons:

- **One round trip, atomic with node state.** A worker claiming a node needs the payload to do
  the work — splitting it into a second GET means every claim becomes two calls (or an
  `?expand=payload` query-param compromise that reintroduces the same design question one level
  down), doubling the latency-sensitive path this whole API optimizes for in §2.
- **JSON has no native binary type**, so *some* text encoding is unavoidable if the payload
  travels in the same body as the rest of the node's metadata; base64 (RFC 4648) is the
  universal, dependency-free choice every JSON library already round-trips correctly.
- **The cost is bounded and known**: base64 inflates by exactly 4/3. For dagworker's target
  shape — small task descriptors, not multi-megabyte blobs — that overhead is negligible against
  the win of atomicity. If a future use case needs large binary payloads, the `payload_encoding`
  field is exactly the escape hatch: introduce `"encoding": "ref"` with a `payload_ref` URI
  pointing at a separate blob store, *without* changing the shape of every other endpoint, since
  the field already declares "how do I interpret this."
- A dedicated binary sub-resource is the right call when payloads are routinely large (video,
  model weights) — genuinely not this system's stated shape (task descriptors and small
  results), so the extra endpoint and the extra round trip it forces on the hot claim path buy
  nothing today.

---

## 9. OpenAPI: spec-first vs code-first, and the grpc-gateway question

### 9.1 Spec-first tools for Go

**oapi-codegen** consumes an OpenAPI 3.0/3.1 document and emits Go types, a router-agnostic
server interface (chi/echo/gin/stdlib `net/http` targets), a "strict server" variant that
handles marshaling/validation so handlers work with typed request/response structs directly,
and a client — explicitly a "spec-first" workflow where "the API contract precedes
implementation" [oapi-codegen](https://github.com/oapi-codegen/oapi-codegen).

**ogen** is a more aggressively performance-oriented alternative: code-generated JSON encoding
via `go-faster/jx` instead of `encoding/json` reflection, "no reflection on the hot path," a
generated static radix router for dispatch, sum types for `oneOf`, pointer-free
optional/nullable wrappers (`OptT`, `NilT`), and built-in OpenTelemetry instrumentation
[ogen.dev](https://ogen.dev/).

### 9.2 The grpc-gateway question, argued explicitly

grpc-gateway generates an HTTP/JSON reverse proxy directly from `google.api.http`-annotated
proto services — same source of truth as the gRPC adapter, automatically. Attractive on paper
for exactly the "one source of truth" reason the assignment raises. But three concrete
frictions make it the wrong tool for *this* API, not a style preference:

1. **Streaming degrades to NDJSON, not real SSE.** grpc-gateway explicitly does not plan "true
   bi-directional streaming" over HTTP and maps server-streaming RPCs to newline-delimited JSON
   chunks [grpc-ecosystem/grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway). That
   forfeits everything §5 argues for — the `id:`/`Last-Event-ID` resume contract, heartbeat
   comments, `EventSource` browser support — none of which grpc-gateway's generated transcoding
   can produce, because it isn't emitting `text/event-stream` at all. The events endpoint would
   have to be hand-written outside the generated gateway regardless, which already breaks the
   "one source of truth" promise for the one endpoint where this dossier's design differs most
   from a naive REST mapping.
2. **The wire shape is protobuf's, wearing a REST costume.** protojson defaults to snake_case
   field names carried over from the `.proto` field names, and duplicate path-parameter
   collisions get mechanically renamed with a `_1` suffix
   [grpc-ecosystem/grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway) — both are
   symptoms of the generator optimizing for round-tripping gRPC semantics, not for producing the
   URL/JSON shapes a human reaching for `curl` would expect, which is this adapter's entire
   audience per the assignment ("clients that just want curl and a JSON parser").
3. **Dependency weight the daemon-optionality goal explicitly cares about.** grpc-gateway pulls
   `google.golang.org/grpc`, `google.golang.org/protobuf`, and its own runtime `ServeMux` into
   the HTTP adapter's dependency graph even for consumers who want *only* HTTP/JSON and never
   touch gRPC [grpc-ecosystem/grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway). The
   brief requires the HTTP adapter to not force gRPC on someone who only wants curl — deriving
   the HTTP surface from the gRPC protos via grpc-gateway does exactly that.

**Decision: hand-write the HTTP/JSON API against an OpenAPI 3.1 document authored directly
(spec-first), generated into Go with oapi-codegen's strict-server mode** (not ogen — ogen's
performance ceiling matters more for a proxy fronting thousands of RPS of tiny CRUD calls than
for an API whose two hot paths, claim and the event stream, are dominated by long-poll wait time
and streaming I/O respectively, not JSON marshal throughput; oapi-codegen's broader
router-target flexibility — plain stdlib `net/http`, matching §10's routing choice — is the
better fit and keeps the generated code closer to what a maintainer hand-reading it expects).
The OpenAPI document becomes the actual contract co-owned with the `.proto` files (kept
consistent by a lint/CI check comparing field sets, not by mechanical derivation), giving up
automatic sync in exchange for an idiomatic surface and a genuinely optional dependency edge.

---

## 10. AuthN/Z, rate limiting, size limits, timeouts

- **AuthN**: `Authorization: Bearer <token>` for all non-SSE calls; SSE additionally accepts a
  short-lived signed token in `?access_token=` specifically because "the specification does not
  permit setting custom request headers from EventSource in browser contexts"
  [WHATWG SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html) (§5.3).
- **AuthZ**: scope-level bearer scopes (`scope:prod:claim`, `scope:prod:read`) — kept
  deliberately coarse; per-node ACLs are out of scope for v1.
- **Rate limiting**: per-token token-bucket, `429` + `Retry-After` + RFC 9457 body
  (`rate-limited` from §6.1); Consul's own guidance against fixed-interval client sleeps in
  favor of token-bucket-style client backoff
  [Consul blocking queries](https://developer.hashicorp.com/consul/api-docs/features/blocking)
  applies symmetrically to how the *server* should throttle, not just how clients should retry.
- **Request size limits**: `http.MaxBytesReader` wrapping every request body, small default
  (e.g. 1MiB) given payloads are meant to be small per §8's own reasoning; a distinct, larger
  limit for the (rare, bulk-import) endpoints that legitimately need one.
- **Timeouts — the classic Go footgun, spelled out**: `Server.ReadHeaderTimeout` protects
  against slow-header (Slowloris-style) clients without touching body-read time;
  `Server.WriteTimeout` "covers the time from the end of the request header read to the end of
  the response write," and critically, over HTTPS the deadline is armed at `Accept` time, before
  the TLS handshake and header read even happen — meaning a single fixed `WriteTimeout` sized for
  ordinary request/response calls **will silently truncate every SSE connection** the instant
  that timeout elapses, mid-stream, with no error surfaced to the handler beyond a failed write.
  The documented fix is `http.ResponseController` — the events handler calls
  `rc.SetWriteDeadline(time.Time{})` (or a far-future time) immediately on entry to disable the
  inherited deadline for that one connection, and calls `rc.Flush()` after every event so data
  isn't held in a buffer waiting for more to accumulate. Every other handler keeps the server's
  default `WriteTimeout`; only the streaming path opts out, per-request, which is precisely what
  `ResponseController` exists for: "SetReadDeadline/SetWriteDeadline... override the
  `Server.ReadTimeout`/`WriteTimeout` for individual requests," plus a `Flush()` for exactly this
  push-as-you-go case [pkg.go.dev/net/http#ResponseController](https://pkg.go.dev/net/http#ResponseController).

```go
func handleEvents(w http.ResponseWriter, r *http.Request) {
    rc := http.NewResponseController(w)
    _ = rc.SetWriteDeadline(time.Time{}) // opt this connection out of Server.WriteTimeout
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("X-Accel-Buffering", "no")
    w.WriteHeader(http.StatusOK)

    heartbeat := time.NewTicker(15 * time.Second)
    defer heartbeat.Stop()
    for {
        select {
        case <-r.Context().Done(): // client gone, or Server.Shutdown closing it — see §11
            return
        case ev := <-sub.Events():
            fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", ev.Cursor, ev.Type, ev.JSON)
            _ = rc.Flush()
        case <-heartbeat.C:
            fmt.Fprint(w, ": heartbeat\n\n")
            _ = rc.Flush()
        }
    }
}
```

---

## 11. Go server implementation notes

- **Routing: stdlib `net/http.ServeMux` (Go 1.22+), not chi.** Go 1.22 added method-prefixed
  patterns (`"POST /v1/scopes/{scope}/nodes/{node}"`), single-segment wildcards `{id}`,
  multi-segment `{path...}`, an explicit trailing-slash matcher `{$}`, "most specific pattern
  wins" precedence with a registration-time panic on genuine ambiguity, and wildcard values
  retrieved via `r.PathValue("id")`
  [go.dev/blog/routing-enhancements](https://go.dev/blog/routing-enhancements) — which covers
  every routing need this API has (method + path + wildcard capture) with zero dependencies,
  directly serving the "core library must not pay for adapters" goal one level further: even the
  *adapter's own* dependency surface should stay minimal. chi remains a reasonable choice
  specifically for its middleware-chaining idioms and `Mount()`-based subrouter composition —
  "chi's middlewares are just stdlib net/http middleware handlers," "100% compatible with
  net/http" [github.com/go-chi/chi](https://github.com/go-chi/chi) — but stdlib `ServeMux` plus
  a handful of hand-written middleware-wrapping functions (`func(http.Handler) http.Handler`,
  the same signature chi uses) covers this API's actual route count without adding a
  dependency for a Go version this project already requires.
- **Graceful shutdown with open streams**: `Server.Shutdown(ctx)` stops accepting new
  connections and waits for idle ones to close, but does **not** forcibly cut active handlers —
  an open SSE stream's handler goroutine will run until the request context is canceled, which
  happens when the underlying connection closes. `Shutdown` alone can therefore hang until every
  SSE client disconnects on its own. The pattern: `Shutdown` with a bounded context
  (`context.WithTimeout`), and every streaming handler must itself watch `r.Context().Done()`
  (as in the snippet above) and return promptly — `Shutdown` canceling the base context is what
  makes `r.Context()` fire in every in-flight handler, but only if the handler is actually
  selecting on it rather than blocked on a channel read with no `ctx.Done()` case.
- **`http.ResponseController.Flush`** (used above) is the documented replacement for the old
  `http.Flusher` type-assertion dance (`w.(http.Flusher).Flush()`), and works correctly even
  when the underlying `ResponseWriter` has been wrapped by middleware, provided the middleware
  implements the optional `Unwrap() http.ResponseWriter` method that `ResponseController` walks
  through — a concrete reason any custom logging/metrics middleware in `dagworkerd` must
  implement `Unwrap`, or SSE flushing silently breaks the moment that middleware is added.

---

## Recommendations for dagworker

- Split resource design by predictability, not by a global REST-vs-RPC stance: plain
  resource CRUD (`PUT`/`GET`/`PATCH`/`DELETE`) for scopes/nodes/edges/leases-as-read-resource,
  and AIP-136 colon-suffixed custom methods (`:claim`, `:complete`, `:fail`, `:renew`) exactly
  where the server — not the client — determines the outcome.
- Implement `claim` as a Consul/Nomad-style blocking query: client-supplied `wait` (clamp to
  60s), server-side jitter of up to `wait/16`, `204 No Content` on timeout-with-nothing-found,
  `200` with an array of leases otherwise — never `200` with an empty array.
- Use the lease's fencing token as the sole idempotency mechanism for `:complete`/`:fail`, and
  do not add a generic `Idempotency-Key` header anywhere — it would duplicate a dedup mechanism
  the fencing-token design already needs for correctness (stale-writer rejection), with no
  independent benefit.
- Require `ETag`/`If-Match` on every `PATCH` (428 if missing, 412 on mismatch); use the node's
  existing internal version counter as the ETag value rather than a content hash.
- Ship SSE as the primary event transport (`id:`/`Last-Event-ID` = the library's resume cursor,
  free reconnect via `EventSource`), with heartbeat comment lines every 15s, `X-Accel-Buffering:
  no`, and require HTTP/2 on any listener serving browser dashboards to escape the six-connection
  cap; keep an NDJSON long-poll fallback on the same event log for environments that mangle
  streaming responses.
- Standardize every error on RFC 9457 `application/problem+json` with a small, explicit
  `https://dagworker.dev/problems/{slug}` registry (table in §6.1) — never a bespoke error JSON
  shape.
- Cursor/keyset-only pagination on node listing, opaque base64url tokens, no `offset` parameter
  ever exposed, and no client-selectable `order_by` beyond the columns actually indexed.
- Base64-inline payloads with an explicit `payload_encoding` field; keep the field name doing
  double duty as the escape hatch for a future out-of-band blob reference without a breaking
  change.
- Hand-write the OpenAPI 3.1 document (spec-first via oapi-codegen, `net/http` target,
  strict-server mode) rather than deriving the HTTP surface from the gRPC protos through
  grpc-gateway — the streaming/SSE mismatch alone forces hand-written code on the events
  endpoint regardless, and grpc-gateway's runtime deps contradict the "HTTP adapter must not
  drag in gRPC" requirement.
- Route with stdlib `net/http.ServeMux` (Go 1.22+ method+wildcard patterns) instead of adding
  chi as a dependency; reserve `http.ResponseController.SetWriteDeadline(time.Time{})` and
  `.Flush()` exclusively for the events handler so the server's normal `WriteTimeout` keeps
  protecting every other endpoint; make any wrapping middleware implement `Unwrap()` so
  `ResponseController` can still reach the real `ResponseWriter`.
- Wire `Server.Shutdown` to a bounded context and make every streaming handler select on
  `r.Context().Done()` explicitly — `Shutdown` alone will hang on open SSE connections
  otherwise.

## Open questions

- Should the `work-generation` counter mentioned in §2.4 ship in v1 as an `If-Ready-Since`
  optimization for claim polling, or is the blocking-query design's own efficiency (server only
  answers early when work truly appears) sufficient without it — needs a benchmark at the
  1M-node/many-idle-worker shape before deciding.
- Multi-node claim (`max_nodes > 1`) interacts with retry/idempotency in a way this dossier did
  not fully resolve: if a worker claims 3 nodes in one call and only 2 of the 3 acks land before
  a crash, is the partial-batch state recoverable cleanly, or should batched claims require
  batched acks as an atomic unit?
- Whether scope-level rate limits and per-token quotas belong in `dagworkerd` itself or should
  be explicitly out of scope and left to a fronting API gateway — the dossier assumes the former
  but the project's "optional daemon" framing might argue for keeping `dagworkerd` free of
  policy concerns entirely.
- Whether the OpenAPI document and the `.proto` definitions should be linted for field-parity in
  CI (as recommended in §9.2) or whether that check is worth the maintenance cost versus simply
  accepting the two surfaces can drift and treating that as a documented, accepted risk.
- Long-term: if a genuinely large-payload use case emerges, whether to add the `"encoding":
  "ref"` binary sub-resource escape hatch from §8 preemptively or wait for a concrete need.
