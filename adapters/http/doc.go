// Package httpadapter serves dagworker over HTTP/JSON, so that a worker
// written in a language other than Go — or simply preferring curl and a JSON
// parser over a generated gRPC stub — can create nodes, claim work, and
// acknowledge it.
//
// # Shape
//
// The package exports exactly what the daemon's adapter contract requires
// (docs/spec/02-adapter-contract.md §1): [New] builds a [Server] over a
// [dagworker.Manager] without starting anything, [Server.Serve] blocks until
// the listener fails or [Server.Shutdown] is called, and Shutdown drains
// in-flight requests against a caller-supplied deadline. Nothing else is
// exported from the server side; every handler, every route, and the
// mux itself are implementation detail reachable only through that shape.
//
// # Protocol
//
// Resources (scopes, nodes, edges) follow plain CRUD verbs; the operations
// where the server, not the client, decides the outcome — claim, and every
// lease acknowledgement — are AIP-136 colon-suffixed custom methods
// (":claim", ":complete", ":fail", ":skip", ":renew"). Claim is a
// Consul-style blocking query: the client's "wait" is clamped server-side to
// 60s and jittered, and a timeout with nothing found is 204 No Content, never
// 200 with an empty array. Errors are RFC 9457 application/problem+json
// against the slug registry the adapter contract fixes. Payloads travel as
// base64 with an explicit payload_encoding field, because JSON has no native
// binary type. Listing is keyset-paginated with opaque cursors; there is no
// offset parameter anywhere in this API.
//
// The one gap between the research dossier (docs/research/14) and this
// implementation is deliberate: there is no PATCH on a node. The dossier
// assumed one, but [dagworker.Manager] exposes no primitive to mutate a
// node's priority or payload after creation — only creation, cancellation,
// and the lease lifecycle — so an HTTP PATCH here would have nothing backing
// it. See the package's delivery notes for the recommendation this gap
// implies for core.
//
// # Events
//
// GET .../events is Server-Sent Events by default, and NDJSON long-poll when
// the client asks for ?mode=poll — the same underlying subscription either
// way. The SSE "id:" field carries the library's own [dagworker.Cursor], so a
// browser's automatic Last-Event-ID reconnect is the library's resume
// protocol for free. The events handler is the one place in this server that
// clears its write deadline (via [net/http.ResponseController]) so the
// server's ordinary WriteTimeout does not truncate a long-lived stream.
//
// # Leases on the wire
//
// The core library has no separate lease-ID resource: a [dagworker.Lease] is
// just (scope, node, epoch), addressed implicitly by the URL it was claimed
// under. This adapter mints an opaque lease token — the node ID and epoch,
// packed and base64url-encoded — so the wire still gets an addressable
// "lease_id" without inventing storage the core library does not have. The
// fencing epoch inside that token is already the idempotency key for
// :complete/:fail/:skip/:renew, per the adapter contract §4, so there is no
// separate X-Fencing-Token header duplicating it.
//
// # The one bug this package exists to not have
//
// A lease's deadline lives in storage and outlives the RPC that granted it.
// Every handler here that acts on an existing lease — :complete, :fail,
// :skip, :renew — uses that HTTP request's own context, never a context
// captured from the claim call, because those are two different HTTP
// requests with two different lifetimes. The one place this is genuinely
// easy to get wrong is the reference client's auto-renew loop, which is
// exactly why it owns a background context of its own rather than reusing
// whatever context the caller passed to Claim.
package httpadapter
