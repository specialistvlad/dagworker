# ADR-0026: Payloads are opaque bytes with a size cap

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/12-dag-semantics-and-state-machine.md §5.2, §5.3

## Context

Every backend this library must support — Redis, PostgreSQL `bytea`/`jsonb`, and the in-memory
store — is fundamentally a bytes store at the point payload data actually leaves or enters the
storage boundary. A generic `Storage[T any]` interface threaded through the core would force one
of two bad outcomes: either every backend adapter owns (de)serialization for an unbounded set of
caller `T`s, or the library fixes one hidden serialization format (JSON? gob? protobuf?) as a
baked-in, hard-to-change default. Neither belongs at the storage boundary — the serialization
choice is the caller's, per node, and should stay changeable without touching adapter code.

There is a second, structural reason a Go generic type parameter cannot be the primitive here:
the event stream (`Subscribe`) is a cross-process, and in the general case cross-language, boundary
— a Redis- or Postgres-backed installation's subscribers are not guaranteed to be the same Go
binary that called `AddNode`. A `Typed[T]` type parameter has no meaning on that wire at all; only
bytes survive it.

**Sizing.** No system surveyed leaves payload size unbounded, and the real-world floor a
multi-backend library must design under is set by whichever number is tightest among the systems
actually in play. AWS SQS's widely-cited message-body figure of 256 KiB
([SQS message quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html))
is small enough to comfortably fit the overwhelming majority of real work-item payloads while
staying cheap to copy on every hot-path read/write, and is independently battle-tested at a scale
this project can credibly cite. (The synthesis draft's original justification for 256 KiB also
leaned on staying under memcached's 1 MiB item ceiling; ADR-0017 removes memcached from the picture
entirely, so that half of the original argument no longer applies — the SQS-precedent half stands
on its own and is the basis kept here.) Redis (`proto-max-bulk-len`, default 512 MiB) and
PostgreSQL (`bytea`/TOAST, practically ~1 GiB) both sit far above any sane node-payload budget, so
neither constrains the choice.

**Per-scope configurability.** The owner's amendment on per-scope configuration (AMD-6) rejects a
single opinionated global default for sizing in favor of a conservative library-wide fallback with
a per-scope override, so that both "many small short-lived scopes" and "few huge long-lived
scopes" deployment shapes are served without either being privileged. Payload cap is explicitly one
of the fields that configuration surface owns; this ADR fixes what the cap *means* and how it is
enforced, not the full shape of the per-scope configuration object (a separate ADR under AMD-6
governs that object's complete field set).

## Decision

`Node.Payload` is `[]byte` (interchangeable with `json.RawMessage` at the caller's discretion) at
every layer that crosses a process or backend boundary: the storage port (`dagstore.Node.Payload`),
the public API (`dagworker.Node.Payload`), and the event envelope. The library never inspects,
parses, or validates payload *content* — only its length.

**Effective cap for a given `AddNode`/`AddNodes` call:**

```
effectiveCap = min(
    scope's configured PayloadCap, if non-zero,   // per-scope override, AMD-6
    libraryDefaultPayloadCap,                     // conservative fallback, 256 KiB
    backend's real introspectable hard limit, if the active backend exposes one,
)
```

Exceeding `effectiveCap` fails the call immediately with a typed `ErrPayloadTooLarge` — never a
silent truncation, and never a deep, backend-driver-specific error surfacing instead (a Postgres
`bytea` overflow or a Redis `proto-max-bulk-len` rejection must never be what a caller sees; the
library checks and rejects before the write is attempted).

**Large blobs are documented, not accommodated by raising the ceiling.** The recommended pattern
for genuinely payload-heavy work items (large files, big JSON blobs) is the one AWS ships for
exactly this case in SQS's own extended-client pattern: store the blob in an out-of-band object
store and put a reference (a key or URL) in `Payload`. The library documents this pattern rather
than growing its own size ceiling to accommodate the minority case.

**Generics remain an optional, in-process-only convenience, never the storage primitive:**

```go
// Typed is a thin encode/decode wrapper around []byte. It is never the type
// the Store interface, Node, or Event are defined in terms of — only a
// single-process ergonomic layer on top of them.
type Typed[T any] struct{ /* unexported */ }

func NewTyped[T any](m *Manager, scope Scope) Typed[T]
func (t Typed[T]) AddNode(ctx context.Context, id NodeID, payload T, deps ...NodeID) error
func (t Typed[T]) Claim(ctx context.Context, opts ...ClaimOption) (TypedClaim[T], error)
```

`Typed[T]`'s codec (JSON by default, swappable) is the caller's choice and lives entirely on the
caller's side of the `[]byte` boundary; it has no bearing on what a cross-process or
different-language subscriber receives from `Subscribe`.

## Consequences

### Positive
- Every backend's payload-storage implementation burden is "store and return N bytes, nothing
  else" — no backend adapter owns a serialization format or a generic-type dispatch table.
- Cross-language, cross-process subscribers get a well-defined, uninterpreted wire type with no
  Go-specific concept (a generic instantiation, an interface value) leaking into the event
  envelope.
- The per-scope override serves both AMD-6 deployment shapes: a job-queue-shaped scope of many
  small payloads can lower its cap for tighter memory; a pipeline-shaped scope with few, large,
  long-lived nodes can raise its cap up to whatever the active backend actually tolerates.

### Negative
- No compile-time payload type safety anywhere in the core engine or across the event stream — a
  bug that writes the wrong shape into `Payload` is caught only by the caller's own decode step,
  never by the type system. `Typed[T]` only helps a single-process caller that opts into it.

### Neutral
- Dropping memcached (ADR-0017) removes the external 1 MiB ceiling that partly motivated the
  original 256 KiB figure; the number is kept because the SQS precedent and the hot-path-cheapness
  argument justify it independently, not because the memcached-shaped reasoning still applies. A
  future revision is free to reconsider the library-wide fallback number now that memcached no
  longer constrains it, but that reconsideration is out of scope here.

## Alternatives considered

**Generic `Storage[T]`/`Node[T]` threaded through the core.** Rejected: forces every backend to own
(de)serialization for an unbounded `T` (breaks "a new backend is a few hundred lines," ADR-0016),
and a Go type parameter has no meaning across a cross-process or cross-language subscriber
boundary.

**Fix one serialization format (e.g., always JSON) as the library's hidden default.** Rejected: it
takes a choice away from the caller that belongs to them, per node, and would force a
proto/gob-preferring host to pay a JSON tax it never asked for.

**A single global hardcoded payload cap, no per-scope override.** Rejected by AMD-6 directly: the
owner explicitly rejected opinionated global defaults for sizing in favor of conservative
library-wide fallbacks with per-scope override, precisely so neither the job-queue-shaped nor the
pipeline-shaped deployment model is privileged.

**Silent truncation past the cap.** Rejected outright: silently discarding part of the caller's own
data is a correctness hazard categorically worse than a loud, typed, immediate
`ErrPayloadTooLarge` at the call that violated it.

## References

- AWS SQS message quotas — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
- Sibling ADRs: ADR-0016 (storage port shape), ADR-0017 (memcached rejected — removes the original
  1 MiB ceiling justification), ADR-0025 (node identity/idempotency — byte-identical payload
  comparison depends on `Payload` staying opaque bytes), ADR-0027 (public API shape, error
  taxonomy)
