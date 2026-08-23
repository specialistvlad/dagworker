# ADR-0025: Node identity is caller-supplied and insertion is idempotent

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/12-dag-semantics-and-state-machine.md §5.1

## Context

Every host program embedding dagworker is, at some point, an at-least-once caller of its own
`AddNode`: a network blip or a process restart between issuing the call and observing its result
is a normal, expected event, not an edge case. What the host does on that retry determines whether
the DAG stays correct or silently forks.

If `NodeID` were library-generated (returned from `AddNode`, in the style of River or Sidekiq's
UUID job IDs), a caller whose call times out before the response carrying the new ID arrives has no
way to decide whether to retry. Retrying risks creating a second, duplicate node under a new ID;
not retrying risks having silently dropped a node the caller believes it inserted. There is no
third option, because the caller never learned the ID the first attempt would have assigned. This
is not a corner case this project can defer — it is the ordinary shape of "call a network service
and the connection drops before the response arrives."

The industry-standard answer to exactly this problem is the idempotency-key pattern: Stripe's API
"saves the resulting status code and body of the first request made for any given idempotency
key… and errors if [incoming parameters are] not the same to prevent accidental misuse"
([Stripe — Idempotent requests](https://docs.stripe.com/api/idempotent_requests)). Applied here,
the natural idempotency key is the node's own identity, because a host program modeling "one node
per business work item" (an order ID, a request ID, a pipeline step name) almost always already
has a stable, natural key for that work item before it ever calls `AddNode`.

Every DAG-definition system surveyed independently supports caller-supplied identity for a second,
unrelated reason: Airflow, Argo, and Step Functions all use caller/definition-supplied names for
tasks/states, because a human author needs a stable name to reference the same node across DAG
edits — none of them generate task identity for the caller. River and Sidekiq, by contrast,
generate job IDs precisely *because* their jobs have no natural pre-existing external key — and,
tellingly, neither of those systems promises idempotent insert by default (a duplicate `Insert`
there creates two jobs, absent River's separate opt-in `UniqueOpts` feature). This project's brief
explicitly wants idempotent insert, which settles the comparison decisively in favor of
caller-supplied IDs.

## Decision

`NodeID` is a required, caller-supplied `string`. `AddNode`/`AddNodes` have no ID-generation code
path as their primary API — the caller always names the node.

**Idempotent insert, defined precisely:**

- Same `NodeID` + byte-identical spec (payload, dependency set, labels, kind, priority — every
  field the caller controls) ⇒ **silent no-op**, returning the existing node's current view.
- Same `NodeID` + **any** differing spec field ⇒ typed `ErrIDConflict`, never a silent overwrite
  and never a merge.
- "Byte-identical" is an exact, mechanical comparison: `bytes.Equal` on `Payload`, a
  set-equality check on the dependency `NodeID`s (order-independent — the caller's slice order is
  not semantically meaningful), a deep-equal on `Labels`, and `==` on `Kind`/`Priority`. No
  semantic or logical equality is attempted — there is no canonicalization step, because any
  partial semantic check (e.g., "canonicalize JSON, then compare") creates a footgun where a
  caller's future backward-compatible payload evolution silently starts colliding, or silently
  stops colliding, depending on canonicalization details nobody audited. `[]byte` in, `[]byte`
  compared, no interpretation (see ADR-0026).

```go
// AddNode is idempotent, keyed on the caller-supplied id (12 §5.1).
func (m *Manager) AddNode(ctx context.Context, scope Scope, id NodeID, payload []byte, opts ...NodeOption) error {
	existing, found, err := m.lookup(ctx, scope, id)
	if err != nil {
		return err
	}
	spec := buildSpec(payload, opts)
	if found {
		if !specEqual(existing.Spec, spec) {
			return &IDConflictError{Scope: scope, ID: id} // wraps ErrIDConflict
		}
		return nil // idempotent no-op
	}
	return m.insert(ctx, scope, id, spec)
}
```

A convenience helper, `GenerateID() NodeID` (a ULID, chosen for lexical sortability, unrelated to
idempotency), is offered for callers with genuinely no natural key. Using it is an explicit,
documented trade: the caller forfeits idempotent-retry safety unless it persists the generated ID
itself across its own retries — exactly the same responsibility a UUID-keyed system already places
on any caller that wants idempotency without a natural key. `GenerateID` is never the default path
and `AddNode` never calls it implicitly.

`AddNodes` (the atomic batch insert) applies the identical per-node idempotency rule inside its own
all-or-nothing batch semantics — a batch containing one conflicting `NodeID` fails the whole batch
with `ErrIDConflict` naming the offending ID, not a partial insert of the non-conflicting ones.

## Consequences

### Positive
- Retry-after-timeout is expressible and safe by construction: a caller can always re-issue the
  exact same `AddNode` call after any failure mode (timeout, connection reset, process crash) and
  get either the original node back or a loud, typed signal that something about its own retry
  logic is wrong (a conflicting spec on the same ID it thought it was retrying identically).
- Matches every DAG-definition prior-art system's naming discipline, so nodes remain referenceable
  by a stable name across dynamic edits (the "add a successor to an already-completed node" pattern
  from docs/research/12 §2.5 depends on the caller being able to name a not-yet-existing successor
  deterministically before it knows whether the predecessor has finished).

### Negative
- Pushes a real design burden onto host programs with no natural per-work-item key: they must
  either invent one (a request ID, a composite of inputs) or accept `GenerateID`'s weaker guarantee
  and take on their own persistence responsibility for it.
- The byte-identical bar is strict and can surprise: two reformatted-but-semantically-identical JSON
  payloads collide with `ErrIDConflict` rather than being treated as equivalent. Callers that
  reconstruct a payload on each retry (rather than replaying the exact same bytes) must normalize
  before calling `AddNode`, or use a stable serialization step upstream of it.

### Neutral
- `ErrIDConflict` becomes a first-class error path every host integration must handle, not a corner
  case — consistent with the sentinel/typed-error taxonomy dagworker's public API already commits
  to.

## Alternatives considered

**Library-generated IDs** (UUID/ULID returned from `AddNode`), matching River's and Sidekiq's job
model. Rejected: makes idempotent retry-after-timeout impossible to express, per the Context
section's core argument — the caller cannot know, on a lost response, whether to retry — and breaks
the natural-name precedent every DAG-definition system (Airflow/Argo/Step Functions) relies on for
referencing the same node across edits.

**A separate idempotency key, distinct from `NodeID`** (Stripe's own general API shape — an
`Idempotency-Key` header independent of the created resource's own ID). Rejected as the default:
legitimate in a general-purpose REST API, but it doubles the identity surface (`NodeID` for graph
structure, a second key purely for retry-dedup) for a concept a DAG node's identity already
naturally *is* — the caller's business key. Unifying them is simpler for the common case this
project targets, at the acknowledged cost of not supporting a "same idempotency key, different
`NodeID`" scenario some general-purpose APIs want and this one does not need.

**Semantic/logical payload equality instead of byte-identical.** Rejected: undecidable in general
for opaque `[]byte` payloads (ADR-0026), and any partial semantic check is a footgun whose failure
mode is silent and delayed rather than loud and immediate — exactly the property `ErrIDConflict`
exists to guarantee.

## References

- Stripe — Idempotent requests — https://docs.stripe.com/api/idempotent_requests
- Airflow task states (caller/definition-supplied task names) — https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html
- Sibling ADRs: ADR-0016 (storage port shape — `CreateNode`/`PutNode` are what `AddNode` composes
  on top of), ADR-0026 (payload is opaque bytes, no semantic comparison), ADR-0027 (error taxonomy,
  `ErrIDConflict` sentinel)
