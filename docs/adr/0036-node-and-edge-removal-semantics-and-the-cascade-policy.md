# ADR-0036: Node and edge removal semantics and the cascade policy

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/12-dag-semantics-and-state-machine.md §2.4, §1.6 (T14), docs/research/00-synthesis.md §4 (public API), §5 (T14)

## Context

The synthesis's public API sketch (§4) shows `RemoveNode(ctx, scope, id) error` with the comment
"rejects in-flight," and its state-machine table carries exactly one row for removal — T14: "any
node with live successors → Error/Removed (successors only) → the removed node is hard-deleted."
That row is a correct summary of *what happens to successors* but leaves the removal API itself
underspecified: it says nothing about what happens when the target node has successors and the
caller passes no policy, nothing about `RemoveEdge`'s own counter semantics, nothing about the
interaction with the topological order ADR-0004 maintains, and nothing about a subscriber whose
resume cursor references history for a node that no longer exists. The owner has reviewed this gap
and made a decision (AMD-3): **`RemoveNode` must reject when the target has successors, unless the
caller explicitly opts into a named cascade policy.** The synthesis's implicit behavior — cascade
straight to `Error/Removed` with no caller signal — is rejected as too surprising a default for what
is, in the common case, meant to be a narrow cleanup call (correcting a mistaken `AddNode`, retracting
a node that should never have been created), not a bulk failure-propagation operation.

The state-machine table's other removal-adjacent guidance is sound and this ADR builds on it rather
than re-deriving it: `RemoveNode` on an `InProgress` node is rejected (`ErrNodeInFlight`) because
deleting live work out from under a worker holding a lease is the identical correctness hazard as
retroactively editing a running node's inputs (§2.2) — the caller must go through the explicit,
auditable `Cancel` path (T13) first. `RemoveEdge` on an unresolved predecessor edge is
dependency-resolution-by-removal, mechanically identical to a predecessor completing (T3) — "if this
was the last unmet dependency, the target becomes ready immediately" is not a special case, it is
the same decrement rule ADR-0003 already implements, applied to a different trigger.

What neither the synthesis nor dossier 12's table works out in full is the exact boundary condition
this project needs: *when does a node "have successors" in the sense that blocks removal?* A
terminal node (`Success` or `Error`) has, by the time it reaches that status, already resolved every
outgoing edge — the successor's `pending` counter (ADR-0003) was decremented in the same atomic
write as the terminal transition, win or lose (trigger-rule evaluation, ADR-0030, decides separately
whether each successor proceeds, is skipped, or itself fails; it does not leave the edge unresolved).
A terminal node therefore has zero *unresolved* outgoing edges, even though the edges and successor
nodes still exist for lineage. "Has an outgoing edge" versus "has an outgoing edge a successor still
depends on" is exactly the boundary AMD-3's cascade requirement needs made precise.

## Decision

**A node's successors are "live" — and therefore block a bare `RemoveNode` call — if and only if at
least one outgoing edge from it is still *unresolved* on the successor's side**, i.e. the successor's
per-edge satisfaction structure (ADR-0005) still lists this node as a not-yet-satisfied predecessor.
By construction this is only possible when the node being removed is itself non-terminal
(`StatusNew`, `PhaseBlocked` or `PhaseReady`): a terminal node has already resolved every outgoing
edge as part of reaching that terminal status, so removing a terminal node is **never** blocked by
this rule, regardless of how many successors reference it.

```go
type CascadePolicy uint8

const (
    // CascadeReject is the zero value and the default: RemoveNode fails with
    // ErrNodeHasSuccessors if the target has any live (unresolved-edge)
    // successor. The caller must retry with an explicit policy.
    CascadeReject CascadePolicy = iota

    // CascadeFail marks every live successor Error/Removed (Outcome.Reason =
    // ReasonRemoved), in the SAME atomic write as the node's own deletion.
    // That Error transition then flows through the ordinary failure-
    // propagation machinery (T11/T12, ADR-0030's trigger rules) exactly as
    // any other terminal Error would — no bespoke recursive "Removed" walk
    // is needed beyond marking the direct live successors.
    CascadeFail

    // CascadeDetach drops the incident edge to every live successor instead
    // of failing it: the successor's unresolved-predecessor entry for this
    // node is removed (identical mechanics to RemoveEdge, below) and its
    // pending counter decremented in the same atomic write as the node's
    // deletion. A successor whose pending count reaches zero becomes Ready
    // (T3-equivalent) as a direct consequence.
    CascadeDetach
)

type RemoveOption interface{ apply(*removeConfig) }

// WithCascade selects the policy RemoveNode uses for a target with live
// successors. Omitting it is equivalent to WithCascade(CascadeReject).
func WithCascade(p CascadePolicy) RemoveOption

func (m *Manager) RemoveNode(ctx context.Context, scope Scope, id NodeID, opts ...RemoveOption) error
func (m *Manager) RemoveEdge(ctx context.Context, scope Scope, from, to NodeID) error

var ErrNodeHasSuccessors = errors.New("dagworker: node has live successors; specify a cascade policy")
```

**`RemoveNode` decision table**, evaluated in this order:

| Target node state | Live successors? | `CascadeReject` (default) | `CascadeFail` | `CascadeDetach` |
|---|---|---|---|---|
| `InProgress` (any) | n/a | `ErrNodeInFlight` — always, regardless of policy; caller must `Cancel` first (T13), then remove once terminal | `ErrNodeInFlight` | `ErrNodeInFlight` |
| `New` (Blocked/Ready) | none | delete, no cascade needed | delete, no cascade needed | delete, no cascade needed |
| `New` (Blocked/Ready) | ≥1 | `ErrNodeHasSuccessors` | delete + mark live successors `Error/Removed` | delete + drop incident edges, decrement successors' pending, ready any that hit zero |
| `Success` or `Error` (terminal) | n/a (never live, see above) | delete — pure graph/lineage cleanup, identical in effect to an on-demand T15 | delete (cascade is a no-op: nothing live to fail) | delete (cascade is a no-op: nothing live to detach) |

`RemoveNode` on a non-terminal node that is still counted in the scope's `notTerminalCount` (ADR-0024)
decrements that counter as part of the same atomic write, regardless of which branch of the table
fires — the node's obligation ends at removal exactly as it would at a terminal transition, and
`Scope.IsComplete()` must be able to reach `true` for a scope whose remaining non-terminal nodes were
all explicitly removed, not only ones that ran to completion. A node already terminal when removed
has already been decremented at the point it became terminal; its removal has no further effect on
the counter (cross-reference ADR-0024).

**`RemoveEdge(scope, from, to)`** — the successor's pending count decreases exactly when the edge
being removed was still unresolved:

1. If `to` is terminal (`Success` or `Error`): pure graph-structural deletion of the edge record.
   Zero effect on `Status`, `Outcome`, or `pending` — a terminal node never regains an unresolved
   dependency, matching the "no transition out of a terminal status" invariant ADR-0001/ADR-0002
   already establish.
2. If `to` is non-terminal and `from` is still listed as an unresolved predecessor (ADR-0005): remove
   `from` from that structure and decrement `to.pending` in one atomic write. If `pending` reaches
   zero, `to` becomes `Ready` immediately — mechanically identical to a predecessor completing (T3);
   `RemoveEdge` is dependency-resolution-by-removal, not a distinct code path.
3. If `to` is non-terminal but `from` is *not* listed as unresolved (the edge already resolved earlier
   — e.g. `from` had already reached `Success` before this call) or the edge does not exist at all:
   pure no-op on `pending`; the edge record is deleted if present, or `ErrNotFound` is returned if it
   is not. This makes `RemoveEdge` idempotent under retry, matching every other mutation primitive in
   this design.

**Interaction with the topological order (ADR-0004):** removal leaves a gap. A removed node's `ord`
slot is never reused and no remaining node is renumbered as a result of a removal alone — because a
deleted node participates in no live edge, the invariant `ord(u) < ord(v)` for every remaining live
edge is trivially preserved. `CascadeDetach`'s dropped edges likewise require no `ord` update: the
surviving nodes' relative order is unaffected by an edge disappearing, only by one being added
(ADR-0004 already handles the addition case).

**Interaction with subscribers holding a resume cursor:** the event log is immutable and
independent of live node-table row existence (ADR-0021's "state-plus-notification, not
event-sourced-log-as-truth" model). Removal does **not** retract, rewrite, or invalidate any
previously-emitted `EventTransition` for the removed node or its successors — a subscriber resuming
`From: Seq` before the removal still replays the full, accurate history up to and including it
(the removed node's own prior transitions, if any, plus each affected successor's `Error/Removed` or
`Ready` transition under the chosen cascade policy). The removed node itself never emits an
`EventTransition` *for its own removal* — removing a non-terminal node is a hard delete with no
`Status` change to report (mirroring T15's own non-eventful deletion), and removing a terminal node
changes nothing about its already-recorded final `Outcome`. A subsequent `GetNode(id)` for the
removed ID after replay returns the ordinary `ErrNotFound` — indistinguishable from any other GC'd
node, and this is deliberate: `ErrCursorExpired` (ADR-0021) fires only when the event *log's own*
retention window has been swept past the cursor's position, never merely because a node the log
mentions is no longer separately readable via `GetNode`. A subscriber must already treat "node not
found" as an expected outcome for old enough IDs; removal introduces no new error class here.

## Consequences

### Positive

- The default (`CascadeReject`) makes an accidental bulk-failure cascade impossible to trigger by
  a plain `RemoveNode` call — the caller must name the blast radius they intend (`CascadeFail` vs.
  `CascadeDetach`) before any successor is touched, which is the explicit, auditable behavior AMD-3
  requires in place of the synthesis's implicit always-cascade default.
- Terminal-node removal needs no cascade policy and can never fail with `ErrNodeHasSuccessors`,
  because the "live successor" definition is derived directly from the same per-edge resolution
  state ADR-0005 already maintains — no new bookkeeping structure is introduced solely to answer
  "does this node have live successors."
- `RemoveEdge`'s idempotency (no-op when the edge is already resolved or already gone) means retrying
  a failed or uncertain removal call is always safe, matching the idempotency discipline the rest of
  the mutation API (`AddNode`, ADR-0025) already commits to.

### Negative

- `CascadeFail` and `CascadeDetach` both require the removal write to touch every direct live
  successor's row in the same atomic operation as the node's own deletion — for a high-fan-out node
  this is a bulk write with the same tail-latency shape ADR-0003 already accepts for completion
  fan-out, and it must be implemented as such (one backend-native bulk primitive, never `N`
  individual round trips) rather than assumed away.
- A caller that always reaches for `CascadeFail` out of convenience reintroduces the surprising
  bulk-failure behavior this ADR requires an explicit opt-in for — a documentation/ergonomics risk
  worth calling out prominently in the `RemoveNode` doc comment, not a design flaw.

### Neutral

- `ReasonRemoved` (ADR-0001) is used only on successors under `CascadeFail`, never on the removed
  node itself, which is always hard-deleted without a final `Outcome` of its own — consistent with
  T14's original phrasing that "the removed node is hard-deleted, not transitioned."
- No distinct "node was removed" audit event class is introduced; an operator needing a removal
  audit trail must correlate an out-of-band log of `RemoveNode` calls with the absent event —
  out of scope here, revisitable if a concrete operational need demonstrates it.

## Alternatives considered

**Always cascade to `Error/Removed` with no caller opt-in** (the synthesis's original, implicit
T14 behavior). Rejected per the owner's explicit AMD-3 decision: silently cascading failure through
an arbitrary number of successors is too surprising a default for what is meant to be a narrow
cleanup call — an operator retracting one mistaken node should not be able to accidentally fail a
large downstream subgraph without a deliberate, named choice.

**Define "live successors" as "has any outgoing edge at all," including already-resolved ones.**
Rejected: this would make `RemoveNode` on almost every terminal node with descendants require a
cascade policy for no reason, since a resolved edge carries no outstanding obligation — the chosen
definition (unresolved-edge-only) matches what a cascade policy would actually act on, and keeps
terminal-node cleanup — the common case for this call — cascade-free.

**Soft-delete/tombstone instead of hard delete for `RemoveNode`.** Rejected for v1: T14 already
specifies a hard delete, and a tombstone here would duplicate the TTL/low-water-mark retention
mechanism dossier 12 §6.2 already defines for ordinary terminal-node GC. A backend that can cheaply
keep a `status`/`outcome`-only tombstone after removal may still do so — a quality-of-implementation
choice, not a semantic requirement here.

**Retract or mark stale the removed node's previously-emitted events.** Rejected: mutating an
already-delivered event log violates ADR-0021's "state-plus-notification, not event-sourced-log-as-
truth" model — a removed node did exist and did emit whatever it emitted; removal changes present
state, not history.

## References

- docs/research/12-dag-semantics-and-state-machine.md §2.4 (removal table), §1.6 (T14), §2.2
  (retroactive-edit precedent for the InProgress rejection rule)
- Airflow `removed` task state (precedent for a dedicated "the DAG changed out from under a live
  run" outcome): https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html
- docs/research/00-synthesis.md §4 (public API `RemoveNode`/`RemoveEdge` signatures), §5 (T14 row)
- ADR-0001 (Outcome.Reason, ReasonRemoved) · ADR-0003 (decrement mechanics reused here)
- ADR-0004 (topological order gap handling) · ADR-0005 (per-edge satisfaction structure)
- ADR-0021 (recovery model / ErrCursorExpired, per synthesis §3 ADR-21 seed)
- ADR-0024 (scope non-terminal counter, decremented on removal of a non-terminal node)
