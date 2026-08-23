---
title: Growing and changing a graph while it runs
description: Adding nodes and edges while workers are claiming, cycle rejection, removal and cascade policies, sealing, and completion detection.
---

A task queue has no edges, so there is nothing dynamic about it: every item
is independent from the moment it's enqueued. dagworker's graph, by
contrast, is expected to grow *while workers are already claiming from it* —
a node's own handler is frequently what adds that node's children. This page
covers what makes that safe: cycle rejection that doesn't cost a full graph
scan, removal that doesn't silently cascade unless you ask it to, and
completion detection that doesn't race the very fan-out it's trying to
detect the end of.

## Adding nodes and edges while running

`AddNode` and the batch form `AddNodes` are the only way nodes enter a
scope, and both are safe to call from inside a worker handling a node in the
same scope. `AddNodes` is atomic per call — every node and edge in the batch
lands, or none does — which is what makes the common "one worker fans out
into several children" pattern safe to write without a transaction of your
own:

```go
lease, _ := m.TryClaim(ctx, "crawl") // lease.Node is "list-pages"
urls := discoverURLs(lease.Node.Payload)

specs := make([]dagworker.NodeSpec, len(urls))
for i, u := range urls {
	specs[i] = dagworker.NodeSpec{
		ID:      dagworker.NodeID(fmt.Sprintf("fetch-%d", i)),
		Deps:    []dagworker.NodeID{"list-pages"},
		Payload: []byte(u),
	}
}
// Add the children BEFORE acking their parent, not after.
if err := m.AddNodes(ctx, "crawl", specs); err != nil {
	_, _ = m.Nack(ctx, lease, err)
	return
}
_ = m.Ack(ctx, lease, nil)
```

The ordering matters. `list-pages` is still `InProgress` — non-terminal — at
the moment `AddNodes` runs, so each `fetch-N` node is born `Blocked` on it.
The same atomic operation that later completes `list-pages` resolves that
edge and pushes every satisfied child into the ready set — there is no
window where a child could be claimed before its dependency is even known
to exist. It also means this handler is safe to retry from the top after a
crash between the two calls: `AddNodes` on an identical batch is a no-op
(see below), so re-running the whole handler after a lease timeout does not
double the fan-out.

**Insertion is idempotent by spec, not by convention.** Re-adding a node
with a byte-identical `NodeSpec` — same payload, kind, labels, priority,
trigger rule, and retry policy — is a no-op that returns success, not an
error. Re-adding the same ID with a *different* spec returns `ErrIDConflict`.
This is what makes "retry the whole handler" a safe default: a caller that
isn't sure whether its last `AddNodes` call actually landed can simply call
it again.

**Forward references aren't supported.** A dependency must already exist in
the same scope, or appear earlier in the same `AddNodes` batch — an edge to
a node that doesn't exist yet can't be cycle-checked, so referencing one
returns `ErrNotFound` rather than being queued speculatively.

## Cycle rejection

`AddEdge` rejects a cycle-forming edge synchronously, and it does so without
paying for a graph-wide reachability search on every call. Every node
carries an integer topological rank; for a proposed edge `u → v`:

- if `rank(u) < rank(v)` already, the edge is accepted in **O(1)** — this is
  the common case, because graphs built in roughly causal order (parents
  registered before children) already satisfy the invariant;
- otherwise, a bounded search runs over just the affected region. If it
  finds a path back from `v` to `u`, the edge is rejected; if not, that same
  search's result is used to renumber the affected region and the edge is
  accepted.

The search *is* the cycle check — there's no separate reachability pass. A
rejected edge comes back as a `*dagworker.CycleError`, which carries the
path that would have closed the loop:

```go
_ = m.AddNodes(ctx, "g", []dagworker.NodeSpec{{ID: "a"}, {ID: "b"}, {ID: "c"}})
_ = m.AddEdge(ctx, "g", "a", "b")
_ = m.AddEdge(ctx, "g", "b", "c")

err := m.AddEdge(ctx, "g", "c", "a")
errors.Is(err, dagworker.ErrCycle) // true

var ce *dagworker.CycleError
errors.As(err, &ce)
ce.Path // [a b c] — the route that already existed from c back to a
```

An edge into a terminal node is rejected too, but for a different reason —
`ErrAlreadyTerminal` — because nothing is allowed to re-block a node that
has already succeeded or failed. That's an invariant, not a limitation:
letting a finished node get a new, unresolved predecessor would mean a
"done" result could un-become done, which no downstream consumer could
safely reason about.

## Removal and cascade policies

`RemoveNode` refuses to guess what you meant when a node has successors
still depending on it — you name the blast radius explicitly:

```go
func (m *Manager) RemoveNode(ctx context.Context, scope Scope, id NodeID, policy CascadePolicy) error
```

| Policy | What happens to live successors |
|---|---|
| `CascadeReject` *(default, zero value)* | `RemoveNode` fails with `ErrHasSuccessors`. Nothing changes. |
| `CascadeDetach` | The incident edges are dropped, as if by `RemoveEdge`. A successor that loses its last unresolved dependency becomes ready immediately. |
| `CascadeFail` | Every live successor transitions to `Error`/`ReasonRemoved`, recursively, before the node itself is deleted. |

A node that's `InProgress` can't be removed under any policy — you get
`ErrNodeInFlight` regardless. A worker holds a lease on it; deleting the
node out from under that lease is the same correctness hazard as rewriting
a running job's inputs. `Cancel` it first, then remove it once it's
terminal.

The default is `CascadeReject` deliberately: a `RemoveNode` call correcting
one mistaken insert should never be able to silently fail an entire
downstream subgraph because you forgot a flag. If you actually want that
blast radius, `CascadeFail` names it explicitly.

**A node that's already terminal never blocks removal, regardless of how
many nodes point at it.** By the time a node reaches `Success` or `Error`,
every one of its outgoing edges has already been resolved — the successor's
predecessor count was decremented in the same atomic write as the terminal
transition. "Has successors" specifically means "has at least one
*unresolved* outgoing edge", so removing a finished node for lineage cleanup
never requires picking a cascade policy at all.

`RemoveEdge(from, to)` is dependency-resolution-by-removal: if the edge was
still unresolved on `to`'s side, dropping it decrements `to`'s predecessor
count exactly as if `from` had completed — and `to` can become ready as a
direct result, in the same operation. If the edge had already resolved (or
never existed), `RemoveEdge` is a no-op, which makes it safe to retry.

## Sealing and completion detection

"Is this scope done" can't be answered from node counts alone while the
graph might still grow — a count of non-terminal nodes hitting zero for one
instant, while a worker is between its `Ack` and the `AddNodes` call that
was about to fan out into new children, would be a false positive with no
way to distinguish it from the real thing.

The library resolves this with an explicit, caller-driven signal rather than
an inferred one:

```go
_ = m.Seal(ctx, scope) // "I will not add nodes to this scope again."
done, _ := m.IsComplete(ctx, scope) // Sealed && zero non-terminal nodes
```

`IsComplete` is `Sealed && nonTerminalCount == 0`, both maintained
incrementally by the same transitions that already change them — it is an
**O(1) read, never a scan.** `Seal` is irreversible for the life of the
scope and idempotent (sealing an already-sealed scope is a no-op, not an
error). `AddNode`/`AddNodes`/`AddEdge` against a sealed scope return
`ErrScopeSealed`.

**The footgun worth naming directly: an unsealed scope never reports
complete, no matter how empty it is.** This is the conservative, correct
default — a scope you forgot to seal isn't broken, it's honestly telling you
it might still receive more work — but it means a program that fans out
dynamically and then simply stops calling `AddNodes` will have
`IsComplete` return `false` forever unless it calls `Seal` explicitly. If a
graph you expect to finish never does, check this before anything else; the
next section covers the tool for that.

## Debugging a graph that looks stuck

`Manager.Inspect` exists specifically for "why is this node not running":

```go
insp, _ := m.Inspect(ctx, "etl", "join")
insp.Phase      // e.g. "blocked"
insp.Waiting    // e.g. []NodeID{"extract-b"} — the predecessor still pending
insp.Successors // this node's own out-edges
insp.Rank       // topological order number
```

`Inspection` carries no compatibility promise across versions and never
appears on the event stream — it's a debugging window into internal
scheduling state, not part of the public data model. [Running it in
production](/dagworker/guide/operations/) covers using it as the first step
when a graph looks stuck in a real deployment.
