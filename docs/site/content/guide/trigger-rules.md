---
title: Trigger rules and branching
description: The five rules that decide when a node becomes claimable, and a worked example using Skip as the branch primitive.
---

A node's `Trigger` decides when it becomes claimable, given how its
predecessors resolved. Five rules ship, and every one of them is evaluable
incrementally — as each predecessor becomes terminal, the library updates a
small per-node tally and checks it, rather than re-scanning every
predecessor on every event. That's what keeps a node's readiness check O(1)
regardless of its in-degree.

## The five rules

| Rule | Becomes ready when | Becomes unsatisfiable (terminates without running) when |
|---|---|---|
| `TriggerAllSuccess` *(default)* | every predecessor succeeded | any predecessor is `Error`, or any is skipped |
| `TriggerAllDone` | every predecessor is terminal, however it finished | never |
| `TriggerNoneFailed` | every predecessor is terminal and none is `Error` | any predecessor is `Error` |
| `TriggerNoneFailedMinOneSuccess` | every predecessor is terminal, none is `Error`, and at least one succeeded | any predecessor is `Error`, or every predecessor resolved without a single success |
| `TriggerAlways` | immediately — predecessors are never consulted | never |

A node with zero predecessors is ready immediately under every rule. Edges
into a `TriggerAlways` node still exist — they're checked for cycles and
they document intent — they just never gate it.

The tally each rule reads is four counters, maintained by the same atomic
operation that completes a predecessor:

```go
type DepCounts struct {
	Unsatisfied uint32 // predecessors not yet terminal
	Succeeded   uint32
	Skipped     uint32
	Failed      uint32
}
```

A predecessor is classified into exactly one of `Succeeded`, `Skipped`, or
`Failed` the instant it becomes terminal — `Skipped` meaning `StatusError`
with `ReasonSkipped` specifically, and `Failed` meaning `StatusError` with
any other reason. That distinction between "this branch wasn't taken" and
"this branch broke" is not cosmetic: it is exactly what separates
`TriggerAllSuccess` (which treats a skip as disqualifying, same as a
failure) from `TriggerNoneFailed` (which tolerates a skip and only cares
about genuine failure).

## Skip: the branch primitive

A worker reports one of three outcomes:

- `Ack` — succeeded.
- `Nack` — the attempt failed. The scope's retry policy, not the worker,
  decides whether that becomes another attempt or a terminal `Error` — see
  [Writing workers](/dagworker/guide/workers/).
- `Skip` — "I looked, and there was nothing to do." Unlike `Nack`, a skip is
  **terminal on the first report**, because a retry would only reach the
  same conclusion again. It records `StatusError` with `ReasonSkipped`.

Skip is what makes branching possible without a special "conditional" node
type. A downstream node written with `TriggerAllSuccess` treats a skipped
predecessor exactly like a failed one — the branch that wasn't taken
disqualifies it, same as a real failure would. A downstream node written
with `TriggerNoneFailed` or `TriggerNoneFailedMinOneSuccess` tolerates the
skip and runs anyway. **The same graph shape means two different things
depending only on which trigger rule the downstream node declares** — that
one-word choice is the entire branching mechanism.

## A worked example

A release pipeline where two optional build steps run in parallel — one of
them might have nothing to do — and a packaging step needs at least one of
them to have actually produced something:

```go
package main

import (
	"context"
	"fmt"

	dagworker "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
)

func main() {
	ctx := context.Background()
	m, _ := dagworker.New(memory.New())
	defer m.Close(ctx)

	_ = m.AddNodes(ctx, "release", []dagworker.NodeSpec{
		{ID: "detect-changes"},
		{ID: "build-docs", Deps: []dagworker.NodeID{"detect-changes"}},
		{ID: "build-code", Deps: []dagworker.NodeID{"detect-changes"}},
		// Needs at least one of the two builds to have produced something,
		// and neither of them to have genuinely failed.
		{
			ID:      "package",
			Deps:    []dagworker.NodeID{"build-docs", "build-code"},
			Trigger: dagworker.TriggerNoneFailedMinOneSuccess,
		},
		// Runs regardless of how package's predecessors turned out — that
		// is what all_done is for.
		{ID: "notify", Deps: []dagworker.NodeID{"package"}, Trigger: dagworker.TriggerAllDone},
	})

	step := func() {
		lease, err := m.TryClaim(ctx, "release")
		if err != nil {
			fmt.Println("claim:", err)
			return
		}
		switch lease.NodeID {
		case "build-docs":
			// Nothing under docs/ changed this run.
			_ = m.Skip(ctx, lease, "no docs changed")
		default:
			_ = m.Ack(ctx, lease, nil)
		}
	}

	step() // detect-changes — the only node ready at the start
	// build-docs and build-code are both ready now; which one a given
	// TryClaim returns first is not specified, so branch on the node it
	// actually handed back rather than assuming an order.
	step()
	step()
	step() // package — one predecessor succeeded, none failed: it runs
	step() // notify — runs regardless

	for _, id := range []dagworker.NodeID{
		"detect-changes", "build-docs", "build-code", "package", "notify",
	} {
		n, _ := m.GetNode(ctx, "release", id)
		fmt.Printf("%-14s %-7s %s\n", id, n.Status, n.Reason)
	}
}
```

This prints:

```
detect-changes success none
build-docs     error   skipped
build-code     success none
package        success none
notify         success none
```

`build-docs` is terminal with `ReasonSkipped`, not `ReasonWorkerError` — the
distinction `package`'s trigger rule depends on. Had `package` been declared
with the default `TriggerAllSuccess` instead, it would have terminated with
`ReasonUpstreamFailed` the moment `build-docs` skipped, without ever running
— same graph, same execution, different one-word decision.

## When a rule becomes unsatisfiable early

A node doesn't wait for every predecessor to finish if the rule can no
longer possibly be satisfied. Under `TriggerAllSuccess`, the instant any
predecessor lands on `Error`, the successor is terminated immediately with
`ReasonUpstreamFailed` — it never sits around waiting for siblings that
would have made no difference. This is what keeps a failure from stalling a
graph rather than propagating through it: a node three levels downstream of
a failure fails immediately, in the same atomic operation as the failure
that caused it, not lazily when someone eventually asks.

The one rule that can never be terminated early by a failure is
`TriggerAllDone` — by definition it only cares that predecessors are
terminal, not how — which is exactly the shape you want for a cleanup or
notification step that has to run whether the rest of the pipeline succeeded
or not.

Next: [Dynamic graphs](/dagworker/guide/dynamic-graphs/) covers what happens
when the edges these rules are evaluated against are still being added while
the graph runs.
