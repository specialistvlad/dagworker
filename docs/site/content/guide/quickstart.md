---
title: Quickstart
description: From go get to a worker draining a real graph, then swapping in Redis or PostgreSQL without changing the rest of the program.
---

This page goes from nothing to a worker draining a dependency graph, on the
in-memory backend, in one program you can paste and run. Then it swaps the
storage out from under that same program to show what actually changes when
you move to Redis or PostgreSQL: two lines.

## Install

```console
$ go get github.com/specialistvlad/dagworker
```

The core module has exactly this dependency: none. It pulls in the standard
library and nothing else. Redis, PostgreSQL, the gRPC and HTTP adapters, and
the `dagworkerd` daemon are separate modules — `go get` on the core pulls in
none of them.

## The shortest useful program

A three-stage release pipeline — `build`, then `test`, then `publish` — run
to completion by one worker in the same process:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	dagworker "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
)

func main() {
	ctx := context.Background()

	m, err := dagworker.New(memory.New())
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close(ctx)

	// A graph. Edges may be added later, while it is running — see the
	// "Dynamic graphs" guide.
	if err := m.AddNodes(ctx, "release", []dagworker.NodeSpec{
		{ID: "build"},
		{ID: "test", Deps: []dagworker.NodeID{"build"}},
		{ID: "publish", Deps: []dagworker.NodeID{"test"}},
	}); err != nil {
		log.Fatal("add:", err)
	}
	// Sealing tells the library "no more nodes are coming to this scope",
	// which is what lets IsComplete ever become true. See "Concepts".
	if err := m.Seal(ctx, "release"); err != nil {
		log.Fatal("seal:", err)
	}

	// A worker. In a real program this loop runs in its own goroutine, and
	// you run as many of them as you have concurrency for.
	for {
		lease, err := m.TryClaim(ctx, "release")
		if errors.Is(err, dagworker.ErrNoWork) {
			break // nothing ready right now, and none of the loop's own doing
		}
		if err != nil {
			log.Fatal("claim:", err)
		}

		fmt.Println("running", lease.NodeID)
		if err := doTheWork(lease.Node); err != nil {
			// Nack schedules a retry or fails the node, per the scope's
			// retry policy. The worker never decides which happened.
			if _, err := m.Nack(ctx, lease, err); err != nil {
				log.Fatal("nack:", err)
			}
			continue
		}
		if err := m.Ack(ctx, lease, nil); err != nil {
			log.Fatal("ack:", err)
		}
	}

	done, _ := m.IsComplete(ctx, "release")
	fmt.Println("complete:", done)
}

func doTheWork(n dagworker.Node) error {
	// Whatever build/test/publish actually means in your program.
	return nil
}
```

Run it and it prints:

```
running build
running test
running publish
complete: true
```

A few things worth noticing before moving on:

- **`TryClaim` never blocks.** It returns [`ErrNoWork`](/dagworker/reference/contract/)
  when nothing is ready, and that is an ordinary outcome, not a failure — check
  it with `errors.Is` and move on. A worker that should wait for the next node
  to become ready calls [`Claim`](/dagworker/guide/workers/) instead, which
  blocks until one is or `ctx` ends.
- **`test` never runs before `build` succeeds**, and nothing in this program
  says so explicitly — it falls out of `Deps: []dagworker.NodeID{"build"}` and
  the default trigger rule, `TriggerAllSuccess`. The [Trigger
  rules](/dagworker/guide/trigger-rules/) guide covers the other four rules,
  which matter the moment a step is allowed to fail without stopping the
  graph.
- **`Ack`/`Nack` present the `Lease` `TryClaim` returned, not the node ID.**
  The lease carries the fencing epoch that makes it safe for two workers to
  race for the same node — the subject of [Concepts](/dagworker/guide/concepts/).

## Swapping in Redis

Nothing about the program above is specific to the in-memory backend. Replace
the store:

```go
import (
	dagworker "github.com/specialistvlad/dagworker"
	redisstore "github.com/specialistvlad/dagworker/storage/redis"
)

store, err := redisstore.Open(ctx, "localhost:6379")
if err != nil {
	log.Fatal(err)
}
defer store.Close(ctx)

m, err := dagworker.New(store)
```

`redisstore.Open` dials for you; if you already carry a `redis.UniversalClient`
elsewhere in your program, `redisstore.New(client)` wraps it instead. Add
`github.com/specialistvlad/dagworker/storage/redis` to your module — it is a
separate `go.mod` from the core, so it is the only place a Redis client
dependency enters your build.

## Swapping in PostgreSQL

Same shape:

```go
import (
	dagworker "github.com/specialistvlad/dagworker"
	postgresstore "github.com/specialistvlad/dagworker/storage/postgres"
)

store, err := postgresstore.Open(ctx, "postgres://user:pass@localhost:5432/dagworker")
if err != nil {
	log.Fatal(err)
}
defer store.Close(ctx)

m, err := dagworker.New(store)
```

`postgresstore.New(pool)` is the equivalent entry point if you already manage
a `*pgxpool.Pool`. Migrations live in `storage/postgres/migrations`; run them
before pointing a `Manager` at a fresh database.

Everything above `store` — the `AddNodes`/`Seal`/`Claim`/`Ack` loop — is
identical on all three backends, because it is written against the `Manager`,
never against a backend's own client. [Choosing a
backend](/dagworker/guide/backends/) covers what actually differs between
them: durability, whether several processes can share one graph, and the
measured cost per operation.

## Where to go next

- [Concepts](/dagworker/guide/concepts/) — nodes, scopes, the four-value
  status, and the lease protocol that makes concurrent workers safe. Read
  this before running anything with more than one worker.
- [Trigger rules](/dagworker/guide/trigger-rules/) — the five rules and a
  worked branching example.
- [Dynamic graphs](/dagworker/guide/dynamic-graphs/) — adding nodes and edges
  while the graph is running, which is the feature that makes this a graph
  engine rather than a task queue.
- [Writing workers](/dagworker/guide/workers/) — worker pools, retries,
  heartbeats for long-running work, and the gRPC/HTTP adapters for workers
  that are not in this process, or not written in Go.
