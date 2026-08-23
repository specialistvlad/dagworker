package dagworker_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
)

// The shortest useful program: a three-stage pipeline, drained by one worker.
func Example() {
	ctx := context.Background()

	m, err := dagworker.New(memory.New())
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = m.Close(ctx) }()

	if err := m.AddNodes(ctx, "release", []dagworker.NodeSpec{
		{ID: "build"},
		{ID: "test", Deps: []dagworker.NodeID{"build"}},
		{ID: "publish", Deps: []dagworker.NodeID{"test"}},
	}); err != nil {
		fmt.Println("add:", err)
		return
	}
	if err := m.Seal(ctx, "release"); err != nil {
		fmt.Println("seal:", err)
		return
	}

	for {
		lease, err := m.TryClaim(ctx, "release")
		if errors.Is(err, dagworker.ErrNoWork) {
			break
		}
		if err != nil {
			fmt.Println("claim:", err)
			return
		}
		fmt.Println("running", lease.NodeID)
		if err := m.Ack(ctx, lease, nil); err != nil {
			fmt.Println("ack:", err)
			return
		}
	}

	done, err := m.IsComplete(ctx, "release")
	if err != nil {
		fmt.Println("complete check:", err)
		return
	}
	fmt.Println("complete:", done)

	// Output:
	// running build
	// running test
	// running publish
	// complete: true
}

// A failure does not leave the rest of the graph waiting forever. Nodes that
// can no longer run are terminated with a reason that says why, and a cleanup
// node declared with TriggerAllDone still runs.
func Example_failurePropagation() {
	ctx := context.Background()
	m, _ := dagworker.New(memory.New())
	defer func() { _ = m.Close(ctx) }()

	_ = m.Configure(ctx, "job", dagworker.ScopeConfig{MaxAttempts: 1})
	_ = m.AddNodes(ctx, "job", []dagworker.NodeSpec{
		{ID: "fetch"},
		{ID: "transform", Deps: []dagworker.NodeID{"fetch"}},
		{ID: "load", Deps: []dagworker.NodeID{"transform"}},
		// Runs either way: that is what all_done is for.
		{ID: "cleanup", Deps: []dagworker.NodeID{"load"}, Trigger: dagworker.TriggerAllDone},
	})

	lease, _ := m.TryClaim(ctx, "job")
	_, _ = m.Nack(ctx, lease, errors.New("upstream returned 503"))

	for _, id := range []dagworker.NodeID{"fetch", "transform", "load", "cleanup"} {
		n, _ := m.GetNode(ctx, "job", id)
		fmt.Printf("%-10s %-7s %s\n", id, n.Status, n.Reason)
	}

	// Output:
	// fetch      error   worker_error
	// transform  error   upstream_failed
	// load       error   upstream_failed
	// cleanup    new     none
}

// A worker that dies loses its node, and another worker picks it up. Nothing
// is lost, and nothing is run twice at once.
func Example_leaseTimeout() {
	ctx := context.Background()

	// A fake clock so the example can skip past the deadline instead of
	// sleeping; in production this is just time.
	clock := newExampleClock()
	m, _ := dagworker.New(memory.New(memory.WithClock(clock)), dagworker.WithoutBackgroundSweeper())
	defer func() { _ = m.Close(ctx) }()

	_ = m.Configure(ctx, "jobs", dagworker.ScopeConfig{
		DefaultLeaseTimeout: 30 * time.Second,
		RetryBaseDelay:      time.Nanosecond,
		RetryMaxDelay:       time.Nanosecond,
	})
	_ = m.AddNode(ctx, "jobs", "slow", nil)

	abandoned, _ := m.TryClaim(ctx, "jobs")
	fmt.Println("worker one claimed at epoch", abandoned.Epoch)

	clock.Advance(time.Minute) // worker one is never heard from again

	recovered, _ := m.TryClaim(ctx, "jobs")
	fmt.Println("worker two claimed at epoch", recovered.Epoch)

	// Worker one comes back and tries to report success. It is refused: its
	// lease was superseded, and accepting it would overwrite worker two.
	err := m.Ack(ctx, abandoned, nil)
	fmt.Println("late acknowledgement refused:", errors.Is(err, dagworker.ErrLeaseMismatch))

	// Output:
	// worker one claimed at epoch 1
	// worker two claimed at epoch 2
	// late acknowledgement refused: true
}

// Several workers on one graph. This is the ordinary case: the store's atomic
// claim is what makes it safe, so nothing here coordinates.
func Example_concurrentWorkers() {
	ctx := context.Background()
	m, _ := dagworker.New(memory.New())
	defer func() { _ = m.Close(ctx) }()

	const jobs = 100
	specs := make([]dagworker.NodeSpec, jobs)
	for i := range specs {
		specs[i] = dagworker.NodeSpec{ID: dagworker.NodeID(fmt.Sprintf("job-%03d", i))}
	}
	_ = m.AddNodes(ctx, "batch", specs)

	var mu sync.Mutex
	done := map[dagworker.NodeID]int{}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				lease, err := m.TryClaim(ctx, "batch")
				if err != nil {
					return // ErrNoWork: the graph is drained
				}
				mu.Lock()
				done[lease.NodeID]++
				mu.Unlock()
				_ = m.Ack(ctx, lease, nil)
			}
		}()
	}
	wg.Wait()

	twice := 0
	for _, n := range done {
		if n > 1 {
			twice++
		}
	}
	fmt.Println("completed:", len(done))
	fmt.Println("handed to two workers at once:", twice)

	// Output:
	// completed: 100
	// handed to two workers at once: 0
}

// Edges may be added while the graph is running. One that would create a cycle
// is refused at insert time, with the path that closes it.
func ExampleManager_AddEdge() {
	ctx := context.Background()
	m, _ := dagworker.New(memory.New())
	defer func() { _ = m.Close(ctx) }()

	_ = m.AddNodes(ctx, "g", []dagworker.NodeSpec{{ID: "a"}, {ID: "b"}, {ID: "c"}})
	_ = m.AddEdge(ctx, "g", "a", "b")
	_ = m.AddEdge(ctx, "g", "b", "c")

	err := m.AddEdge(ctx, "g", "c", "a")
	fmt.Println("cycle refused:", errors.Is(err, dagworker.ErrCycle))

	var ce *dagworker.CycleError
	if errors.As(err, &ce) {
		fmt.Println("path:", ce.Path)
	}

	// Output:
	// cycle refused: true
	// path: [a b c]
}

// Subscribe is an observation feed. Nothing about correctness depends on an
// event arriving, which is why a missed one costs latency and not a wrong
// answer.
func ExampleManager_Subscribe() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m, _ := dagworker.New(memory.New())
	defer func() { _ = m.Close(context.Background()) }()

	sub, err := m.Subscribe(ctx, dagworker.SubscribeOptions{
		Scope: "pipeline",
		Kinds: []dagworker.EventKind{dagworker.EventTransition},
	})
	if err != nil {
		fmt.Println("subscribe:", err)
		return
	}
	defer func() { _ = sub.Close() }()

	_ = m.AddNode(ctx, "pipeline", "step", nil)
	lease, _ := m.TryClaim(ctx, "pipeline")
	_ = m.Ack(ctx, lease, nil)

	// Printed in arrival order, deliberately: events for one node are totally
	// ordered by their sequence, which is the guarantee this demonstrates.
	// There is no such guarantee between nodes.
	for range 2 {
		select {
		case ev := <-sub.Events():
			fmt.Printf("%s: %s -> %s\n", ev.NodeID, ev.From, ev.To)
		case <-time.After(time.Second):
			fmt.Println("timed out")
		}
	}

	// Output:
	// step: new -> in_progress
	// step: in_progress -> success
}

// Inspect answers the first question anyone asks about a stalled graph.
func ExampleManager_Inspect() {
	ctx := context.Background()
	m, _ := dagworker.New(memory.New())
	defer func() { _ = m.Close(ctx) }()

	_ = m.AddNodes(ctx, "etl", []dagworker.NodeSpec{
		{ID: "extract-a"},
		{ID: "extract-b"},
		{ID: "join", Deps: []dagworker.NodeID{"extract-a", "extract-b"}},
	})
	lease, _ := m.TryClaim(ctx, "etl", dagworker.OfKind())
	_ = m.Ack(ctx, lease, nil)

	insp, _ := m.Inspect(ctx, "etl", "join")
	fmt.Println("phase:", insp.Phase)
	fmt.Println("still waiting on:", insp.Waiting)

	// Output:
	// phase: blocked
	// still waiting on: [extract-b]
}

// Typed removes the encoding boilerplate for callers working in one process.
func ExampleTyped() {
	type render struct {
		Frame int    `json:"frame"`
		Scene string `json:"scene"`
	}

	ctx := context.Background()
	m, _ := dagworker.New(memory.New())
	defer func() { _ = m.Close(ctx) }()

	frames := dagworker.NewTyped[render](m, "movie")
	_ = frames.AddNode(ctx, "frame-1", render{Frame: 1, Scene: "opening"})

	lease, err := frames.TryClaim(ctx)
	if err != nil {
		fmt.Println("claim:", err)
		return
	}
	fmt.Printf("rendering frame %d of %q\n", lease.Payload.Frame, lease.Payload.Scene)
	_ = frames.Ack(ctx, lease, map[string]string{"output": "frame-1.png"})

	// Output:
	// rendering frame 1 of "opening"
}

// exampleClock is a minimal manual clock, so the lease-timeout example can move
// past a deadline without sleeping through it.
type exampleClock struct {
	mu  sync.Mutex
	now time.Time
}

func newExampleClock() *exampleClock {
	return &exampleClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *exampleClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *exampleClock) Since(t time.Time) time.Duration { return c.Now().Sub(t) }

func (c *exampleClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

func (c *exampleClock) AfterFunc(time.Duration, func()) func() bool {
	return func() bool { return true }
}

func (c *exampleClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
