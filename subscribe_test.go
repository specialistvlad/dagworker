package dagworker_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/memory"
)

// drain collects events until the deadline or until want have arrived.
func drain(t *testing.T, sub *dw.Subscription, want int, d time.Duration) []dw.Event {
	t.Helper()
	var got []dw.Event
	deadline := time.After(d)
	for len(got) < want {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				return got
			}
			got = append(got, ev)
		case <-deadline:
			return got
		}
	}
	return got
}

func TestSubscribeDeliversTransitions(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	sub, err := f.m.Subscribe(f.ctx, dw.SubscribeOptions{Scope: "s"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	f.add("a")
	events := drain(t, sub, 2, 2*time.Second)
	if len(events) < 2 {
		t.Fatalf("received %d events, want the creation and the readiness", len(events))
	}
	if events[0].Kind != dw.EventCreated || events[0].NodeID != "a" {
		t.Fatalf("first event is %+v, want a creation for a", events[0])
	}
	if events[0].Seq == 0 || events[0].Cursor == 0 {
		t.Fatalf("event carries no sequence or cursor: %+v", events[0])
	}
	if events[1].Kind != dw.EventReady {
		t.Fatalf("second event is %v, want a readiness doorbell", events[1].Kind)
	}
}

func TestSubscribeFilters(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	byScope, err := f.m.Subscribe(f.ctx, dw.SubscribeOptions{Scope: "other"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = byScope.Close() }()

	byKind, err := f.m.Subscribe(f.ctx, dw.SubscribeOptions{
		Scope: "s", Kinds: []dw.EventKind{dw.EventCreated}, NodeKinds: []string{"gpu"},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = byKind.Close() }()

	f.add("cpu-node", dw.WithKind("cpu"))
	f.add("gpu-node", dw.WithKind("gpu"))

	got := drain(t, byKind, 1, time.Second)
	if len(got) != 1 || got[0].NodeID != "gpu-node" || got[0].Kind != dw.EventCreated {
		t.Fatalf("kind filter delivered %+v", got)
	}
	if extra := drain(t, byScope, 1, 200*time.Millisecond); len(extra) != 0 {
		t.Fatalf("a subscription to another scope received %+v", extra)
	}
}

// A slow subscriber must lose events rather than stall the scheduler, and must
// be told truthfully that it lost them.
func TestOverflowDropsOldestAndFlagsTheGap(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	sub, err := f.m.Subscribe(f.ctx, dw.SubscribeOptions{Scope: "s", BufferSize: 2})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	// Produce far more than the buffer holds, without reading any of it. The
	// producer must not block; if it does, this test hangs and that is the
	// failure being guarded against.
	for i := range 50 {
		f.add(dw.NodeID(string(rune('a'+i%26)) + string(rune('a'+i/26))))
	}

	if sub.Dropped() == 0 {
		t.Fatal("a subscriber that read nothing while 50 nodes were created dropped nothing")
	}
	sawGap := false
	for _, ev := range drain(t, sub, 2, time.Second) {
		if ev.Gap {
			sawGap = true
		}
	}
	if !sawGap {
		t.Fatal("events were dropped but no delivered event carried the gap flag")
	}
}

func TestOverflowCloseSlowTerminates(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	pol := dw.OverflowCloseSlow
	sub, err := f.m.Subscribe(f.ctx, dw.SubscribeOptions{Scope: "s", BufferSize: 1, Overflow: &pol})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	for i := range 20 {
		f.add(dw.NodeID(string(rune('a' + i))))
	}
	// Reading to exhaustion: the channel is closed once the policy fires.
	for range sub.Events() { //nolint:revive // draining
	}
	if err := sub.Err(); !errors.Is(err, dw.ErrSubscriberLagged) {
		t.Fatalf("a lagging subscription ended with %v, want ErrSubscriberLagged", err)
	}
}

func TestSubscriptionEndsWithContext(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx, cancel := context.WithCancel(f.ctx)
	sub, err := f.m.Subscribe(ctx, dw.SubscribeOptions{Scope: "s"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()

	select {
	case _, open := <-sub.Events():
		if open {
			// One buffered event may still arrive; drain to closure.
			for range sub.Events() { //nolint:revive // draining
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelling the context did not end the subscription")
	}
	if err := sub.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("subscription ended with %v, want context.Canceled", err)
	}
}

func TestSubscribeValidates(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	bad := dw.OverflowPolicy(99)
	cases := map[string]dw.SubscribeOptions{
		"empty scope with a cursor": {From: 5},
		"unknown event kind":        {Kinds: []dw.EventKind{99}},
		"oversized node kind":       {NodeKinds: []string{string(make([]byte, 200))}},
		"negative buffer":           {BufferSize: -1},
		"unknown overflow policy":   {Overflow: &bad},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := f.m.Subscribe(f.ctx, opts); !errors.Is(err, dw.ErrInvalidArgument) {
				t.Fatalf("got %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestDurableSubscriptionReplays(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	// Write first, subscribe afterwards: a durable stream must be able to
	// deliver what happened before the subscriber existed.
	f.add("early")

	sub, err := f.m.Subscribe(f.ctx, dw.SubscribeOptions{Scope: "s", Durable: true, Replay: true})
	if err != nil {
		t.Fatalf("Subscribe(durable): %v", err)
	}
	defer func() { _ = sub.Close() }()

	got := drain(t, sub, 1, 2*time.Second)
	if len(got) == 0 || got[0].NodeID != "early" {
		t.Fatalf("a replaying subscription received %+v", got)
	}
}

func TestDurableSubscriptionResumesFromACursor(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.add("first")
	stats, err := f.m.Stats(f.ctx, "s")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	sub, err := f.m.Subscribe(f.ctx, dw.SubscribeOptions{Scope: "s", Durable: true, From: stats.Cursor})
	if err != nil {
		t.Fatalf("Subscribe(resume): %v", err)
	}
	defer func() { _ = sub.Close() }()

	f.add("second")
	got := drain(t, sub, 1, 2*time.Second)
	if len(got) == 0 {
		t.Fatal("resuming from the current cursor delivered nothing")
	}
	if got[0].NodeID != "second" {
		t.Fatalf("resumed stream began with %q, want the node created after the cursor", got[0].NodeID)
	}
}

func TestHandleInvokesTheCallback(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	seen := make(chan dw.NodeID, 8)
	stop, err := f.m.Handle(f.ctx, dw.SubscribeOptions{
		Scope: "s", Kinds: []dw.EventKind{dw.EventCreated},
	}, func(ev dw.Event) {
		select {
		case seen <- ev.NodeID:
		default:
		}
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	f.add("handled")
	select {
	case id := <-seen:
		if id != "handled" {
			t.Fatalf("handler saw %q", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler was never called")
	}

	stop()
	stop() // idempotent

	if _, err := f.m.Handle(f.ctx, dw.SubscribeOptions{}, nil); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("Handle with a nil callback gave %v", err)
	}
}

func TestSubscriptionCloseIsIdempotent(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	sub, err := f.m.Subscribe(f.ctx, dw.SubscribeOptions{Scope: "s"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("Close twice: %v", err)
	}
	if sub.Err() != nil {
		t.Fatalf("a cleanly closed subscription reports %v", sub.Err())
	}
}

func TestWatchRejectsAnExpiredCursor(t *testing.T) {
	t.Parallel()
	clk := dagstoretest.NewFakeClock()
	// A tiny log so the cursor falls out of retention almost immediately.
	st := memory.New(memory.WithClock(clk), memory.WithEventLogSize(4))
	t.Cleanup(func() { _ = st.Close(context.Background()) })

	ctx := t.Context()
	for i := range 40 {
		if _, err := st.AddNodes(ctx, "s", []dw.NodeSpec{{ID: dw.NodeID(string(rune('a'+i%26)) + string(rune('0'+i/26)))}}); err != nil {
			t.Fatalf("AddNodes: %v", err)
		}
	}
	if _, err := st.Watch(ctx, dw.WatchRequest{Scope: "s", From: 1}); !errors.Is(err, dw.ErrCursorExpired) {
		t.Fatalf("watching from a cursor past retention gave %v, want ErrCursorExpired", err)
	}
	// Watching every scope from a cursor is meaningless, because cursors are
	// per scope.
	if _, err := st.Watch(ctx, dw.WatchRequest{From: 5}); !errors.Is(err, dw.ErrUnsupported) {
		t.Fatalf("watching all scopes from a cursor gave %v", err)
	}
}

// TestPublishDoesNotScanForeignScopes: a write in one scope must not cost
// anything proportional to how many subscribers other scopes have.
//
// The registry was one flat map. Every completion copied the whole slice under
// a lock and called wants() on every entry, so a Manager serving a thousand
// scopes with one subscriber each taxed every single write with a thousand
// filter calls that could only ever return false. It is the completion path,
// which is the path that is supposed to be O(1).
func TestPublishDoesNotScanForeignScopes(t *testing.T) {
	t.Parallel()

	st := memory.New()
	m, err := dw.New(st, dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})
	ctx := t.Context()

	// Many subscribers, none of them on the scope being written to.
	const foreign = 200
	for i := range foreign {
		sub, err := m.Subscribe(ctx, dw.SubscribeOptions{
			Scope: dw.Scope("other-" + strconv.Itoa(i)),
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		t.Cleanup(func() { sub.Close() })
	}

	// One subscriber that does care.
	mine, err := m.Subscribe(ctx, dw.SubscribeOptions{Scope: "mine"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { mine.Close() })

	if considered := m.SubscribersConsideredFor("mine"); considered > 1 {
		t.Errorf("a write to \"mine\" would iterate %d subscriptions with %d of them on "+
			"other scopes: the registry is not indexed by scope", considered, foreign)
	}

	if err := m.AddNode(ctx, "mine", "n", nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	select {
	case ev := <-mine.Events():
		if ev.NodeID != "n" {
			t.Fatalf("got an event for %q", ev.NodeID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the subscriber on the written scope got nothing")
	}
}

func TestSubscribeRejectsAnExpiredResumeCursor(t *testing.T) {
	t.Parallel()
	clk := dagstoretest.NewFakeClock()
	st := memory.New(memory.WithClock(clk), memory.WithEventLogSize(4))
	m, err := dw.New(st, dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})
	ctx := t.Context()
	for i := range 40 {
		if err := m.AddNode(ctx, "s", dw.NodeID(string(rune('a'+i%26))+string(rune('0'+i/26))), nil); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
	if _, err := m.Subscribe(ctx, dw.SubscribeOptions{Scope: "s", From: 1}); !errors.Is(err, dw.ErrCursorExpired) {
		t.Fatalf("resuming from an evicted cursor gave %v, want ErrCursorExpired", err)
	}
}
