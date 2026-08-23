package dagstoretest

import (
	"sort"
	"sync"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// FakeClock is a [dagworker.Clock] whose time only moves when told to. Backends
// that own their own clock — anything talking to a real server — cannot use it;
// it exists for the in-memory backend and for unit tests of library timing.
//
// Each instance is independent, so tests that hold one each may run in
// parallel.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	nextID int64
}

type fakeTimer struct {
	id   int64
	at   time.Time
	fn   func()
	ch   chan time.Time
	done bool
}

// NewFakeClock returns a clock started at a fixed, arbitrary instant. The
// instant is deliberately not time.Now: a test that accidentally depends on
// wall-clock time should fail obviously rather than pass until midnight.
func NewFakeClock() *FakeClock {
	return &FakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

// Now implements [dagworker.Clock].
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Since implements [dagworker.Clock].
func (c *FakeClock) Since(t time.Time) time.Duration { return c.Now().Sub(t) }

// After implements [dagworker.Clock].
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.nextID++
	c.timers = append(c.timers, &fakeTimer{id: c.nextID, at: c.now.Add(d), ch: ch})
	return ch
}

// AfterFunc implements [dagworker.Clock].
func (c *FakeClock) AfterFunc(d time.Duration, fn func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	t := &fakeTimer{id: c.nextID, at: c.now.Add(d), fn: fn}
	c.timers = append(c.timers, t)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if t.done {
			return false
		}
		t.done = true
		return true
	}
}

// Advance moves time forward and fires every timer whose deadline the move
// crossed, in deadline order. Callbacks run outside the clock's lock so that a
// callback may itself schedule another timer without deadlocking.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	target := c.now

	var due []*fakeTimer
	kept := c.timers[:0]
	for _, t := range c.timers {
		switch {
		case t.done:
			// dropped
		case !t.at.After(target):
			t.done = true
			due = append(due, t)
		default:
			kept = append(kept, t)
		}
	}
	c.timers = kept
	c.mu.Unlock()

	sort.Slice(due, func(i, j int) bool { return due[i].at.Before(due[j].at) })
	for _, t := range due {
		if t.ch != nil {
			t.ch <- t.at
		}
		if t.fn != nil {
			t.fn()
		}
	}
}

// Set moves the clock to an absolute instant. Moving backwards is allowed and
// fires nothing: real clocks do jump backwards, and code that cannot survive it
// should be found out by a test rather than by an NTP correction in production.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	behind := t.Before(c.now)
	c.mu.Unlock()
	if behind {
		c.mu.Lock()
		c.now = t
		c.mu.Unlock()
		return
	}
	c.Advance(t.Sub(c.Now()))
}

var _ dw.Clock = (*FakeClock)(nil)
