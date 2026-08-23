// Package file implements the dagworker storage port on durable files, with no
// database and no server. See ADR-0047.
//
// The graph lives in an in-memory backend and every mutation is appended to a
// log and fsynced. On start the log is replayed. This is Redis's AOF, which is
// the standard answer because it is the right one.
//
// # Why the log records readings and not outcomes
//
// A command log alone cannot work: Claim stamps a deadline from the store's own
// clock (ADR-0008), so replaying the command produces a different deadline than
// the original run. The usual fix is to log outcomes instead, which needs the
// in-memory backend's internals.
//
// It is not needed here, because every source of nondeterminism in that backend
// is already injectable and there are only two -- the clock and the retry
// backoff jitter. Everything else is deterministic: the ready set and the
// successor and predecessor lists are slices rather than maps, so no iteration
// order varies between runs, and AddNodes settles in caller order.
//
// So a record carries the command AND the readings it consumed, and replay
// feeds them back. The result is not approximately the original state, it is
// exactly the original state, guaranteed by construction rather than by a
// serialiser being kept in step with the thing it serialises. One
// implementation of the semantics: this backend cannot disagree with the
// in-memory one about what a claim does, because it is the in-memory one with
// a log around it.
package file

import (
	"sync"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// readings is the nondeterminism one command consumed, in the order it was
// consumed. Recording every reading rather than one per command matters: Claim
// reads the clock four times, and replaying a single timestamp for all four
// would drift from what actually happened.
type readings struct {
	Clock  []int64 // Unix nanoseconds, in call order
	Jitter []int64 // results of the jitter function, in call order
}

// gate is the one clock and the one jitter source the in-memory backend sees
// for the whole life of the store, in two modes.
//
// It has to be one object rather than two, because the backend takes its clock
// at construction and offers no way to swap it afterwards. Replaying with a
// recorded clock and then serving live traffic therefore cannot be done by
// building two stores -- the state would be in the wrong one. So the clock
// itself changes mode instead, which needs no cooperation from the backend at
// all.
//
// Of the four Clock methods the in-memory backend calls exactly one, Now, so
// that is the only one whose answers can reach stored state. After, AfterFunc
// and Since are present to satisfy the interface and are never consulted; they
// deliberately consume no readings, so adding a call to one of them later
// cannot silently desynchronise an existing log.
type gate struct {
	live       dw.Clock
	liveJitter func(int64) int64

	mu        sync.Mutex
	replaying bool
	clock     []int64
	jitter    []int64
	lastClock int64
	exhausted bool
	cur       *readings
}

func newGate(clock dw.Clock, jitter func(int64) int64) *gate {
	return &gate{live: clock, liveJitter: jitter, replaying: true}
}

// feed loads the readings for the next record to be replayed.
func (g *gate) feed(r readings) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.clock, g.jitter = r.Clock, r.Jitter
	g.exhausted = false
}

// drifted reports whether replay asked for a reading the log did not have,
// which means the log was written by something that consumed them differently.
func (g *gate) drifted() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.exhausted
}

// golive ends replay. Everything after it is answered by the real clock and
// recorded.
func (g *gate) golive() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.replaying = false
	g.clock, g.jitter = nil, nil
}

// begin starts recording one command; the returned function ends it and yields
// what was read. Mutations are serialised by the Store, so at most one command
// records at a time.
func (g *gate) begin() func() readings {
	g.mu.Lock()
	g.cur = &readings{}
	g.mu.Unlock()
	return func() readings {
		g.mu.Lock()
		defer g.mu.Unlock()
		out := readings{}
		if g.cur != nil {
			out = *g.cur
			g.cur = nil
		}
		return out
	}
}

func (g *gate) Now() time.Time {
	g.mu.Lock()
	if g.replaying {
		defer g.mu.Unlock()
		if len(g.clock) == 0 {
			// Falling back to the wall clock here would produce a state that
			// looks plausible and is wrong, so it is recorded as drift and the
			// caller refuses the log.
			g.exhausted = true
			return time.Unix(0, g.lastClock)
		}
		g.lastClock = g.clock[0]
		g.clock = g.clock[1:]
		return time.Unix(0, g.lastClock)
	}
	g.mu.Unlock()

	t := g.live.Now()
	g.mu.Lock()
	if g.cur != nil {
		g.cur.Clock = append(g.cur.Clock, t.UnixNano())
	}
	g.mu.Unlock()
	return t
}

// Jitter is the retry-backoff randomness, recorded and replayed like the clock.
func (g *gate) Jitter(n int64) int64 {
	g.mu.Lock()
	if g.replaying {
		defer g.mu.Unlock()
		if len(g.jitter) == 0 {
			g.exhausted = true
			return 0
		}
		v := g.jitter[0]
		g.jitter = g.jitter[1:]
		return v
	}
	g.mu.Unlock()

	v := g.liveJitter(n)
	g.mu.Lock()
	if g.cur != nil {
		g.cur.Jitter = append(g.cur.Jitter, v)
	}
	g.mu.Unlock()
	return v
}

// After, AfterFunc and Since satisfy [dagworker.Clock] and are never called by
// the in-memory backend. They consume no readings on purpose: a log written
// before one of them was used must stay replayable by a build that uses it.
func (g *gate) After(d time.Duration) <-chan time.Time { return g.live.After(d) }

func (g *gate) AfterFunc(d time.Duration, f func()) (stop func() bool) {
	return g.live.AfterFunc(d, f)
}
func (g *gate) Since(t time.Time) time.Duration { return g.live.Now().Sub(t) }
