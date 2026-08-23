package dagworker_test

import (
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
)

func TestSystemClock(t *testing.T) {
	t.Parallel()
	var c dw.Clock = dw.SystemClock{}
	if c.Now().IsZero() {
		t.Fatal("SystemClock.Now returned the zero time")
	}
	if c.Since(c.Now().Add(-time.Second)) < time.Second/2 {
		t.Fatal("SystemClock.Since is implausible")
	}
	select {
	case <-c.After(time.Millisecond):
	case <-time.After(time.Second):
		t.Fatal("SystemClock.After never fired")
	}

	fired := make(chan struct{})
	stop := c.AfterFunc(time.Millisecond, func() { close(fired) })
	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("SystemClock.AfterFunc never ran")
	}
	if stop() {
		t.Fatal("stopping an already-fired timer reported success")
	}

	stopped := c.AfterFunc(time.Hour, func() { t.Error("a stopped timer ran") })
	if !stopped() {
		t.Fatal("stopping a pending timer reported failure")
	}
}

func TestFakeClockDrivesTimers(t *testing.T) {
	t.Parallel()
	c := dagstoretest.NewFakeClock()
	start := c.Now()

	ch := c.After(time.Minute)
	select {
	case <-ch:
		t.Fatal("a timer fired before the clock moved")
	default:
	}

	ran := make(chan struct{}, 1)
	stop := c.AfterFunc(30*time.Second, func() { ran <- struct{}{} })

	c.Advance(time.Hour)
	select {
	case <-ch:
	default:
		t.Fatal("advancing past the deadline did not fire the timer")
	}
	select {
	case <-ran:
	default:
		t.Fatal("advancing past the deadline did not run the callback")
	}
	if stop() {
		t.Fatal("stopping a fired timer reported success")
	}
	if !c.Now().After(start) {
		t.Fatal("Advance did not move the clock")
	}

	// A cancelled timer must not fire.
	cancelled := c.AfterFunc(time.Minute, func() { t.Error("a cancelled timer ran") })
	if !cancelled() {
		t.Fatal("cancelling a pending timer reported failure")
	}
	c.Advance(time.Hour)

	// Real clocks jump backwards; Set allows it and fires nothing.
	c.Set(start)
	if !c.Now().Equal(start) {
		t.Fatalf("Set did not move the clock back: %s", c.Now())
	}
	c.Set(start.Add(time.Hour))
	if !c.Now().Equal(start.Add(time.Hour)) {
		t.Fatal("Set forwards did not move the clock")
	}
}
