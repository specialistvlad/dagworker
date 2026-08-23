package dagworker

import "time"

// Clock is the library's only source of time. Every timer, deadline, and
// backoff goes through it so that tests can drive time deterministically
// instead of sleeping.
//
// Note the division of responsibility, which is easy to get wrong: this clock
// schedules the library's own work — retry backoffs, sweep tickers, poll
// intervals. It does *not* decide whether a lease has expired. That comparison
// is always made by the storage backend against its own clock, because two
// parties reading two clocks can never agree on a hard boundary. See
// docs/adr/0008.
type Clock interface {
	// Now returns the current time.
	Now() time.Time

	// After returns a channel that receives once, after d elapses.
	After(d time.Duration) <-chan time.Time

	// AfterFunc runs f in its own goroutine after d elapses. The returned stop
	// function reports whether it prevented f from running.
	AfterFunc(d time.Duration, f func()) (stop func() bool)

	// Since is a convenience for Now().Sub(t).
	Since(t time.Time) time.Duration
}

// SystemClock is the production [Clock], backed by the time package. It is the
// default and the zero value is usable.
type SystemClock struct{}

func (SystemClock) Now() time.Time                         { return time.Now() }
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (SystemClock) Since(t time.Time) time.Duration        { return time.Since(t) }

func (SystemClock) AfterFunc(d time.Duration, f func()) func() bool {
	t := time.AfterFunc(d, f)
	return t.Stop
}

var _ Clock = SystemClock{}
