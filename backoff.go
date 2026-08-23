package dagworker

import "time"

// Backoff returns the delay before a given attempt is retried, using full
// jitter: a value drawn uniformly from [0, min(maxDelay, base*2^(attempt-1))).
//
// Full jitter, rather than plain exponential backoff or equal jitter, is the
// AWS Architecture Blog's measured recommendation: spreading retries across the
// whole window rather than clustering them at its end both completes work
// sooner and puts less load on the contended resource. Under contention the
// difference is not marginal — synchronised retries are how a transient blip
// becomes a sustained outage.
//
// jitter must return a value in [0, n). Every backend must compute backoff with
// this function, or reproduce it exactly, so that a node's retry schedule does
// not depend on which backend happens to be storing it.
func Backoff(attempt uint32, base, maxDelay time.Duration, jitter func(n int64) int64) time.Duration {
	if attempt == 0 {
		attempt = 1
	}
	if base <= 0 {
		base = DefaultRetryBaseDelay
	}
	if maxDelay <= 0 {
		maxDelay = DefaultRetryMaxDelay
	}

	window := base
	// Shift rather than multiply, and stop as soon as the cap is reached, so a
	// large attempt count cannot overflow into a negative duration.
	for i := uint32(1); i < attempt; i++ {
		if window >= maxDelay/2 {
			window = maxDelay
			break
		}
		window *= 2
	}
	window = min(window, maxDelay)

	if jitter == nil || window <= 0 {
		return window
	}
	return time.Duration(jitter(int64(window)))
}
