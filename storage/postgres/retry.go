package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// PostgreSQL's two transient transaction failures. Both mean "this
// transaction could not be serialised against a concurrent one; nothing was
// written; run it again" — the server's own documentation says an application
// must be prepared to retry them, and neither is a defect in the statement
// that hit it.
const (
	// sqlstateSerializationFailure is 40001.
	sqlstateSerializationFailure = "40001"
	// sqlstateDeadlockDetected is 40P01. Reachable here because a graph
	// mutation locks node rows in the order the caller named them while a
	// completion locks a node and then its successors: two callers can want
	// the same pair in opposite orders, and the deadlock detector picks one to
	// abort. Serialising graph edits per scope with an advisory lock removes
	// this between two graph edits, not between a graph edit and a completion,
	// which deliberately takes no advisory lock so that claims and completions
	// never wait on structural work.
	sqlstateDeadlockDetected = "40P01"
)

// maxTransactionAttempts bounds retrying. Three is not a tuning parameter so
// much as an assertion: a conflict that survives two independent retries is
// not the transient kind, and looping harder on it turns a failed call into a
// slow failed call.
const maxTransactionAttempts = 3

// isTransientConflict reports whether err is one of the two failures above.
func isTransientConflict(err error) bool {
	var pg *pgconn.PgError
	if !errors.As(err, &pg) {
		return false
	}
	return pg.Code == sqlstateSerializationFailure || pg.Code == sqlstateDeadlockDetected
}

// retryTransient runs fn, retrying it whole on a serialization failure or a
// deadlock.
//
// Retrying the whole operation is what makes this safe rather than merely
// hopeful. An aborted transaction wrote nothing, so a retry starts from the
// same state the first attempt saw: `AddNodes` and `AddEdges` are idempotent
// by construction (ADR-0025), and a rolled-back `Complete` left the node's
// epoch untouched, so the second attempt's fencing check succeeds exactly
// where the first one would have. Nothing here retries a partially-applied
// write, because there is no such thing to retry.
//
// The wait between attempts is jittered from the Store's own source: two
// transactions that just deadlocked against each other retrying in lockstep
// would deadlock again for the same reason.
func retryTransient[T any](ctx context.Context, s *Store, op string, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	for attempt := 1; ; attempt++ {
		out, err := fn(ctx)
		if err == nil || !isTransientConflict(err) || attempt == maxTransactionAttempts {
			if err != nil && isTransientConflict(err) {
				return zero, fmt.Errorf("postgres: %s: gave up after %d attempts: %w",
					op, attempt, err)
			}
			return out, err
		}
		// Grow the window each round so a three-way pileup separates rather
		// than re-colliding at the same scale.
		window := time.Duration(attempt) * time.Millisecond
		select {
		case <-time.After(time.Duration(s.jitter(int64(window)))):
		case <-ctx.Done():
			return zero, ctx.Err()
		}
		// Checked again after the wait rather than only inside the select: a
		// zero jitter makes both cases ready at once, and select picks between
		// ready cases at random, so without this a cancelled caller would
		// sometimes get one more attempt.
		if err := ctx.Err(); err != nil {
			return zero, err
		}
	}
}
