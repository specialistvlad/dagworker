package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// pgErr builds a PgError carrying one SQLSTATE, which is all these tests read.
func pgErr(code string) error { return &pgconn.PgError{Code: code, Message: "test"} }

// fakeStore is enough of a Store for retryTransient: it only reads s.jitter.
func fakeStore() *Store { return &Store{jitter: func(int64) int64 { return 0 }} }

func TestIsTransientConflict(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"deadlock", pgErr(sqlstateDeadlockDetected), true},
		{"serialization failure", pgErr(sqlstateSerializationFailure), true},
		{"wrapped deadlock", fmt.Errorf("postgres: AddEdges: %w", pgErr(sqlstateDeadlockDetected)), true},
		// A unique violation is the caller's problem and retrying it produces
		// the identical failure forever.
		{"unique violation", pgErr("23505"), false},
		{"undefined table", pgErr("42P01"), false},
		{"a plain error", errors.New("connection reset"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		if got := isTransientConflict(tc.err); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestRetryTransientRetriesUntilItSucceeds(t *testing.T) {
	t.Parallel()

	calls := 0
	got, err := retryTransient(context.Background(), fakeStore(), "op",
		func(context.Context) (int, error) {
			calls++
			if calls < maxTransactionAttempts {
				return 0, fmt.Errorf("postgres: op: %w", pgErr(sqlstateDeadlockDetected))
			}
			return 42, nil
		})
	if err != nil {
		t.Fatalf("retryTransient: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want the successful attempt's result", got)
	}
	if calls != maxTransactionAttempts {
		t.Fatalf("made %d attempts, want %d", calls, maxTransactionAttempts)
	}
}

func TestRetryTransientGivesUp(t *testing.T) {
	t.Parallel()

	// A conflict that survives every retry is not the transient kind, and
	// looping harder on it just makes the failure slower.
	calls := 0
	_, err := retryTransient(context.Background(), fakeStore(), "op",
		func(context.Context) (int, error) {
			calls++
			return 0, pgErr(sqlstateDeadlockDetected)
		})
	if err == nil {
		t.Fatal("a permanent conflict returned no error")
	}
	if calls != maxTransactionAttempts {
		t.Fatalf("made %d attempts, want exactly %d", calls, maxTransactionAttempts)
	}
	if !isTransientConflict(err) {
		t.Fatalf("the give-up error no longer unwraps to the conflict: %v", err)
	}
}

func TestRetryTransientDoesNotRetryOtherErrors(t *testing.T) {
	t.Parallel()

	calls := 0
	sentinel := errors.New("not a conflict")
	_, err := retryTransient(context.Background(), fakeStore(), "op",
		func(context.Context) (int, error) {
			calls++
			return 0, sentinel
		})
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want the original error unchanged", err)
	}
	if calls != 1 {
		t.Fatalf("made %d attempts on a non-retryable error, want 1", calls)
	}
}

func TestRetryTransientStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	_, err := retryTransient(ctx, fakeStore(), "op", func(context.Context) (int, error) {
		calls++
		cancel() // the caller gives up while the backoff is waiting
		return 0, pgErr(sqlstateDeadlockDetected)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("made %d attempts after cancellation, want 1", calls)
	}
}
