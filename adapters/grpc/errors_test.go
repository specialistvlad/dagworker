package grpcadapter

import (
	"context"
	"errors"
	"testing"

	dw "github.com/specialistvlad/dagworker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestMapErrorTable pins docs/spec/02-adapter-contract.md §3 row by row: this
// is the cross-adapter contract, so a regression here is a contract break,
// not just a local bug.
func TestMapErrorTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		code codes.Code
	}{
		{"not found", dw.ErrNotFound, codes.NotFound},
		{"id conflict", dw.ErrIDConflict, codes.AlreadyExists},
		{"cycle", &dw.CycleError{From: "a", To: "b"}, codes.FailedPrecondition},
		{"cross scope edge", dw.ErrCrossScopeEdge, codes.InvalidArgument},
		{"already terminal", dw.ErrAlreadyTerminal, codes.FailedPrecondition},
		{"node in flight", dw.ErrNodeInFlight, codes.FailedPrecondition},
		{"has successors", dw.ErrHasSuccessors, codes.FailedPrecondition},
		{"lease mismatch", dw.ErrLeaseMismatch, codes.Aborted},
		{"lease expired", dw.ErrLeaseExpired, codes.Aborted},
		{"scope sealed", dw.ErrScopeSealed, codes.FailedPrecondition},
		{"payload too large", &dw.PayloadTooLargeError{Size: 10, Cap: 1}, codes.InvalidArgument},
		{"subscriber lagged", dw.ErrSubscriberLagged, codes.Aborted},
		{"cursor expired", dw.ErrCursorExpired, codes.OutOfRange},
		{"invalid argument", dw.ErrInvalidArgument, codes.InvalidArgument},
		{"unsupported", dw.ErrUnsupported, codes.Unimplemented},
		{"closed", dw.ErrClosed, codes.Unavailable},
		{"context canceled", context.Canceled, codes.Canceled},
		{"context deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mapError(tc.err)
			st, ok := status.FromError(got)
			if !ok {
				t.Fatalf("mapError(%v) did not produce a *status.Status", tc.err)
			}
			if st.Code() != tc.code {
				t.Fatalf("mapError(%v) code = %v, want %v", tc.err, st.Code(), tc.code)
			}
		})
	}
}

// TestMapErrorNil documents that a nil error stays nil rather than becoming a
// misleading OK-coded status.
func TestMapErrorNil(t *testing.T) {
	t.Parallel()
	if err := mapError(nil); err != nil {
		t.Fatalf("mapError(nil) = %v, want nil", err)
	}
}

// TestMapErrorWrapped confirms the table matches through errors.Is, not
// identity — every real call site wraps a sentinel in a fmt.Errorf("%w", ...).
func TestMapErrorWrapped(t *testing.T) {
	t.Parallel()
	wrapped := errors.Join(errors.New("context"), dw.ErrLeaseMismatch)
	st, ok := status.FromError(mapError(wrapped))
	if !ok || st.Code() != codes.Aborted {
		t.Fatalf("mapError(wrapped ErrLeaseMismatch) = %v, want Aborted", mapError(wrapped))
	}
}

// TestMapErrorUnknownFallsBackToInternal guards the fallback path: an error
// that matches no sentinel is this adapter's own bug to fix, not the
// caller's to retry around, so it becomes INTERNAL rather than UNKNOWN.
func TestMapErrorUnknownFallsBackToInternal(t *testing.T) {
	t.Parallel()
	st, ok := status.FromError(mapError(errors.New("something nobody mapped")))
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("mapError(unmapped) code = %v, want Internal", st.Code())
	}
}
