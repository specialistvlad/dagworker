package grpcadapter

import (
	"context"
	"errors"
	"strings"

	dw "github.com/specialistvlad/dagworker"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errorDomain is ErrorInfo.domain on every status this adapter returns
// (AIP-193). It is the stable half of the (reason, domain) pair a client
// should branch on — message text is free to improve across releases.
const errorDomain = "dagworker.v1"

// mapping is one row of docs/spec/02-adapter-contract.md §3. It is a cross-
// adapter contract: the HTTP adapter maps the same sentinel to the same
// problem-type slug, just through a different table with a different target
// vocabulary (HTTP status instead of gRPC code).
type mapping struct {
	err  error
	code codes.Code
	slug string
}

// table is exactly docs/spec/02-adapter-contract.md §3, transcribed in the
// same order, minus ErrNoWork: that row is "not an error — empty result" and
// has no status to construct, so every caller on the claim path must check
// for it with errors.Is before ever reaching mapError (see (*Server).ClaimNode).
var table = []mapping{
	{dw.ErrNotFound, codes.NotFound, "not-found"},
	{dw.ErrIDConflict, codes.AlreadyExists, "id-conflict"},
	{dw.ErrCycle, codes.FailedPrecondition, "cycle"},
	{dw.ErrCrossScopeEdge, codes.InvalidArgument, "cross-scope-edge"},
	{dw.ErrAlreadyTerminal, codes.FailedPrecondition, "already-terminal"},
	{dw.ErrNodeInFlight, codes.FailedPrecondition, "node-in-flight"},
	{dw.ErrHasSuccessors, codes.FailedPrecondition, "has-successors"},
	{dw.ErrLeaseMismatch, codes.Aborted, "lease-superseded"},
	{dw.ErrLeaseExpired, codes.Aborted, "lease-expired"},
	{dw.ErrScopeSealed, codes.FailedPrecondition, "scope-sealed"},
	{dw.ErrPayloadTooLarge, codes.InvalidArgument, "payload-too-large"},
	{dw.ErrSubscriberLagged, codes.Aborted, "subscriber-lagged"},
	{dw.ErrCursorExpired, codes.OutOfRange, "cursor-expired"},
	{dw.ErrInvalidArgument, codes.InvalidArgument, "invalid-argument"},
	{dw.ErrInvalidConfig, codes.InvalidArgument, "invalid-argument"},
	{dw.ErrUnsupported, codes.Unimplemented, "unsupported"},
	{dw.ErrClosed, codes.Unavailable, "shutting-down"},
	{dw.ErrNilStore, codes.InvalidArgument, "invalid-argument"},
}

// mapError converts a dagworker sentinel-wrapped error into the gRPC status
// docs/spec/02-adapter-contract.md §3 requires, with an ErrorInfo detail
// carrying a stable, machine-readable reason — so a client can distinguish
// "your lease was superseded" from "the database is down" without parsing
// prose, which is the contract's whole point.
//
// mapError never special-cases ErrLeaseMismatch as retryable: it is ABORTED
// like every other row, and gRPC's own retry guidance for ABORTED already
// tells a well-behaved client to restart the operation from scratch — which
// for a superseded lease means claiming a new one, not resending this call.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	// A context that ended is not a dagworker sentinel; it is transport-level
	// and gRPC has its own canonical mapping for exactly this.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	for _, m := range table {
		if errors.Is(err, m.err) {
			return withReason(m.code, err.Error(), m.slug)
		}
	}
	// Never reached by a well-behaved backend — every error the Store
	// interface's doc comment promises unwraps to one of the sentinels above.
	// Falling back to Internal rather than Unknown is deliberate: an
	// unmapped error is this adapter's bug to fix, not the caller's fault to
	// retry around.
	return withReason(codes.Internal, err.Error(), "internal")
}

func withReason(code codes.Code, msg, slug string) error {
	st := status.New(code, msg)
	reason := strings.ToUpper(strings.ReplaceAll(slug, "-", "_"))
	if withDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: errorDomain,
	}); detailErr == nil {
		st = withDetails
	}
	return st.Err()
}
