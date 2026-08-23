package httpadapter

import (
	"context"
	"errors"
	"fmt"
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*
	"time"

	dagworker "github.com/specialistvlad/dagworker"
)

// clampWait resolves the client's requested "wait" against the server's
// ceiling and reserves headroom for marshaling and flushing the response
// before the client's own deadline, or an intermediary's idle timeout, fires
// (dossier 14 §2.2). An omitted or zero wait means "check once, don't hold
// the connection open" — the same non-blocking default Consul's own blocking
// queries use when a caller does not opt in.
func clampWait(raw string, maxWait, budget time.Duration) (time.Duration, error) {
	d, err := parseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: wait: %v", dagworker.ErrInvalidArgument, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("%w: wait must not be negative", dagworker.ErrInvalidArgument)
	}
	effective := min(d, maxWait)
	if effective > budget {
		effective -= budget
	}
	return effective, nil
}

// claimOptionsFromWire builds the [dagworker.ClaimOption]s a claim request
// asks for. Kinds, not the dossier's illustrative "labels" field, is the real
// claim predicate — [dagworker.Node.Labels]' own doc comment is explicit that
// labels are for subscription filtering, not an indexed claim predicate, so
// this wire deliberately does not offer a "labels" filter that would silently
// do nothing.
func claimOptionsFromWire(req claimRequest) []dagworker.ClaimOption {
	var opts []dagworker.ClaimOption
	if req.WorkerID != "" {
		opts = append(opts, dagworker.AsWorker(req.WorkerID))
	}
	if len(req.Kinds) > 0 {
		opts = append(opts, dagworker.OfKind(req.Kinds...))
	}
	if req.LeaseSeconds > 0 {
		opts = append(opts, dagworker.WithLeaseTimeout(time.Duration(req.LeaseSeconds)*time.Second))
	}
	return opts
}

// handleClaim implements POST /v1/scopes/{scope}/nodes:claim: a Consul-style
// blocking query (dossier 14 §2). It returns 200 with at least one lease, or
// 204 with no body when nothing was ready even after waiting — by
// construction, never 200 with an empty array, since the 200 branch below is
// only reached once leases is non-empty.
//
// The immediate attempt is a batch [dagworker.Manager.ClaimBatch] call, not
// [dagworker.Manager.Claim]: Claim's own blocking loop checks ctx.Err() before
// its first attempt, so handing it an already-expired context (an omitted or
// zero "wait", clamped straight to a zero-duration deadline) would skip the
// attempt entirely rather than performing the one immediate check every
// blocking-query design promises. ClaimBatch has no such precondition, so it
// is what actually guarantees "check once" means "check", not "maybe check
// depending on a race with the deadline."
func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	scope := dagworker.Scope(r.PathValue("scope"))

	var req claimRequest
	if err := decodeJSON(r, &req); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	wait, err := clampWait(req.Wait, s.opts.maxWait, s.opts.waitBudget)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	maxNodes := max(req.MaxNodes, 1)
	claimOpts := claimOptionsFromWire(req)

	leases, err := s.mgr.ClaimBatch(r.Context(), scope, maxNodes, claimOpts...)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}

	if len(leases) == 0 && wait > 0 {
		leases, err = s.blockForOne(r, scope, wait, maxNodes, claimOpts)
		if err != nil {
			s.writeProblem(w, r, err)
			return
		}
	}

	if len(leases) == 0 {
		// Having no work is ordinary, never an error (adapter contract §2):
		// the immediate check found nothing, and either wait was zero or the
		// blocking wait elapsed with nothing either.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	wire := make([]leaseWire, len(leases))
	for i, l := range leases {
		wire[i] = leaseToWire(l)
	}
	writeJSON(w, http.StatusOK, "application/json", claimResponse{Leases: wire})
}

// blockForOne waits up to wait for a single ready node via
// [dagworker.Manager.Claim] — the library's own three-part wakeup protocol
// (immediate attempt, doorbell, jittered poll) — and, if one turns up, tops
// the batch up to maxNodes with a further non-blocking [Manager.ClaimBatch]
// call. It returns a nil, non-error slice when the wait elapsed with nothing
// found, which the caller turns into 204.
func (s *Server) blockForOne(
	r *http.Request, scope dagworker.Scope, wait time.Duration, maxNodes int, claimOpts []dagworker.ClaimOption,
) ([]dagworker.Lease, error) {
	// Server-side jitter, added to the server's own deadline rather than left
	// to the client, so a fleet that reconnected in the same instant times out
	// at visibly different moments instead of retrying in lockstep forever
	// (dossier 14 §2.3).
	wait += s.opts.jitter(wait / 16)

	cctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-s.done:
			cancel()
		case <-cctx.Done():
		case <-stop:
		}
	}()

	lease, err := s.mgr.Claim(cctx, scope, claimOpts...)
	switch {
	case err == nil:
		leases := []dagworker.Lease{lease}
		if maxNodes > 1 {
			// Genuinely best-effort: a failure here does not undo the lease
			// already granted above, so it is logged and otherwise ignored
			// rather than turning a successful claim into a failed response.
			extra, topUpErr := s.mgr.ClaimBatch(r.Context(), scope, maxNodes-1, claimOpts...)
			if topUpErr != nil {
				s.opts.logger.WarnContext(r.Context(), "dagworker/http: batch top-up after claim failed",
					"scope", scope, "error", topUpErr)
			} else {
				leases = append(leases, extra...)
			}
		}
		return leases, nil
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return nil, nil
	default:
		return nil, err
	}
}
