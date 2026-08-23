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
// 204 with no body when the wait elapsed with nothing ready — by
// construction, never 200 with an empty array, because the 200 branch below
// is only reached after [dagworker.Manager.Claim] has already returned one.
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
	// Server-side jitter, added to the server's own deadline rather than left
	// to the client, so a fleet that reconnected in the same instant times out
	// at visibly different moments instead of retrying in lockstep forever
	// (dossier 14 §2.3).
	wait += s.opts.jitter(wait / 16)

	maxNodes := max(req.MaxNodes, 1)
	claimOpts := claimOptionsFromWire(req)

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
		s.respondClaimed(w, r, scope, lease, maxNodes, claimOpts)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		// Having no work is ordinary, never an error (adapter contract §2):
		// timeout-with-nothing-found and "the client hung up while we waited"
		// both resolve the same way, with nothing left to tell anyone.
		w.WriteHeader(http.StatusNoContent)
	default:
		s.writeProblem(w, r, err)
	}
}

// respondClaimed writes the 200 response for a successful claim, topping the
// batch up to maxNodes with an additional non-blocking [Manager.ClaimBatch]
// call when the caller asked for more than one. The top-up is genuinely
// best-effort: a failure there does not undo the lease already granted by the
// blocking call above, so its error is logged and otherwise ignored rather
// than turning a successful claim into a failed response.
func (s *Server) respondClaimed(
	w http.ResponseWriter, r *http.Request, scope dagworker.Scope,
	first dagworker.Lease, maxNodes int, claimOpts []dagworker.ClaimOption,
) {
	leases := []dagworker.Lease{first}
	if maxNodes > 1 {
		extra, err := s.mgr.ClaimBatch(r.Context(), scope, maxNodes-1, claimOpts...)
		if err != nil {
			s.opts.logger.WarnContext(r.Context(), "dagworker/http: batch top-up after claim failed",
				"scope", scope, "error", err)
		} else {
			leases = append(leases, extra...)
		}
	}
	wire := make([]leaseWire, len(leases))
	for i, l := range leases {
		wire[i] = leaseToWire(l)
	}
	writeJSON(w, http.StatusOK, "application/json", claimResponse{Leases: wire})
}
