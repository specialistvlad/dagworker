package httpadapter

import (
	"context"
	"errors"
	"fmt"
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*
	"time"

	dagworker "github.com/specialistvlad/dagworker"
)

// handleGetLease implements GET /v1/scopes/{scope}/leases/{lease}: inspecting
// a held lease. The core library keeps no separate lease record to read back
// (doc.go), so this reconstructs the answer from [dagworker.Manager.Inspect]:
// a lease is "active" exactly when the node is still claimed and the epoch its
// current lease was granted at still matches the epoch this token names.
//
// That comparison is against [dagworker.Inspection.LeaseEpoch] and not against
// the node's attempt count. The two were one field until ADR-0043 separated
// them, and they still agree for a node that has never been deleted -- which
// is exactly what makes reading the wrong one a bug that passes every test
// until an identifier is recycled.
func (s *Server) handleGetLease(w http.ResponseWriter, r *http.Request) {
	scope := dagworker.Scope(r.PathValue("scope"))
	token := r.PathValue("lease")

	node, epoch, err := decodeLeaseID(token)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	insp, err := s.mgr.Inspect(r.Context(), scope, node)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}

	active := insp.Phase == dagworker.PhaseClaimed && insp.LeaseEpoch == epoch
	resp := leaseInspectResponse{
		LeaseID:      token,
		Node:         resourceName(scope, node),
		FencingEpoch: epoch,
		Active:       active,
	}
	if active {
		resp.LeaseDeadline = insp.LeaseDeadline
	}
	writeJSON(w, http.StatusOK, "application/json", resp)
}

// handleLeaseAction implements the four lease custom methods:
// POST .../leases/{lease}:complete, :fail, :skip, and :renew. All four are
// fenced on the epoch packed into the lease token — see doc.go on why that
// token, not a separate X-Fencing-Token header, is this API's idempotency
// key.
func (s *Server) handleLeaseAction(w http.ResponseWriter, r *http.Request) {
	scope := dagworker.Scope(r.PathValue("scope"))
	token, verb, ok := splitVerb(r.PathValue("tail"))
	if !ok {
		s.writeProblemStatus(w, r, http.StatusNotFound, "not-found", "no such lease operation")
		return
	}
	node, epoch, err := decodeLeaseID(token)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	lease := dagworker.Lease{Scope: scope, NodeID: node, Epoch: epoch}

	switch verb {
	case "complete":
		s.handleComplete(w, r, lease)
	case "fail":
		s.handleFail(w, r, lease)
	case "skip":
		s.handleSkip(w, r, lease)
	case "renew":
		s.handleRenew(w, r, lease)
	default:
		s.writeProblemStatus(w, r, http.StatusNotFound, "not-found", fmt.Sprintf("unknown lease operation %q", verb))
	}
}

func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request, lease dagworker.Lease) {
	var body completeRequest
	if err := decodeJSON(r, &body); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	result, err := decodePayload(body.ResultEncoding, body.Result)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	if err := s.mgr.Ack(r.Context(), lease, result); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	s.writeCompletion(w, r, lease)
}

func (s *Server) handleFail(w http.ResponseWriter, r *http.Request, lease dagworker.Lease) {
	var body failRequest
	if err := decodeJSON(r, &body); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	var cause error
	if body.Message != "" {
		cause = errors.New(body.Message)
	}
	outcome, err := s.mgr.Nack(r.Context(), lease, cause)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	s.writeCompletionWithOutcome(w, r, lease, &outcome)
}

func (s *Server) handleSkip(w http.ResponseWriter, r *http.Request, lease dagworker.Lease) {
	var body skipRequest
	if err := decodeJSON(r, &body); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	if err := s.mgr.Skip(r.Context(), lease, body.Reason); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	s.writeCompletion(w, r, lease)
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request, lease dagworker.Lease) {
	var body renewRequest
	if err := decodeJSON(r, &body); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	extended, err := s.mgr.Extend(r.Context(), lease, time.Duration(body.LeaseSeconds)*time.Second)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, "application/json", renewResponse{
		LeaseID:       encodeLeaseID(extended.NodeID, extended.Epoch),
		FencingEpoch:  extended.Epoch,
		LeaseDeadline: extended.Deadline,
	})
}

// writeCompletion reports the node's resulting state after :complete, :fail,
// or :skip. It re-reads through [dagworker.Manager.Inspect] rather than
// trusting the ack call's own return value, because Ack/Nack/Skip report only
// an error (claim.go) — the richer CompleteResult (whether a retry was
// scheduled, and when) is Store-level detail the Manager does not surface on
// those three convenience methods, so the honest way to answer "what happened
// to the node" is to look at the node.
func (s *Server) writeCompletion(w http.ResponseWriter, r *http.Request, lease dagworker.Lease) {
	s.writeCompletionWithOutcome(w, r, lease, nil)
}

// writeCompletionWithOutcome renders the node's post-completion state, with the
// retry fields taken from the completing operation itself when the caller has
// them.
//
// That distinction matters. Reading the node back is a second, unfenced look at
// a node that is claimable again the instant it is scheduled for retry -- so on
// a busy scope another worker can claim and complete it between the two calls,
// and the response would describe that worker's attempt rather than this one's.
// Manager.Nack returns the decision from inside the atomic write, so where it
// is available it wins.
func (s *Server) writeCompletionWithOutcome(
	w http.ResponseWriter, r *http.Request, lease dagworker.Lease, outcome *dagworker.AttemptResult,
) {
	resp, err := s.completionResponse(r.Context(), lease.Scope, lease.NodeID)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	if outcome != nil {
		resp.Retrying = outcome.Retrying
		resp.NextAttemptAt = nil
		if !outcome.NextAttemptAt.IsZero() {
			t := outcome.NextAttemptAt
			resp.NextAttemptAt = &t
		}
	}
	writeJSON(w, http.StatusOK, "application/json", resp)
}

func (s *Server) completionResponse(ctx context.Context, scope dagworker.Scope, id dagworker.NodeID) (completeResponse, error) {
	insp, err := s.mgr.Inspect(ctx, scope, id)
	if err != nil {
		return completeResponse{}, err
	}
	n := insp.Node
	resp := completeResponse{
		Node:        resourceName(scope, id),
		Status:      n.Status,
		Reason:      n.Reason,
		CompletedAt: n.UpdatedAt,
	}
	if insp.Phase == dagworker.PhaseScheduled {
		resp.Retrying = true
		t := insp.ReadyAt
		resp.NextAttemptAt = &t
	}
	return resp, nil
}
