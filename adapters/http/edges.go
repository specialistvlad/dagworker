package httpadapter

import (
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*

	dagworker "github.com/specialistvlad/dagworker"
)

// handlePutEdge implements PUT /v1/scopes/{scope}/edges/{from}..{to}. See
// routes.go's splitEdgeTail for why the endpoints share one flat segment
// rather than nesting under either side.
func (s *Server) handlePutEdge(w http.ResponseWriter, r *http.Request) {
	from, to, ok := splitEdgeTail(r.PathValue("tail"))
	if !ok {
		s.writeProblemStatus(w, r, http.StatusNotFound, "not-found", `edge path must be "{from}..{to}"`)
		return
	}
	scope := dagworker.Scope(r.PathValue("scope"))
	if err := s.mgr.AddEdge(r.Context(), scope, dagworker.NodeID(from), dagworker.NodeID(to)); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// handleDeleteEdge implements DELETE /v1/scopes/{scope}/edges/{from}..{to}.
func (s *Server) handleDeleteEdge(w http.ResponseWriter, r *http.Request) {
	from, to, ok := splitEdgeTail(r.PathValue("tail"))
	if !ok {
		s.writeProblemStatus(w, r, http.StatusNotFound, "not-found", `edge path must be "{from}..{to}"`)
		return
	}
	scope := dagworker.Scope(r.PathValue("scope"))
	if err := s.mgr.RemoveEdge(r.Context(), scope, dagworker.NodeID(from), dagworker.NodeID(to)); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
