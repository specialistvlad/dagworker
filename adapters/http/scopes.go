package httpadapter

import (
	"context"
	"fmt"
	"net/http"

	dagworker "github.com/specialistvlad/dagworker"
)

// scopeSnapshot reads a scope's stored policy and live counters together,
// since [scopeWire] always carries both.
func (s *Server) scopeSnapshot(ctx context.Context, scope dagworker.Scope) (scopeWire, error) {
	cfg, err := s.mgr.ScopeConfig(ctx, scope)
	if err != nil {
		return scopeWire{}, err
	}
	stats, err := s.mgr.Stats(ctx, scope)
	if err != nil {
		return scopeWire{}, err
	}
	return scopeWire{
		Name:   string(scope),
		Config: scopeConfigToWire(cfg),
		Stats:  scopeStatsToWire(stats),
	}, nil
}

// handleListScopes implements GET /v1/scopes.
func (s *Server) handleListScopes(w http.ResponseWriter, r *http.Request) {
	scopes, err := s.mgr.Scopes(r.Context())
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	names := make([]string, len(scopes))
	for i, sc := range scopes {
		names[i] = string(sc)
	}
	writeJSON(w, http.StatusOK, "application/json", listScopesResponse{Scopes: names})
}

// handlePutScope implements PUT /v1/scopes/{scope}: setting a scope's policy.
// Scopes themselves have no separate creation step — they are created
// implicitly on first write (identifier.go) — so this endpoint is the
// pre-provisioning sugar dossier 14 §1 describes, not a required step before
// PUT-ing a node into a fresh scope name.
func (s *Server) handlePutScope(w http.ResponseWriter, r *http.Request) {
	scope := dagworker.Scope(r.PathValue("scope"))

	var wire scopeConfigWire
	if err := decodeJSON(r, &wire); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	cfg, err := wire.toDomain()
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	if err := s.mgr.Configure(r.Context(), scope, cfg); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	snap, err := s.scopeSnapshot(r.Context(), scope)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, "application/json", snap)
}

// handleGetScope implements GET /v1/scopes/{scope}.
func (s *Server) handleGetScope(w http.ResponseWriter, r *http.Request) {
	scope := dagworker.Scope(r.PathValue("scope"))
	snap, err := s.scopeSnapshot(r.Context(), scope)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, "application/json", snap)
}

// handleScopeAction implements the scope-level custom methods,
// POST /v1/scopes/{scope}:seal and POST /v1/scopes/{scope}:cancel. See
// routes.go for why this is a wildcard-plus-split rather than a native
// pattern.
func (s *Server) handleScopeAction(w http.ResponseWriter, r *http.Request) {
	name, verb, ok := splitVerb(r.PathValue("tail"))
	if !ok {
		s.writeProblemStatus(w, r, http.StatusNotFound, "not-found", "no such scope operation")
		return
	}
	scope := dagworker.Scope(name)

	var err error
	switch verb {
	case "seal":
		err = s.mgr.Seal(r.Context(), scope)
	case "cancel":
		err = s.mgr.CancelScope(r.Context(), scope)
	default:
		s.writeProblemStatus(w, r, http.StatusNotFound, "not-found", fmt.Sprintf("unknown scope operation %q", verb))
		return
	}
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	snap, err := s.scopeSnapshot(r.Context(), scope)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, "application/json", snap)
}
