package httpadapter

import (
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*
	"strings"
)

// routes registers every endpoint on s.mux.
//
// Item-level custom methods (AIP-136's ":verb" suffix on a resource that
// already has a caller-unknown ID, e.g. "/scopes/{scope}:seal") cannot be
// expressed as a single net/http.ServeMux pattern: Go 1.22's pattern syntax
// requires a "{name}" wildcard to occupy an entire path segment, and panics
// at registration time on a segment mixing a wildcard with literal text such
// as "{scope}:seal" (verified against go1.25's net/http; there is no escape
// hatch). Collection-level custom methods (":claim" appended straight to the
// literal "nodes", with no wildcard in that segment at all) have no such
// problem and are registered directly.
//
// So every item-level custom method is registered as a plain wildcard
// capturing the whole trailing segment, and the handler splits it on the
// segment's own last colon — see splitVerb — to recover the ID and the verb.
// This is exactly as reliable as a native pattern would have been: a lease
// token and a scope name never contain a colon (lease IDs are base64url;
// scope names are validated ASCII-safe in practice), so the split is
// unambiguous for every value this server itself hands out. A node ID is the
// one identifier the core library allows to contain a colon (identifier.go
// permits any valid UTF-8), so a colon-suffixed node action requires the
// caller to percent-encode a colon that is part of the ID itself — documented
// on the :cancel operation in openapi.yaml.
func (s *Server) routes() {
	s.mux.HandleFunc("GET /v1/scopes", s.handleListScopes)
	s.mux.HandleFunc("PUT /v1/scopes/{scope}", s.handlePutScope)
	s.mux.HandleFunc("GET /v1/scopes/{scope}", s.handleGetScope)
	s.mux.HandleFunc("POST /v1/scopes/{tail}", s.handleScopeAction)

	s.mux.HandleFunc("PUT /v1/scopes/{scope}/nodes/{node}", s.handlePutNode)
	s.mux.HandleFunc("GET /v1/scopes/{scope}/nodes/{node}", s.handleGetNode)
	s.mux.HandleFunc("DELETE /v1/scopes/{scope}/nodes/{node}", s.handleDeleteNode)
	s.mux.HandleFunc("GET /v1/scopes/{scope}/nodes", s.handleListNodes)
	s.mux.HandleFunc("POST /v1/scopes/{scope}/nodes:claim", s.handleClaim)
	s.mux.HandleFunc("POST /v1/scopes/{scope}/nodes/{tail}", s.handleNodeAction)

	s.mux.HandleFunc("PUT /v1/scopes/{scope}/edges/{tail}", s.handlePutEdge)
	s.mux.HandleFunc("DELETE /v1/scopes/{scope}/edges/{tail}", s.handleDeleteEdge)

	s.mux.HandleFunc("GET /v1/scopes/{scope}/leases/{lease}", s.handleGetLease)
	s.mux.HandleFunc("POST /v1/scopes/{scope}/leases/{tail}", s.handleLeaseAction)

	s.mux.HandleFunc("GET /v1/scopes/{scope}/events", s.handleEvents)

	s.mux.HandleFunc("GET /openapi.yaml", s.handleOpenAPI)
}

// splitVerb splits a "{id}:{verb}" trailing path segment on its last colon.
// ok is false when there is no colon, or the verb half is empty — both mean
// "this was not a custom-method call", which callers turn into 404 or 400 as
// appropriate for that route.
func splitVerb(segment string) (id, verb string, ok bool) {
	i := strings.LastIndexByte(segment, ':')
	if i <= 0 || i == len(segment)-1 {
		return "", "", false
	}
	return segment[:i], segment[i+1:], true
}

// splitEdgeTail splits an "{from}..{to}" edge path segment, the compound-but-
// flat encoding dossier 14 §1.3 argues for over nesting an edge under either
// endpoint. Split on the first "..": node IDs containing ".." themselves are
// not supported by this path shape, the same documented limitation the
// dossier's own encoding carries.
func splitEdgeTail(segment string) (from, to string, ok bool) {
	i := strings.Index(segment, "..")
	if i <= 0 || i+2 >= len(segment) {
		return "", "", false
	}
	return segment[:i], segment[i+2:], true
}

// handleOpenAPI serves the hand-written OpenAPI 3.1 document embedded at
// build time (openapi.go), so the contract this server implements ships with
// the binary rather than living only in the repo.
func (s *Server) handleOpenAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(openapiSpec)
}
