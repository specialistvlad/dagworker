package httpadapter

import (
	"context"
	"errors"
	"fmt"
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*

	dagworker "github.com/specialistvlad/dagworker"
)

// checkIfMatch enforces RFC 9110 §13's conditional-request precedence for a
// mutation against an existing resource: If-Match wins, and "*" means "any
// representation must currently exist." existing is nil when the resource was
// not found — the caller passes that through unconditionally, since "does not
// exist" fails every If-Match value including "*".
func checkIfMatch(r *http.Request, existing *dagworker.Node) error {
	want := r.Header.Get("If-Match")
	if want == "" {
		return nil
	}
	if existing == nil {
		return errPreconditionFailed
	}
	if want != "*" && want != nodeETag(*existing) {
		return errPreconditionFailed
	}
	return nil
}

// errPreconditionFailed is this package's own sentinel for a 412: it is not
// part of the core library's taxonomy (the core has no concept of an ETag),
// so it does not appear in the adapter-contract error table and is mapped
// directly rather than through mapError.
var errPreconditionFailed = errors.New("httpadapter: precondition failed")

func (s *Server) writePreconditionFailed(w http.ResponseWriter, r *http.Request, current *dagworker.Node) {
	detail := "the resource has been modified"
	extra := map[string]any{}
	if current != nil {
		detail = "current ETag is " + nodeETag(*current)
		extra["current_etag"] = nodeETag(*current)
	}
	p := problem{
		Type:     s.opts.problemBaseURI + "precondition-failed",
		Title:    "Precondition Failed",
		Status:   http.StatusPreconditionFailed,
		Detail:   detail,
		Instance: r.URL.Path,
		Extra:    extra,
	}
	writeJSON(w, http.StatusPreconditionFailed, problemContentType, p)
}

// lookupNode returns the node and true, or false when it does not exist. Any
// other error is returned as-is for the caller to map.
func (s *Server) lookupNode(ctx context.Context, scope dagworker.Scope, id dagworker.NodeID) (dagworker.Node, bool, error) {
	n, err := s.mgr.GetNode(ctx, scope, id)
	if err != nil {
		if errors.Is(err, dagworker.ErrNotFound) {
			return dagworker.Node{}, false, nil
		}
		return dagworker.Node{}, false, err
	}
	return n, true, nil
}

// handlePutNode implements PUT /v1/scopes/{scope}/nodes/{node}: idempotent
// creation, per RFC 9110 §9.2.2 (dossier 14 §1.3). Re-PUTing a byte-identical
// spec is a no-op at the core (node.go's own Validate/AddNodes contract); this
// handler additionally honors If-Match when the caller sent one, so a client
// that read a node and wants to assert nothing else changed it first can.
func (s *Server) handlePutNode(w http.ResponseWriter, r *http.Request) {
	scope := dagworker.Scope(r.PathValue("scope"))
	id := dagworker.NodeID(r.PathValue("node"))

	existing, existed, err := s.lookupNode(r.Context(), scope, id)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	var existingPtr *dagworker.Node
	if existed {
		existingPtr = &existing
	}
	if err := checkIfMatch(r, existingPtr); err != nil {
		s.writePreconditionFailed(w, r, existingPtr)
		return
	}

	var req createNodeRequest
	if err := decodeJSON(r, &req); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	spec, err := req.toNodeSpec(id)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	if err := s.mgr.AddNode(r.Context(), scope, spec.ID, spec.Payload,
		nodeOptionsFromSpec(spec)...); err != nil {
		s.writeProblem(w, r, err)
		return
	}

	created, err := s.mgr.GetNode(r.Context(), scope, id)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	status := http.StatusCreated
	if existed {
		status = http.StatusOK
	}
	w.Header().Set("ETag", nodeETag(created))
	w.Header().Set("Location", r.URL.Path)
	writeJSON(w, status, "application/json", nodeToWire(created))
}

// nodeOptionsFromSpec re-expresses a NodeSpec as the functional options
// [dagworker.Manager.AddNode] takes. It exists because the HTTP layer builds
// a full NodeSpec once (toNodeSpec validates and decodes everything in one
// place) but AddNode's single-node convenience form wants options, not a
// spec — AddNodes (the batch form) would take the spec directly, but a single
// PUT has no batch to amortize.
func nodeOptionsFromSpec(spec dagworker.NodeSpec) []dagworker.NodeOption {
	opts := []dagworker.NodeOption{
		dagworker.WithKind(spec.Kind),
		dagworker.WithPriority(spec.Priority),
		dagworker.WithTrigger(spec.Trigger),
	}
	if len(spec.Deps) > 0 {
		opts = append(opts, dagworker.WithDeps(spec.Deps...))
	}
	if len(spec.Labels) > 0 {
		opts = append(opts, dagworker.WithLabels(spec.Labels))
	}
	if spec.Retry != (dagworker.RetryPolicy{}) {
		opts = append(opts, dagworker.WithRetry(spec.Retry.MaxAttempts, spec.Retry.BaseDelay, spec.Retry.MaxDelay))
	}
	return opts
}

// handleGetNode implements GET /v1/scopes/{scope}/nodes/{node}.
func (s *Server) handleGetNode(w http.ResponseWriter, r *http.Request) {
	scope := dagworker.Scope(r.PathValue("scope"))
	id := dagworker.NodeID(r.PathValue("node"))

	n, err := s.mgr.GetNode(r.Context(), scope, id)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	etag := nodeETag(n)
	if inm := r.Header.Get("If-None-Match"); inm != "" && inm == etag {
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("ETag", etag)
	writeJSON(w, http.StatusOK, "application/json", nodeToWire(n))
}

// handleDeleteNode implements DELETE /v1/scopes/{scope}/nodes/{node}.
func (s *Server) handleDeleteNode(w http.ResponseWriter, r *http.Request) {
	scope := dagworker.Scope(r.PathValue("scope"))
	id := dagworker.NodeID(r.PathValue("node"))

	if r.Header.Get("If-Match") != "" {
		existing, existed, err := s.lookupNode(r.Context(), scope, id)
		if err != nil {
			s.writeProblem(w, r, err)
			return
		}
		var existingPtr *dagworker.Node
		if existed {
			existingPtr = &existing
		}
		if err := checkIfMatch(r, existingPtr); err != nil {
			s.writePreconditionFailed(w, r, existingPtr)
			return
		}
	}

	policy, err := cascadeFromWire(r.URL.Query().Get("cascade"))
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	if err := s.mgr.RemoveNode(r.Context(), scope, id, policy); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleListNodes implements GET /v1/scopes/{scope}/nodes: keyset pagination
// only, per store.go's own ListOptions — there is no offset parameter to
// accept in the first place, so this handler cannot expose one even by
// accident (docs/research/14 §7).
func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	scope := dagworker.Scope(r.PathValue("scope"))

	statuses, err := statusesFromWire(queryCSV(r, "status"))
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	limit, err := queryInt(r, "page_size", 0)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}

	res, err := s.mgr.ListNodes(r.Context(), scope, dagworker.ListOptions{
		Statuses: statuses,
		Kinds:    queryCSV(r, "kind"),
		Cursor:   r.URL.Query().Get("cursor"),
		Limit:    limit,
	})
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}

	wire := make([]nodeWire, len(res.Nodes))
	for i, n := range res.Nodes {
		wire[i] = nodeToWire(n)
	}
	writeJSON(w, http.StatusOK, "application/json", listNodesResponse{
		Nodes:         wire,
		NextPageToken: res.Next,
	})
}

// handleNodeAction implements the node-level custom method,
// POST /v1/scopes/{scope}/nodes/{node}:cancel.
func (s *Server) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	scope := dagworker.Scope(r.PathValue("scope"))
	id, verb, ok := splitVerb(r.PathValue("tail"))
	if !ok {
		s.writeProblemStatus(w, r, http.StatusNotFound, "not-found", "no such node operation")
		return
	}
	if verb != "cancel" {
		s.writeProblemStatus(w, r, http.StatusNotFound, "not-found", fmt.Sprintf("unknown node operation %q", verb))
		return
	}
	if err := s.mgr.Cancel(r.Context(), scope, dagworker.NodeID(id)); err != nil {
		s.writeProblem(w, r, err)
		return
	}
	n, err := s.mgr.GetNode(r.Context(), scope, dagworker.NodeID(id))
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, "application/json", nodeToWire(n))
}
