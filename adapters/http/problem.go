package httpadapter

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*

	dagworker "github.com/specialistvlad/dagworker"
)

// problemContentType is RFC 9457's media type. Every error response on this
// server uses it; there is no bespoke error JSON shape (adapter contract §3).
const problemContentType = "application/problem+json"

// problem is an RFC 9457 Problem Details object. Extra carries slug-specific
// members (e.g. "current_epoch" on a superseded lease) so a client can branch
// on machine-readable detail beyond the fixed fields, without every problem
// type needing its own Go struct.
type problem struct {
	Type     string
	Title    string
	Status   int
	Detail   string
	Instance string
	Extra    map[string]any
}

// MarshalJSON flattens Extra alongside the fixed RFC 9457 members, so a
// client sees one JSON object rather than a nested "extra" property.
func (p problem) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, len(p.Extra)+5)
	for k, v := range p.Extra {
		m[k] = v
	}
	m["type"] = p.Type
	m["title"] = p.Title
	m["status"] = p.Status
	if p.Detail != "" {
		m["detail"] = p.Detail
	}
	if p.Instance != "" {
		m["instance"] = p.Instance
	}
	return json.Marshal(m)
}

// slugEntry is one row of the adapter contract's error mapping table
// (docs/spec/02-adapter-contract.md §3). status is the HTTP status; slug
// becomes both the problem "type" suffix and, uppercased with underscores,
// its default title.
type slugEntry struct {
	status int
	slug   string
	title  string
}

// errorTable maps the core error taxonomy to HTTP, in the exact order the
// adapter contract specifies. Order matters only in that every sentinel here
// is checked with errors.Is against the same err — a wrapped error unwraps to
// exactly one sentinel per errors.go's own contract, so there is no shadowing
// to worry about; the order mirrors the contract table for a reader comparing
// the two side by side.
var errorTable = []struct {
	err   error
	entry slugEntry
}{
	{dagworker.ErrNotFound, slugEntry{http.StatusNotFound, "not-found", "Not Found"}},
	{dagworker.ErrIDConflict, slugEntry{http.StatusConflict, "id-conflict", "ID Conflict"}},
	{dagworker.ErrCycle, slugEntry{http.StatusConflict, "cycle", "Dependency Cycle"}},
	{dagworker.ErrCrossScopeEdge, slugEntry{http.StatusBadRequest, "cross-scope-edge", "Cross-Scope Edge"}},
	{dagworker.ErrAlreadyTerminal, slugEntry{http.StatusConflict, "already-terminal", "Already Terminal"}},
	{dagworker.ErrNodeInFlight, slugEntry{http.StatusConflict, "node-in-flight", "Node In Flight"}},
	{dagworker.ErrHasSuccessors, slugEntry{http.StatusConflict, "has-successors", "Has Successors"}},
	{dagworker.ErrLeaseMismatch, slugEntry{http.StatusConflict, "lease-superseded", "Lease Superseded"}},
	{dagworker.ErrLeaseExpired, slugEntry{http.StatusConflict, "lease-expired", "Lease Expired"}},
	{dagworker.ErrScopeSealed, slugEntry{http.StatusConflict, "scope-sealed", "Scope Sealed"}},
	{dagworker.ErrPayloadTooLarge, slugEntry{http.StatusRequestEntityTooLarge, "payload-too-large", "Payload Too Large"}},
	{dagworker.ErrSubscriberLagged, slugEntry{http.StatusConflict, "subscriber-lagged", "Subscriber Lagged"}},
	{dagworker.ErrCursorExpired, slugEntry{http.StatusGone, "cursor-expired", "Cursor Expired"}},
	{dagworker.ErrInvalidArgument, slugEntry{http.StatusBadRequest, "invalid-argument", "Invalid Argument"}},
	{dagworker.ErrInvalidConfig, slugEntry{http.StatusBadRequest, "invalid-argument", "Invalid Argument"}},
	{dagworker.ErrUnsupported, slugEntry{http.StatusNotImplemented, "unsupported", "Unsupported"}},
	{dagworker.ErrClosed, slugEntry{http.StatusServiceUnavailable, "shutting-down", "Shutting Down"}},
}

// mapError resolves err to the table row it matches. An err that unwraps to
// none of the sentinels — a defect somewhere, since every error this server's
// dependencies return is documented to wrap one — falls back to 500 with a
// slug that says exactly that, rather than guessing.
func mapError(err error) slugEntry {
	for _, row := range errorTable {
		if errors.Is(err, row.err) {
			return row.entry
		}
	}
	return slugEntry{http.StatusInternalServerError, "internal", "Internal Server Error"}
}

// writeProblem sends err as an RFC 9457 problem+json response. instance is
// the request path the error concerns; it becomes the "instance" member so a
// client with several in-flight requests can tell which one this is about.
func (s *Server) writeProblem(w http.ResponseWriter, r *http.Request, err error) {
	entry := mapError(err)
	p := problem{
		Type:     s.opts.problemBaseURI + entry.slug,
		Title:    entry.title,
		Status:   entry.status,
		Detail:   err.Error(),
		Instance: r.URL.Path,
	}

	var lm *dagworker.InvalidArgumentError
	if errors.As(err, &lm) {
		p.Extra = map[string]any{"field": lm.Field}
	}
	var cyc *dagworker.CycleError
	if errors.As(err, &cyc) {
		p.Extra = map[string]any{"from": string(cyc.From), "to": string(cyc.To)}
	}
	var tooBig *dagworker.PayloadTooLargeError
	if errors.As(err, &tooBig) {
		p.Extra = map[string]any{"size": tooBig.Size, "cap": tooBig.Cap}
	}

	if entry.status >= http.StatusInternalServerError {
		s.opts.logger.ErrorContext(r.Context(), "dagworker/http: request failed",
			"method", r.Method, "path", r.URL.Path, "error", err)
	}
	writeJSON(w, entry.status, problemContentType, p)
}

// writeProblemStatus writes a problem response for a condition the handler
// detected itself — a malformed request body, a bad query parameter — rather
// than one that came back from the core library. slug and title are chosen by
// the caller from the same registry writeProblem draws from.
func (s *Server) writeProblemStatus(w http.ResponseWriter, r *http.Request, status int, slug, detail string) {
	p := problem{
		Type:     s.opts.problemBaseURI + slug,
		Title:    http.StatusText(status),
		Status:   status,
		Detail:   detail,
		Instance: r.URL.Path,
	}
	writeJSON(w, status, problemContentType, p)
}

// writeJSON marshals v and writes it with the given status and content type,
// logging rather than panicking if marshaling itself fails — which would only
// happen for a defect in one of this package's own response types, never
// because of anything a client sent.
func writeJSON(w http.ResponseWriter, status int, contentType string, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", problemContentType)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"type":"about:blank","title":"Internal Server Error","status":500}`))
		slog.Default().Error("dagworker/http: failed to marshal response", "error", err)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
