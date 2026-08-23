package httpadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*
	"strconv"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
)

// eventWire is one event rendered for the wire, shared by SSE "data:" lines
// and NDJSON lines — the same subscription backs both transports (doc.go),
// so there is exactly one JSON shape for an event regardless of which one
// delivered it.
type eventWire struct {
	Scope    string    `json:"scope"`
	Node     string    `json:"node"`
	Kind     string    `json:"kind"`
	NodeKind string    `json:"node_kind,omitempty"`
	From     string    `json:"from,omitempty"`
	To       string    `json:"to,omitempty"`
	Reason   string    `json:"reason,omitempty"`
	Message  string    `json:"message,omitempty"`
	Attempt  uint32    `json:"attempt,omitempty"`
	At       time.Time `json:"at"`
	Cursor   uint64    `json:"cursor"`
	Gap      bool      `json:"gap,omitempty"`
}

func eventToWire(ev dagworker.Event) eventWire {
	w := eventWire{
		Scope:    string(ev.Scope),
		Node:     string(ev.NodeID),
		Kind:     eventKindToWire(ev.Kind),
		NodeKind: ev.NodeKind,
		Message:  ev.Message,
		Attempt:  ev.Attempt,
		At:       ev.At,
		Cursor:   uint64(ev.Cursor),
		Gap:      ev.Gap,
	}
	if ev.Reason != dagworker.ReasonNone {
		w.Reason = ev.Reason.String()
	}
	if ev.Kind == dagworker.EventTransition || ev.Kind == dagworker.EventCreated {
		w.From = ev.From.String()
		w.To = ev.To.String()
	}
	return w
}

// subscribeOptionsFromRequest builds the [dagworker.SubscribeOptions] common
// to both the SSE and NDJSON-poll transports. Last-Event-ID takes precedence
// over ?cursor= because it is what a reconnecting EventSource sends
// automatically (dossier 14 §5.2); ?cursor= exists for the poll fallback,
// which has no EventSource underneath it to send that header for it.
func (s *Server) subscribeOptionsFromRequest(r *http.Request, scope dagworker.Scope) (dagworker.SubscribeOptions, error) {
	opts := dagworker.SubscribeOptions{Scope: scope}

	for _, k := range queryCSV(r, "event_kinds") {
		ek, err := eventKindFromWire(k)
		if err != nil {
			return opts, err
		}
		opts.Kinds = append(opts.Kinds, ek)
	}
	opts.NodeKinds = queryCSV(r, "node_kinds")

	if r.URL.Query().Get("replay") == "true" {
		opts.Replay = true
	}

	cursorStr := r.Header.Get("Last-Event-ID")
	if cursorStr == "" {
		cursorStr = r.URL.Query().Get("cursor")
	}
	if cursorStr != "" {
		n, err := strconv.ParseUint(cursorStr, 10, 64)
		if err != nil {
			return opts, fmt.Errorf("%w: cursor/Last-Event-ID must be a decimal cursor, got %q",
				dagworker.ErrInvalidArgument, cursorStr)
		}
		opts.From = dagworker.Cursor(n)
	}
	return opts, nil
}

// handleEvents implements GET /v1/scopes/{scope}/events: SSE by default,
// NDJSON long-poll when the client asks for ?mode=poll — for the minority of
// environments where a proxy fully buffers text/event-stream regardless of
// headers (dossier 14 §5.4).
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	scope := dagworker.Scope(r.PathValue("scope"))
	opts, err := s.subscribeOptionsFromRequest(r, scope)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}

	if r.URL.Query().Get("mode") == "poll" {
		s.handleEventsPoll(w, r, opts)
		return
	}
	s.handleEventsSSE(w, r, opts)
}

// handleEventsSSE streams events as Server-Sent Events. It subscribes before
// writing any header, so a subscribe-time failure (an expired cursor, a
// backend without durable-event support) still gets a normal
// application/problem+json response instead of a stream that opened and then
// went silent.
func (s *Server) handleEventsSSE(w http.ResponseWriter, r *http.Request, opts dagworker.SubscribeOptions) {
	sub, err := s.mgr.Subscribe(r.Context(), opts)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	defer func() { _ = sub.Close() }()

	rc := http.NewResponseController(w)
	// Opt this one connection out of the server's ordinary WriteTimeout — a
	// long-lived stream is exactly what that timeout exists to cut off
	// everywhere else. Every other handler stays covered by it (doc.go).
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		s.opts.logger.WarnContext(r.Context(), "dagworker/http: could not clear SSE write deadline", "error", err)
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_ = rc.Flush()

	heartbeat := time.NewTicker(s.opts.heartbeat)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.done:
			// Shutdown was called. Draining is prompt (adapter contract §2):
			// this stream ends now rather than waiting for the client to
			// disconnect on its own, which is exactly what would otherwise
			// hang Server.Shutdown.
			return
		case ev, ok := <-sub.Events():
			if !ok {
				return
			}
			if err := writeSSEEvent(w, ev); err != nil {
				return
			}
			_ = rc.Flush()
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			_ = rc.Flush()
		}
	}
}

// writeSSEEvent writes one event in WHATWG SSE wire format. The "id:" field
// carries the library's own Cursor verbatim, so a browser's automatic
// Last-Event-ID reconnect header round-trips straight back into
// [dagworker.SubscribeOptions.From] with no translation (doc.go).
func writeSSEEvent(w http.ResponseWriter, ev dagworker.Event) error {
	data, err := json.Marshal(eventToWire(ev))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", uint64(ev.Cursor), eventKindToWire(ev.Kind), data)
	return err
}

// handleEventsPoll implements the NDJSON long-poll fallback: the same
// blocking-query shape as claim (clamp, jitter, 204 on nothing found), but
// against the event log instead of the ready set.
func (s *Server) handleEventsPoll(w http.ResponseWriter, r *http.Request, opts dagworker.SubscribeOptions) {
	wait, err := clampWait(r.URL.Query().Get("wait"), s.opts.maxWait, s.opts.waitBudget)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	wait += s.opts.jitter(wait / 16)

	sub, err := s.mgr.Subscribe(r.Context(), opts)
	if err != nil {
		s.writeProblem(w, r, err)
		return
	}
	defer func() { _ = sub.Close() }()

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

	var events []dagworker.Event
	select {
	case ev, ok := <-sub.Events():
		if ok {
			events = append(events, ev)
		}
	case <-cctx.Done():
	}
	events = append(events, drainAvailable(sub)...)

	if len(events) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	for _, ev := range events {
		// Encode appends '\n' after each value, which is exactly NDJSON's
		// line delimiter (github.com/ndjson/ndjson-spec) — no extra
		// formatting needed.
		if err := enc.Encode(eventToWire(ev)); err != nil {
			return
		}
	}
}

// drainAvailable collects whatever is already buffered on sub without
// waiting for more, so one poll response can carry a small burst instead of
// forcing the client back into another round trip immediately.
func drainAvailable(sub *dagworker.Subscription) []dagworker.Event {
	var out []dagworker.Event
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				return out
			}
			out = append(out, ev)
		default:
			return out
		}
	}
}
