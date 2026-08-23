package httpadapter

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*
	"strconv"
	"strings"

	dagworker "github.com/specialistvlad/dagworker"
)

// decodeJSON reads and decodes r's body into v. An empty body decodes to v's
// zero value rather than erroring, since several of this API's POST bodies
// are entirely optional (":cancel" needs no fields at all).
//
// It rejects unknown fields: a client that misspells "resutl" for "result"
// gets a 400 pointing at exactly that, not a silently-ignored field and a
// confusing downstream failure.
func decodeJSON(r *http.Request, v any) error {
	if r.Body == nil {
		return nil
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("%w: malformed request body: %v", dagworker.ErrInvalidArgument, err)
	}
	return nil
}

// queryInt parses a query parameter as an int, returning def when the
// parameter is absent.
func queryInt(r *http.Request, name string, def int) (int, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %s must be an integer, got %q", dagworker.ErrInvalidArgument, name, raw)
	}
	return n, nil
}

// queryCSV splits a comma-separated query parameter into its parts. It also
// accepts the parameter repeated (?kind=a&kind=b), merging both forms, since
// either is a reasonable way for a client to spell "more than one".
func queryCSV(r *http.Request, name string) []string {
	var out []string
	for _, v := range r.URL.Query()[name] {
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

// statusesFromWire parses the ?status= filter on list-nodes.
func statusesFromWire(values []string) ([]dagworker.Status, error) {
	out := make([]dagworker.Status, 0, len(values))
	for _, v := range values {
		var st dagworker.Status
		if err := st.UnmarshalText([]byte(v)); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}
