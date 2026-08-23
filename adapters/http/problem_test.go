package httpadapter

import (
	"net/http"
	"testing"
)

// TestProblem_Shape confirms every error response is RFC 9457
// application/problem+json against the adapter contract's slug registry —
// never a bespoke error shape — across a representative sample of the error
// table.
func TestProblem_Shape(t *testing.T) {
	t.Parallel()
	// t.Cleanup, not defer: the subtests below call t.Parallel(), which
	// suspends them until this function returns — a bare defer would tear the
	// server down right then, before any subtest actually ran its body.
	// t.Cleanup runs after every subtest has finished instead.
	base, _, shutdown := testServer(t)
	t.Cleanup(shutdown)

	tests := []struct {
		name       string
		method     string
		path       string
		body       any
		wantStatus int
		wantSlug   string
	}{
		{
			name:       "not-found",
			method:     http.MethodGet,
			path:       "/v1/scopes/proj/nodes/does-not-exist",
			wantStatus: http.StatusNotFound,
			wantSlug:   "not-found",
		},
		{
			name:       "invalid-argument: bad payload encoding",
			method:     http.MethodPut,
			path:       "/v1/scopes/proj/nodes/bad-node",
			body:       map[string]any{"payload_encoding": "gzip", "payload": "eA=="},
			wantStatus: http.StatusBadRequest,
			wantSlug:   "invalid-argument",
		},
		{
			name:       "unsupported: unknown lease verb",
			method:     http.MethodPost,
			path:       "/v1/scopes/proj/leases/bm90LWEtcmVhbC1sZWFzZQ:frobnicate",
			wantStatus: http.StatusNotFound,
			wantSlug:   "not-found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := doRequest(t, tc.method, base+tc.path, tc.body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if ct := resp.Header.Get("Content-Type"); ct != problemContentType {
				t.Fatalf("Content-Type = %q, want %q", ct, problemContentType)
			}
			var p problem
			decodeBody(t, resp, &p)

			wantType := defaultProblemBaseURI + tc.wantSlug
			if p.Type != wantType {
				t.Errorf("type = %q, want %q", p.Type, wantType)
			}
			if p.Status != tc.wantStatus {
				t.Errorf("body status field = %d, want %d (must match the HTTP status)", p.Status, tc.wantStatus)
			}
			if p.Title == "" {
				t.Error("title must not be empty")
			}
			if p.Instance == "" {
				t.Error("instance must not be empty")
			}
		})
	}
}
