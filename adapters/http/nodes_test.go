package httpadapter

import (
	"net/http"
	"testing"
)

// TestNodes_PutGetDeleteAndETag covers node CRUD plus the ETag/If-Match
// optimistic-concurrency path: a stale If-Match on DELETE is refused with
// 412, and a current one succeeds.
func TestNodes_PutGetDeleteAndETag(t *testing.T) {
	t.Parallel()
	base, _, shutdown := testServer(t)
	defer shutdown()

	putResp := doRequest(t, http.MethodPut, base+"/v1/scopes/proj/nodes/n1", map[string]any{
		"kind":     "render",
		"priority": 5,
	})
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT status = %d, want 201", putResp.StatusCode)
	}
	var created nodeWire
	decodeBody(t, putResp, &created)
	if created.Kind != "render" {
		t.Errorf("kind = %q, want render", created.Kind)
	}

	getResp := doRequest(t, http.MethodGet, base+"/v1/scopes/proj/nodes/n1", nil)
	defer func() { _ = getResp.Body.Close() }()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", getResp.StatusCode)
	}
	etag := getResp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("GET must set an ETag header")
	}

	// Re-PUTing the identical spec is a no-op: 200, not 201.
	idempotentResp := doRequest(t, http.MethodPut, base+"/v1/scopes/proj/nodes/n1", map[string]any{
		"kind":     "render",
		"priority": 5,
	})
	if idempotentResp.StatusCode != http.StatusOK {
		t.Fatalf("idempotent re-PUT status = %d, want 200", idempotentResp.StatusCode)
	}
	_ = idempotentResp.Body.Close()

	// DELETE with a stale If-Match is refused.
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodDelete, base+"/v1/scopes/proj/nodes/n1", nil)
	req.Header.Set("If-Match", `"v999999"`)
	staleDelete, err := httpClient().Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer func() { _ = staleDelete.Body.Close() }()
	if staleDelete.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("stale If-Match DELETE status = %d, want 412", staleDelete.StatusCode)
	}

	// DELETE with the current ETag succeeds.
	req2, _ := http.NewRequestWithContext(t.Context(), http.MethodDelete, base+"/v1/scopes/proj/nodes/n1", nil)
	req2.Header.Set("If-Match", etag)
	okDelete, err := httpClient().Do(req2)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer func() { _ = okDelete.Body.Close() }()
	if okDelete.StatusCode != http.StatusNoContent {
		t.Fatalf("current If-Match DELETE status = %d, want 204", okDelete.StatusCode)
	}

	goneResp := doRequest(t, http.MethodGet, base+"/v1/scopes/proj/nodes/n1", nil)
	defer func() { _ = goneResp.Body.Close() }()
	if goneResp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", goneResp.StatusCode)
	}
}

// TestEdges_PutAndCycleRejected covers edge creation and the cycle-rejection
// path mapping to 409 "cycle".
func TestEdges_PutAndCycleRejected(t *testing.T) {
	t.Parallel()
	base, _, shutdown := testServer(t)
	defer shutdown()

	for _, id := range []string{"a", "b"} {
		resp := doRequest(t, http.MethodPut, base+"/v1/scopes/proj/nodes/"+id, nil)
		_ = resp.Body.Close()
	}

	edgeResp := doRequest(t, http.MethodPut, base+"/v1/scopes/proj/edges/a..b", nil)
	defer func() { _ = edgeResp.Body.Close() }()
	if edgeResp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT edge status = %d, want 201", edgeResp.StatusCode)
	}

	cycleResp := doRequest(t, http.MethodPut, base+"/v1/scopes/proj/edges/b..a", nil)
	defer func() { _ = cycleResp.Body.Close() }()
	if cycleResp.StatusCode != http.StatusConflict {
		t.Fatalf("cycle-creating edge status = %d, want 409", cycleResp.StatusCode)
	}
	var p problem
	decodeBody(t, cycleResp, &p)
	if want := defaultProblemBaseURI + "cycle"; p.Type != want {
		t.Errorf("problem type = %q, want %q", p.Type, want)
	}
}
