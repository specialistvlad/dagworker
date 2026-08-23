package httpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
)

// startServer runs srv on a fresh loopback listener and returns the base URL
// to hit plus a function that shuts the server down and waits for Serve to
// return. The listener is already bound before this returns (net.Listen is
// synchronous), so there is no race to sleep around before the first request.
func startServer(t *testing.T, srv *Server) (baseURL string, shutdown func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	serveCtx, cancelServe := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(serveCtx, lis); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()

	shutdown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		cancelServe()
		<-done
	}
	return "http://" + lis.Addr().String(), shutdown
}

// testServer is the common case: a fresh in-memory backend and Manager, a
// Server with jitter disabled for deterministic timing, all on a real
// listener. Nothing here is shared across tests, so every test using it may
// run in parallel.
func testServer(t *testing.T, opts ...Option) (baseURL string, mgr *dagworker.Manager, shutdown func()) {
	t.Helper()

	store := memory.New()
	mgr, err := dagworker.New(store, dagworker.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("dagworker.New: %v", err)
	}

	// Deterministic, fast waits by default: real jitter is exercised by its
	// own unit test (jitter_test.go), not by every handler test that happens
	// to exercise a blocking path.
	allOpts := append([]Option{withJitter(func(time.Duration) time.Duration { return 0 })}, opts...)
	srv, err := New(mgr, allOpts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	url, stopServer := startServer(t, srv)
	shutdown = func() {
		stopServer()
		if err := mgr.Close(context.Background()); err != nil {
			t.Errorf("Manager.Close: %v", err)
		}
	}
	return url, mgr, shutdown
}

// httpClient is the shared shape for tests that just need sane defaults.
func httpClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second}
}

// doRequest performs one request, marshaling body as JSON when non-nil.
func doRequest(t *testing.T, method, url string, body any) *http.Response {
	t.Helper()
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := httpClient().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

// decodeBody decodes resp's JSON body into v and closes it.
func decodeBody(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
}
