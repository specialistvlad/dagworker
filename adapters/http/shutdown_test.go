package httpadapter

import (
	"context"
	"net/http"
	"testing"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
)

// TestShutdown_DoesNotHangOnOpenStream is the one behavior a global
// WriteTimeout and a naive Shutdown implementation both get wrong: an open
// SSE connection must not keep Shutdown blocked until the client disconnects
// on its own or the shutdown context's own deadline is reached by attrition.
// Server.Shutdown must actively signal the stream to end and return promptly.
func TestShutdown_DoesNotHangOnOpenStream(t *testing.T) {
	t.Parallel()

	store := memory.New()
	mgr, err := dagworker.New(store, dagworker.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("dagworker.New: %v", err)
	}
	srv, err := New(mgr)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	base, stop := startServer(t, srv)

	// Open an SSE stream on a scope that will never see an event: nothing
	// but Shutdown itself, or the client's own context, can end it.
	streamCtx, cancelStream := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStream()
	req, err := http.NewRequestWithContext(streamCtx, http.MethodGet, base+"/v1/scopes/quiet/events", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := httpClient().Do(req)
	if err != nil {
		t.Fatalf("GET events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events status = %d, want 200", resp.StatusCode)
	}

	const grace = 3 * time.Second
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), grace)
	defer cancelShutdown()

	start := time.Now()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown returned an error instead of draining cleanly: %v", err)
	}
	elapsed := time.Since(start)

	// A generous margin under the full grace period: Shutdown should return
	// as soon as the stream handler notices s.done, not after riding out the
	// entire budget as if nothing had signaled it.
	if elapsed > grace/2 {
		t.Errorf("Shutdown took %s, wanted well under half of the %s grace period "+
			"(a naive Shutdown would hang until the grace period expires)", elapsed, grace)
	}

	stop() // Serve should also have returned nil by now; startServer's cleanup asserts that.
	if err := mgr.Close(context.Background()); err != nil {
		t.Errorf("Manager.Close: %v", err)
	}
}
