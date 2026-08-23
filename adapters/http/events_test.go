package httpadapter

import (
	"bufio"
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
)

// sseFrame is one parsed "id:/event:/data:" block from an SSE stream.
type sseFrame struct {
	id, event, data string
}

// readSSEFrames reads n frames from an SSE response body, skipping heartbeat
// comment lines. It fails the test if n frames do not arrive within timeout.
func readSSEFrames(t *testing.T, body *http.Response, n int, timeout time.Duration) []sseFrame {
	t.Helper()
	frames := make(chan sseFrame, n)
	go func() {
		sc := bufio.NewScanner(body.Body)
		var cur sseFrame
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, ":"):
				// heartbeat comment; ignore
			case strings.HasPrefix(line, "id: "):
				cur.id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				cur.event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				cur.data = strings.TrimPrefix(line, "data: ")
			case line == "":
				if cur.event != "" {
					select {
					case frames <- cur:
					default:
					}
					cur = sseFrame{}
				}
			}
		}
	}()

	out := make([]sseFrame, 0, n)
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case f := <-frames:
			out = append(out, f)
		case <-deadline:
			t.Fatalf("timed out waiting for SSE frames: got %d of %d", len(out), n)
		}
	}
	return out
}

// TestEvents_SSEDeliversAndResumes covers SSE delivery and Last-Event-ID
// resumption: a subscriber sees a node's transition live, disconnects, and a
// second connection using the first frame's id as Last-Event-ID picks up
// exactly where it left off rather than replaying or skipping.
func TestEvents_SSEDeliversAndResumes(t *testing.T) {
	t.Parallel()
	base, mgr, shutdown := testServer(t)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/scopes/proj/events", nil)
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
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	// Drive a transition: create then claim then complete produces at least
	// a created event and a transition to in_progress.
	if err := mgr.AddNode(ctx, "proj", "n1", nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	frames := readSSEFrames(t, resp, 1, 5*time.Second)
	first := frames[0]
	if first.id == "" {
		t.Fatal("first frame has no id:")
	}
	if first.event == "" {
		t.Fatal("first frame has no event:")
	}
	if !strings.Contains(first.data, `"n1"`) {
		t.Errorf("first frame data = %q, want it to mention node n1", first.data)
	}
	cancel() // close the first connection

	// Produce a second event while nobody is subscribed, so resuming is the
	// only way to see it.
	if _, err := mgr.TryClaim(context.Background(), "proj", dagworker.AsWorker("w1")); err != nil {
		t.Fatalf("TryClaim: %v", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	req2, err := http.NewRequestWithContext(ctx2, http.MethodGet, base+"/v1/scopes/proj/events", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req2.Header.Set("Accept", "text/event-stream")
	req2.Header.Set("Last-Event-ID", first.id)
	resp2, err := httpClient().Do(req2)
	if err != nil {
		t.Fatalf("GET events (resume): %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("resumed events status = %d, want 200", resp2.StatusCode)
	}

	resumed := readSSEFrames(t, resp2, 1, 5*time.Second)
	firstCursor, err := strconv.ParseUint(first.id, 10, 64)
	if err != nil {
		t.Fatalf("parsing first cursor: %v", err)
	}
	resumedCursor, err := strconv.ParseUint(resumed[0].id, 10, 64)
	if err != nil {
		t.Fatalf("parsing resumed cursor: %v", err)
	}
	if resumedCursor <= firstCursor {
		t.Errorf("resumed cursor %d must be strictly after the first connection's %d", resumedCursor, firstCursor)
	}
	if !strings.Contains(resumed[0].data, `"n1"`) {
		t.Errorf("resumed frame data = %q, want it to mention node n1 (the claim transition)", resumed[0].data)
	}
}
