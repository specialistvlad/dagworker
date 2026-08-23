package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestIsLoopbackAddr(t *testing.T) {
	t.Parallel()
	cases := []struct {
		addr string
		want bool
	}{
		{"", true},
		{"127.0.0.1:8080", true},
		{"localhost:8080", true},
		{"[::1]:8080", true},
		{"127.53.0.1:8080", true},
		// The dangerous ones. A wildcard bind is the usual way a service ends
		// up reachable from outside the host without anyone deciding to.
		{":8080", false},
		{"0.0.0.0:8080", false},
		{"[::]:8080", false},
		{"10.0.0.7:8080", false},
		{"dagworkerd.internal:8080", false},
		// Unparseable is public: being wrong that way costs a flag.
		{"nonsense", false},
	}
	for _, tc := range cases {
		if got := isLoopbackAddr(tc.addr); got != tc.want {
			t.Errorf("isLoopbackAddr(%q) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

func TestParseTokens(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		contents string
		want     int
	}{
		{"one token", "s3cret", 1},
		{"trailing newline", "s3cret\n", 1},
		{"two for rotation", "old\nnew\n", 2},
		{"blank lines and comments", "# the token\n\nonly\n\n", 1},
		{"windows line endings", "a\r\nb\r\n", 2},
		{"empty file", "", 0},
		{"comments only", "# nothing here\n", 0},
		{"whitespace only", "  \n\t\n", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := len(parseTokens(tc.contents)); got != tc.want {
				t.Fatalf("parseTokens(%q) returned %d tokens, want %d", tc.contents, got, tc.want)
			}
		})
	}
}

func TestCheckAuthPosture(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		cfg     Config
		tokens  []string
		wantErr bool
	}{
		{"loopback with no auth is fine", Config{HTTPAddr: "127.0.0.1:8080"}, nil, false},
		{"public with a token is fine", Config{HTTPAddr: ":8080"}, []string{"t"}, false},
		{
			"public with --insecure is the operator's call",
			Config{HTTPAddr: ":8080", Insecure: true},
			nil, false,
		},
		{"public http with no auth is refused", Config{HTTPAddr: ":8080"}, nil, true},
		{"public grpc with no auth is refused", Config{GRPCAddr: "0.0.0.0:9443"}, nil, true},
		{
			"one loopback does not excuse the other public one",
			Config{HTTPAddr: "127.0.0.1:8080", GRPCAddr: ":9443"},
			nil, true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := checkAuthPosture(tc.cfg, tc.tokens)
			if tc.wantErr && !errors.Is(err, errPublicWithoutAuth) {
				t.Fatalf("got %v, want errPublicWithoutAuth", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("got %v, want nil", err)
			}
		})
	}
}

// TestDaemonRefusesPublicPortWithoutAuth is the point of all of the above: a
// configuration that would put an anonymous claim endpoint on a reachable
// address must not start. Loopback binds are left to startDaemon's own
// harness; this one deliberately uses a wildcard address, and never runs the
// daemon — newDaemon must fail before anything is bound.
func TestDaemonRefusesPublicPortWithoutAuth(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.AdminAddr = "127.0.0.1:0"
	cfg.HTTPAddr = "0.0.0.0:0"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, err := newDaemon(ctx, cfg, testLogger())
	if d != nil {
		_ = d.Run(ctx)
	}
	if !errors.Is(err, errPublicWithoutAuth) {
		t.Fatalf("newDaemon on a public address with no credential returned %v, want a refusal", err)
	}
}

func TestDaemonAcceptsPublicPortWithAToken(t *testing.T) {
	t.Parallel()

	// The same configuration, with the one thing that makes it defensible.
	// The listener is a wildcard bind on port 0, so it is genuinely the
	// refused shape, not a loopback one in disguise.
	tokenFile := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokenFile, []byte("# rotation window\nold\nnew\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	cfg := defaultConfig()
	cfg.AdminAddr = "127.0.0.1:0"
	cfg.HTTPAddr = "0.0.0.0:0"
	cfg.AuthTokenFile = tokenFile
	cfg.ShutdownTimeout = 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d, err := newDaemon(ctx, cfg, testLogger())
	if err != nil {
		t.Fatalf("newDaemon: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()
	t.Cleanup(func() {
		cancelRun()
		if err := <-done; err != nil {
			t.Errorf("daemon.Run: %v", err)
		}
	})
	waitForAdmin(t, d)

	base := "http://127.0.0.1:" + strconv.Itoa(d.HTTPAddr().(*net.TCPAddr).Port) + "/v1/scopes"
	for _, tc := range []struct {
		name, header string
		want         int
	}{
		{"no credential", "", http.StatusUnauthorized},
		{"wrong credential", "Bearer nope", http.StatusForbidden},
		{"the token being rotated out", "Bearer old", http.StatusOK},
		{"the token being rotated in", "Bearer new", http.StatusOK},
	} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		if tc.header != "" {
			req.Header.Set("Authorization", tc.header)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, resp.StatusCode, tc.want)
		}
	}
}

func TestDaemonRefusesAnEmptyTokenFile(t *testing.T) {
	t.Parallel()

	// An empty token file configures an authorizer that accepts nothing,
	// which is indistinguishable in the logs from one that works — right up
	// until every worker is locked out.
	tokenFile := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokenFile, []byte("# no tokens here\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	cfg := defaultConfig()
	cfg.AdminAddr = "127.0.0.1:0"
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.AuthTokenFile = tokenFile

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d, err := newDaemon(ctx, cfg, testLogger())
	if d != nil {
		_ = d.Run(ctx)
	}
	if !errors.Is(err, errEmptyTokenFile) {
		t.Fatalf("newDaemon with an empty token file returned %v, want a refusal", err)
	}
}
