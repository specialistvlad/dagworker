package main

import (
	"strings"
	"testing"
)

func TestRun_VersionFlagPrintsAndExitsZero(t *testing.T) {
	t.Parallel()

	var stdout, stderr strings.Builder
	code := run([]string{"--version"}, noEnv, &stdout, &stderr)

	if code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "dagworkerd") {
		t.Errorf("stdout = %q, want it to mention dagworkerd", stdout.String())
	}
	if stderr.String() != "" {
		t.Errorf("stderr = %q, want empty", stderr.String())
	}
}

func TestRun_HelpPrintsUsageAndExitsZero(t *testing.T) {
	t.Parallel()

	var stdout, stderr strings.Builder
	code := run([]string{"--help"}, noEnv, &stdout, &stderr)

	if code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	if !strings.Contains(stderr.String(), "-store") {
		t.Errorf("stderr usage output = %q, want it to mention the -store flag", stderr.String())
	}
}

func TestRun_InvalidConfigExitsTwoWithMessage(t *testing.T) {
	t.Parallel()

	var stdout, stderr strings.Builder
	// No adapter enabled: fails Config.validate before anything is started.
	code := run(nil, noEnv, &stdout, &stderr)

	if code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "at least one of") {
		t.Errorf("stderr = %q, want the no-adapter-enabled message", stderr.String())
	}
}

func TestRun_UnknownFlagExitsTwo(t *testing.T) {
	t.Parallel()

	var stdout, stderr strings.Builder
	code := run([]string{"--this-flag-does-not-exist"}, noEnv, &stdout, &stderr)

	if code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}
}

func TestAddrString_NilReportsDisabled(t *testing.T) {
	t.Parallel()

	if got := addrString(nil); got != "disabled" {
		t.Errorf("addrString(nil) = %q, want %q", got, "disabled")
	}
}
