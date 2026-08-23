package main

import (
	"fmt"
	"os"
	"strings"
)

// readSecretFile reads a secret's value from a file path rather than
// accepting the value itself on a flag or in an environment variable — see
// [Config]'s doc comment for why. An empty path means "no secret configured"
// and returns "", nil rather than an error, so callers can pass an optional
// field straight through without an extra branch.
//
// A trailing newline is trimmed because that is how a shell redirect
// (`echo "$PASS" > file`) and most secret-mounting sidecars (Kubernetes
// Secret volumes, Docker secrets) write the file, and a password compared
// byte-for-byte against a backend that trimmed its own copy would otherwise
// fail for a reason invisible in any log line — this function is exactly the
// one place that log line does not exist, by design.
func readSecretFile(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	data, err := os.ReadFile(path) //nolint:gosec // path is an operator-supplied *-file flag/env/config value — reading it from disk is this function's entire purpose
	if err != nil {
		return "", fmt.Errorf("dagworkerd: reading secret file %q: %w", path, err)
	}
	return strings.TrimRight(string(data), "\r\n"), nil
}
