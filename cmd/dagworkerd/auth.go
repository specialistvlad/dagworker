package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// errPublicWithoutAuth is what a deployment that binds a reachable port and
// configures no credential gets. It is a startup failure rather than a
// warning on purpose: a warning in a JSON log line at boot is read by nobody,
// and the failure mode it precedes is an anonymous peer claiming and
// completing other people's work — the adapters' own trust model assumes
// callers are cooperative (ADR-0035), which is only true if they are
// authenticated first.
var errPublicWithoutAuth = errors.New(
	"dagworkerd: refusing to serve a non-loopback address with no credential configured; " +
		"set --auth-token-file, or bind a loopback address, or pass --insecure to say you meant it")

// errEmptyTokenFile catches the failure that would otherwise be silent: a
// token file that exists but is empty configures an authorizer that accepts
// nothing, which looks identical in the logs to one that works, right up
// until every worker is locked out.
var errEmptyTokenFile = errors.New("dagworkerd: --auth-token-file names a file with no tokens in it")

// parseTokens reads the contents of an auth token file: one token per line,
// blank lines and "#" comments ignored. Several lines are allowed so a token
// can be rotated without a window in which one of the two is rejected — write
// the new one alongside the old, restart, then drop the old.
func parseTokens(contents string) []string {
	var out []string
	for line := range strings.SplitSeq(contents, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out
}

// isLoopbackAddr reports whether a listen address is reachable only from this
// host. It answers conservatively: anything it cannot resolve to a loopback
// literal — a wildcard bind, a hostname, an empty host — counts as public,
// because being wrong in that direction costs a startup flag and being wrong
// in the other costs an open port.
func isLoopbackAddr(addr string) bool {
	if addr == "" {
		return true // not bound at all
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	switch host {
	case "":
		// ":8080" binds every interface, which is the single most common way
		// a service ends up reachable from outside the host by accident.
		return false
	case "localhost":
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// checkAuthPosture rejects a configuration that would serve a reachable port
// anonymously. It runs after the token file has been read, so "the file was
// configured" and "the file had a token in it" are two different answers.
func checkAuthPosture(cfg Config, tokens []string) error {
	if len(tokens) > 0 || cfg.Insecure {
		return nil
	}
	for _, addr := range []string{cfg.GRPCAddr, cfg.HTTPAddr} {
		if !isLoopbackAddr(addr) {
			return fmt.Errorf("%w (address %q)", errPublicWithoutAuth, addr)
		}
	}
	return nil
}

// loadAuthTokens reads the configured token file, if there is one. An
// unreadable file is a startup failure, and so is one with no tokens in it:
// both mean the operator asked for authentication and did not get it, and
// starting anyway would either serve anonymously or lock every worker out.
func loadAuthTokens(cfg Config) ([]string, error) {
	if cfg.AuthTokenFile == "" {
		return nil, nil
	}
	contents, err := readSecretFile(cfg.AuthTokenFile)
	if err != nil {
		return nil, err
	}
	tokens := parseTokens(contents)
	if len(tokens) == 0 {
		return nil, fmt.Errorf("%w: %s", errEmptyTokenFile, cfg.AuthTokenFile)
	}
	return tokens, nil
}
