// Package client is a reference HTTP client for dagworker's worker protocol
// (adapters/http). It is not required to talk to the server — any HTTP
// client and a JSON parser will do, which is the entire design point of the
// protocol (docs/research/14-http-json-worker-protocol.md §0) — but it is a
// working demonstration of the one rule that is easy to get wrong from the
// client side too: a lease outlives the RPC that granted it, so the code
// that keeps a lease alive must not reuse the context of the call that
// claimed it. See [Handle].
package client
