package grpcadapter

// ScopeOfRequest exposes scopeOfRequest so its fail-closed behaviour can be
// tested directly. Every request type in the current API names its scope one
// of the two supported ways, so the refusal path is unreachable through the
// public surface -- and it is the path that matters most, because it is what
// stops a future RPC from silently skipping scope authorization.
//
// Test-only: compiled for this package's tests and not part of the adapter.
func ScopeOfRequest(req any) (string, error) { return scopeOfRequest(req) }
