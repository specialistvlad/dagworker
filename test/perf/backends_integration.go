//go:build integration

package perf

// networkedBackends returns the database-backed stores. They live behind a
// build tag so that the default build of this module does not require the
// backend modules to compile at all -- which keeps the suite usable while a
// backend is still being written.
func networkedBackends() []Backend { return integrationBackends() }
