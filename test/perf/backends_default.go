//go:build !integration

package perf

// integrationBackends is empty without the integration build tag, so the
// default build of this module does not need the backend modules to compile.
func integrationBackends() []Backend { return nil }
