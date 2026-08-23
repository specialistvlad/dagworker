//go:build !integration

package perf

// networkedBackends is empty without the integration build tag.
func networkedBackends() []Backend { return nil }
