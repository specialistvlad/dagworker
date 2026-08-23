//go:build !integration

package e2e

// integrationBackends is empty without the integration build tag.
func integrationBackends() []Backend { return nil }
