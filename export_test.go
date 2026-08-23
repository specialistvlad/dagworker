package dagworker

// SubscribersConsideredFor reports how many subscriptions [Manager.publish]
// would iterate for a write in scope. It is the one thing a test cannot
// observe from outside the package and cannot measure with a clock: the cost
// is a handful of nanoseconds per subscriber, invisible in any timing a CI
// machine can produce reliably, and it is on the completion path, where a
// per-write cost that grows with the number of unrelated subscribers is
// exactly the shape this library promises not to have.
//
// Test-only: this file is compiled for this package's tests and is not part
// of the shipped library.
func (m *Manager) SubscribersConsideredFor(scope Scope) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.subsFor(scope))
}
