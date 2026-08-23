// Package e2e exercises the whole stack -- Manager over a real backend --
// rather than the storage port in isolation.
//
// The conformance suite in dagstoretest proves a backend implements the port.
// This proves the library built on top of it behaves, including the parts no
// single backend can be asked about: two Manager instances sharing one store,
// workers in separate goroutines competing for the same graph, and a whole DAG
// draining from insertion to completion.
package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
)

// Backend is one store the end-to-end suite runs against.
type Backend struct {
	Name string
	// New returns a store, isolated from any other this suite creates, so that
	// subtests can run in parallel against a shared database.
	New func(tb testing.TB) dw.Store
	// Shared reports whether two independently created stores from this
	// backend see the same data. Only a shared backend can be asked the
	// multi-instance questions.
	Shared bool
}

// Backends returns everything reachable in this environment. The in-memory
// backend is always present; the networked ones need DAGWORKER_INTEGRATION,
// so a plain `go test ./...` on a laptop runs what it can rather than failing
// on what it cannot reach.
func Backends() []Backend {
	out := make([]Backend, 0, 3)
	out = append(out, Backend{
		Name: "memory",
		New: func(tb testing.TB) dw.Store {
			tb.Helper()
			st := memory.New()
			tb.Cleanup(func() { _ = st.Close(context.Background()) })
			return st
		},
	})
	if os.Getenv("DAGWORKER_INTEGRATION") == "" {
		return out
	}
	return append(out, integrationBackends()...)
}

// UniqueScope keeps parallel subtests from colliding inside a shared database.
func UniqueScope(tb testing.TB) dw.Scope {
	tb.Helper()
	return dw.Scope(fmt.Sprintf("e2e-%s-%d", sanitize(tb.Name()), time.Now().UnixNano()))
}

func sanitize(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}
