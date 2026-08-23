package memory_test

import (
	"context"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/memory"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	dagstoretest.RunConformance(t, dagstoretest.Harness{
		Name: "memory",
		New: func(t *testing.T) (dw.Store, func(time.Duration)) {
			t.Helper()
			clk := dagstoretest.NewFakeClock()
			st := memory.New(
				memory.WithClock(clk),
				// Deterministic backoff: the midpoint of the jitter window, so
				// a retry schedule is reproducible across runs.
				memory.WithJitter(func(n int64) int64 { return n / 2 }),
			)
			t.Cleanup(func() { _ = st.Close(context.Background()) })
			return st, clk.Advance
		},
	})
}
