package file_test

import (
	"context"
	"testing"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/file"
)

// The whole point of the port: this backend must be indistinguishable from the
// others, or the promised one-line migration to a database is not one.
func TestConformance(t *testing.T) {
	t.Parallel()
	dagstoretest.RunConformance(t, dagstoretest.Harness{
		Name: "file",
		New: func(t *testing.T) (dw.Store, func(time.Duration)) {
			t.Helper()
			clk := dagstoretest.NewFakeClock()
			st, _, err := file.Open(context.Background(), t.TempDir(), file.WithClock(clk))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = st.Close(context.Background()) })
			return st, clk.Advance
		},
	})
}

// TestCapabilitiesAreHonest: the backend exists to answer "state must survive a
// restart", so the capability that says so has to be set -- and the one it
// cannot deliver must not be.
func TestCapabilitiesAreHonest(t *testing.T) {
	t.Parallel()
	st, _, err := file.Open(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close(context.Background()) }()

	caps := st.Capabilities()
	if !caps.Has(dw.CapDurableStorage) {
		t.Error("CapDurableStorage is not set on the backend whose purpose is durability")
	}
	if caps.Has(dw.CapCrossProcess) {
		t.Error("CapCrossProcess is set; two processes over one directory would diverge silently")
	}
}
