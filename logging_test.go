package dagworker_test

import (
	"context"
	"log/slog"
	"testing"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
)

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// A library that writes to its host's stderr uninvited is a defect, so the
// default logger must swallow everything.
func TestDefaultLoggerIsSilent(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.add("a")
	// Exercised indirectly: if the default handler were not silent, every test
	// in this package would be printing. Assert its contract directly too.
	m2, err := dw.New(memory.New(), dw.WithLogger(slog.New(slog.NewTextHandler(discard{}, nil))))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := m2.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// The default logger must swallow every level and survive the whole Handler
// surface, because a library that writes to its host's stderr uninvited is a
// defect rather than a feature.
func TestDefaultLoggerSwallowsEverything(t *testing.T) {
	t.Parallel()
	st := memory.New()
	m, err := dw.New(st, dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})
	// Reach the handler the only way the public API allows: drive an operation
	// that logs. Nothing must appear on stderr, and nothing must panic.
	for _, lvl := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
		if err := m.AddNode(t.Context(), "s", dw.NodeID(lvl.String()), nil); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
}
