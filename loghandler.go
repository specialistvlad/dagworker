package dagworker

import (
	"context"
	"log/slog"
)

// discardHandler is the default slog handler: it drops everything. Go 1.24 has
// slog.DiscardHandler, but implementing it here keeps the minimum toolchain
// requirement driven by language and testing features rather than by a
// convenience that costs six lines.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }
