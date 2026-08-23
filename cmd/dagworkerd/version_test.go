package main

import (
	"strings"
	"testing"
)

func TestBuildVersion_NeverEmpty(t *testing.T) {
	t.Parallel()

	got := buildVersion()
	if got == "" {
		t.Fatalf("buildVersion() returned an empty string")
	}
	if !strings.HasPrefix(got, "dagworkerd ") {
		t.Errorf("buildVersion() = %q, want a %q prefix", got, "dagworkerd ")
	}
}

func TestPrintVersion_WritesOneLine(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	printVersion(&buf)

	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Errorf("printVersion output = %q, want newline-terminated", out)
	}
	if strings.Count(out, "\n") != 1 {
		t.Errorf("printVersion output = %q, want exactly one line", out)
	}
}
