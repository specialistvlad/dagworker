package main

import (
	"path/filepath"
	"testing"
)

func TestReadSecretFile_EmptyPathIsNotAnError(t *testing.T) {
	t.Parallel()

	got, err := readSecretFile("")
	if err != nil {
		t.Fatalf("readSecretFile(\"\"): %v", err)
	}
	if got != "" {
		t.Errorf("readSecretFile(\"\") = %q, want empty", got)
	}
}

func TestReadSecretFile_TrimsTrailingNewline(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "secret")
	writeFile(t, path, "hunter2\n")

	got, err := readSecretFile(path)
	if err != nil {
		t.Fatalf("readSecretFile: %v", err)
	}
	if got != "hunter2" {
		t.Errorf("readSecretFile = %q, want %q", got, "hunter2")
	}
}

func TestReadSecretFile_PreservesInternalWhitespace(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "secret")
	writeFile(t, path, "line one\nline two")

	got, err := readSecretFile(path)
	if err != nil {
		t.Fatalf("readSecretFile: %v", err)
	}
	if got != "line one\nline two" {
		t.Errorf("readSecretFile = %q, want internal newline preserved", got)
	}
}

func TestReadSecretFile_MissingFileIsAnError(t *testing.T) {
	t.Parallel()

	if _, err := readSecretFile(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Errorf("readSecretFile: expected an error for a missing file")
	}
}
