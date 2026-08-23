package main

import (
	"fmt"
	"io"
	"runtime/debug"
)

// buildVersion reports what --version prints, preferring the VCS metadata
// `go build`'s -buildvcs=auto stamps into the binary automatically (module
// version, commit, dirty flag) so a plain `go build`/`go install` of a clean
// checkout already reports something identifiable with no release-specific
// ldflags step required.
func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dagworkerd (unknown build)"
	}

	version := info.Main.Version
	if version == "" || version == "(devel)" {
		version = "devel"
	}

	var revision string
	dirty := ""
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
			if len(revision) > 12 {
				revision = revision[:12]
			}
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}

	if revision == "" {
		return fmt.Sprintf("dagworkerd %s (%s)", version, info.GoVersion)
	}
	return fmt.Sprintf("dagworkerd %s (%s%s, %s)", version, revision, dirty, info.GoVersion)
}

// printVersion writes [buildVersion]'s report to w, terminated the way any
// well-behaved --version output is: one line, newline-terminated, nothing else.
func printVersion(w io.Writer) {
	_, _ = fmt.Fprintln(w, buildVersion())
}
