// Command site is the dagworker documentation site generator. It renders the
// repository's Markdown (READMEs, the ADRs, the research dossiers, the
// normative spec, plus a handful of hand-written guide pages) into a static
// HTML site under public/, ready to publish with GitHub Pages. Stdlib only.
package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func main() {
	repoRoot := "../.."
	if len(os.Args) > 1 {
		repoRoot = os.Args[1]
	}
	outDir := "public"

	if err := build(repoRoot, outDir); err != nil {
		fmt.Fprintln(os.Stderr, "site: "+err.Error())
		os.Exit(1)
	}
}

// docEntry is one rendered ADR or research dossier: enough to link to it from
// an index page.
type docEntry struct {
	Slug  string
	Title string
}

func build(repoRoot, outDir string) error {
	if err := os.RemoveAll(outDir); err != nil {
		return fmt.Errorf("clearing %s: %w", outDir, err)
	}

	nav := buildNav()
	pages := 0

	write := func(slug string, d pageData) error {
		d.Slug = slug
		d.Nav = nav
		if err := writeFile(outDir, slug, renderPage(d)); err != nil {
			return err
		}
		pages++
		return nil
	}

	// ---- index -------------------------------------------------------
	if err := renderContentPage(write, "", "content/index.md", ""); err != nil {
		return err
	}

	// ---- guide/* -------------------------------------------------------
	guideSlugs := []string{
		"guide/quickstart",
		"guide/concepts",
		"guide/trigger-rules",
		"guide/dynamic-graphs",
		"guide/workers",
		"guide/backends",
		"guide/operations",
		"guide/performance",
	}
	for _, slug := range guideSlugs {
		if err := renderContentPage(write, slug, "content/"+slug+".md", ""); err != nil {
			return err
		}
	}

	// ---- reference/* -----------------------------------------------------
	if err := renderRepoPage(write, repoRoot, "reference/contract", "docs/spec/01-contract.md",
		"The normative contract: transition table, guarantees, and complexity bounds."); err != nil {
		return err
	}
	if err := renderRepoPage(write, repoRoot, "reference/adapters", "docs/spec/02-adapter-contract.md",
		"The optional gRPC and HTTP/JSON adapter contracts."); err != nil {
		return err
	}

	// ---- adr/* -----------------------------------------------------------
	adrFiles, err := listMarkdown(filepath.Join(repoRoot, "docs/adr"))
	if err != nil {
		return err
	}
	var adrEntries []docEntry
	for _, name := range adrFiles {
		slug := "adr/" + strings.TrimSuffix(name, ".md")
		title, err := renderRepoPageEntry(write, repoRoot, slug, "docs/adr/"+name, "")
		if err != nil {
			return err
		}
		adrEntries = append(adrEntries, docEntry{Slug: slug, Title: title})
	}
	if err := write("adr", pageData{
		Title:       "Architecture Decision Records",
		H1:          "Architecture Decision Records",
		InjectH1:    true,
		Description: "All " + fmt.Sprint(len(adrEntries)) + " architecture decision records, in order.",
		Body: renderIndexList(
			"Every accepted decision behind dagworker's design, in the order it was made. "+
				"An ADR is immutable once accepted; a changed decision gets a new ADR that amends the old one.",
			toIndexRows(adrEntries)),
	}); err != nil {
		return err
	}

	// ---- research/* --------------------------------------------------
	researchFiles, err := listMarkdown(filepath.Join(repoRoot, "docs/research"))
	if err != nil {
		return err
	}
	var researchEntries []docEntry
	for _, name := range researchFiles {
		slug := "research/" + strings.TrimSuffix(name, ".md")
		title, err := renderRepoPageEntry(write, repoRoot, slug, "docs/research/"+name, "")
		if err != nil {
			return err
		}
		researchEntries = append(researchEntries, docEntry{Slug: slug, Title: title})
	}
	if err := write("research", pageData{
		Title:       "Research Dossiers",
		H1:          "Research Dossiers",
		InjectH1:    true,
		Description: "The " + fmt.Sprint(len(researchEntries)) + " primary-source research dossiers the design was derived from.",
		Body: renderIndexList(
			"The primary-source research the design was derived from, plus the synthesis that reconciled it. "+
				"Every ADR's \"Backing research\" line points back into one of these.",
			toIndexRows(researchEntries)),
	}); err != nil {
		return err
	}

	// ---- contributing / changelog -----------------------------------
	if err := renderRepoPage(write, repoRoot, "contributing", "CONTRIBUTING.md", "How to propose a change, and what has to be true before it merges."); err != nil {
		return err
	}
	if err := renderRepoPage(write, repoRoot, "changelog", "CHANGELOG.md", "Notable changes, release by release."); err != nil {
		return err
	}

	fmt.Printf("site: wrote %d pages to %s\n", pages, outDir)
	return nil
}

func toIndexRows(entries []docEntry) []struct {
	Slug  string
	Title string
} {
	rows := make([]struct {
		Slug  string
		Title string
	}, len(entries))
	for i, e := range entries {
		rows[i] = struct {
			Slug  string
			Title string
		}{e.Slug, e.Title}
	}
	return rows
}

type writeFunc func(slug string, d pageData) error

// renderContentPage renders a hand-authored page from docs/site/content:
// front matter supplies the title, the body has no H1 of its own, and the
// template injects one.
func renderContentPage(write writeFunc, slug, path, description string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}
	meta, body := splitFrontMatter(string(raw))
	title := meta["title"]
	if title == "" {
		return fmt.Errorf("%s: missing \"title\" in front matter", path)
	}
	if description == "" {
		description = meta["description"]
	}
	res := renderMarkdown(body, nil)
	return write(slug, pageData{
		Title:       title,
		H1:          title,
		InjectH1:    true,
		Description: description,
		Body:        res.HTML,
		TOC:         res.TOC,
	})
}

// renderRepoPage renders a page whose content is an existing repository
// Markdown file, verbatim: the file's own first heading is the page H1, and
// repo-relative links are rewritten to their place on the site.
func renderRepoPage(write writeFunc, repoRoot, slug, repoPath, description string) error {
	_, err := renderRepoPageEntry(write, repoRoot, slug, repoPath, description)
	return err
}

// renderRepoPageEntry is renderRepoPage plus the page's plain-text title, for
// callers that need to list it on an index page afterwards.
func renderRepoPageEntry(write writeFunc, repoRoot, slug, repoPath, description string) (string, error) {
	full := filepath.Join(repoRoot, repoPath)
	raw, err := os.ReadFile(full)
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", full, err)
	}
	res := renderMarkdown(string(raw), resolveRepoLink)
	title := res.H1
	if title == "" {
		title = slug
	}
	err = write(slug, pageData{
		Title:       title,
		InjectH1:    false, // the file's own "# ..." heading is already in Body
		Description: description,
		Body:        res.HTML,
		TOC:         res.TOC,
		EditPath:    repoPath,
	})
	return title, err
}

var mdNameRe = regexp.MustCompile(`\.md$`)

func listMarkdown(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", dir, err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() || !mdNameRe.MatchString(e.Name()) {
			continue
		}
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out, nil
}

func writeFile(outDir, slug, html string) error {
	var target string
	if slug == "" {
		target = filepath.Join(outDir, "index.html")
	} else {
		target = filepath.Join(outDir, filepath.FromSlash(slug), "index.html")
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("creating directory for %s: %w", target, err)
	}
	if err := os.WriteFile(target, []byte(html), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", target, err)
	}
	return nil
}

func buildNav() []NavGroup {
	return []NavGroup{
		{Title: "Start", Items: []NavItem{
			{Title: "Home", Slug: ""},
		}},
		{Title: "Guide", Items: []NavItem{
			{Title: "Quickstart", Slug: "guide/quickstart"},
			{Title: "Concepts", Slug: "guide/concepts"},
			{Title: "Trigger rules & branching", Slug: "guide/trigger-rules"},
			{Title: "Dynamic graphs", Slug: "guide/dynamic-graphs"},
			{Title: "Writing workers", Slug: "guide/workers"},
			{Title: "Choosing a backend", Slug: "guide/backends"},
			{Title: "Running in production", Slug: "guide/operations"},
			{Title: "Performance", Slug: "guide/performance"},
		}},
		{Title: "Reference", Items: []NavItem{
			{Title: "Storage & manager contract", Slug: "reference/contract"},
			{Title: "Adapter contract", Slug: "reference/adapters"},
		}},
		{Title: "Design record", Items: []NavItem{
			{Title: "Architecture Decisions", Slug: "adr"},
			{Title: "Research Dossiers", Slug: "research"},
		}},
		{Title: "Project", Items: []NavItem{
			{Title: "Contributing", Slug: "contributing"},
			{Title: "Changelog", Slug: "changelog"},
		}},
	}
}

// ---------------------------------------------------------------------------
// Repo-relative link resolution
//
// The ADRs, the research dossiers and the spec almost never link to each
// other (cross-references are bare text like "ADR-0017"), but CONTRIBUTING.md
// and CHANGELOG.md do link to files this site renders, and to a few it
// doesn't. This rewrites the former to their page on the site and the latter
// to their file on GitHub, so nothing a rendered repo file links to 404s.
// ---------------------------------------------------------------------------

const githubBlobBase = "https://github.com/specialistvlad/dagworker"

func resolveRepoLink(raw string) string {
	if raw == "" {
		return raw
	}
	if strings.HasPrefix(raw, "#") ||
		strings.HasPrefix(raw, "http://") ||
		strings.HasPrefix(raw, "https://") ||
		strings.HasPrefix(raw, "mailto:") {
		return raw
	}

	clean, frag := raw, ""
	if i := strings.Index(raw, "#"); i != -1 {
		clean, frag = raw[:i], raw[i:]
	}
	clean = strings.TrimPrefix(clean, "./")
	for strings.HasPrefix(clean, "../") {
		clean = clean[len("../"):]
	}
	trimmed := strings.TrimSuffix(clean, "/")

	switch {
	case clean == "":
		return raw
	case strings.HasPrefix(clean, "docs/adr/") && strings.HasSuffix(clean, ".md"):
		name := strings.TrimSuffix(strings.TrimPrefix(clean, "docs/adr/"), ".md")
		return href("adr/"+name) + frag
	case trimmed == "docs/adr":
		return href("adr") + frag
	case strings.HasPrefix(clean, "docs/research/") && strings.HasSuffix(clean, ".md"):
		name := strings.TrimSuffix(strings.TrimPrefix(clean, "docs/research/"), ".md")
		return href("research/"+name) + frag
	case trimmed == "docs/research":
		return href("research") + frag
	case clean == "docs/spec/01-contract.md":
		return href("reference/contract") + frag
	case clean == "docs/spec/02-adapter-contract.md":
		return href("reference/adapters") + frag
	case clean == "CONTRIBUTING.md":
		return href("contributing") + frag
	case clean == "CHANGELOG.md":
		return href("changelog") + frag
	case clean == "README.md":
		return href("") + frag
	default:
		kind := "blob"
		if strings.HasSuffix(clean, "/") || path.Ext(clean) == "" {
			kind = "tree"
		}
		return githubBlobBase + "/" + kind + "/main/" + clean + frag
	}
}
