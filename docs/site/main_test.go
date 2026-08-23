package main

import (
	"strings"
	"testing"
)

// contains fails the test with the full rendered HTML if want is not found,
// which is far more useful for debugging a markdown renderer than a bare
// boolean mismatch.
func contains(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Errorf("output does not contain %q\n--- got ---\n%s", want, got)
	}
}

func notContains(t *testing.T, got, want string) {
	t.Helper()
	if strings.Contains(got, want) {
		t.Errorf("output unexpectedly contains %q\n--- got ---\n%s", want, got)
	}
}

func TestHeadings(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want []string
	}{
		{
			name: "h1 through h4 get anchored ids",
			src:  "# Top\n\n## Sub\n\n### SubSub\n\n#### Deep",
			want: []string{
				`<h1 id="top">`,
				`<h2 id="sub">`,
				`<h3 id="subsub">`,
				`<h4 id="deep">`,
			},
		},
		{
			name: "duplicate headings get de-duplicated ids",
			src:  "## Retry\n\n## Retry",
			want: []string{`id="retry"`, `id="retry-1"`},
		},
		{
			name: "heading text can carry inline markup",
			src:  "## The `Store` interface",
			want: []string{`<h2 id="the-store-interface">The <code>Store</code> interface`},
		},
		{
			name: "trailing closing hashes are trimmed",
			src:  "## Section ##",
			want: []string{`<h2 id="section">Section<a`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := renderMarkdown(tc.src, nil)
			for _, w := range tc.want {
				contains(t, res.HTML, w)
			}
		})
	}
}

func TestTOC(t *testing.T) {
	src := "# Title\n\n## One\n\n### One A\n\n## Two"
	res := renderMarkdown(src, nil)
	if len(res.TOC) != 4 {
		t.Fatalf("got %d TOC entries, want 4: %+v", len(res.TOC), res.TOC)
	}
	if !res.HasH1 || res.H1 != "Title" {
		t.Errorf("H1 = %q, HasH1 = %v; want %q, true", res.H1, res.HasH1, "Title")
	}
	if res.TOC[1].Level != 2 || res.TOC[1].Text != "One" {
		t.Errorf("TOC[1] = %+v, want level 2 %q", res.TOC[1], "One")
	}
}

func TestParagraphsAndEscaping(t *testing.T) {
	res := renderMarkdown("A paragraph with <html> & \"quotes\".\n\nA second one.", nil)
	contains(t, res.HTML, "<p>A paragraph with &lt;html&gt; &amp; \"quotes\".</p>")
	contains(t, res.HTML, "<p>A second one.</p>")
}

func TestInlineCode(t *testing.T) {
	res := renderMarkdown("Use `Manager.Claim` to get work.", nil)
	contains(t, res.HTML, "<code>Manager.Claim</code>")
}

func TestPipeInsideInlineCodeSurvivesTableSplitting(t *testing.T) {
	src := "| Test | Meaning |\n|---|---|\n| `T-CRUD-ROUNDTRIP` | `Create`/`Get`/`Put` round-trip |\n"
	res := renderMarkdown(src, nil)
	contains(t, res.HTML, "<code>T-CRUD-ROUNDTRIP</code>")
	contains(t, res.HTML, "<code>Create</code>/<code>Get</code>/<code>Put</code> round-trip")
	// Exactly two data cells, not three: the pipe inside `Create`/`Get`/`Put`
	// must not have been treated as a column separator.
	if n := strings.Count(res.HTML, "<td>"); n != 2 {
		t.Errorf("got %d <td> cells, want 2 (pipes inside code spans must not split the row)\n%s", n, res.HTML)
	}
}

func TestBoldAndItalic(t *testing.T) {
	cases := []struct {
		name, src, want string
	}{
		{"bold stars", "**bold**", "<strong>bold</strong>"},
		{"bold underscores", "__bold__", "<strong>bold</strong>"},
		{"italic star", "*italic*", "<em>italic</em>"},
		{"italic underscore with word boundary", "before _italic_ after", "before <em>italic</em> after"},
		{"bold italic", "***both***", "<strong><em>both</em></strong>"},
		{"snake_case is not emphasis", "the `up_for_retry` style name has underscores in prose too: up_for_retry", "up_for_retry"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := renderMarkdown(tc.src, nil)
			contains(t, res.HTML, tc.want)
		})
	}
	// The intraword case must NOT be turned into emphasis.
	res := renderMarkdown("a plain sentence with up_for_retry inside it", nil)
	notContains(t, res.HTML, "<em>")
}

func TestLinks(t *testing.T) {
	t.Run("inline link", func(t *testing.T) {
		res := renderMarkdown("See [the contract](docs/spec/01-contract.md) for details.", nil)
		contains(t, res.HTML, `<a href="docs/spec/01-contract.md">the contract</a>`)
	})

	t.Run("link whose URL itself contains parens", func(t *testing.T) {
		res := renderMarkdown("[CSR format](https://en.wikipedia.org/wiki/Sparse_matrix#Compressed_sparse_row_(CSR,_CRS_or_Yale_format))", nil)
		contains(t, res.HTML, `href="https://en.wikipedia.org/wiki/Sparse_matrix#Compressed_sparse_row_(CSR,_CRS_or_Yale_format)"`)
	})

	t.Run("link with a title", func(t *testing.T) {
		res := renderMarkdown(`[Go](https://go.dev "The Go site")`, nil)
		contains(t, res.HTML, `href="https://go.dev" title="The Go site"`)
	})

	t.Run("full reference link", func(t *testing.T) {
		res := renderMarkdown("See [the spec][spec-ref].\n\n[spec-ref]: https://example.com/spec", nil)
		contains(t, res.HTML, `<a href="https://example.com/spec">the spec</a>`)
	})

	t.Run("shortcut reference link", func(t *testing.T) {
		res := renderMarkdown("## [Unreleased]\n\n[Unreleased]: https://example.com/compare", nil)
		contains(t, res.HTML, `<a href="https://example.com/compare">Unreleased</a>`)
	})

	t.Run("repo-relative links are rewritten by the resolver", func(t *testing.T) {
		res := renderMarkdown("[ADR-0041](docs/adr/0041-amendments-discovered-during-implementation.md)", resolveRepoLink)
		contains(t, res.HTML, `<a href="/dagworker/adr/0041-amendments-discovered-during-implementation/">ADR-0041</a>`)
	})
}

func TestImage(t *testing.T) {
	res := renderMarkdown(`![A diagram](diagram.png "Diagram")`, nil)
	contains(t, res.HTML, `<img src="diagram.png" alt="A diagram" title="Diagram"`)
}

func TestLists(t *testing.T) {
	t.Run("tight unordered list", func(t *testing.T) {
		res := renderMarkdown("- one\n- two\n- three", nil)
		contains(t, res.HTML, "<ul>\n<li>one</li>\n<li>two</li>\n<li>three</li>\n</ul>")
	})

	t.Run("ordered list preserves start number", func(t *testing.T) {
		res := renderMarkdown("1. first\n2. second\n3. third", nil)
		contains(t, res.HTML, "<ol>")
		contains(t, res.HTML, "<li>first</li>")
		contains(t, res.HTML, "<li>third</li>")
	})

	t.Run("nested unordered list stays inside its parent item", func(t *testing.T) {
		src := "- outer one\n" +
			"  - inner a\n" +
			"  - inner b\n" +
			"- outer two\n"
		res := renderMarkdown(src, nil)
		// The nested <ul> must be a descendant of the first <li>, not a sibling
		// at the top level: exactly one top-level <ul>...<ul> nesting.
		contains(t, res.HTML, "outer one")
		contains(t, res.HTML, "<ul>\n<li>inner a</li>\n<li>inner b</li>\n</ul>")
		firstLI := strings.Index(res.HTML, "<li>outer one")
		nestedUL := strings.Index(res.HTML, "<li>inner a</li>")
		secondOuter := strings.Index(res.HTML, "outer two")
		if !(firstLI < nestedUL && nestedUL < secondOuter) {
			t.Errorf("nested list is not positioned inside the first outer item:\n%s", res.HTML)
		}
	})

	t.Run("multi-paragraph item keeps its lead line unwrapped", func(t *testing.T) {
		src := "- lead line\n\n  second paragraph\n"
		res := renderMarkdown(src, nil)
		contains(t, res.HTML, "lead line")
		contains(t, res.HTML, "<p>second paragraph</p>")
	})
}

func TestBlockquote(t *testing.T) {
	res := renderMarkdown("> A quoted line.\n> A second quoted line.", nil)
	contains(t, res.HTML, "<blockquote>")
	contains(t, res.HTML, "A quoted line.\nA second quoted line.")
}

func TestHorizontalRule(t *testing.T) {
	cases := []string{"---", "***", "___", "- - -"}
	for _, src := range cases {
		res := renderMarkdown("above\n\n"+src+"\n\nbelow", nil)
		contains(t, res.HTML, "<hr>")
	}
}

func TestHorizontalRuleNotConfusedWithList(t *testing.T) {
	// "- item" must stay a list; only a bare run of 3+ markers is a rule.
	res := renderMarkdown("- item one\n- item two", nil)
	notContains(t, res.HTML, "<hr>")
	contains(t, res.HTML, "<ul>")
}

func TestTable(t *testing.T) {
	src := "| Name | Kind |\n|---|---|\n| build | root |\n| test | child |\n"
	res := renderMarkdown(src, nil)
	contains(t, res.HTML, "<table>")
	contains(t, res.HTML, "<th>Name</th><th>Kind</th>")
	contains(t, res.HTML, "<td>build</td><td>root</td>")
}

func TestFencedCodeBlockWithLanguageClass(t *testing.T) {
	res := renderMarkdown("```go\nfunc main() {}\n```", nil)
	contains(t, res.HTML, `<pre><code class="language-go">`)
	contains(t, res.HTML, "func")
}

// TestFencedCodeBlockHidesFakeHeading is the classic markdown-renderer trap:
// a line starting with "#" inside a fenced code block must render as literal
// text, never as an ATX heading.
func TestFencedCodeBlockHidesFakeHeading(t *testing.T) {
	src := "```yaml\n# This looks like a heading but is a YAML comment\nkey: value\n```"
	res := renderMarkdown(src, nil)
	notContains(t, res.HTML, "<h1")
	contains(t, res.HTML, "This looks like a heading but is a YAML comment")
}

func TestFencedCodeBlockContentIsNotInterpretedAsMarkdown(t *testing.T) {
	src := "```\n*not emphasis*, [not a link](nope), | not | a | table |\n```"
	res := renderMarkdown(src, nil)
	notContains(t, res.HTML, "<em>")
	notContains(t, res.HTML, "<a href")
	notContains(t, res.HTML, "<table>")
	contains(t, res.HTML, "*not emphasis*")
}

func TestSyntaxHighlighting(t *testing.T) {
	cases := []struct {
		name, lang, src string
		want            []string
	}{
		{"go", "go", "func main() {\n\treturn // done\n}", []string{`<span class="tok-kw">func</span>`, `<span class="tok-kw">return</span>`, `<span class="tok-com">// done</span>`}},
		{"sql", "sql", "SELECT * FROM nodes WHERE id = 1", []string{`<span class="tok-kw">SELECT</span>`, `<span class="tok-num">1</span>`}},
		{"yaml", "yaml", "enabled: true # flag", []string{`<span class="tok-kw">true</span>`, `<span class="tok-com"># flag</span>`}},
		{"json", "json", `{"ok": true}`, []string{`<span class="tok-kw">true</span>`}},
		{"shell", "shell", "echo $HOME # home dir", []string{`<span class="tok-var">$HOME</span>`, `<span class="tok-com"># home dir</span>`}},
		{"protobuf", "protobuf", "message Node { string id = 1; }", []string{`<span class="tok-kw">message</span>`, `<span class="tok-num">1</span>`}},
		{"lua", "lua", "local x = 1 -- comment", []string{`<span class="tok-kw">local</span>`, `<span class="tok-com">-- comment</span>`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := renderMarkdown("```"+tc.lang+"\n"+tc.src+"\n```", nil)
			for _, w := range tc.want {
				contains(t, res.HTML, w)
			}
		})
	}
}

func TestFrontMatter(t *testing.T) {
	meta, body := splitFrontMatter("---\ntitle: Quickstart\ndescription: A short guide\n---\n\nBody text.")
	if meta["title"] != "Quickstart" {
		t.Errorf("title = %q, want %q", meta["title"], "Quickstart")
	}
	if meta["description"] != "A short guide" {
		t.Errorf("description = %q, want %q", meta["description"], "A short guide")
	}
	if strings.TrimSpace(body) != "Body text." {
		t.Errorf("body = %q, want %q", body, "Body text.")
	}
}

func TestFrontMatterAbsentIsPassthrough(t *testing.T) {
	_, body := splitFrontMatter("# Just a document\n")
	if body != "# Just a document\n" {
		t.Errorf("body was altered: %q", body)
	}
}

func TestResolveRepoLink(t *testing.T) {
	cases := []struct{ in, want string }{
		{"docs/adr/0017-memcached-rejected-as-storage-backend.md", "/dagworker/adr/0017-memcached-rejected-as-storage-backend/"},
		{"docs/research/00-synthesis.md", "/dagworker/research/00-synthesis/"},
		{"docs/spec/01-contract.md", "/dagworker/reference/contract/"},
		{"docs/spec/02-adapter-contract.md", "/dagworker/reference/adapters/"},
		{"CONTRIBUTING.md", "/dagworker/contributing/"},
		{"CHANGELOG.md", "/dagworker/changelog/"},
		{"docs/adr/", "/dagworker/adr/"},
		{"https://example.com/x", "https://example.com/x"},
		{"#section", "#section"},
		{"SECURITY.md", "https://github.com/specialistvlad/dagworker/blob/main/SECURITY.md"},
		{"dagstoretest/", "https://github.com/specialistvlad/dagworker/tree/main/dagstoretest/"},
	}
	for _, tc := range cases {
		if got := resolveRepoLink(tc.in); got != tc.want {
			t.Errorf("resolveRepoLink(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSlugifyDeduplication(t *testing.T) {
	used := map[string]int{}
	first := slugify("Retry", used)
	second := slugify("Retry", used)
	third := slugify("Retry", used)
	if first != "retry" || second != "retry-1" || third != "retry-2" {
		t.Errorf("got %q, %q, %q", first, second, third)
	}
}
