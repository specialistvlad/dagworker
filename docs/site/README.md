# dagworker documentation site

The generator behind <https://specialistvlad.github.io/dagworker/>. `main.go` (plus
`markdown.go`, `inline.go`, `highlight.go`, `render.go`, `css.go`) renders this
repository's Markdown — the READMEs, all 42 ADRs, all 16 research dossiers, the
normative spec, and a handful of hand-written guide pages — into `public/`, a folder of
plain HTML ready for GitHub Pages. Standard library only: no Node, no Ruby, no CDN, no
JavaScript framework. `docs/site` is its own Go module (`go.mod`), independent of the
core library's.

## Build and preview, in two commands

```
cd docs/site
GOWORK=off go run . -serve
```

Then open <http://localhost:8000/dagworker/>. `-serve` builds `public/` and serves it
under the same `/dagworker/` path prefix GitHub Pages uses, so every link, including the
root-relative ones the generator emits, resolves exactly as it will in production.
Ctrl-C to stop.

To only build — what CI does on every push and pull request — drop `-serve`:

```
GOWORK=off go run .
```

Either command accepts the repository root as a positional argument; it defaults to
`../..`, correct when run from `docs/site` itself:

```
GOWORK=off go run . -serve /path/to/dag-worker-go
```

### Why `GOWORK=off`

The repository root has a `go.work` that resolves the core module against its
backends and adapters (ADR-0031). `docs/site` is deliberately not one of that
workspace's `use` directories — a documentation generator has no business being part of
the library's module graph — so if Go finds `go.work` by walking upward from this
directory, it refuses to treat `docs/site` as a package at all. `GOWORK=off` tells the
go tool to ignore that file and use `docs/site/go.mod` on its own terms, which is what
this module actually is.

## Layout

- `main.go` — the page manifest, repo-relative link resolution, and output writing.
- `markdown.go` / `inline.go` — the Markdown renderer: block structure (headings,
  paragraphs, fenced code, blockquotes, lists at any nesting depth, GitHub-flavoured
  pipe tables, horizontal rules, link reference definitions) and inline formatting
  (code spans, bold/italic, links, images), all HTML-escaped by construction.
- `highlight.go` — a small per-language tokenizer (keywords, strings, comments,
  numbers) for Go, SQL, Lua, YAML, JSON, protobuf and shell fenced code blocks.
- `render.go` / `css.go` — the page shell (header, sidebar, footer, skip link, table of
  contents) and the stylesheet: one accent colour, a ~70ch measure, a light and a dark
  palette as CSS custom properties, and a theme toggle that persists to `localStorage`.
- `main_test.go` — table-driven tests for the renderer, including the cases that break a
  hand-rolled parser: a pipe inside inline code, a fenced code block containing a line
  that looks like a heading, nested lists, and a link whose URL itself contains parens.
- `content/` — hand-written source: the real landing page (`index.md`) and the eight
  guide pages under `guide/`. The guide pages are placeholders: title and front matter
  are final, the prose is for the next pass to write.
- `public/` — generated output. Not committed — `.gitignore`d, and rebuilt by CI on
  every deploy.

## Tests

```
GOWORK=off go test ./...
```
