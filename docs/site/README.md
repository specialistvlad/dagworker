# dagworker documentation site

The generator behind <https://specialistvlad.github.io/dagworker/>. It is an
[Astro](https://astro.build) project using the
[Starlight](https://starlight.astro.build) documentation theme, and it renders
this repository's Markdown — the guide pages, all 46 ADRs, all 16 research
dossiers, the normative spec, `CONTRIBUTING.md` and `CHANGELOG.md` — into
`dist/`, ready for GitHub Pages.

## Build and preview, in two commands

```
cd docs/site
npm install
npm run dev
```

Then open the URL it prints. `npm run build` produces `dist/`; `npm run
preview` serves what was built.

Both `dev` and `build` run the ingest step first, so the rendered site can
never be built from a stale copy of the repository's Markdown.

## How the content gets here

The ADRs, dossiers and spec are **not** moved into this directory, and they
should not be. Their paths are referenced from Go doc comments, from
`CLAUDE.md`, from GitHub issues, and from each other; relocating them to suit a
documentation tool would be the tool wagging the project.

Instead `scripts/ingest.mjs` copies them into `src/content/docs/`, which is
what Astro's content collection reads. That directory is generated on every
build and is not committed. The script adds the front matter Starlight needs,
deriving each page's title from its own H1 and then removing that H1 so the
title is not printed twice, and it points each page's "Edit page" link at the
real source file rather than at the generated copy.

Two remark plugins handle what a copy cannot:

- **`plugins/remark-repo-links.mjs`** rewrites repository-relative links
  (`docs/adr/0044-….md`) to the URLs this site serves, and sends anything the
  site does not publish to the file on GitHub. It is a port of the Go
  generator's `resolveRepoLink`, moved to the syntax tree so that a
  link-shaped string inside a code fence is left alone.
- **`plugins/remark-legacy-heading-ids.mjs`** reproduces the previous
  generator's heading-anchor slugs. The two schemes disagree about
  punctuation — `1.1 Static Kahn (1962)` was `1-1-static-kahn-1962` and would
  otherwise become `11-static-kahn-1962` — which would have broken 398 of the
  site's 1,010 anchors and every deep link into them.

## Why this is not a Go module

It was one, and the generator was 3,011 lines of hand-written Go: a Markdown
parser, an inline-span parser, and a syntax highlighter, which together were
54% of it. Those are solved problems, and the reason to have written them —
this repository's zero-dependency rule — does not apply here.

That rule (ADR-0037) binds the **core module**, and is enforced on it by
`go mod tidy -diff` and a `depguard` block. `docs/site` is not the core module.
It produces no published artifact, appears in nobody's dependency graph, is
absent from the root `go.work`, and is absent from `MODULES` in the `Makefile`
— so it is outside `make check` entirely and cannot affect that gate's
ten-second budget. It is built by one job in `.github/workflows/pages.yml` and
nothing else.

Search is [Pagefind](https://pagefind.app), which Starlight builds into the
site automatically. It runs entirely in the browser, needs no service and no
account, and it is the reason 62 dense documents are now findable.
