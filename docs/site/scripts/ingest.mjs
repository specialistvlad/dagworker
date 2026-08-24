// Copies this repository's Markdown into src/content/docs/, which is what
// Astro's content collection reads.
//
// The documents are NOT moved. The ADRs, the research dossiers and the spec
// stay where they are, because their paths are referenced from Go doc
// comments, from CLAUDE.md, from GitHub issues, and from each other — moving
// them to satisfy a documentation tool would be the tool wagging the project.
// This script is the adapter between the two layouts, and src/content/docs is
// generated output that is never committed.
//
// It does two things a copy cannot: it adds the front matter Starlight needs
// (deriving the title from each file's own H1) and it strips that H1, because
// Starlight renders the front-matter title as the page heading and leaving the
// original would print it twice.
//
// Link rewriting is deliberately NOT done here. It happens in
// plugins/remark-repo-links.mjs, against the parsed syntax tree, so that a
// link-shaped string inside a code fence is left alone. Doing it with a
// regular expression over the raw text would rewrite the examples.

import { mkdir, readdir, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const siteDir = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const repoRoot = resolve(siteDir, '../..')
const outDir = join(siteDir, 'src/content/docs')

/** Descriptions the previous generator supplied per page; preserved verbatim. */
const DESCRIPTIONS = {
  'reference/contract': 'The normative contract: transition table, guarantees, and complexity bounds.',
  'reference/adapters': 'The optional gRPC and HTTP/JSON adapter contracts.',
  contributing: 'How to propose a change, and what has to be true before it merges.',
  changelog: 'Notable changes, release by release.',
}

/** yaml quotes a scalar. JSON's string form is valid YAML double-quoted style. */
const yaml = (s) => JSON.stringify(s)

/** hasFrontMatter reports whether src already opens with a --- block. */
const hasFrontMatter = (src) => src.startsWith('---\n')

/**
 * splitTitle pulls the leading H1 off a document and returns it with the
 * remaining body. A file with no leading H1 keeps its body untouched and
 * reports no title, which the caller turns into an error rather than a guess.
 */
function splitTitle(src) {
  const lines = src.split('\n')
  let i = 0
  while (i < lines.length && lines[i].trim() === '') i++
  const m = /^#\s+(.+?)\s*$/.exec(lines[i] ?? '')
  if (!m) return { title: null, body: src }
  i++
  while (i < lines.length && lines[i].trim() === '') i++
  return { title: m[1], body: lines.slice(i).join('\n') }
}

const EDIT_BASE = 'https://github.com/specialistvlad/dagworker/edit/main/'

async function emit(slug, { title, description, body, source }) {
  const target = join(outDir, `${slug}.md`)
  await mkdir(dirname(target), { recursive: true })
  const fm = ['---', `title: ${yaml(title)}`]
  if (description) fm.push(`description: ${yaml(description)}`)
  // Point "Edit page" at the real file. Starlight would otherwise derive it
  // from this generated copy's path, which exists only during a build.
  fm.push(`editUrl: ${yaml(source ? EDIT_BASE + source : false)}`)
  fm.push('---', '')
  await writeFile(target, fm.join('\n') + body.replace(/^\n+/, '') + '\n')
}

/** copyWithFrontMatter ingests one repo file that has no front matter of its own. */
async function copyDerived(srcPath, slug) {
  const src = await readFile(join(repoRoot, srcPath), 'utf8')
  const { title, body } = splitTitle(src)
  if (!title) throw new Error(`${srcPath}: no leading H1 to derive a title from`)
  await emit(slug, { title, description: DESCRIPTIONS[slug], body, source: srcPath })
  return title
}

/** copyAuthored ingests a file that already carries Starlight-shaped front matter. */
async function copyAuthored(srcPath, slug) {
  const src = await readFile(join(repoRoot, srcPath), 'utf8')
  if (!hasFrontMatter(src)) throw new Error(`${srcPath}: expected front matter`)
  const target = join(outDir, `${slug}.md`)
  await mkdir(dirname(target), { recursive: true })
  await writeFile(target, src)
}

const listMarkdown = async (dir) =>
  (await readdir(join(repoRoot, dir)))
    .filter((n) => /^[\w.-]+\.md$/.test(n))
    .sort()

/** indexPage renders the ADR / research listing pages the previous site had. */
async function indexPage(slug, title, description, intro, entries) {
  const rows = entries.map((e) => `- [${e.title}](/dagworker/${e.slug}/)`).join('\n')
  // No source: these two pages are assembled here, not checked in.
  await emit(slug, { title, description, body: `${intro}\n\n${rows}\n`, source: null })
}

// The previous generator wrote its finished HTML into docs/site/public/, which
// is now Astro's static-asset directory: anything there is copied over the
// built site verbatim. A checkout that still has that output silently shadows
// every page Astro renders, so a build succeeds, the pages look right, and they
// are the old ones. It cost a confused half hour once; it should cost nobody a
// second one.
//
// public/index.html is the unambiguous signature -- Astro renders the home page
// itself and would never have a file there -- so this refuses only on the stale
// output, and leaves a public/ holding real assets alone.
async function refuseStaleGeneratorOutput() {
  const stale = join(repoRoot, 'docs/site/public/index.html')
  try {
    await stat(stale)
  } catch {
    return
  }
  console.error(
    'ingest: docs/site/public/ still holds output from the previous generator.\n' +
      '        Astro copies that directory over the built site, so every page would\n' +
      '        be the old one. Remove it:\n\n' +
      '            rm -rf docs/site/public\n',
  )
  process.exit(1)
}

async function main() {
  await refuseStaleGeneratorOutput()
  await rm(outDir, { recursive: true, force: true })
  await mkdir(outDir, { recursive: true })

  // Home and the guide keep their authored front matter.
  await copyAuthored('docs/site/content/index.md', 'index')
  for (const name of await listMarkdown('docs/site/content/guide')) {
    await copyAuthored(`docs/site/content/guide/${name}`, `guide/${name.replace(/\.md$/, '')}`)
  }

  await copyDerived('docs/spec/01-contract.md', 'reference/contract')
  await copyDerived('docs/spec/02-adapter-contract.md', 'reference/adapters')

  const adrs = []
  for (const name of await listMarkdown('docs/adr')) {
    const slug = `adr/${name.replace(/\.md$/, '')}`
    adrs.push({ slug, title: await copyDerived(`docs/adr/${name}`, slug) })
  }
  await indexPage(
    'adr',
    'Architecture Decision Records',
    `All ${adrs.length} architecture decision records, in order.`,
    'Every accepted decision behind dagworker’s design, in the order it was made. ' +
      'An ADR is immutable once accepted; a changed decision gets a new ADR that amends the old one.',
    adrs,
  )

  const research = []
  for (const name of await listMarkdown('docs/research')) {
    const slug = `research/${name.replace(/\.md$/, '')}`
    research.push({ slug, title: await copyDerived(`docs/research/${name}`, slug) })
  }
  await indexPage(
    'research',
    'Research Dossiers',
    `The ${research.length} primary-source research dossiers the design was derived from.`,
    'The primary-source research the design was derived from, plus the synthesis that reconciled it. ' +
      'Every ADR’s “Backing research” line points back into one of these.',
    research,
  )

  await copyDerived('CONTRIBUTING.md', 'contributing')
  await copyDerived('CHANGELOG.md', 'changelog')

  console.log(`ingest: ${adrs.length} ADRs, ${research.length} dossiers, and the guide into ${outDir}`)
}

await main()
