// Fails the build when a rendered link points at nothing.
//
// The site is assembled from Markdown that lives all over the repository and
// links to itself by repository path, so every link is rewritten on the way in
// (plugins/remark-repo-links.mjs). A rewrite rule that misfires produces a URL
// that is well-formed, looks plausible in the sidebar, and 404s -- which is
// exactly what happened to every absolute /dagworker/... link once, unnoticed,
// because nothing checked. This checks.
//
// It reads the built site rather than the sources, so it sees the links a
// reader would click, and it resolves them two ways:
//
//   - a link into this site must land on a page the build produced, and its
//     fragment must match an id on that page;
//   - a link into the repository on GitHub must name a path that exists in
//     this checkout, as the kind (blob for a file, tree for a directory) the
//     URL claims.
//
// Off-site links are not fetched. A link checker that makes network calls is a
// link checker that fails when somebody else's server is slow, and this one
// runs inside the build.

import { readdir, readFile, stat } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const siteDir = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const repoRoot = resolve(siteDir, '../..')
const distDir = join(siteDir, 'dist')

const BASE = '/dagworker'
const REPO = 'https://github.com/specialistvlad/dagworker'

/** walk yields every file under dir whose name matches suffix. */
async function* walk(dir, suffix) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name)
    if (entry.isDirectory()) yield* walk(full, suffix)
    else if (entry.name.endsWith(suffix)) yield full
  }
}

const exists = async (path) => {
  try {
    return await stat(path)
  } catch {
    return null
  }
}

/** anchors returns every href on a page, in source order, with its link text. */
function anchors(html) {
  const out = []
  const re = /<a\b[^>]*?\shref="([^"]*)"[^>]*>(.*?)<\/a>/gs
  for (const m of html.matchAll(re)) {
    out.push({ href: decodeEntities(m[1]), text: m[2].replace(/<[^>]*>/g, '').trim() })
  }
  return out
}

/** ids returns every id attribute on a page: the set a fragment may name. */
function ids(html) {
  return new Set([...html.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1]))
}

const decodeEntities = (s) =>
  s.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')

/** pageOf maps a site path such as /dagworker/adr/ to its built HTML file. */
const pageOf = (path) => {
  const rel = path.slice(BASE.length).replace(/^\//, '')
  return rel === '' || rel.endsWith('/') ? join(distDir, rel, 'index.html') : join(distDir, rel)
}

async function main() {
  const pages = new Map()
  for await (const file of walk(distDir, '.html')) {
    pages.set(file, await readFile(file, 'utf8'))
  }

  const idCache = new Map()
  const idsOf = (file) => {
    if (!idCache.has(file)) idCache.set(file, ids(pages.get(file) ?? ''))
    return idCache.get(file)
  }

  const problems = []
  for (const [file, html] of pages) {
    const from = file.slice(distDir.length + 1)
    const report = (href, why) => problems.push({ from, href, why })

    for (const { href } of anchors(html)) {
      if (href === '' || href.startsWith('mailto:') || href.startsWith('data:')) continue

      if (href.startsWith('#')) {
        if (!idsOf(file).has(href.slice(1))) report(href, 'no such id on this page')
        continue
      }

      const repoLink = new RegExp(`^${REPO}/(blob|tree|edit)/main/`).exec(href)
      if (repoLink) {
        const kind = repoLink[1]
        const path = decodeURIComponent(href.slice(repoLink[0].length))
          .split('#')[0]
          .replace(/\/$/, '')
        const st = path === '' ? null : await exists(join(repoRoot, path))
        if (!st) report(href, `no such path in the repository: ${path || '(empty)'}`)
        else if (kind !== 'tree' && st.isDirectory()) report(href, `directory linked as ${kind}`)
        else if (kind === 'tree' && !st.isDirectory()) report(href, 'file linked as tree')
        continue
      }

      if (/^https?:\/\//.test(href)) continue

      if (!href.startsWith(`${BASE}/`) && href !== BASE) {
        report(href, 'link is neither off-site nor under the site base')
        continue
      }

      const [path, frag] = href.split('#')
      const target = pageOf(path)
      if (!pages.has(target)) report(href, `no page was built at ${path}`)
      else if (frag && !idsOf(target).has(frag)) report(href, `${path} has no id "${frag}"`)
    }
  }

  if (problems.length === 0) {
    console.log(`check-links: ${pages.size} pages, every link resolves`)
    return
  }
  const byPage = new Map()
  for (const p of problems) byPage.set(p.from, [...(byPage.get(p.from) ?? []), p])
  for (const [page, list] of [...byPage].sort()) {
    console.error(`\n${page}`)
    for (const p of list) console.error(`  ${p.href}\n      ${p.why}`)
  }
  const plural = (n, one) => `${n} ${one}${n === 1 ? '' : 's'}`
  console.error(
    `\ncheck-links: ${plural(problems.length, 'broken link')} on ${plural(byPage.size, 'page')}`,
  )
  process.exit(1)
}

await main()
