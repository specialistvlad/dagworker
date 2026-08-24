// Rewrites repository-relative Markdown links to the URLs this site serves.
//
// A direct port of resolveRepoLink from the Go generator this replaces, with
// one deliberate difference: it runs against the parsed syntax tree rather
// than the raw text, so a link-shaped string inside a code fence or an inline
// code span is left exactly as the author wrote it. Several documents show
// Markdown examples, and a regular expression over the source would rewrite
// them.
//
// Anything that is not a page this site publishes resolves to the file on
// GitHub, so a link in a rendered repository file never 404s.

import { statSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const GITHUB = 'https://github.com/specialistvlad/dagworker'
const BASE = '/dagworker'

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../../..')

const href = (slug) => (slug === '' ? `${BASE}/` : `${BASE}/${slug}/`)

/**
 * githubKind picks between GitHub's two path forms for a repository path.
 *
 * The checkout is right here during a build, so ask it: an extension is a poor
 * proxy for "is a file" and gets LICENSE, Makefile and Dockerfile wrong every
 * time. The extension heuristic remains as the answer for a path that is not
 * in the checkout at all — a link that check-links.mjs will reject anyway.
 */
function githubKind(path) {
  try {
    return statSync(join(repoRoot, path.replace(/\/$/, ''))).isDirectory() ? 'tree' : 'blob'
  } catch {
    return path.endsWith('/') || !/\.[^/.]+$/.test(path) ? 'tree' : 'blob'
  }
}

export function resolveRepoLink(raw) {
  if (!raw) return raw
  if (/^(#|https?:\/\/|mailto:)/.test(raw)) return raw

  // A root-relative link is already a URL this site serves — the guide pages
  // link to their siblings as /dagworker/guide/... — so it is not a repository
  // path and must not be resolved as one. Treating it as one is how every
  // absolute link on the site came to point at
  // github.com/…/tree/main//dagworker/guide/workers/, which is nothing.
  if (raw.startsWith('/')) return raw

  const hash = raw.indexOf('#')
  let clean = hash === -1 ? raw : raw.slice(0, hash)
  const frag = hash === -1 ? '' : raw.slice(hash)

  clean = clean.replace(/^\.\//, '')
  while (clean.startsWith('../')) clean = clean.slice(3)
  const trimmed = clean.replace(/\/$/, '')

  if (clean === '') return raw

  let m
  if ((m = /^docs\/adr\/(.+)\.md$/.exec(clean))) return href(`adr/${m[1]}`) + frag
  if (trimmed === 'docs/adr') return href('adr') + frag
  if ((m = /^docs\/research\/(.+)\.md$/.exec(clean))) return href(`research/${m[1]}`) + frag
  if (trimmed === 'docs/research') return href('research') + frag
  if (clean === 'docs/spec/01-contract.md') return href('reference/contract') + frag
  if (clean === 'docs/spec/02-adapter-contract.md') return href('reference/adapters') + frag
  if (clean === 'CONTRIBUTING.md') return href('contributing') + frag
  if (clean === 'CHANGELOG.md') return href('changelog') + frag
  if (clean === 'README.md') return href('') + frag

  return `${GITHUB}/${githubKind(clean)}/main/${clean}${frag}`
}

/** remarkRepoLinks visits every link and definition node and resolves its target. */
export function remarkRepoLinks() {
  return (tree) => {
    const walk = (node) => {
      if (node.type === 'link' || node.type === 'definition') {
        node.url = resolveRepoLink(node.url)
      }
      if (node.children) node.children.forEach(walk)
    }
    walk(tree)
  }
}

export default remarkRepoLinks
