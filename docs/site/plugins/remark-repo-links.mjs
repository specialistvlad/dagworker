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

const GITHUB = 'https://github.com/specialistvlad/dagworker'
const BASE = '/dagworker'

const href = (slug) => (slug === '' ? `${BASE}/` : `${BASE}/${slug}/`)

export function resolveRepoLink(raw) {
  if (!raw) return raw
  if (/^(#|https?:\/\/|mailto:)/.test(raw)) return raw

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

  const kind = clean.endsWith('/') || !/\.[^/.]+$/.test(clean) ? 'tree' : 'blob'
  return `${GITHUB}/${kind}/main/${clean}${frag}`
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
