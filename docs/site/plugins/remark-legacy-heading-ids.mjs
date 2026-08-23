// Reproduces the heading-anchor slugs of the Go generator this replaces.
//
// Without it, 398 of the site's 1,010 heading anchors change and every deep
// link into them — from a GitHub issue, a bookmark, a search result, another
// document — silently lands at the top of the page instead of the section it
// named. The two schemes disagree on punctuation: the old one replaced every
// run of non-alphanumerics with a hyphen, so "1.1 Static Kahn (1962)" became
// "1-1-static-kahn-1962", where github-slugger deletes the punctuation instead
// and produces "11-static-kahn-1962". Section-numbered headings are pervasive
// in the research dossiers, which is why the count is so high.
//
// This runs in the remark phase rather than the rehype one on purpose. Setting
// data.hProperties.id here means the id exists before rehype-slug looks at the
// heading — rehype-slug leaves an element that already has one alone — so the
// anchor, the table of contents and the id all agree. Assigning it later would
// change the id out from under a table of contents already built from the old
// value.

const NON_ALNUM = /[^a-z0-9]+/g

/** plainText is the reader-visible text of a heading, markup removed. */
function plainText(node) {
  if (node.value) return node.value
  if (!node.children) return ''
  return node.children.map(plainText).join('')
}

/** legacySlug ports slugify() from the Go generator, dedupe rule included. */
export function legacySlug(raw, used) {
  let s = plainText(raw).toLowerCase().replace(NON_ALNUM, '-').replace(/^-+|-+$/g, '')
  if (s === '') s = 'section'
  if (!used) return s
  const n = (used.get(s) ?? 0) + 1
  used.set(s, n)
  return n > 1 ? `${s}-${n - 1}` : s
}

export function remarkLegacyHeadingIds() {
  return (tree) => {
    const used = new Map()
    const walk = (node) => {
      if (node.type === 'heading') {
        const id = legacySlug(node, used)
        node.data ??= {}
        node.data.hProperties = { ...(node.data.hProperties ?? {}), id }
      }
      node.children?.forEach(walk)
    }
    walk(tree)
  }
}

export default remarkLegacyHeadingIds
