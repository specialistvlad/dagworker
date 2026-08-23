// @ts-check
import { defineConfig } from 'astro/config'
import starlight from '@astrojs/starlight'
import { remarkRepoLinks } from './plugins/remark-repo-links.mjs'
import { remarkLegacyHeadingIds } from './plugins/remark-legacy-heading-ids.mjs'

// base is the GitHub Pages project-page prefix. Several documents already link
// to sibling pages as absolute /dagworker/... paths, so changing it means
// changing those too — grep before touching it.
export default defineConfig({
  site: 'https://specialistvlad.github.io',
  base: '/dagworker',
  trailingSlash: 'always',
  markdown: { remarkPlugins: [remarkRepoLinks, remarkLegacyHeadingIds] },
  integrations: [
    starlight({
      title: 'dagworker',
      description:
        'A Go library that manages a dynamic DAG of work and hands ready items to ' +
        'workers you already have, under a fenced lease.',
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/specialistvlad/dagworker' },
      ],
      // Each page carries its own editUrl, set by scripts/ingest.mjs, because
      // src/content/docs is generated: the default would point at a path that
      // exists only inside a build.
      editLink: { baseUrl: 'https://github.com/specialistvlad/dagworker/edit/main/' },
      // The sidebar mirrors the previous site's navigation exactly, so that a
      // reader's bookmarks and mental model both survive the move. The 46 ADRs
      // and 16 dossiers are reached through their index pages, as before.
      sidebar: [
        { label: 'Start', items: [{ label: 'Home', link: '/' }] },
        {
          label: 'Guide',
          items: [
            { label: 'Quickstart', link: '/guide/quickstart/' },
            { label: 'Concepts', link: '/guide/concepts/' },
            { label: 'Trigger rules & branching', link: '/guide/trigger-rules/' },
            { label: 'Dynamic graphs', link: '/guide/dynamic-graphs/' },
            { label: 'Writing workers', link: '/guide/workers/' },
            { label: 'Choosing a backend', link: '/guide/backends/' },
            { label: 'Running in production', link: '/guide/operations/' },
            { label: 'Performance', link: '/guide/performance/' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'Storage & manager contract', link: '/reference/contract/' },
            { label: 'Adapter contract', link: '/reference/adapters/' },
          ],
        },
        {
          label: 'Design record',
          items: [
            { label: 'Architecture Decisions', link: '/adr/' },
            { label: 'Research Dossiers', link: '/research/' },
          ],
        },
        {
          label: 'Project',
          items: [
            { label: 'Contributing', link: '/contributing/' },
            { label: 'Changelog', link: '/changelog/' },
          ],
        },
      ],
    }),
  ],
})
