package main

// siteCSS is the entire stylesheet, inlined into every page so the site has
// no external asset dependencies at all. The palette lives once, as custom
// properties on :root; light is the default, dark is layered on top per the
// three-state pattern (system preference, then an explicit toggle that wins
// in both directions).
const siteCSS = `
:root {
  --bg: #ffffff;
  --bg-raised: #f5f6f8;
  --bg-sidebar: #fafafb;
  --fg: #1b1e24;
  --fg-muted: #5a6070;
  --border: #e2e5ea;
  --accent: #2954d6;
  --accent-fg: #ffffff;
  --link: #2954d6;
  --code-bg: #f5f6f8;
  --code-border: #e2e5ea;
  --shadow: 0 4px 20px rgba(20, 22, 30, 0.08);

  --tok-kw: #7a3e9d;
  --tok-str: #1a7f4b;
  --tok-com: #767c8c;
  --tok-num: #b1580c;
  --tok-var: #0a6e91;

  color-scheme: light;
}

@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --bg: #14161b;
    --bg-raised: #1b1e25;
    --bg-sidebar: #17191f;
    --fg: #e7e9ee;
    --fg-muted: #9aa1b0;
    --border: #2b2f3a;
    --accent: #7fa2ff;
    --accent-fg: #0e1116;
    --link: #7fa2ff;
    --code-bg: #1b1e25;
    --code-border: #2b2f3a;
    --shadow: 0 4px 24px rgba(0, 0, 0, 0.4);

    --tok-kw: #c894e8;
    --tok-str: #6fd196;
    --tok-com: #8890a2;
    --tok-num: #e3a45f;
    --tok-var: #5fc4e8;

    color-scheme: dark;
  }
}

:root[data-theme="dark"] {
  --bg: #14161b;
  --bg-raised: #1b1e25;
  --bg-sidebar: #17191f;
  --fg: #e7e9ee;
  --fg-muted: #9aa1b0;
  --border: #2b2f3a;
  --accent: #7fa2ff;
  --accent-fg: #0e1116;
  --link: #7fa2ff;
  --code-bg: #1b1e25;
  --code-border: #2b2f3a;
  --shadow: 0 4px 24px rgba(0, 0, 0, 0.4);

  --tok-kw: #c894e8;
  --tok-str: #6fd196;
  --tok-com: #8890a2;
  --tok-num: #e3a45f;
  --tok-var: #5fc4e8;

  color-scheme: dark;
}

:root[data-theme="light"] { color-scheme: light; }

* { box-sizing: border-box; }

html { -webkit-text-size-adjust: 100%; }

body {
  margin: 0;
  background: var(--bg);
  color: var(--fg);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  font-size: 16px;
  line-height: 1.65;
  text-rendering: optimizeLegibility;
  -webkit-font-smoothing: antialiased;
}

code, pre, kbd, samp {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
  font-size: 0.875em;
}

/* ---------------------------------------------------------------------- */
/* Skip link + focus                                                      */
/* ---------------------------------------------------------------------- */

.skip-link {
  position: absolute;
  left: 1rem;
  top: -3rem;
  background: var(--accent);
  color: var(--accent-fg);
  padding: 0.6rem 1rem;
  border-radius: 0.375rem;
  z-index: 100;
  transition: top 0.15s ease;
  text-decoration: none;
  font-weight: 600;
}
.skip-link:focus { top: 1rem; }

:focus-visible {
  outline: 3px solid var(--accent);
  outline-offset: 2px;
  border-radius: 2px;
}

/* ---------------------------------------------------------------------- */
/* Header                                                                  */
/* ---------------------------------------------------------------------- */

.site-header {
  position: sticky;
  top: 0;
  z-index: 40;
  background: var(--bg);
  border-bottom: 1px solid var(--border);
}
.header-inner {
  max-width: 100rem;
  margin: 0 auto;
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.75rem 1.25rem;
}
.header-spacer { flex: 1; }
.brand {
  font-weight: 700;
  font-size: 1.05rem;
  color: var(--fg);
  text-decoration: none;
  letter-spacing: -0.01em;
}
.brand:hover { color: var(--accent); }
.header-link {
  color: var(--fg-muted);
  text-decoration: none;
  font-size: 0.9rem;
  padding: 0.4rem 0.5rem;
}
.header-link:hover { color: var(--fg); }

.icon-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 2.25rem;
  height: 2.25rem;
  border-radius: 0.5rem;
  border: 1px solid transparent;
  background: transparent;
  color: var(--fg-muted);
  cursor: pointer;
}
.icon-btn:hover { background: var(--bg-raised); color: var(--fg); }

.nav-toggle { display: none; }

.icon-moon { display: none; }
:root[data-theme="dark"] .icon-sun { display: none; }
:root[data-theme="dark"] .icon-moon { display: inline-block; }
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) .icon-sun { display: none; }
  :root:not([data-theme="light"]) .icon-moon { display: inline-block; }
}

/* ---------------------------------------------------------------------- */
/* Layout                                                                  */
/* ---------------------------------------------------------------------- */

.layout {
  max-width: 100rem;
  margin: 0 auto;
  display: grid;
  grid-template-columns: 16rem minmax(0, 1fr);
  gap: 0;
  align-items: start;
}

.sidebar {
  position: sticky;
  top: 3.4rem;
  align-self: start;
  height: calc(100vh - 3.4rem);
  overflow-y: auto;
  background: var(--bg-sidebar);
  border-right: 1px solid var(--border);
  padding: 1.5rem 1rem 3rem;
}
.nav-group { margin-bottom: 1.5rem; }
.nav-heading {
  font-size: 0.72rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--fg-muted);
  margin: 0 0 0.5rem 0.6rem;
}
.sidebar ul { list-style: none; margin: 0; padding: 0; }
.sidebar li { margin: 0; }
.sidebar a {
  display: block;
  padding: 0.4rem 0.6rem;
  border-radius: 0.375rem;
  color: var(--fg-muted);
  text-decoration: none;
  font-size: 0.92rem;
  line-height: 1.35;
}
.sidebar a:hover { background: var(--bg-raised); color: var(--fg); }
.sidebar a.active {
  background: var(--bg-raised);
  color: var(--accent);
  font-weight: 600;
}

.nav-overlay { display: none; }

.content {
  padding: 2.5rem 2rem 6rem;
  min-width: 0;
}
.content > * {
  max-width: 70ch;
}
.content h1, .content h2, .content h3, .content h4 { max-width: none; }

/* ---------------------------------------------------------------------- */
/* Footer                                                                  */
/* ---------------------------------------------------------------------- */

.site-footer {
  border-top: 1px solid var(--border);
  padding: 2rem 1.25rem 3rem;
  text-align: center;
  color: var(--fg-muted);
  font-size: 0.875rem;
}
.site-footer a { color: var(--fg-muted); }
.site-footer a:hover { color: var(--accent); }

/* ---------------------------------------------------------------------- */
/* Typography                                                              */
/* ---------------------------------------------------------------------- */

.content h1 {
  font-size: 2.25rem;
  line-height: 1.2;
  letter-spacing: -0.015em;
  margin: 0 0 1.25rem;
}
.content h2 {
  font-size: 1.5rem;
  line-height: 1.3;
  margin: 2.5rem 0 1rem;
  padding-top: 0.25rem;
  border-top: 1px solid var(--border);
}
.content h1 + .toc + h2, .content h1 + h2 { border-top: none; padding-top: 0; }
.content h3 { font-size: 1.2rem; margin: 1.75rem 0 0.75rem; }
.content h4 { font-size: 1.02rem; margin: 1.5rem 0 0.6rem; }

.anchor {
  margin-left: 0.4em;
  color: var(--fg-muted);
  text-decoration: none;
  opacity: 0;
  font-weight: 400;
  font-size: 0.8em;
}
h1:hover .anchor, h2:hover .anchor, h3:hover .anchor, h4:hover .anchor,
.anchor:focus { opacity: 1; }

.content p { margin: 0 0 1.1rem; }
.content a { color: var(--link); }
.content strong { font-weight: 650; }

.content ul, .content ol {
  margin: 0 0 1.1rem;
  padding-left: 1.4rem;
}
.content li { margin: 0.3rem 0; }
.content li > ul, .content li > ol { margin: 0.3rem 0 0.3rem; }

.content blockquote {
  margin: 0 0 1.1rem;
  padding: 0.2rem 1rem;
  border-left: 3px solid var(--accent);
  background: var(--bg-raised);
  color: var(--fg-muted);
  border-radius: 0 0.375rem 0.375rem 0;
}
.content blockquote p:last-child { margin-bottom: 0.2rem; }

.content hr {
  border: none;
  border-top: 1px solid var(--border);
  margin: 2.5rem 0;
}

.content code {
  background: var(--code-bg);
  border: 1px solid var(--code-border);
  border-radius: 0.3rem;
  padding: 0.1em 0.35em;
}

.content pre {
  background: var(--code-bg);
  border: 1px solid var(--code-border);
  border-radius: 0.5rem;
  padding: 1rem 1.1rem;
  margin: 0 0 1.25rem;
  overflow-x: auto;
  max-width: 100%;
}
.content pre code {
  background: none;
  border: none;
  padding: 0;
  white-space: pre;
}

.tok-kw { color: var(--tok-kw); font-weight: 600; }
.tok-str { color: var(--tok-str); }
.tok-com { color: var(--tok-com); font-style: italic; }
.tok-num { color: var(--tok-num); }
.tok-var { color: var(--tok-var); }

.table-wrap {
  overflow-x: auto;
  margin: 0 0 1.25rem;
  border: 1px solid var(--border);
  border-radius: 0.5rem;
}
.content table {
  border-collapse: collapse;
  width: 100%;
  font-size: 0.92rem;
}
.content th, .content td {
  padding: 0.55rem 0.8rem;
  border-bottom: 1px solid var(--border);
  text-align: left;
  vertical-align: top;
}
.content thead th {
  background: var(--bg-raised);
  font-weight: 650;
  white-space: nowrap;
}
.content tbody tr:last-child td { border-bottom: none; }

.toc {
  background: var(--bg-raised);
  border: 1px solid var(--border);
  border-radius: 0.5rem;
  padding: 1rem 1.25rem;
  margin: 0 0 1.75rem;
  max-width: 100%;
}
.toc-heading {
  margin: 0 0 0.5rem;
  font-size: 0.75rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--fg-muted);
}
.toc ul { list-style: none; margin: 0; padding-left: 1rem; font-size: 0.9rem; }
.toc > ul { padding-left: 0; }
.toc li { margin: 0.2rem 0; }
.toc a { color: var(--fg-muted); text-decoration: none; }
.toc a:hover { color: var(--link); }

.doc-index { padding-left: 1.4rem; }
.doc-index li { margin: 0.45rem 0; }

.source-link { margin-top: 2.5rem; font-size: 0.875rem; }
.source-link a { color: var(--fg-muted); }

/* ---------------------------------------------------------------------- */
/* Responsive                                                              */
/* ---------------------------------------------------------------------- */

@media (max-width: 900px) {
  .layout { grid-template-columns: 1fr; }
  .nav-toggle { display: inline-flex; }
  .sidebar {
    position: fixed;
    top: 0;
    left: 0;
    height: 100vh;
    width: 17rem;
    max-width: 85vw;
    transform: translateX(-100%);
    transition: transform 0.2s ease;
    z-index: 60;
    box-shadow: var(--shadow);
    padding-top: 4.5rem;
  }
  body.nav-open .sidebar { transform: translateX(0); }
  body.nav-open .nav-overlay {
    display: block;
    position: fixed;
    inset: 0;
    background: rgba(10, 11, 14, 0.4);
    z-index: 50;
  }
  body.nav-open { overflow: hidden; }
  .content { padding: 2rem 1.25rem 4rem; }
  .content h1 { font-size: 1.8rem; }
}

@media (max-width: 520px) {
  .header-link { display: none; }
}
`
