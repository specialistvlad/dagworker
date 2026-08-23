package main

import (
	"fmt"
	"strings"
)

// basePath is the path prefix the site is served under on GitHub Pages
// (a project site at specialistvlad.github.io/dagworker/). Every internal
// href the generator emits is rooted at this prefix so links work regardless
// of how deep the current page sits.
const basePath = "/dagworker"

// href turns a manifest slug ("" for the home page, "guide/quickstart", ...)
// into an absolute, base-path-prefixed URL ending in "/".
func href(slug string) string {
	if slug == "" {
		return basePath + "/"
	}
	return basePath + "/" + slug + "/"
}

// NavItem is one link in the sidebar.
type NavItem struct {
	Title string
	Slug  string
}

// NavGroup is one labelled section of the sidebar.
type NavGroup struct {
	Title string
	Items []NavItem
}

type pageData struct {
	Title       string // <title> tag content, page name only (site name appended)
	H1          string // rendered as the page's <h1> when InjectH1 is true
	InjectH1    bool
	Description string
	Body        string // already-rendered HTML
	TOC         []TOCEntry
	Slug        string // current page slug, for nav aria-current
	Nav         []NavGroup
	EditPath    string // repo-relative source path shown as "source" link, or ""
}

func renderPage(d pageData) string {
	var b strings.Builder

	b.WriteString("<!doctype html>\n<html lang=\"en\">\n<head>\n")
	b.WriteString("<meta charset=\"utf-8\">\n")
	b.WriteString("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n")
	titleTag := d.Title + " · dagworker"
	if d.Slug == "" {
		titleTag = d.Title
	}
	fmt.Fprintf(&b, "<title>%s</title>\n", escapeAttr(titleTag))
	if d.Description != "" {
		fmt.Fprintf(&b, "<meta name=\"description\" content=\"%s\">\n", escapeAttr(d.Description))
	}
	b.WriteString("<meta name=\"color-scheme\" content=\"light dark\">\n")
	fmt.Fprintf(&b, "<link rel=\"canonical\" href=\"https://specialistvlad.github.io%s\">\n", href(d.Slug))
	b.WriteString("<style>\n")
	b.WriteString(siteCSS)
	b.WriteString("\n</style>\n")
	b.WriteString(themeInitScript)
	b.WriteString("</head>\n<body>\n")

	b.WriteString("<a class=\"skip-link\" href=\"#main\">Skip to content</a>\n")

	// Header.
	b.WriteString("<header class=\"site-header\">\n<div class=\"header-inner\">\n")
	fmt.Fprintf(&b, "<button id=\"nav-toggle\" class=\"icon-btn nav-toggle\" aria-expanded=\"false\" aria-controls=\"sidebar\" aria-label=\"Toggle navigation\">%s</button>\n", menuIconSVG)
	fmt.Fprintf(&b, "<a class=\"brand\" href=\"%s\">dagworker</a>\n", href(""))
	b.WriteString("<div class=\"header-spacer\"></div>\n")
	b.WriteString("<a class=\"header-link\" href=\"https://github.com/specialistvlad/dagworker\" rel=\"noopener noreferrer\">GitHub</a>\n")
	fmt.Fprintf(&b, "<button id=\"theme-toggle\" class=\"icon-btn\" aria-label=\"Toggle color theme\">%s</button>\n", themeIconSVG)
	b.WriteString("</div>\n</header>\n")

	b.WriteString("<div class=\"layout\">\n")
	b.WriteString("<div id=\"nav-overlay\" class=\"nav-overlay\"></div>\n")

	// Sidebar.
	b.WriteString("<nav id=\"sidebar\" class=\"sidebar\" aria-label=\"Primary\">\n")
	for _, group := range d.Nav {
		fmt.Fprintf(&b, "<div class=\"nav-group\">\n<p class=\"nav-heading\">%s</p>\n<ul>\n", escapeText(group.Title))
		for _, item := range group.Items {
			current := ""
			ariaCurrent := ""
			if item.Slug == d.Slug {
				current = " class=\"active\""
				ariaCurrent = " aria-current=\"page\""
			}
			fmt.Fprintf(&b, "<li><a%s href=\"%s\"%s>%s</a></li>\n", current, href(item.Slug), ariaCurrent, escapeText(item.Title))
		}
		b.WriteString("</ul>\n</div>\n")
	}
	b.WriteString("</nav>\n")

	// Main content.
	b.WriteString("<main id=\"main\" class=\"content\">\n")
	if d.InjectH1 {
		fmt.Fprintf(&b, "<h1>%s</h1>\n", escapeText(d.H1))
	}
	if len(d.TOC) >= 2 {
		b.WriteString(renderTOC(d.TOC))
	}
	b.WriteString(d.Body)
	if d.EditPath != "" {
		fmt.Fprintf(&b, "\n<p class=\"source-link\"><a href=\"https://github.com/specialistvlad/dagworker/blob/main/%s\" rel=\"noopener noreferrer\">View source: %s</a></p>\n",
			escapeAttr(d.EditPath), escapeText(d.EditPath))
	}
	b.WriteString("</main>\n")
	b.WriteString("</div>\n") // .layout

	// Footer.
	b.WriteString("<footer class=\"site-footer\">\n<p>")
	b.WriteString("MIT Licensed · <a href=\"https://github.com/specialistvlad/dagworker\" rel=\"noopener noreferrer\">github.com/specialistvlad/dagworker</a> · built by a stdlib-only static site generator")
	b.WriteString("</p>\n</footer>\n")

	b.WriteString(pageScript)
	b.WriteString("</body>\n</html>\n")
	return b.String()
}

// renderTOC turns a flat heading list into a nested "On this page" box.
func renderTOC(entries []TOCEntry) string {
	var b strings.Builder
	b.WriteString("<nav class=\"toc\" aria-label=\"On this page\">\n<p class=\"toc-heading\">On this page</p>\n<ul>\n")
	minLevel := entries[0].Level
	for _, e := range entries {
		if e.Level < minLevel {
			minLevel = e.Level
		}
	}
	level := minLevel
	for _, e := range entries {
		for level < e.Level {
			b.WriteString("<ul>\n")
			level++
		}
		for level > e.Level {
			b.WriteString("</ul>\n")
			level--
		}
		fmt.Fprintf(&b, "<li><a href=\"#%s\">%s</a></li>\n", e.ID, escapeText(e.Text))
	}
	for level > minLevel {
		b.WriteString("</ul>\n")
		level--
	}
	b.WriteString("</ul>\n</nav>\n")
	return b.String()
}

// renderIndexList renders the generated ADR/research index page body: an
// ordered walk of every doc in the collection, linking to its rendered page.
func renderIndexList(intro string, entries []struct {
	Slug  string
	Title string
},
) string {
	var b strings.Builder
	b.WriteString("<p>" + intro + "</p>\n<ol class=\"doc-index\">\n")
	for _, e := range entries {
		fmt.Fprintf(&b, "<li><a href=\"%s\">%s</a></li>\n", href(e.Slug), escapeText(e.Title))
	}
	b.WriteString("</ol>\n")
	return b.String()
}

const menuIconSVG = `<svg viewBox="0 0 24 24" width="20" height="20" aria-hidden="true" focusable="false"><path fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" d="M4 6h16M4 12h16M4 18h16"/></svg>`

const themeIconSVG = `<svg class="icon-sun" viewBox="0 0 24 24" width="18" height="18" aria-hidden="true" focusable="false"><circle cx="12" cy="12" r="4" fill="none" stroke="currentColor" stroke-width="2"/><path stroke="currentColor" stroke-width="2" stroke-linecap="round" d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg><svg class="icon-moon" viewBox="0 0 24 24" width="18" height="18" aria-hidden="true" focusable="false"><path fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round" d="M20 14.5A8 8 0 1 1 9.5 4a6.5 6.5 0 0 0 10.5 10.5z"/></svg>`

// themeInitScript runs before first paint so there is no flash of the wrong
// theme: it reads the persisted choice and stamps data-theme on <html>.
const themeInitScript = `<script>
(function(){
  try {
    var t = localStorage.getItem('dw-theme');
    if (t === 'light' || t === 'dark') {
      document.documentElement.setAttribute('data-theme', t);
    }
  } catch (e) {}
})();
</script>
`

const pageScript = `<script>
(function(){
  function setTheme(t){
    try { localStorage.setItem('dw-theme', t); } catch (e) {}
    if (t) { document.documentElement.setAttribute('data-theme', t); }
    else { document.documentElement.removeAttribute('data-theme'); }
  }
  var toggle = document.getElementById('theme-toggle');
  if (toggle) {
    toggle.addEventListener('click', function(){
      var current = document.documentElement.getAttribute('data-theme');
      if (!current) {
        var prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
        current = prefersDark ? 'dark' : 'light';
      }
      setTheme(current === 'dark' ? 'light' : 'dark');
    });
  }

  var navToggle = document.getElementById('nav-toggle');
  var sidebar = document.getElementById('sidebar');
  var overlay = document.getElementById('nav-overlay');
  function closeNav(){
    document.body.classList.remove('nav-open');
    if (navToggle) navToggle.setAttribute('aria-expanded', 'false');
  }
  function openNav(){
    document.body.classList.add('nav-open');
    if (navToggle) navToggle.setAttribute('aria-expanded', 'true');
  }
  if (navToggle && sidebar) {
    navToggle.addEventListener('click', function(){
      if (document.body.classList.contains('nav-open')) closeNav(); else openNav();
    });
  }
  if (overlay) overlay.addEventListener('click', closeNav);
  document.addEventListener('keydown', function(e){
    if (e.key === 'Escape') closeNav();
  });
})();
</script>
`
