package main

import (
	"strings"
)

// renderInline turns one run of Markdown source (a heading's text, a
// paragraph, a table cell, ...) into HTML: code spans, images, links
// (inline/reference/shortcut), emphasis, and escaping of everything else.
// ctx may be nil when no reference-link resolution or TOC bookkeeping is
// needed (e.g. while computing plain text for a slug).
func renderInline(src string, ctx *docCtx) string {
	src, protected := protectCodeSpans(src)
	p := &inlineParser{src: []rune(src), ctx: ctx, protected: protected}
	return p.parse(false)
}

// protectCodeSpans replaces every backtick-delimited code span with a private
// -use placeholder holding its final, already-escaped <code> HTML, so that
// emphasis/link scanning never looks inside one.
func protectCodeSpans(s string) (string, []string) {
	var out strings.Builder
	var store []string
	runes := []rune(s)
	i := 0
	for i < len(runes) {
		if runes[i] == '`' {
			j := i
			for j < len(runes) && runes[j] == '`' {
				j++
			}
			fenceLen := j - i
			// Find a closing run of exactly fenceLen backticks.
			k := j
			closeStart := -1
			for k < len(runes) {
				if runes[k] == '`' {
					m := k
					for m < len(runes) && runes[m] == '`' {
						m++
					}
					if m-k == fenceLen {
						closeStart = k
						break
					}
					k = m
					continue
				}
				k++
			}
			if closeStart == -1 {
				out.WriteRune('`')
				i++
				continue
			}
			content := string(runes[j:closeStart])
			content = strings.TrimSpace(content)
			html := "<code>" + escapeText(content) + "</code>"
			store = append(store, html)
			out.WriteRune(placeholderRune)
			out.WriteString(itoa(len(store) - 1))
			out.WriteRune(placeholderRune)
			i = closeStart + fenceLen
			continue
		}
		out.WriteRune(runes[i])
		i++
	}
	return out.String(), store
}

const placeholderRune = '\uE000'

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

type inlineParser struct {
	src       []rune
	pos       int
	ctx       *docCtx
	protected []string
}

// parse consumes runes until the end of input (or, when insideBracket is
// true, until an unescaped ']' that closes an enclosing link's text) and
// returns the rendered HTML.
func (p *inlineParser) parse(insideBracket bool) string {
	var b strings.Builder
	for p.pos < len(p.src) {
		r := p.src[p.pos]

		if insideBracket && r == ']' {
			return b.String()
		}

		switch r {
		case placeholderRune:
			b.WriteString(p.readPlaceholder())
			continue
		case '&':
			b.WriteString("&amp;")
			p.pos++
			continue
		case '<':
			b.WriteString("&lt;")
			p.pos++
			continue
		case '>':
			b.WriteString("&gt;")
			p.pos++
			continue
		case '!':
			if p.peekAt(1) == '[' {
				if out, ok := p.tryImage(); ok {
					b.WriteString(out)
					continue
				}
			}
			b.WriteRune('!')
			p.pos++
			continue
		case '[':
			if out, ok := p.tryLink(); ok {
				b.WriteString(out)
				continue
			}
			b.WriteString("[")
			p.pos++
			continue
		case '*', '_':
			if out, ok := p.tryEmphasis(); ok {
				b.WriteString(out)
				continue
			}
			b.WriteRune(r)
			p.pos++
			continue
		case '\\':
			if p.pos+1 < len(p.src) && isEscapable(p.src[p.pos+1]) {
				b.WriteString(escapeRune(p.src[p.pos+1]))
				p.pos += 2
				continue
			}
			b.WriteRune('\\')
			p.pos++
			continue
		default:
			b.WriteRune(r)
			p.pos++
		}
	}
	return b.String()
}

func (p *inlineParser) readPlaceholder() string {
	// consumes: placeholderRune digits placeholderRune
	p.pos++ // opening marker
	start := p.pos
	for p.pos < len(p.src) && p.src[p.pos] != placeholderRune {
		p.pos++
	}
	numStr := string(p.src[start:p.pos])
	if p.pos < len(p.src) {
		p.pos++ // closing marker
	}
	idx := 0
	for _, c := range numStr {
		idx = idx*10 + int(c-'0')
	}
	if idx >= 0 && idx < len(p.protected) {
		return p.protected[idx]
	}
	return ""
}

func (p *inlineParser) peekAt(off int) rune {
	if p.pos+off < len(p.src) {
		return p.src[p.pos+off]
	}
	return 0
}

func isEscapable(r rune) bool {
	switch r {
	case '\\', '`', '*', '_', '{', '}', '[', ']', '(', ')', '#', '+', '-', '.', '!', '<', '>', '"', '\'', '|':
		return true
	}
	return false
}

func escapeRune(r rune) string {
	switch r {
	case '&':
		return "&amp;"
	case '<':
		return "&lt;"
	case '>':
		return "&gt;"
	default:
		return string(r)
	}
}

func escapeText(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Links and images
// ---------------------------------------------------------------------------

// findBracketEnd returns the index (into p.src) of the ']' that closes the
// '[' at p.pos, respecting nested brackets and code placeholders.
func (p *inlineParser) findBracketEnd(open int) int {
	depth := 1
	i := open + 1
	for i < len(p.src) {
		switch p.src[i] {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return i
			}
		case placeholderRune:
			i++
			for i < len(p.src) && p.src[i] != placeholderRune {
				i++
			}
		}
		i++
	}
	return -1
}

// findParenEnd returns the index of the ')' matching the '(' at open,
// honoring balanced nested parens so a URL like "foo(bar)baz" survives.
func (p *inlineParser) findParenEnd(open int) int {
	depth := 1
	i := open + 1
	for i < len(p.src) {
		switch p.src[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
		i++
	}
	return -1
}

func (p *inlineParser) tryLink() (string, bool) {
	open := p.pos
	close := p.findBracketEnd(open)
	if close == -1 {
		return "", false
	}
	textSrc := string(p.src[open+1 : close])

	after := close + 1
	// Inline form: [text](url "title"?)
	if after < len(p.src) && p.src[after] == '(' {
		parenClose := p.findParenEnd(after)
		if parenClose != -1 {
			inside := strings.TrimSpace(string(p.src[after+1 : parenClose]))
			url, title := splitURLTitle(inside)
			html := p.renderAnchor(textSrc, url, title)
			p.pos = parenClose + 1
			return html, true
		}
	}
	// Full reference form: [text][label]
	if after < len(p.src) && p.src[after] == '[' {
		labelClose := p.findBracketEnd(after)
		if labelClose != -1 {
			label := string(p.src[after+1 : labelClose])
			if label == "" {
				label = textSrc
			}
			if url, ok := p.lookupRef(label); ok {
				html := p.renderAnchor(textSrc, url, "")
				p.pos = labelClose + 1
				return html, true
			}
		}
	}
	// Shortcut reference form: [label]
	if url, ok := p.lookupRef(textSrc); ok {
		html := p.renderAnchor(textSrc, url, "")
		p.pos = close + 1
		return html, true
	}
	return "", false
}

func (p *inlineParser) tryImage() (string, bool) {
	bracketOpen := p.pos + 1 // position of '['
	close := p.findBracketEnd(bracketOpen)
	if close == -1 {
		return "", false
	}
	alt := plainInline(string(p.src[bracketOpen+1:close]), p.ctx)
	after := close + 1
	if after < len(p.src) && p.src[after] == '(' {
		parenClose := p.findParenEnd(after)
		if parenClose != -1 {
			inside := strings.TrimSpace(string(p.src[after+1 : parenClose]))
			url, title := splitURLTitle(inside)
			url = resolveURL(p.ctx, url)
			titleAttr := ""
			if title != "" {
				titleAttr = ` title="` + escapeAttr(title) + `"`
			}
			html := `<img src="` + escapeAttr(url) + `" alt="` + escapeAttr(alt) + `"` + titleAttr + " loading=\"lazy\">"
			p.pos = parenClose + 1
			return html, true
		}
	}
	return "", false
}

func (p *inlineParser) lookupRef(label string) (string, bool) {
	if p.ctx == nil {
		return "", false
	}
	url, ok := p.ctx.refs[normalizeLabel(label)]
	return url, ok
}

func (p *inlineParser) renderAnchor(textSrc, url, title string) string {
	url = resolveURL(p.ctx, url)
	inner := p.renderSub(textSrc)
	titleAttr := ""
	if title != "" {
		titleAttr = ` title="` + escapeAttr(title) + `"`
	}
	rel := ""
	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		rel = ` rel="noopener noreferrer"`
	}
	return `<a href="` + escapeAttr(url) + `"` + titleAttr + rel + `>` + inner + `</a>`
}

// renderSub renders a nested inline run (link text) using the same
// reference table and protected-code store as the parent parser.
func (p *inlineParser) renderSub(s string) string {
	sub := &inlineParser{src: []rune(s), ctx: p.ctx, protected: p.protected}
	return sub.parse(false)
}

func plainInline(s string, ctx *docCtx) string {
	rendered := renderInline(s, ctx)
	return html_stripTags(rendered)
}

func splitURLTitle(s string) (url, title string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	if s[0] == '<' {
		if end := strings.Index(s, ">"); end != -1 {
			url = s[1:end]
			rest := strings.TrimSpace(s[end+1:])
			title = extractTitle(rest)
			return url, title
		}
	}
	// Split on the first whitespace that precedes a quoted title, if any.
	i := strings.IndexAny(s, " \t")
	if i == -1 {
		return s, ""
	}
	url = s[:i]
	title = extractTitle(strings.TrimSpace(s[i+1:]))
	return url, title
}

func extractTitle(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	return ""
}

func escapeAttr(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '&':
			b.WriteString("&amp;")
		case '"':
			b.WriteString("&quot;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Emphasis
// ---------------------------------------------------------------------------

func (p *inlineParser) tryEmphasis() (string, bool) {
	delim := p.src[p.pos]
	runLen := 1
	for p.pos+runLen < len(p.src) && p.src[p.pos+runLen] == delim {
		runLen++
	}
	if runLen > 3 {
		runLen = 3 // longer runs are treated as a triple, matched below
	}

	before := p.peekBefore()
	after := p.peekAtOffset(runLen)
	if !canOpen(before, after) {
		return "", false
	}
	// Underscore emphasis must not open intraword.
	if delim == '_' && isAlnum(before) {
		return "", false
	}

	for try := runLen; try >= 1; try-- {
		closeAt := p.findEmphasisClose(delim, try, p.pos+runLen)
		if closeAt == -1 {
			continue
		}
		innerStart := p.pos + try
		innerSrc := string(p.src[innerStart:closeAt])
		if strings.TrimSpace(innerSrc) == "" {
			continue
		}
		inner := p.renderSub(innerSrc)
		var html string
		switch try {
		case 3:
			html = "<strong><em>" + inner + "</em></strong>"
		case 2:
			html = "<strong>" + inner + "</strong>"
		default:
			html = "<em>" + inner + "</em>"
		}
		p.pos = closeAt + try
		return html, true
	}
	return "", false
}

func (p *inlineParser) findEmphasisClose(delim rune, runLen int, from int) int {
	i := from
	for i < len(p.src) {
		if p.src[i] == delim {
			j := i
			for j < len(p.src) && p.src[j] == delim {
				j++
			}
			got := j - i
			if got >= runLen {
				before := rune(' ')
				if i > 0 {
					before = p.src[i-1]
				}
				after := rune(' ')
				if j < len(p.src) {
					after = p.src[j]
				}
				// A closer must be right-flanking (something real just
				// before it)...
				if isSpace(before) {
					i = j
					continue
				}
				// ...and for underscore specifically, not intraword: a
				// closer immediately followed by another alphanumeric
				// (as in the middle "_" of snake_case_word) doesn't count.
				if delim == '_' && isAlnum(after) {
					i = j
					continue
				}
				return i
			}
			i = j
			continue
		}
		if p.src[i] == placeholderRune {
			i++
			for i < len(p.src) && p.src[i] != placeholderRune {
				i++
			}
		}
		i++
	}
	return -1
}

func (p *inlineParser) peekBefore() rune {
	if p.pos == 0 {
		return ' '
	}
	return p.src[p.pos-1]
}

func (p *inlineParser) peekAtOffset(off int) rune {
	if p.pos+off < len(p.src) {
		return p.src[p.pos+off]
	}
	return ' '
}

func canOpen(before, after rune) bool {
	if after == ' ' || after == 0 {
		return false
	}
	return true
}

func isAlnum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}

// html_stripTags removes any HTML tags from a small already-rendered string;
// used only to build plain alt text for images.
func html_stripTags(s string) string {
	return stripTagsRe.ReplaceAllString(s, "")
}
