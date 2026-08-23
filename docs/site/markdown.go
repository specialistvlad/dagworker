// Package main: markdown.go implements a small, dependency-free Markdown-to-HTML
// renderer tailored to the constructs actually used in this repository's docs:
// ATX headings, paragraphs, fenced code blocks, inline code, bold/italic, links
// (inline, reference and shortcut-reference), images, unordered/ordered lists
// (including nesting), blockquotes, GitHub-flavoured pipe tables, horizontal
// rules, and link reference definitions. It is not a general CommonMark engine.
package main

import (
	"fmt"
	"html"
	"regexp"
	"strconv"
	"strings"
)

// TOCEntry is one heading collected while rendering a document, used to build
// a per-page table of contents.
type TOCEntry struct {
	Level int
	ID    string
	Text  string // plain text, safe to embed as text content
}

// renderResult carries everything a page needs out of one Markdown document.
type renderResult struct {
	HTML  string
	TOC   []TOCEntry
	H1    string // plain text of the first level-1 heading, if any
	HasH1 bool
}

// ---------------------------------------------------------------------------
// Front matter
// ---------------------------------------------------------------------------

// splitFrontMatter pulls a leading "---\n ... \n---" block off src and parses
// it as a flat set of "key: value" pairs. If src has no front matter, meta is
// empty and body is src unchanged.
func splitFrontMatter(src string) (meta map[string]string, body string) {
	meta = map[string]string{}
	if !strings.HasPrefix(src, "---\n") && src != "---" {
		return meta, src
	}
	rest := src[len("---\n"):]
	end := strings.Index(rest, "\n---")
	if end == -1 {
		return meta, src
	}
	header := rest[:end]
	// Skip the closing fence line itself.
	after := rest[end+len("\n---"):]
	after = strings.TrimPrefix(after, "\n")
	for _, line := range strings.Split(header, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		i := strings.Index(line, ":")
		if i == -1 {
			continue
		}
		key := strings.TrimSpace(line[:i])
		val := strings.TrimSpace(line[i+1:])
		val = strings.Trim(val, `"`)
		meta[key] = val
	}
	return meta, after
}

// ---------------------------------------------------------------------------
// Top-level render
// ---------------------------------------------------------------------------

func renderMarkdown(src string, resolve func(string) string) renderResult {
	src = strings.ReplaceAll(src, "\r\n", "\n")
	lines := strings.Split(src, "\n")

	refs, lines := extractRefDefs(lines)

	ctx := &docCtx{refs: refs, usedIDs: map[string]int{}, resolve: resolve}
	blocks := parseBlocks(lines)

	var b strings.Builder
	for _, blk := range blocks {
		renderBlock(&b, blk, ctx, 0)
	}

	res := renderResult{HTML: b.String(), TOC: ctx.toc}
	for _, t := range ctx.toc {
		if t.Level == 1 {
			res.H1 = t.Text
			res.HasH1 = true
			break
		}
	}
	return res
}

type docCtx struct {
	refs    map[string]string
	usedIDs map[string]int
	toc     []TOCEntry
	resolve func(string) string // rewrites a link href; nil means leave it alone
}

func resolveURL(ctx *docCtx, u string) string {
	if ctx == nil || ctx.resolve == nil {
		return u
	}
	return ctx.resolve(u)
}

// ---------------------------------------------------------------------------
// Link reference definitions: "[label]: url" possibly followed by an
// indented title line. Collected from anywhere outside fenced code, removed
// from the line stream so they never render as a paragraph.
// ---------------------------------------------------------------------------

var refDefRe = regexp.MustCompile(`^ {0,3}\[([^\]]+)\]:\s*(\S+)\s*$`)

func extractRefDefs(lines []string) (map[string]string, []string) {
	refs := map[string]string{}
	out := make([]string, 0, len(lines))
	inFence := false
	var fenceMarker string
	for _, line := range lines {
		trimmed := strings.TrimRight(line, " ")
		if !inFence {
			if marker, ok := fenceStart(trimmed); ok {
				inFence = true
				fenceMarker = marker
				out = append(out, line)
				continue
			}
			if m := refDefRe.FindStringSubmatch(line); m != nil {
				label := normalizeLabel(m[1])
				url := strings.Trim(m[2], "<>")
				if _, exists := refs[label]; !exists {
					refs[label] = url
				}
				continue
			}
			out = append(out, line)
		} else {
			out = append(out, line)
			if fenceEnd(trimmed, fenceMarker) {
				inFence = false
			}
		}
	}
	return refs, out
}

func normalizeLabel(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// ---------------------------------------------------------------------------
// Block model
// ---------------------------------------------------------------------------

type blockKind int

const (
	blockParagraph blockKind = iota
	blockHeading
	blockCode
	blockBlockquote
	blockList
	blockTable
	blockHR
)

type block struct {
	kind    blockKind
	level   int      // heading level
	lang    string   // code language
	text    string   // raw text (paragraph/heading source, joined with \n)
	lines   []string // code content, verbatim
	inner   []block  // blockquote children
	items   []listItem
	ordered bool
	start   int // ordered list start number
	rows    [][]string
	aligns  []string
}

type listItem struct {
	blocks []block
}

// ---------------------------------------------------------------------------
// Fence helpers
// ---------------------------------------------------------------------------

var fenceOpenRe = regexp.MustCompile("^ {0,3}([`~]{3,})\\s*([A-Za-z0-9_+.-]*)\\s*$")

// fenceStart reports whether the line opens a fenced code block and returns
// the exact fence marker (e.g. "```" or "~~~~") that must close it.
func fenceStart(line string) (marker string, ok bool) {
	m := fenceOpenRe.FindStringSubmatch(line)
	if m == nil {
		return "", false
	}
	return m[1], true
}

func fenceLang(line string) string {
	m := fenceOpenRe.FindStringSubmatch(line)
	if m == nil {
		return ""
	}
	return m[2]
}

func fenceEnd(line, marker string) bool {
	trimmed := strings.TrimSpace(line)
	if len(trimmed) < len(marker) {
		return false
	}
	ch := marker[0]
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] != ch {
			return false
		}
	}
	return len(trimmed) >= len(marker)
}

var hrRe = regexp.MustCompile(`^ {0,3}([-*_])( *\x01){2,}$`)

func isHR(line string) bool {
	t := line
	if strings.TrimSpace(t) == "" {
		return false
	}
	// Normalize: replace the repeated char with \x01 so hrRe can match any of
	// the three marker characters uniformly.
	trimmed := strings.TrimLeft(t, " ")
	indent := len(t) - len(trimmed)
	if indent > 3 {
		return false
	}
	if len(trimmed) == 0 {
		return false
	}
	ch := trimmed[0]
	if ch != '-' && ch != '*' && ch != '_' {
		return false
	}
	count := 0
	for _, r := range trimmed {
		if r == rune(ch) {
			count++
		} else if r == ' ' {
			continue
		} else {
			return false
		}
	}
	return count >= 3
}

var headingRe = regexp.MustCompile(`^(#{1,6})\s+(.*?)\s*#*\s*$`)

var (
	ulMarkerRe = regexp.MustCompile(`^( {0,3})([-*+])\s+(.*)$`)
	olMarkerRe = regexp.MustCompile(`^( {0,3})(\d{1,9})[.)]\s+(.*)$`)
)

var blockquoteRe = regexp.MustCompile(`^ {0,3}>\s?(.*)$`)

// ---------------------------------------------------------------------------
// Block parser
// ---------------------------------------------------------------------------

func parseBlocks(lines []string) []block {
	var out []block
	i := 0
	n := len(lines)
	for i < n {
		line := lines[i]

		if strings.TrimSpace(line) == "" {
			i++
			continue
		}

		// Fenced code block.
		if marker, ok := fenceStart(line); ok {
			lang := fenceLang(line)
			j := i + 1
			var content []string
			for j < n && !fenceEnd(lines[j], marker) {
				content = append(content, lines[j])
				j++
			}
			if j < n {
				j++ // consume closing fence
			}
			out = append(out, block{kind: blockCode, lang: lang, lines: content})
			i = j
			continue
		}

		// Horizontal rule, checked before list: a line like "- - -" matches
		// both a list-item marker and a thematic break, and CommonMark gives
		// the break priority.
		if isHR(line) {
			out = append(out, block{kind: blockHR})
			i++
			continue
		}

		// ATX heading.
		if m := headingRe.FindStringSubmatch(line); m != nil {
			out = append(out, block{kind: blockHeading, level: len(m[1]), text: m[2]})
			i++
			continue
		}

		// Blockquote.
		if blockquoteRe.MatchString(line) {
			var raw []string
			for i < n && (blockquoteRe.MatchString(lines[i]) || strings.TrimSpace(lines[i]) == "") {
				if strings.TrimSpace(lines[i]) == "" {
					// A blank line ends the blockquote unless followed by more '>' lines.
					if i+1 < n && blockquoteRe.MatchString(lines[i+1]) {
						raw = append(raw, "")
						i++
						continue
					}
					break
				}
				m := blockquoteRe.FindStringSubmatch(lines[i])
				raw = append(raw, m[1])
				i++
			}
			out = append(out, block{kind: blockBlockquote, inner: parseBlocks(raw)})
			continue
		}

		// GFM pipe table: a header row immediately followed by a delimiter row.
		if strings.Contains(line, "|") && i+1 < n && isTableDelim(lines[i+1]) {
			header := splitTableRow(line)
			aligns := tableAligns(lines[i+1])
			j := i + 2
			var rows [][]string
			for j < n {
				t := lines[j]
				if strings.TrimSpace(t) == "" || !strings.Contains(t, "|") {
					break
				}
				rows = append(rows, splitTableRow(t))
				j++
			}
			out = append(out, block{kind: blockTable, rows: append([][]string{header}, rows...), aligns: aligns})
			i = j
			continue
		}

		// Lists (unordered/ordered), possibly nested.
		if ulMarkerRe.MatchString(line) || olMarkerRe.MatchString(line) {
			items, ordered, start, consumed := parseList(lines[i:], 0)
			out = append(out, block{kind: blockList, items: items, ordered: ordered, start: start})
			i += consumed
			continue
		}

		// Paragraph: everything up to the next blank line or block starter.
		var para []string
		for i < n {
			t := lines[i]
			if strings.TrimSpace(t) == "" {
				break
			}
			if _, ok := fenceStart(t); ok {
				break
			}
			if isHR(t) && len(para) > 0 {
				break
			}
			if headingRe.MatchString(t) {
				break
			}
			if blockquoteRe.MatchString(t) {
				break
			}
			if (ulMarkerRe.MatchString(t) || olMarkerRe.MatchString(t)) && len(para) > 0 {
				break
			}
			if strings.Contains(t, "|") && i+1 < n && isTableDelim(lines[i+1]) {
				break
			}
			para = append(para, t)
			i++
		}
		out = append(out, block{kind: blockParagraph, text: strings.Join(para, "\n")})
	}
	return out
}

var tableDelimRe = regexp.MustCompile(`^\s*:?-+:?\s*$`)

func isTableDelim(line string) bool {
	line = strings.TrimSpace(line)
	if !strings.Contains(line, "-") {
		return false
	}
	line = strings.Trim(line, "|")
	cells := strings.Split(line, "|")
	if len(cells) == 0 {
		return false
	}
	for _, c := range cells {
		if !tableDelimRe.MatchString(c) {
			return false
		}
	}
	return true
}

func tableAligns(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.Trim(line, "|")
	cells := strings.Split(line, "|")
	out := make([]string, len(cells))
	for i, c := range cells {
		c = strings.TrimSpace(c)
		left := strings.HasPrefix(c, ":")
		right := strings.HasSuffix(c, ":")
		switch {
		case left && right:
			out[i] = "center"
		case right:
			out[i] = "right"
		case left:
			out[i] = "left"
		default:
			out[i] = ""
		}
	}
	return out
}

// splitTableRow splits a pipe-table row into cells, ignoring pipes that fall
// inside an inline code span and honoring a leading/trailing empty cell
// caused by the row's outer pipes.
func splitTableRow(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "|")
	line = strings.TrimSuffix(line, "|")

	var cells []string
	var cur strings.Builder
	inCode := false
	runes := []rune(line)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case r == '\\' && i+1 < len(runes) && runes[i+1] == '|':
			cur.WriteRune('|')
			i++
		case r == '`':
			inCode = !inCode
			cur.WriteRune(r)
		case r == '|' && !inCode:
			cells = append(cells, strings.TrimSpace(cur.String()))
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	cells = append(cells, strings.TrimSpace(cur.String()))
	return cells
}

// ---------------------------------------------------------------------------
// List parser
//
// baseIndent is the column the marker itself is expected at for this call.
// Each item's body (first-line remainder plus any subsequent lines indented
// at least to the item's content column) is dedented and recursively parsed
// as its own block sequence, which is what gives us nested lists and
// multi-paragraph items for free.
// ---------------------------------------------------------------------------

func parseList(lines []string, baseIndent int) (items []listItem, ordered bool, start int, consumed int) {
	n := len(lines)
	i := 0
	first := true
	for i < n {
		line := lines[i]
		if strings.TrimSpace(line) == "" {
			// A blank line continues the list only if something indented
			// enough (or another marker at baseIndent) follows.
			if i+1 >= n {
				i++
				break
			}
			next := lines[i+1]
			if strings.TrimSpace(next) == "" {
				break
			}
			indent := leadingSpaces(next)
			if indent < baseIndent && !markerAt(next, baseIndent) {
				break
			}
			i++
			continue
		}

		indent := leadingSpaces(line)
		if indent != baseIndent {
			break
		}

		var marker string
		var rest string
		isOrdered := false
		num := 0
		if m := ulMarkerRe.FindStringSubmatch(line); m != nil && len(m[1]) == baseIndent {
			marker = m[2] + " "
			rest = m[3]
		} else if m := olMarkerRe.FindStringSubmatch(line); m != nil && len(m[1]) == baseIndent {
			isOrdered = true
			num, _ = strconv.Atoi(m[2])
			marker = m[2] + ". "
			rest = m[3]
		} else {
			break
		}
		if first {
			ordered = isOrdered
			start = num
			if !ordered {
				start = 1
			}
			first = false
		}

		contentCol := indent + len(marker)
		itemLines := []string{rest}
		i++
		for i < n {
			t := lines[i]
			if strings.TrimSpace(t) == "" {
				if i+1 < n {
					ni := leadingSpaces(lines[i+1])
					if strings.TrimSpace(lines[i+1]) != "" && ni >= contentCol {
						itemLines = append(itemLines, "")
						i++
						continue
					}
				}
				break
			}
			ti := leadingSpaces(t)
			if ti >= contentCol {
				itemLines = append(itemLines, t[min(contentCol, len(t)):])
				i++
				continue
			}
			break
		}
		items = append(items, listItem{blocks: parseBlocks(itemLines)})
	}
	return items, ordered, start, i
}

func leadingSpaces(s string) int {
	n := 0
	for n < len(s) && s[n] == ' ' {
		n++
	}
	if n == len(s) {
		return 0
	}
	return n
}

func markerAt(line string, indent int) bool {
	if m := ulMarkerRe.FindStringSubmatch(line); m != nil && len(m[1]) == indent {
		return true
	}
	if m := olMarkerRe.FindStringSubmatch(line); m != nil && len(m[1]) == indent {
		return true
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

func renderBlock(b *strings.Builder, blk block, ctx *docCtx, depth int) {
	switch blk.kind {
	case blockHeading:
		id := slugify(blk.text, ctx.usedIDs)
		plain := plainText(blk.text, ctx)
		ctx.toc = append(ctx.toc, TOCEntry{Level: blk.level, ID: id, Text: plain})
		fmt.Fprintf(b, "<h%d id=\"%s\">%s<a class=\"anchor\" href=\"#%s\" aria-label=\"Link to this section\">#</a></h%d>\n",
			blk.level, id, renderInline(blk.text, ctx), id, blk.level)

	case blockParagraph:
		b.WriteString("<p>")
		b.WriteString(renderInline(blk.text, ctx))
		b.WriteString("</p>\n")

	case blockCode:
		lang := strings.ToLower(strings.TrimSpace(blk.lang))
		code := strings.Join(blk.lines, "\n")
		class := ""
		if lang != "" {
			class = fmt.Sprintf(" class=\"language-%s\"", html.EscapeString(lang))
		}
		fmt.Fprintf(b, "<pre><code%s>%s</code></pre>\n", class, highlightCode(lang, code))

	case blockHR:
		b.WriteString("<hr>\n")

	case blockBlockquote:
		b.WriteString("<blockquote>\n")
		for _, inner := range blk.inner {
			renderBlock(b, inner, ctx, depth+1)
		}
		b.WriteString("</blockquote>\n")

	case blockList:
		tag := "ul"
		attr := ""
		if blk.ordered {
			tag = "ol"
			if blk.start != 1 {
				attr = fmt.Sprintf(" start=\"%d\"", blk.start)
			}
		}
		fmt.Fprintf(b, "<%s%s>\n", tag, attr)
		for _, item := range blk.items {
			b.WriteString("<li>")
			renderItemBlocks(b, item.blocks, ctx, depth+1)
			b.WriteString("</li>\n")
		}
		fmt.Fprintf(b, "</%s>\n", tag)

	case blockTable:
		renderTable(b, blk, ctx)
	}
}

// renderItemBlocks renders a list item's body. A "tight" item — exactly one
// paragraph, nothing else — is rendered inline without a <p> wrapper, which
// matches how a reader expects short bullet points to look.
func renderItemBlocks(b *strings.Builder, blocks []block, ctx *docCtx, depth int) {
	if len(blocks) == 1 && blocks[0].kind == blockParagraph {
		b.WriteString(renderInline(blocks[0].text, ctx))
		return
	}
	for i, blk := range blocks {
		if blk.kind == blockParagraph && len(blocks) > 1 {
			// Keep the first paragraph of a multi-block item unwrapped so it
			// reads as the item's lead line; later blocks render normally.
			if i == 0 {
				b.WriteString(renderInline(blk.text, ctx))
				if len(blocks) > 1 {
					b.WriteString("\n")
				}
				continue
			}
		}
		renderBlock(b, blk, ctx, depth)
	}
}

func renderTable(b *strings.Builder, blk block, ctx *docCtx) {
	if len(blk.rows) == 0 {
		return
	}
	b.WriteString("<div class=\"table-wrap\"><table>\n<thead><tr>")
	header := blk.rows[0]
	for i, c := range header {
		align := ""
		if i < len(blk.aligns) && blk.aligns[i] != "" {
			align = fmt.Sprintf(" style=\"text-align:%s\"", blk.aligns[i])
		}
		fmt.Fprintf(b, "<th%s>%s</th>", align, renderInline(c, ctx))
	}
	b.WriteString("</tr></thead>\n<tbody>\n")
	for _, row := range blk.rows[1:] {
		b.WriteString("<tr>")
		for i := 0; i < len(header); i++ {
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			align := ""
			if i < len(blk.aligns) && blk.aligns[i] != "" {
				align = fmt.Sprintf(" style=\"text-align:%s\"", blk.aligns[i])
			}
			fmt.Fprintf(b, "<td%s>%s</td>", align, renderInline(cell, ctx))
		}
		b.WriteString("</tr>\n")
	}
	b.WriteString("</tbody></table></div>\n")
}

// ---------------------------------------------------------------------------
// Heading IDs / TOC support
// ---------------------------------------------------------------------------

var slugNonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

func slugify(raw string, used map[string]int) string {
	plain := plainText(raw, nil)
	s := strings.ToLower(plain)
	s = slugNonAlnum.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if s == "" {
		s = "section"
	}
	if used == nil {
		return s
	}
	used[s]++
	if used[s] > 1 {
		return fmt.Sprintf("%s-%d", s, used[s]-1)
	}
	return s
}

// plainText strips inline markup down to reader-visible text, for use in
// heading ids and the table of contents. ctx may be nil.
func plainText(raw string, ctx *docCtx) string {
	rendered := renderInline(raw, ctx)
	rendered = stripTagsRe.ReplaceAllString(rendered, "")
	return html.UnescapeString(rendered)
}

var stripTagsRe = regexp.MustCompile(`<[^>]*>`)
