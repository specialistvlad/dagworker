package main

import "strings"

// highlightCode renders a fenced code block's content as HTML, wrapping
// keywords, strings, comments and numbers in <span class="tok-*"> so the
// stylesheet can colour them. Unrecognized languages, and plain text inside
// a recognized one, are HTML-escaped and passed through untouched. This is a
// small hand-written tokenizer, not a real lexer: good enough to read, not
// meant to validate syntax.
func highlightCode(lang, code string) string {
	spec, ok := langSpecs[lang]
	if !ok {
		return escapeText(code)
	}
	return tokenize(code, spec)
}

type langSpec struct {
	lineComments []string
	blockComment [2]string
	strings      string // quote characters that start a string
	rawString    byte   // a quote character whose strings have no backslash escapes (Go backtick)
	keywords     map[string]bool
	caseInsensKW bool
	numbers      bool
	shellVars    bool
}

func kwSet(words ...string) map[string]bool {
	m := make(map[string]bool, len(words))
	for _, w := range words {
		m[w] = true
	}
	return m
}

var langSpecs = map[string]langSpec{
	"go": {
		lineComments: []string{"//"},
		blockComment: [2]string{"/*", "*/"},
		strings:      `"'`,
		rawString:    '`',
		numbers:      true,
		keywords: kwSet(
			"break", "case", "chan", "const", "continue", "default", "defer", "else",
			"fallthrough", "for", "func", "go", "goto", "if", "import", "interface",
			"map", "package", "range", "return", "select", "struct", "switch", "type",
			"var", "nil", "true", "false", "iota",
			"string", "bool", "byte", "rune", "error", "any",
			"int", "int8", "int16", "int32", "int64",
			"uint", "uint8", "uint16", "uint32", "uint64", "uintptr",
			"float32", "float64", "complex64", "complex128",
			"make", "new", "len", "cap", "append", "copy", "delete", "panic", "recover",
		),
	},
	"sql": {
		lineComments: []string{"--"},
		blockComment: [2]string{"/*", "*/"},
		strings:      `'"`,
		numbers:      true,
		caseInsensKW: true,
		keywords: kwSet(
			"select", "from", "where", "insert", "into", "values", "update", "set",
			"delete", "create", "table", "index", "unique", "primary", "key", "foreign",
			"references", "not", "null", "default", "and", "or", "in", "is", "as",
			"join", "inner", "left", "right", "outer", "on", "group", "by", "order",
			"having", "limit", "offset", "for", "update", "skip", "locked", "returning",
			"with", "cte", "distinct", "union", "all", "case", "when", "then", "else",
			"end", "begin", "commit", "rollback", "transaction", "alter", "drop",
			"constraint", "check", "cascade", "trigger", "function", "language",
			"listen", "notify", "using", "clock_timestamp", "now",
		),
	},
	"lua": {
		lineComments: []string{"--"},
		blockComment: [2]string{"--[[", "]]"},
		strings:      `'"`,
		numbers:      true,
		keywords: kwSet(
			"and", "break", "do", "else", "elseif", "end", "false", "for", "function",
			"goto", "if", "in", "local", "nil", "not", "or", "repeat", "return",
			"then", "true", "until", "while",
		),
	},
	"yaml": {
		lineComments: []string{"#"},
		strings:      `'"`,
		numbers:      true,
		keywords:     kwSet("true", "false", "null", "~", "yes", "no"),
	},
	"json": {
		strings:  `"`,
		numbers:  true,
		keywords: kwSet("true", "false", "null"),
	},
	"proto": {
		lineComments: []string{"//"},
		blockComment: [2]string{"/*", "*/"},
		strings:      `"'`,
		numbers:      true,
		keywords: kwSet(
			"syntax", "package", "import", "option", "message", "enum", "service",
			"rpc", "returns", "repeated", "optional", "required", "reserved", "oneof",
			"map", "stream", "true", "false",
			"int32", "int64", "uint32", "uint64", "sint32", "sint64", "fixed32",
			"fixed64", "sfixed32", "sfixed64", "bool", "string", "bytes", "double",
			"float",
		),
	},
	"protobuf": {}, // filled in below as an alias of proto
	"shell":    {}, // filled in below
	"bash":     {}, // filled in below
	"sh":       {}, // filled in below
}

func init() {
	langSpecs["protobuf"] = langSpecs["proto"]

	shell := langSpec{
		lineComments: []string{"#"},
		strings:      `'"`,
		numbers:      true,
		shellVars:    true,
		keywords: kwSet(
			"if", "then", "else", "elif", "fi", "for", "while", "until", "do", "done",
			"case", "esac", "function", "in", "select", "time", "local", "export",
			"return", "break", "continue", "exit", "set", "shift", "trap", "readonly",
		),
	}
	langSpecs["shell"] = shell
	langSpecs["bash"] = shell
	langSpecs["sh"] = shell
}

func tokenize(code string, spec langSpec) string {
	var b strings.Builder
	runes := []rune(code)
	n := len(runes)
	i := 0

	matchesAt := func(pos int, s string) bool {
		sr := []rune(s)
		if pos+len(sr) > n {
			return false
		}
		for k, r := range sr {
			if runes[pos+k] != r {
				return false
			}
		}
		return true
	}

	for i < n {
		r := runes[i]

		// Line comments.
		matchedLineComment := ""
		for _, lc := range spec.lineComments {
			if matchesAt(i, lc) {
				matchedLineComment = lc
				break
			}
		}
		if matchedLineComment != "" {
			j := i
			for j < n && runes[j] != '\n' {
				j++
			}
			b.WriteString(`<span class="tok-com">`)
			b.WriteString(escapeText(string(runes[i:j])))
			b.WriteString(`</span>`)
			i = j
			continue
		}

		// Block comments.
		if spec.blockComment[0] != "" && matchesAt(i, spec.blockComment[0]) {
			end := indexFrom(runes, spec.blockComment[1], i+len(spec.blockComment[0]))
			j := n
			if end != -1 {
				j = end + len([]rune(spec.blockComment[1]))
			}
			b.WriteString(`<span class="tok-com">`)
			b.WriteString(escapeText(string(runes[i:j])))
			b.WriteString(`</span>`)
			i = j
			continue
		}

		// Raw strings (Go backtick): no escapes.
		if spec.rawString != 0 && byte(r) == spec.rawString && r < 128 {
			j := i + 1
			for j < n && runes[j] != r {
				j++
			}
			if j < n {
				j++
			}
			b.WriteString(`<span class="tok-str">`)
			b.WriteString(escapeText(string(runes[i:j])))
			b.WriteString(`</span>`)
			i = j
			continue
		}

		// Quoted strings.
		if strings.ContainsRune(spec.strings, r) {
			quote := r
			j := i + 1
			for j < n {
				if runes[j] == '\\' && j+1 < n {
					j += 2
					continue
				}
				if runes[j] == quote {
					j++
					break
				}
				j++
			}
			b.WriteString(`<span class="tok-str">`)
			b.WriteString(escapeText(string(runes[i:j])))
			b.WriteString(`</span>`)
			i = j
			continue
		}

		// Shell variables: $NAME, ${NAME}.
		if spec.shellVars && r == '$' {
			j := i + 1
			if j < n && runes[j] == '{' {
				j++
				for j < n && runes[j] != '}' {
					j++
				}
				if j < n {
					j++
				}
			} else {
				for j < n && isIdentRune(runes[j]) {
					j++
				}
			}
			if j > i+1 {
				b.WriteString(`<span class="tok-var">`)
				b.WriteString(escapeText(string(runes[i:j])))
				b.WriteString(`</span>`)
				i = j
				continue
			}
		}

		// Numbers.
		if spec.numbers && (isDigit(r) || (r == '.' && i+1 < n && isDigit(runes[i+1]))) {
			j := i
			for j < n && (isDigit(runes[j]) || runes[j] == '.' || runes[j] == '_' ||
				runes[j] == 'x' || runes[j] == 'X' || isHexRune(runes[j]) ||
				runes[j] == 'e' || runes[j] == 'E' ||
				((runes[j] == '+' || runes[j] == '-') && j > i && (runes[j-1] == 'e' || runes[j-1] == 'E'))) {
				j++
			}
			b.WriteString(`<span class="tok-num">`)
			b.WriteString(escapeText(string(runes[i:j])))
			b.WriteString(`</span>`)
			i = j
			continue
		}

		// Identifiers / keywords.
		if isIdentStart(r) {
			j := i
			for j < n && isIdentRune(runes[j]) {
				j++
			}
			word := string(runes[i:j])
			key := word
			if spec.caseInsensKW {
				key = strings.ToLower(word)
			}
			if spec.keywords[key] {
				b.WriteString(`<span class="tok-kw">`)
				b.WriteString(escapeText(word))
				b.WriteString(`</span>`)
			} else {
				b.WriteString(escapeText(word))
			}
			i = j
			continue
		}

		b.WriteString(escapeText(string(r)))
		i++
	}
	return b.String()
}

func indexFrom(runes []rune, needle string, from int) int {
	nr := []rune(needle)
	for i := from; i+len(nr) <= len(runes); i++ {
		match := true
		for k, r := range nr {
			if runes[i+k] != r {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func isDigit(r rune) bool { return r >= '0' && r <= '9' }
func isHexRune(r rune) bool {
	return (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

func isIdentStart(r rune) bool {
	return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func isIdentRune(r rune) bool {
	return isIdentStart(r) || isDigit(r)
}
