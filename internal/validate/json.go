package validate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"
)

// DefaultMaxDepth bounds JSON nesting when a policy does not set one.
const DefaultMaxDepth = 64

// ParseError is a strict-JSON violation.
type ParseError struct {
	Code string // invalid_utf8, syntax, too_deep, duplicate_key, trailing_data
	Msg  string
	Path string
}

func (e *ParseError) Error() string { return e.Msg }

// ParseStrict decodes JSON into map[string]any / []any / json.Number / string /
// bool / nil. Unlike encoding/json it rejects duplicate object keys (parsers
// disagree on which one wins, which is exactly the validator/consumer mismatch
// this project exists to prevent), invalid UTF-8, trailing data and nesting
// deeper than maxDepth. It runs in one pass and never recurses deeper than
// maxDepth.
func ParseStrict(data []byte, maxDepth int) (any, error) {
	if maxDepth <= 0 {
		maxDepth = DefaultMaxDepth
	}
	if !utf8.Valid(data) {
		return nil, &ParseError{Code: "invalid_utf8", Msg: "payload is not valid UTF-8"}
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := parseValue(dec, 0, maxDepth, "")
	if err != nil {
		var pe *ParseError
		if errors.As(err, &pe) {
			return nil, pe
		}
		return nil, &ParseError{Code: "syntax", Msg: "malformed JSON: " + err.Error()}
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, &ParseError{Code: "trailing_data", Msg: "unexpected data after the JSON document"}
	}
	return v, nil
}

func parseValue(dec *json.Decoder, depth, maxDepth int, path string) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		if err == io.EOF {
			return nil, errors.New("unexpected end of input")
		}
		return nil, err
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return tok, nil
	}
	if depth >= maxDepth {
		return nil, &ParseError{Code: "too_deep", Msg: fmt.Sprintf("nesting deeper than %d", maxDepth), Path: path}
	}
	switch d {
	case '{':
		m := map[string]any{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return nil, err
			}
			k, ok := kt.(string)
			if !ok {
				return nil, errors.New("object key is not a string")
			}
			if _, dup := m[k]; dup {
				return nil, &ParseError{Code: "duplicate_key", Msg: fmt.Sprintf("duplicate object key %q", k), Path: path + "/" + escape(k)}
			}
			v, err := parseValue(dec, depth+1, maxDepth, path+"/"+escape(k))
			if err != nil {
				return nil, err
			}
			m[k] = v
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return m, nil
	case '[':
		a := []any{}
		for dec.More() {
			v, err := parseValue(dec, depth+1, maxDepth, fmt.Sprintf("%s/%d", path, len(a)))
			if err != nil {
				return nil, err
			}
			a = append(a, v)
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return a, nil
	}
	return nil, fmt.Errorf("unexpected delimiter %q", d)
}

func escape(s string) string {
	return strings.NewReplacer("~", "~0", "/", "~1").Replace(s)
}

func unescape(s string) string {
	return strings.NewReplacer("~1", "/", "~0", "~").Replace(s)
}

// Match is one value selected by a pointer pattern.
type Match struct {
	Path  string
	Value any
}

// Select resolves a JSON pointer in which a "*" segment matches every member
// of an object or array. "" selects the root.
func Select(root any, pattern string) ([]Match, error) {
	if pattern == "" {
		return []Match{{"", root}}, nil
	}
	if !strings.HasPrefix(pattern, "/") {
		return nil, fmt.Errorf("pointer %q must be empty or start with /", pattern)
	}
	cur := []Match{{"", root}}
	for _, seg := range strings.Split(pattern[1:], "/") {
		var next []Match
		for _, m := range cur {
			switch v := m.Value.(type) {
			case map[string]any:
				if seg == "*" {
					keys := make([]string, 0, len(v))
					for k := range v {
						keys = append(keys, k)
					}
					sort.Strings(keys)
					for _, k := range keys {
						next = append(next, Match{m.Path + "/" + escape(k), v[k]})
					}
				} else if c, ok := v[unescape(seg)]; ok {
					next = append(next, Match{m.Path + "/" + seg, c})
				}
			case []any:
				if seg == "*" {
					for i, c := range v {
						next = append(next, Match{fmt.Sprintf("%s/%d", m.Path, i), c})
					}
				} else {
					var i int
					if _, err := fmt.Sscanf(seg, "%d", &i); err == nil && i >= 0 && i < len(v) && fmt.Sprint(i) == seg {
						next = append(next, Match{m.Path + "/" + seg, v[i]})
					}
				}
			}
		}
		cur = next
	}
	return cur, nil
}

// Items returns the element count of an array or object, or -1.
func Items(v any) int64 {
	switch x := v.(type) {
	case []any:
		return int64(len(x))
	case map[string]any:
		return int64(len(x))
	}
	return -1
}

// Change is one difference between two documents.
type Change struct {
	Op, Path, Before, After string
}

// Diff lists structural differences between two parsed documents, capped at
// max entries. Values are rendered as compact JSON and truncated; the diff is
// for operators and is never logged by the server.
func Diff(a, b any, max int) (changes []Change, total int) {
	var walk func(path string, x, y any, xok, yok bool)
	emit := func(c Change) {
		total++
		if len(changes) < max {
			changes = append(changes, c)
		}
	}
	walk = func(path string, x, y any, xok, yok bool) {
		switch {
		case !xok:
			emit(Change{"added", path, "", render(y)})
			return
		case !yok:
			emit(Change{"removed", path, render(x), ""})
			return
		}
		switch xv := x.(type) {
		case map[string]any:
			if yv, ok := y.(map[string]any); ok {
				keys := map[string]struct{}{}
				for k := range xv {
					keys[k] = struct{}{}
				}
				for k := range yv {
					keys[k] = struct{}{}
				}
				sorted := make([]string, 0, len(keys))
				for k := range keys {
					sorted = append(sorted, k)
				}
				sort.Strings(sorted)
				for _, k := range sorted {
					xc, xo := xv[k]
					yc, yo := yv[k]
					walk(path+"/"+escape(k), xc, yc, xo, yo)
				}
				return
			}
		case []any:
			if yv, ok := y.([]any); ok {
				n := max2(len(xv), len(yv))
				for i := 0; i < n; i++ {
					var xc, yc any
					if i < len(xv) {
						xc = xv[i]
					}
					if i < len(yv) {
						yc = yv[i]
					}
					walk(fmt.Sprintf("%s/%d", path, i), xc, yc, i < len(xv), i < len(yv))
				}
				return
			}
		}
		if rx, ry := render(x), render(y); rx != ry {
			emit(Change{"changed", path, rx, ry})
		}
	}
	walk("", a, b, a != nil, b != nil)
	return changes, total
}

func max2(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func render(v any) string {
	b, _ := json.Marshal(v)
	if len(b) > 120 {
		return string(b[:117]) + "..."
	}
	return string(b)
}
