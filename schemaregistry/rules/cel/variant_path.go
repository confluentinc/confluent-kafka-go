/**
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cel

import (
	"fmt"
	"strconv"
	"unicode"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/variant"
)

// The JSONPath subset used by variants.path(v, path) - a port of the Java/Python VariantPath.
// Supports $, $.field, $.field.subfield, $[i], $["quoted key"] / $['quoted key'].
// Resolution failures (missing field, out-of-bounds index, type mismatch) return (nil, nil);
// malformed paths return an error. Identifier names follow [A-Za-z_][A-Za-z0-9_]*; use the
// quoted form for other keys. Negative indices are rejected. Quoted-key escapes recognize
// only \\ and backslash+quote (option B); any other escape is a parse error.

type variantPathSeg struct {
	isIndex bool
	key     string
	index   int
}

func walkVariantPath(root variant.Variant, path string) (*variant.Variant, error) {
	segs, err := parseVariantPath(path)
	if err != nil {
		return nil, err
	}
	current := &root
	for _, seg := range segs {
		if current == nil {
			return nil, nil
		}
		if seg.isIndex {
			if current.GetType() != variant.Array {
				return nil, nil
			}
			current = current.GetElementAtIndex(seg.index)
		} else {
			if current.GetType() != variant.Object {
				return nil, nil
			}
			current = current.GetFieldByKey(seg.key)
		}
	}
	return current, nil
}

func parseVariantPath(path string) ([]variantPathSeg, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("variant path must start with '$'")
	}
	if path[0] != '$' {
		return nil, fmt.Errorf("variant path must start with '$', got: %s", path)
	}
	var out []variantPathSeg
	pos := 1
	for pos < len(path) {
		ch := path[pos]
		switch ch {
		case '.':
			pos++
			if pos >= len(path) || !(isIdentStart(rune(path[pos]))) {
				return nil, fmt.Errorf(
					"expected identifier (starting with a letter or '_') after '.' in variant path: %s", path)
			}
			start := pos
			pos++
			for pos < len(path) && isIdentPart(rune(path[pos])) {
				pos++
			}
			out = append(out, variantPathSeg{key: path[start:pos]})
		case '[':
			pos++
			if pos >= len(path) {
				return nil, fmt.Errorf("unexpected end of input after '[' in variant path: %s", path)
			}
			if path[pos] == '"' || path[pos] == '\'' {
				key, next, err := readQuotedKey(path, pos)
				if err != nil {
					return nil, err
				}
				pos = next
				out = append(out, variantPathSeg{key: key})
			} else {
				idx, next, err := readIndex(path, pos)
				if err != nil {
					return nil, err
				}
				pos = next
				out = append(out, variantPathSeg{isIndex: true, index: idx})
			}
			if pos >= len(path) || path[pos] != ']' {
				return nil, fmt.Errorf("expected ']' in variant path: %s", path)
			}
			pos++
		default:
			return nil, fmt.Errorf("unexpected character '%c' in variant path: %s", ch, path)
		}
	}
	return out, nil
}

func readQuotedKey(path string, pos int) (string, int, error) {
	quote := path[pos]
	pos++
	var sb []byte
	for pos < len(path) {
		c := path[pos]
		pos++
		if c == '\\' {
			if pos >= len(path) {
				return "", 0, fmt.Errorf("unterminated escape at end of quoted key in variant path: %s", path)
			}
			esc := path[pos]
			pos++
			if esc == '\\' || esc == quote {
				sb = append(sb, esc)
			} else {
				return "", 0, fmt.Errorf(
					"unsupported escape in quoted key of variant path (only '\\\\' and backslash+quote are allowed): %s", path)
			}
		} else if c == quote {
			return string(sb), pos, nil
		} else {
			sb = append(sb, c)
		}
	}
	return "", 0, fmt.Errorf("unterminated quoted key in variant path: %s", path)
}

func readIndex(path string, pos int) (int, int, error) {
	if path[pos] == '-' {
		return 0, 0, fmt.Errorf("negative indices are not supported in variant path: %s", path)
	}
	start := pos
	for pos < len(path) && path[pos] >= '0' && path[pos] <= '9' {
		pos++
	}
	if pos == start {
		return 0, 0, fmt.Errorf("expected integer index in variant path: %s", path)
	}
	n, err := strconv.ParseInt(path[start:pos], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("index out of int range in variant path: %s", path)
	}
	return int(n), pos, nil
}

func isIdentStart(r rune) bool { return unicode.IsLetter(r) || r == '_' }

func isIdentPart(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' }
