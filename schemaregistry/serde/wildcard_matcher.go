/**
 * Copyright 2024 Confluent Inc.
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

package serde

import (
	"regexp"
	"strings"
)

/**
 * Matches fully-qualified names that use dot (.) as the name boundary.
 *
 * <p>A '?' matches a single character.
 * A '*' matches one or more characters within a name boundary.
 * A '**' matches one or more characters across name boundaries.
 *
 * <p>Examples:
 * <pre>
 * wildcardMatch("eve", "eve*")                  --&gt; true
 * wildcardMatch("alice.bob.eve", "a*.bob.eve")  --&gt; true
 * wildcardMatch("alice.bob.eve", "a*.bob.e*")   --&gt; true
 * wildcardMatch("alice.bob.eve", "a*")          --&gt; false
 * wildcardMatch("alice.bob.eve", "a**")         --&gt; true
 * wildcardMatch("alice.bob.eve", "alice.bob*")  --&gt; false
 * wildcardMatch("alice.bob.eve", "alice.bob**") --&gt; true
 * </pre>
 *
 * @param str             the string to match on
 * @param wildcardMatcher the wildcard string to match against
 * @return true if the string matches the wildcard string
 */
func match(str string, wildcardMatcher string) bool {
	re := wildcardToRegexp(wildcardMatcher, '.')
	pattern, err := regexp.Compile(re)
	if err != nil {
		return false
	}
	idx := pattern.FindStringIndex(str)
	return idx != nil && idx[0] == 0 && idx[1] == len(str)
}

func wildcardToRegexp(globExp string, separator rune) string {
	var dst strings.Builder
	chars := strings.ReplaceAll(globExp, "**"+string(separator)+"*", "**")
	src := []rune(chars)
	i, size := 0, len(src)
	for i < size {
		c := src[i]
		i++
		switch c {
		case '*':
			// One char lookahead for **
			if i < size && src[i] == '*' {
				dst.WriteString(".*")
				i++
			} else {
				dst.WriteString("[^")
				dst.WriteRune(separator)
				dst.WriteString("]*")
			}
		case '?':
			dst.WriteString("[^")
			dst.WriteRune(separator)
			dst.WriteString("]")
		case '.', '+', '{', '}', '(', ')', '|', '^', '$':
			// These need to be escaped in regular expressions
			dst.WriteRune('\\')
			dst.WriteRune(c)
		case '\\':
			i = doubleSlashes(dst, src, i)
		default:
			dst.WriteRune(c)
		}
	}
	return dst.String()
}

func doubleSlashes(dst strings.Builder, src []rune, i int) int {
	// Emit the next character without special interpretation
	dst.WriteRune('\\')
	if i+1 < len(src) {
		dst.WriteRune('\\')
		dst.WriteRune(src[i])
		i++
	} else {
		// A backslash at the very end is treated like an escaped backslash
		dst.WriteRune('\\')
	}
	return i
}
