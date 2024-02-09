package util

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// EncodeByteString converts a slice of bytes into a string where every byte is an escaped unicode codepoint, ex. 255 -> '\u00ff'
func EncodeByteString(b []byte) string {
	var builder strings.Builder
	builder.WriteRune('"')
	for _, c := range b {
		fmt.Fprintf(&builder, "\\u%04x", c)
	}
	builder.WriteRune('"')
	return builder.String()
}

// DecodeByteString converts a unicode string into a byte slice where each byte is one code point
func DecodeByteString(s string) []byte {
	b := make([]byte, utf8.RuneCountInString(s))
	i := 0
	j := 0
	for i < len(s) {
		r, l := utf8.DecodeRuneInString(s[i:])
		b[j] = byte(r)
		i += l
		j++
	}
	return b
}
