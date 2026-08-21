// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package variant

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

// ParseJSON parses a JSON string into a Variant. Number handling follows Java
// VariantUtils.fromJsonNode: a fractional JSON number becomes a DOUBLE; an
// integer becomes the smallest int1/2/4/8 that fits, or a scale-0 decimal when
// wider than 64 bits. Object key order in the encoded value follows the JSON
// document order for the metadata dictionary; the object header itself is
// key-sorted (matching VariantBuilder.cs).
func ParseJSON(jsonStr string) (Variant, error) {
	b := &builder{dictionary: map[string]int{}}
	dec := json.NewDecoder(strings.NewReader(jsonStr))
	dec.UseNumber()
	if err := b.process(dec); err != nil {
		return Variant{}, err
	}
	// Reject trailing content after the top-level value: only a clean io.EOF is
	// acceptable (a valid token or a parse error both mean junk follows).
	if _, err := dec.Token(); err != io.EOF {
		return Variant{}, fmt.Errorf("variant: trailing content after JSON value")
	}
	value, metadata := b.finish()
	return New(value, metadata), nil
}

type fieldEntry struct {
	key    string
	id     int
	offset int
}

type builder struct {
	value          []byte
	dictionary     map[string]int
	dictionaryKeys [][]byte
}

// process reads one JSON value from the decoder and appends its encoding.
func (b *builder) process(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	return b.processToken(dec, tok)
}

func (b *builder) processToken(dec *json.Decoder, tok json.Token) error {
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			return b.processObject(dec)
		case '[':
			return b.processArray(dec)
		default:
			return fmt.Errorf("variant: unexpected JSON delimiter %q", t)
		}
	case string:
		b.appendString(t)
		return nil
	case json.Number:
		return b.appendNumber(string(t))
	case bool:
		b.appendBoolean(t)
		return nil
	case nil:
		b.appendNull()
		return nil
	default:
		return fmt.Errorf("variant: unsupported JSON value: %T", tok)
	}
}

func (b *builder) processObject(dec *json.Decoder) error {
	start := len(b.value)
	var fields []fieldEntry
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := keyTok.(string)
		if !ok {
			return fmt.Errorf("variant: object key is not a string")
		}
		id := b.addKey(key)
		fields = append(fields, fieldEntry{key: key, id: id, offset: len(b.value) - start})
		if err := b.process(dec); err != nil {
			return err
		}
	}
	// Consume the closing '}'.
	if _, err := dec.Token(); err != nil {
		return err
	}
	b.finishWritingObject(start, fields)
	return nil
}

func (b *builder) processArray(dec *json.Decoder) error {
	start := len(b.value)
	var offsets []int
	for dec.More() {
		offsets = append(offsets, len(b.value)-start)
		if err := b.process(dec); err != nil {
			return err
		}
	}
	// Consume the closing ']'.
	if _, err := dec.Token(); err != nil {
		return err
	}
	b.finishWritingArray(start, offsets)
	return nil
}

func (b *builder) addKey(key string) int {
	if existing, ok := b.dictionary[key]; ok {
		return existing
	}
	id := len(b.dictionaryKeys)
	b.dictionary[key] = id
	b.dictionaryKeys = append(b.dictionaryKeys, []byte(key))
	return id
}

func primitiveHeader(typeCode int) byte {
	return byte((typeCode << basicTypeBits) | basicPrimitive)
}

func (b *builder) appendBoolean(v bool) {
	if v {
		b.value = append(b.value, primitiveHeader(tTrue))
	} else {
		b.value = append(b.value, primitiveHeader(tFalse))
	}
}

func (b *builder) appendNull() {
	b.value = append(b.value, primitiveHeader(tNull))
}

func (b *builder) appendString(s string) {
	text := []byte(s)
	if len(text) > maxShortStrSize {
		b.value = append(b.value, primitiveHeader(tLongStr))
		appendUintLE(&b.value, len(text), u32Size)
	} else {
		b.value = append(b.value, byte((len(text)<<basicTypeBits)|basicShortStr))
	}
	b.value = append(b.value, text...)
}

// appendNumber classifies a raw JSON number token: an integer literal (no '.',
// 'e', or 'E') becomes the smallest int1/2/4/8 that fits, or a scale-0 decimal
// when wider than 64 bits; a fractional literal becomes a DOUBLE.
func (b *builder) appendNumber(s string) error {
	if !strings.ContainsAny(s, ".eE") {
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			b.appendInt(i)
			return nil
		}
		bi, ok := new(big.Int).SetString(s, 10)
		if !ok {
			return fmt.Errorf("variant: invalid integer literal %q", s)
		}
		return b.appendDecimal(bi, 0)
	}
	d, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("variant: invalid number literal %q: %w", s, err)
	}
	b.appendDouble(d)
	return nil
}

func (b *builder) appendInt(i int64) {
	switch {
	case i >= math.MinInt8 && i <= math.MaxInt8:
		b.value = append(b.value, primitiveHeader(tInt1))
		appendLongLE(&b.value, i, 1)
	case i >= math.MinInt16 && i <= math.MaxInt16:
		b.value = append(b.value, primitiveHeader(tInt2))
		appendLongLE(&b.value, i, 2)
	case i >= math.MinInt32 && i <= math.MaxInt32:
		b.value = append(b.value, primitiveHeader(tInt4))
		appendLongLE(&b.value, i, 4)
	default:
		b.value = append(b.value, primitiveHeader(tInt8))
		appendLongLE(&b.value, i, 8)
	}
}

func (b *builder) appendDecimal(unscaled *big.Int, scale int) error {
	digits := len(new(big.Int).Abs(unscaled).String())
	var code, width int
	switch {
	case scale <= 9 && digits <= 9:
		code, width = tDecimal4, 4
	case scale <= 18 && digits <= 18:
		code, width = tDecimal8, 8
	case scale <= 38 && digits <= 38:
		code, width = tDecimal16, 16
	default:
		return fmt.Errorf("variant: decimal exceeds maximum precision (38)")
	}
	b.value = append(b.value, primitiveHeader(code), byte(scale))
	appendBigIntLE(&b.value, unscaled, width)
	return nil
}

func (b *builder) appendDouble(d float64) {
	b.value = append(b.value, primitiveHeader(tDouble))
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(d))
	b.value = append(b.value, buf[:]...)
}

func (b *builder) finishWritingArray(start int, offsets []int) {
	dataSize := len(b.value) - start
	numOffsets := len(offsets)
	largeSize := numOffsets > 0xFF
	sizeBytes := 1
	if largeSize {
		sizeBytes = u32Size
	}
	offsetSize := integerSize(dataSize)
	var header []byte
	ls := 0
	if largeSize {
		ls = 1
	}
	header = append(header, byte((ls<<(basicTypeBits+2))|
		((offsetSize-1)<<basicTypeBits)|basicArray))
	appendUintLE(&header, numOffsets, sizeBytes)
	for _, offset := range offsets {
		appendUintLE(&header, offset, offsetSize)
	}
	appendUintLE(&header, dataSize, offsetSize)
	b.insertAt(start, header)
}

func (b *builder) finishWritingObject(start int, fields []fieldEntry) {
	numFields := len(fields)
	sort.Slice(fields, func(i, j int) bool { return fields[i].key < fields[j].key })
	maxID := 0
	for _, f := range fields {
		if f.id > maxID {
			maxID = f.id
		}
	}
	dataSize := len(b.value) - start
	largeSize := numFields > 0xFF
	sizeBytes := 1
	if largeSize {
		sizeBytes = u32Size
	}
	idSize := integerSize(maxID)
	offsetSize := integerSize(dataSize)
	var header []byte
	ls := 0
	if largeSize {
		ls = 1
	}
	header = append(header, byte((ls<<(basicTypeBits+4))|
		((idSize-1)<<(basicTypeBits+2))|
		((offsetSize-1)<<basicTypeBits)|basicObject))
	appendUintLE(&header, numFields, sizeBytes)
	for _, f := range fields {
		appendUintLE(&header, f.id, idSize)
	}
	for _, f := range fields {
		appendUintLE(&header, f.offset, offsetSize)
	}
	appendUintLE(&header, dataSize, offsetSize)
	b.insertAt(start, header)
}

// insertAt inserts header bytes into b.value at the given index.
func (b *builder) insertAt(start int, header []byte) {
	b.value = append(b.value, make([]byte, len(header))...)
	copy(b.value[start+len(header):], b.value[start:])
	copy(b.value[start:], header)
}

func (b *builder) finish() (value, metadata []byte) {
	numKeys := len(b.dictionaryKeys)
	dictStringSize := 0
	for _, k := range b.dictionaryKeys {
		dictStringSize += len(k)
	}
	offsetSize := integerSize(maxInt(dictStringSize, numKeys))

	var md []byte
	md = append(md, byte(version|((offsetSize-1)<<6)))
	appendUintLE(&md, numKeys, offsetSize)
	currentOffset := 0
	for _, k := range b.dictionaryKeys {
		appendUintLE(&md, currentOffset, offsetSize)
		currentOffset += len(k)
	}
	appendUintLE(&md, currentOffset, offsetSize)
	for _, k := range b.dictionaryKeys {
		md = append(md, k...)
	}

	if b.value == nil {
		b.value = []byte{}
	}
	return b.value, md
}

// --- low-level builder helpers ---

func integerSize(v int) int {
	if v <= 0xFF {
		return 1
	}
	if v <= 0xFFFF {
		return 2
	}
	return 3
}

func appendUintLE(out *[]byte, v, numBytes int) {
	for i := 0; i < numBytes; i++ {
		*out = append(*out, byte((v>>(8*i))&0xFF))
	}
}

func appendLongLE(out *[]byte, v int64, width int) {
	for i := 0; i < width; i++ {
		*out = append(*out, byte(v&0xFF))
		v >>= 8
	}
}

// appendBigIntLE appends width bytes of little-endian two's-complement encoding
// of n (matching .NET BigInteger.ToByteArray padded to a fixed width).
func appendBigIntLE(out *[]byte, n *big.Int, width int) {
	mag := new(big.Int).Abs(n).Bytes() // big-endian magnitude
	buf := make([]byte, width)
	for i := 0; i < len(mag) && i < width; i++ {
		buf[i] = mag[len(mag)-1-i] // little-endian
	}
	if n.Sign() < 0 {
		carry := 1
		for i := 0; i < width; i++ {
			v := int(^buf[i]) + carry
			buf[i] = byte(v & 0xFF)
			carry = (v >> 8) & 0x1
		}
	}
	*out = append(*out, buf...)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
