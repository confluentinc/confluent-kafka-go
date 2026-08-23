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
	"errors"
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

// --- fixed-width scalar appends (used by the exported VariantBuilder) ---
//
// These write a specific primitive width (symmetric with the reader's granular
// getters), unlike appendInt, which auto-selects the smallest int width for
// ParseJSON. The byte layout is identical to what ParseJSON produces for a value
// of the same width.

func (b *builder) appendInt8(v int8) {
	b.value = append(b.value, primitiveHeader(tInt1))
	appendLongLE(&b.value, int64(v), 1)
}

func (b *builder) appendInt16(v int16) {
	b.value = append(b.value, primitiveHeader(tInt2))
	appendLongLE(&b.value, int64(v), 2)
}

func (b *builder) appendInt32(v int32) {
	b.value = append(b.value, primitiveHeader(tInt4))
	appendLongLE(&b.value, int64(v), 4)
}

func (b *builder) appendInt64(v int64) {
	b.value = append(b.value, primitiveHeader(tInt8))
	appendLongLE(&b.value, v, 8)
}

func (b *builder) appendFloat(v float32) {
	b.value = append(b.value, primitiveHeader(tFloat))
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], math.Float32bits(v))
	b.value = append(b.value, buf[:]...)
}

func (b *builder) appendBinary(data []byte) {
	b.value = append(b.value, primitiveHeader(tBinary))
	appendUintLE(&b.value, len(data), u32Size)
	b.value = append(b.value, data...)
}

func (b *builder) appendUUID(u [16]byte) {
	b.value = append(b.value, primitiveHeader(tUUID))
	b.value = append(b.value, u[:]...)
}

func (b *builder) appendTemporal(code int, width int, v int64) {
	b.value = append(b.value, primitiveHeader(code))
	appendLongLE(&b.value, v, width)
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
		// A magnitude overflow (e.g. 1e400) is syntactically valid: ParseFloat
		// returns ±Inf with ErrRange. Store the non-finite double rather than
		// rejecting it (the cross-language contract; toJson emits the Infinity
		// bareword). Underflow returns a signed zero, also fine to store. Only a
		// genuine syntax error is a hard failure.
		if !errors.Is(err, strconv.ErrRange) {
			return fmt.Errorf("variant: invalid number literal %q: %w", s, err)
		}
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
	sort.Slice(fields, func(i, j int) bool { return fields[i].key < fields[j].key })

	// Deduplicate keys, keeping the last-written value (last-wins), mirroring the reference
	// builder. Duplicate keys reach here from a JSON object literal (the streaming decoder
	// does not collapse them) or from repeated AppendKey calls.
	fields = b.dedupObjectFields(start, fields)

	numFields := len(fields)
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

// dedupObjectFields removes duplicate keys from a key-sorted field list, keeping the
// last-written occurrence of each key (the entry with the greatest data offset). When a
// duplicate is dropped, the retained values are compacted leftward in the data region,
// their offsets are recomputed, and the data region is truncated. `fields` must already be
// sorted by key. Values are laid out contiguously in insertion order, so a field's value
// spans from its offset to the next-inserted field's offset.
func (b *builder) dedupObjectFields(start int, fields []fieldEntry) []fieldEntry {
	numFields := len(fields)
	if numFields <= 1 {
		return fields
	}

	// Value byte length of each field, keyed by its (unique) data offset.
	dataSize := len(b.value) - start
	offsets := make([]int, numFields)
	for i, f := range fields {
		offsets[i] = f.offset
	}
	sort.Ints(offsets)
	lenAt := make(map[int]int, numFields)
	for i, o := range offsets {
		end := dataSize
		if i+1 < numFields {
			end = offsets[i+1]
		}
		lenAt[o] = end - o
	}

	// Collapse adjacent equal keys, keeping the entry with the greater offset (last write).
	distinctPos := 0
	for i := 1; i < numFields; i++ {
		if fields[i].id == fields[distinctPos].id {
			if fields[distinctPos].offset < fields[i].offset {
				fields[distinctPos] = fields[i]
			}
		} else {
			distinctPos++
			fields[distinctPos] = fields[i]
		}
	}
	if distinctPos+1 == numFields {
		return fields // no duplicates
	}
	fields = fields[:distinctPos+1]

	// Compact retained values leftward (ascending source offset keeps the copy safe) and
	// recompute offsets, then truncate the now-shorter data region.
	sort.Slice(fields, func(i, j int) bool { return fields[i].offset < fields[j].offset })
	curr := 0
	for i := range fields {
		o := fields[i].offset
		l := lenAt[o]
		if curr != o {
			copy(b.value[start+curr:], b.value[start+o:start+o+l])
		}
		fields[i].offset = curr
		curr += l
	}
	b.value = b.value[:start+curr]

	// Restore key order for header emission.
	sort.Slice(fields, func(i, j int) bool { return fields[i].key < fields[j].key })
	return fields
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
	if v <= 0xFFFFFF {
		return 3
	}
	return 4
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

// --- public flat streaming VariantBuilder ---

// VariantBuilder programmatically constructs a Variant using a flat
// streaming-writer model with an internal nesting stack (the arrow-dotnet
// VariantValueWriter shape). A single builder emits scalars and opens/closes
// containers; appends target the current slot (the root, the next array element,
// or the current object field once its key has been set via AppendKey). Object
// fields are sorted by key at EndObject (canonical form) and the metadata
// dictionary accumulates keys in append order.
//
// The output is byte-identical to ParseJSON of the equivalent JSON document.
//
// A VariantBuilder is single-use: after Build (or on the first error) it should
// be discarded. It is not safe for concurrent use.
//
//	b := NewVariantBuilder()
//	b.StartObject()
//	b.AppendKey("id"); b.AppendLong(42)
//	b.AppendKey("tags"); b.StartArray()
//	    b.AppendString("x"); b.AppendString("y")
//	b.EndArray()
//	b.EndObject()
//	v, err := b.Build()
type VariantBuilder struct {
	b           *builder
	stack       []builderFrame
	rootWritten bool
}

type builderFrameKind int

const (
	frameObject builderFrameKind = iota
	frameArray
)

type builderFrame struct {
	kind  builderFrameKind
	start int
	// object state
	fields        []fieldEntry
	pendingKey    string
	pendingID     int
	hasPendingKey bool
	// array state
	offsets []int
}

// NewVariantBuilder returns a new, empty VariantBuilder.
func NewVariantBuilder() *VariantBuilder {
	return &VariantBuilder{b: &builder{dictionary: map[string]int{}}}
}

// prepareSlot records the current write position in the enclosing container (if
// any) so the value about to be written is addressable, and enforces the slot
// rules (a lone root value; a preceding AppendKey inside an object). It must be
// called immediately before any value bytes are written.
func (vb *VariantBuilder) prepareSlot() error {
	if len(vb.stack) == 0 {
		if vb.rootWritten {
			return fmt.Errorf("variant: builder already has a root value")
		}
		vb.rootWritten = true
		return nil
	}
	top := &vb.stack[len(vb.stack)-1]
	switch top.kind {
	case frameArray:
		top.offsets = append(top.offsets, len(vb.b.value)-top.start)
	case frameObject:
		if !top.hasPendingKey {
			return fmt.Errorf("variant: value appended to object without a preceding AppendKey")
		}
		top.fields = append(top.fields, fieldEntry{
			key:    top.pendingKey,
			id:     top.pendingID,
			offset: len(vb.b.value) - top.start,
		})
		top.hasPendingKey = false
	}
	return nil
}

// AppendNull appends a null value to the current slot.
func (vb *VariantBuilder) AppendNull() error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendNull()
	return nil
}

// AppendBoolean appends a boolean value.
func (vb *VariantBuilder) AppendBoolean(v bool) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendBoolean(v)
	return nil
}

// AppendByte appends an INT8 value.
func (vb *VariantBuilder) AppendByte(v int8) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendInt8(v)
	return nil
}

// AppendShort appends an INT16 value.
func (vb *VariantBuilder) AppendShort(v int16) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendInt16(v)
	return nil
}

// AppendInt appends an INT32 value.
func (vb *VariantBuilder) AppendInt(v int32) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendInt32(v)
	return nil
}

// AppendLong appends an INT64 value.
func (vb *VariantBuilder) AppendLong(v int64) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendInt64(v)
	return nil
}

// AppendFloat appends a FLOAT (32-bit) value.
func (vb *VariantBuilder) AppendFloat(v float32) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendFloat(v)
	return nil
}

// AppendDouble appends a DOUBLE (64-bit) value.
func (vb *VariantBuilder) AppendDouble(v float64) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendDouble(v)
	return nil
}

// AppendDecimal appends a decimal value from its unscaled integer (big-endian
// two's-complement bytes) and scale. The width (Decimal4/8/16) is selected from
// the digit count and scale, matching ParseJSON.
func (vb *VariantBuilder) AppendDecimal(unscaledBigEndian []byte, scale int32) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	n := bigIntFromTwosComplementBE(unscaledBigEndian)
	return vb.b.appendDecimal(n, int(scale))
}

// AppendString appends a string, auto-selecting the short-string (<=63 bytes) or
// long-string encoding.
func (vb *VariantBuilder) AppendString(s string) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendString(s)
	return nil
}

// AppendBinary appends a binary (byte-string) value.
func (vb *VariantBuilder) AppendBinary(data []byte) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendBinary(data)
	return nil
}

// AppendUuid appends a UUID value (16 raw big-endian bytes).
func (vb *VariantBuilder) AppendUuid(u [16]byte) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendUUID(u)
	return nil
}

// AppendDate appends a DATE value (days since the Unix epoch).
func (vb *VariantBuilder) AppendDate(daysSinceEpoch int32) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendTemporal(tDate, 4, int64(daysSinceEpoch))
	return nil
}

// AppendTime appends a TIME_NTZ value (microseconds since midnight).
func (vb *VariantBuilder) AppendTime(microsSinceMidnight int64) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendTemporal(tTime, 8, microsSinceMidnight)
	return nil
}

// AppendTimestampTz appends a TIMESTAMP (with time zone) value in microseconds.
func (vb *VariantBuilder) AppendTimestampTz(micros int64) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendTemporal(tTimestamp, 8, micros)
	return nil
}

// AppendTimestampNtz appends a TIMESTAMP_NTZ value in microseconds.
func (vb *VariantBuilder) AppendTimestampNtz(micros int64) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendTemporal(tTimestampNtz, 8, micros)
	return nil
}

// AppendTimestampNanosTz appends a TIMESTAMP_NANOS (with time zone) value in
// nanoseconds.
func (vb *VariantBuilder) AppendTimestampNanosTz(nanos int64) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendTemporal(tTimestampNanos, 8, nanos)
	return nil
}

// AppendTimestampNanosNtz appends a TIMESTAMP_NANOS_NTZ value in nanoseconds.
func (vb *VariantBuilder) AppendTimestampNanosNtz(nanos int64) error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.b.appendTemporal(tTimestampNanosNz, 8, nanos)
	return nil
}

// StartObject opens a new object. Subsequent AppendKey/value pairs populate it
// until the matching EndObject.
func (vb *VariantBuilder) StartObject() error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.stack = append(vb.stack, builderFrame{kind: frameObject, start: len(vb.b.value)})
	return nil
}

// AppendKey sets the key for the next appended value. It is valid only directly
// inside an object and only once per value.
func (vb *VariantBuilder) AppendKey(key string) error {
	if len(vb.stack) == 0 || vb.stack[len(vb.stack)-1].kind != frameObject {
		return fmt.Errorf("variant: AppendKey called outside an object")
	}
	top := &vb.stack[len(vb.stack)-1]
	if top.hasPendingKey {
		return fmt.Errorf("variant: AppendKey called twice without an intervening value")
	}
	top.pendingKey = key
	top.pendingID = vb.b.addKey(key)
	top.hasPendingKey = true
	return nil
}

// EndObject closes the current object, sorting its fields by key.
func (vb *VariantBuilder) EndObject() error {
	if len(vb.stack) == 0 || vb.stack[len(vb.stack)-1].kind != frameObject {
		return fmt.Errorf("variant: EndObject called without a matching StartObject")
	}
	top := vb.stack[len(vb.stack)-1]
	if top.hasPendingKey {
		return fmt.Errorf("variant: EndObject called with a pending key and no value")
	}
	vb.stack = vb.stack[:len(vb.stack)-1]
	vb.b.finishWritingObject(top.start, top.fields)
	return nil
}

// StartArray opens a new array. Subsequent value appends become its elements
// until the matching EndArray.
func (vb *VariantBuilder) StartArray() error {
	if err := vb.prepareSlot(); err != nil {
		return err
	}
	vb.stack = append(vb.stack, builderFrame{kind: frameArray, start: len(vb.b.value)})
	return nil
}

// EndArray closes the current array.
func (vb *VariantBuilder) EndArray() error {
	if len(vb.stack) == 0 || vb.stack[len(vb.stack)-1].kind != frameArray {
		return fmt.Errorf("variant: EndArray called without a matching StartArray")
	}
	top := vb.stack[len(vb.stack)-1]
	vb.stack = vb.stack[:len(vb.stack)-1]
	vb.b.finishWritingArray(top.start, top.offsets)
	return nil
}

// Build finalizes the builder and returns the constructed Variant. It errors if
// a container is still open or no value has been appended.
func (vb *VariantBuilder) Build() (Variant, error) {
	if len(vb.stack) != 0 {
		return Variant{}, fmt.Errorf("variant: Build called with an open container")
	}
	if !vb.rootWritten {
		return Variant{}, fmt.Errorf("variant: Build called with no value appended")
	}
	value, metadata := vb.b.finish()
	return New(value, metadata), nil
}
