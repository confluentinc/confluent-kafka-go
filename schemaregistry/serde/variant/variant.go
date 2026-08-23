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

// Package variant is a self-contained codec for the Spark/Parquet Variant binary
// format (a metadata key-dictionary plus a self-describing value stream). It is the
// Go counterpart of the .NET Confluent.SchemaRegistry Variant / VariantBuilder and
// of Java's io.confluent.kafka.schemaregistry.type Variant.
//
// This is a leaf package: it imports only the Go standard library (and math/big),
// so higher layers (e.g. the CEL rule engine) can depend on it without import
// cycles.
//
// ToJSON renders temporal types as ISO-8601 with the seconds field always present
// (0/3/6/9-digit fractional grouping) and decimals in fixed-point - the
// cross-language contract. ParseJSON follows Java number handling: a fractional
// JSON number becomes a DOUBLE, an integer wider than 64 bits a scale-0 decimal.
package variant

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Type is the value type of a Variant, mirroring Java's Variant.Type. Integer,
// decimal, and timestamp widths are kept distinct here; the CEL layer collapses
// them.
type Type int

// Variant value types. The iota order matches the C#/C++/Python ports exactly.
const (
	Object Type = iota
	Array
	Null
	Boolean
	Byte
	Short
	Int
	Long
	String
	Double
	Decimal4
	Decimal8
	Decimal16
	Date
	TimestampTz
	TimestampNtz
	Float
	Binary
	Time
	TimestampNanosTz
	TimestampNanosNtz
	Uuid
)

// Format constants (see VariantFormat / Variant.cs).
const (
	// Basic types (low 2 bits of the header byte).
	basicPrimitive = 0
	basicShortStr  = 1
	basicObject    = 2
	basicArray     = 3

	// Primitive type codes (upper 6 bits when basic type == Primitive).
	tNull             = 0
	tTrue             = 1
	tFalse            = 2
	tInt1             = 3
	tInt2             = 4
	tInt4             = 5
	tInt8             = 6
	tDouble           = 7
	tDecimal4         = 8
	tDecimal8         = 9
	tDecimal16        = 10
	tDate             = 11
	tTimestamp        = 12
	tTimestampNtz     = 13
	tFloat            = 14
	tBinary           = 15
	tLongStr          = 16
	tTime             = 17
	tTimestampNanos   = 18
	tTimestampNanosNz = 19
	tUUID             = 20

	basicTypeMask    = 0x3
	basicTypeBits    = 2
	typeInfoMask     = 0x3F
	maxShortStrSize  = 0x3F
	version          = 1
	versionMask      = 0x0F
	u32Size          = 4
	binarySearchThld = 32
)

// Variant is a read-only view over a Variant value at a byte position. Navigation
// (GetFieldByKey / GetElementAtIndex) returns a sub-Variant sharing the same
// buffers (Go slices share their backing array), mirroring the C#/C++ design.
type Variant struct {
	value    []byte
	metadata []byte
	pos      int
}

// New constructs a Variant from raw value + metadata byte slices. Note the
// argument order is (value, metadata), matching the cross-client contract.
func New(value, metadata []byte) Variant {
	return Variant{value: value, metadata: metadata, pos: 0}
}

// ValueBytes returns the raw value bytes (the whole buffer, shared across
// sub-variants).
func (v Variant) ValueBytes() []byte { return v.value }

// MetadataBytes returns the raw metadata bytes (the key dictionary).
func (v Variant) MetadataBytes() []byte { return v.metadata }

// StandaloneValueBytes returns the value buffer sliced from this node's start
// offset - a self-contained value encoding for this node (trailing sibling bytes
// are harmless; the decoder reads only what it needs). A sub-variant re-encodes as
// New(sub.StandaloneValueBytes(), sub.MetadataBytes()). For a root Variant this
// equals ValueBytes().
func (v Variant) StandaloneValueBytes() []byte {
	if v.pos >= len(v.value) {
		return []byte{}
	}
	return v.value[v.pos:]
}

// GetType returns the value type at this position. On malformed data it returns a
// best-effort value; the errorable internal path is used for JSON serialization.
func (v Variant) GetType() Type {
	t, _ := v.variantType()
	return t
}

func (v Variant) variantType() (Type, error) {
	if err := checkIndex(v.pos, len(v.value)); err != nil {
		return 0, err
	}
	basicType := int(v.value[v.pos]) & basicTypeMask
	typeInfo := (int(v.value[v.pos]) >> basicTypeBits) & typeInfoMask
	switch basicType {
	case basicShortStr:
		return String, nil
	case basicObject:
		return Object, nil
	case basicArray:
		return Array, nil
	}
	switch typeInfo {
	case tNull:
		return Null, nil
	case tTrue, tFalse:
		return Boolean, nil
	case tInt1:
		return Byte, nil
	case tInt2:
		return Short, nil
	case tInt4:
		return Int, nil
	case tInt8:
		return Long, nil
	case tDouble:
		return Double, nil
	case tDecimal4:
		return Decimal4, nil
	case tDecimal8:
		return Decimal8, nil
	case tDecimal16:
		return Decimal16, nil
	case tDate:
		return Date, nil
	case tTimestamp:
		return TimestampTz, nil
	case tTimestampNtz:
		return TimestampNtz, nil
	case tFloat:
		return Float, nil
	case tBinary:
		return Binary, nil
	case tLongStr:
		return String, nil
	case tTime:
		return Time, nil
	case tTimestampNanos:
		return TimestampNanosTz, nil
	case tTimestampNanosNz:
		return TimestampNanosNtz, nil
	case tUUID:
		return Uuid, nil
	default:
		return 0, fmt.Errorf("variant: unknown primitive type: %d", typeInfo)
	}
}

// --- scalar getters ---

func (v Variant) primitiveInfo() (int, error) {
	if err := checkIndex(v.pos, len(v.value)); err != nil {
		return 0, err
	}
	basicType := int(v.value[v.pos]) & basicTypeMask
	if basicType != basicPrimitive {
		return 0, fmt.Errorf("variant: expected a primitive value")
	}
	return (int(v.value[v.pos]) >> basicTypeBits) & typeInfoMask, nil
}

// GetBoolean returns the boolean value.
func (v Variant) GetBoolean() (bool, error) {
	ti, err := v.primitiveInfo()
	if err != nil {
		return false, err
	}
	if ti != tTrue && ti != tFalse {
		return false, fmt.Errorf("variant: not a boolean")
	}
	return ti == tTrue, nil
}

// GetByte returns the value of an INT8-backed variant. It does not widen: only a
// byte (INT8) value is accepted.
func (v Variant) GetByte() (int8, error) {
	ti, err := v.primitiveInfo()
	if err != nil {
		return 0, err
	}
	if ti != tInt1 {
		return 0, fmt.Errorf("variant: not a byte")
	}
	n, err := readSignedLong(v.value, v.pos+1, 1)
	if err != nil {
		return 0, err
	}
	return int8(n), nil
}

// GetShort returns the value of an integer-backed variant no wider than INT16
// (byte/short), widening narrower widths.
func (v Variant) GetShort() (int16, error) {
	ti, err := v.primitiveInfo()
	if err != nil {
		return 0, err
	}
	switch ti {
	case tInt1:
		n, err := readSignedLong(v.value, v.pos+1, 1)
		return int16(n), err
	case tInt2:
		n, err := readSignedLong(v.value, v.pos+1, 2)
		return int16(n), err
	default:
		return 0, fmt.Errorf("variant: not a short")
	}
}

// GetInt returns the value of an integer-backed variant no wider than INT32
// (byte/short/int), widening narrower widths.
func (v Variant) GetInt() (int32, error) {
	ti, err := v.primitiveInfo()
	if err != nil {
		return 0, err
	}
	switch ti {
	case tInt1:
		n, err := readSignedLong(v.value, v.pos+1, 1)
		return int32(n), err
	case tInt2:
		n, err := readSignedLong(v.value, v.pos+1, 2)
		return int32(n), err
	case tInt4:
		n, err := readSignedLong(v.value, v.pos+1, 4)
		return int32(n), err
	default:
		return 0, fmt.Errorf("variant: not an int")
	}
}

// GetLong returns the raw integer for any integer-backed type (byte/short/int/
// long, date days, timestamp micros, time micros, timestamp-nanos) - mirrors
// Java getLong.
func (v Variant) GetLong() (int64, error) {
	ti, err := v.primitiveInfo()
	if err != nil {
		return 0, err
	}
	switch ti {
	case tInt1:
		return readSignedLong(v.value, v.pos+1, 1)
	case tInt2:
		return readSignedLong(v.value, v.pos+1, 2)
	case tInt4, tDate:
		return readSignedLong(v.value, v.pos+1, 4)
	case tInt8, tTimestamp, tTimestampNtz, tTime, tTimestampNanos, tTimestampNanosNz:
		return readSignedLong(v.value, v.pos+1, 8)
	default:
		return 0, fmt.Errorf("variant: not an integer-backed type")
	}
}

// GetFloat returns the FLOAT value. It is exact-typed: only a FLOAT value is
// accepted (a DOUBLE is not narrowed).
func (v Variant) GetFloat() (float32, error) {
	ti, err := v.primitiveInfo()
	if err != nil {
		return 0, err
	}
	if ti != tFloat {
		return 0, fmt.Errorf("variant: not a float")
	}
	return readFloatLE(v.value, v.pos+1)
}

// GetDouble returns the DOUBLE value. It is exact-typed: only a DOUBLE value is
// accepted (a FLOAT is not widened; use GetFloat for that).
func (v Variant) GetDouble() (float64, error) {
	ti, err := v.primitiveInfo()
	if err != nil {
		return 0, err
	}
	if ti != tDouble {
		return 0, fmt.Errorf("variant: not a double")
	}
	return readDoubleLE(v.value, v.pos+1)
}

// GetDecimalParts returns the unscaled integer (as big-endian two's-complement
// bytes) and scale of a decimal value (scale preserved).
func (v Variant) GetDecimalParts() (unscaledBigEndian []byte, scale int, err error) {
	ti, err := v.primitiveInfo()
	if err != nil {
		return nil, 0, err
	}
	if err = checkIndex(v.pos+1, len(v.value)); err != nil {
		return nil, 0, err
	}
	scale = int(v.value[v.pos+1])
	var width int
	switch ti {
	case tDecimal4:
		width = 4
	case tDecimal8:
		width = 8
	case tDecimal16:
		width = 16
	default:
		return nil, 0, fmt.Errorf("variant: not a decimal")
	}
	if err = checkIndex(v.pos+2+width-1, len(v.value)); err != nil {
		return nil, 0, err
	}
	// Value bytes are little-endian two's-complement; reverse for big-endian.
	be := make([]byte, width)
	for i := 0; i < width; i++ {
		be[i] = v.value[v.pos+2+(width-1-i)]
	}
	return be, scale, nil
}

// GetDecimalString returns the plain decimal string (fixed-point, never
// scientific), i.e. .NET BigDecimal.ToPlainString / Java toPlainString.
func (v Variant) GetDecimalString() (string, error) {
	be, scale, err := v.GetDecimalParts()
	if err != nil {
		return "", err
	}
	n := bigIntFromTwosComplementBE(be)
	return decimalPlainString(n, scale), nil
}

// GetBinary returns the binary value.
func (v Variant) GetBinary() ([]byte, error) {
	ti, err := v.primitiveInfo()
	if err != nil {
		return nil, err
	}
	if ti != tBinary {
		return nil, fmt.Errorf("variant: not binary")
	}
	length, err := readUnsignedLE(v.value, v.pos+1, u32Size)
	if err != nil {
		return nil, err
	}
	start := v.pos + 1 + u32Size
	if err = checkIndex(start+length-1, len(v.value)); err != nil {
		return nil, err
	}
	result := make([]byte, length)
	copy(result, v.value[start:start+length])
	return result, nil
}

// GetUuid returns the UUID as its canonical big-endian hex string
// (e.g. "00112233-4455-6677-8899-aabbccddeeff").
func (v Variant) GetUuid() (string, error) {
	ti, err := v.primitiveInfo()
	if err != nil {
		return "", err
	}
	if ti != tUUID {
		return "", fmt.Errorf("variant: not a uuid")
	}
	start := v.pos + 1
	if err = checkIndex(start+15, len(v.value)); err != nil {
		return "", err
	}
	return formatUUID(v.value, start), nil
}

// GetString returns the string value.
func (v Variant) GetString() (string, error) {
	if err := checkIndex(v.pos, len(v.value)); err != nil {
		return "", err
	}
	basicType := int(v.value[v.pos]) & basicTypeMask
	typeInfo := (int(v.value[v.pos]) >> basicTypeBits) & typeInfoMask
	var start, length int
	if basicType == basicShortStr {
		start = v.pos + 1
		length = typeInfo
	} else if basicType == basicPrimitive && typeInfo == tLongStr {
		var err error
		if length, err = readUnsignedLE(v.value, v.pos+1, u32Size); err != nil {
			return "", err
		}
		start = v.pos + 1 + u32Size
	} else {
		return "", fmt.Errorf("variant: not a string")
	}
	if length == 0 {
		return "", nil
	}
	if err := checkIndex(start+length-1, len(v.value)); err != nil {
		return "", err
	}
	return string(v.value[start : start+length]), nil
}

// --- object / array navigation ---

type objectInfo struct {
	numFields, idSize, offsetSize, idStart, offsetStart, dataStart int
}

func (v Variant) objectInfo() (objectInfo, error) {
	var o objectInfo
	if err := checkIndex(v.pos, len(v.value)); err != nil {
		return o, err
	}
	basicType := int(v.value[v.pos]) & basicTypeMask
	typeInfo := (int(v.value[v.pos]) >> basicTypeBits) & typeInfoMask
	if basicType != basicObject {
		return o, fmt.Errorf("variant: not an object")
	}
	largeSize := ((typeInfo >> 4) & 0x1) != 0
	sizeBytes := 1
	if largeSize {
		sizeBytes = u32Size
	}
	n, err := readUnsignedLE(v.value, v.pos+1, sizeBytes)
	if err != nil {
		return o, err
	}
	o.numFields = n
	o.idSize = ((typeInfo >> 2) & 0x3) + 1
	o.offsetSize = (typeInfo & 0x3) + 1
	o.idStart = v.pos + 1 + sizeBytes
	o.offsetStart = o.idStart + o.numFields*o.idSize
	o.dataStart = o.offsetStart + (o.numFields+1)*o.offsetSize
	return o, nil
}

type arrayInfo struct {
	numFields, offsetSize, offsetStart, dataStart int
}

func (v Variant) arrayInfo() (arrayInfo, error) {
	var a arrayInfo
	if err := checkIndex(v.pos, len(v.value)); err != nil {
		return a, err
	}
	basicType := int(v.value[v.pos]) & basicTypeMask
	typeInfo := (int(v.value[v.pos]) >> basicTypeBits) & typeInfoMask
	if basicType != basicArray {
		return a, fmt.Errorf("variant: not an array")
	}
	largeSize := ((typeInfo >> 2) & 0x1) != 0
	sizeBytes := 1
	if largeSize {
		sizeBytes = u32Size
	}
	n, err := readUnsignedLE(v.value, v.pos+1, sizeBytes)
	if err != nil {
		return a, err
	}
	a.numFields = n
	a.offsetSize = (typeInfo & 0x3) + 1
	a.offsetStart = v.pos + 1 + sizeBytes
	a.dataStart = a.offsetStart + (a.numFields+1)*a.offsetSize
	return a, nil
}

// NumObjectFields returns the number of fields in an object (0 on error).
func (v Variant) NumObjectFields() int {
	o, err := v.objectInfo()
	if err != nil {
		return 0
	}
	return o.numFields
}

// NumArrayElements returns the number of elements in an array (0 on error).
func (v Variant) NumArrayElements() int {
	a, err := v.arrayInfo()
	if err != nil {
		return 0
	}
	return a.numFields
}

// GetFieldByKey returns the object field with the given key, or nil if absent (or
// if this is not an object).
func (v Variant) GetFieldByKey(key string) *Variant {
	o, err := v.objectInfo()
	if err != nil {
		return nil
	}
	if o.numFields < binarySearchThld {
		for i := 0; i < o.numFields; i++ {
			id, err := readUnsignedLE(v.value, o.idStart+o.idSize*i, o.idSize)
			if err != nil {
				return nil
			}
			k, err := v.getMetadataKey(id)
			if err != nil {
				return nil
			}
			if k == key {
				offset, err := readUnsignedLE(v.value, o.offsetStart+o.offsetSize*i, o.offsetSize)
				if err != nil {
					return nil
				}
				sub := Variant{value: v.value, metadata: v.metadata, pos: o.dataStart + offset}
				return &sub
			}
		}
		return nil
	}
	low, high := 0, o.numFields-1
	for low <= high {
		mid := (low + high) >> 1
		midID, err := readUnsignedLE(v.value, o.idStart+o.idSize*mid, o.idSize)
		if err != nil {
			return nil
		}
		k, err := v.getMetadataKey(midID)
		if err != nil {
			return nil
		}
		cmp := strings.Compare(k, key)
		if cmp < 0 {
			low = mid + 1
		} else if cmp > 0 {
			high = mid - 1
		} else {
			offset, err := readUnsignedLE(v.value, o.offsetStart+o.offsetSize*mid, o.offsetSize)
			if err != nil {
				return nil
			}
			sub := Variant{value: v.value, metadata: v.metadata, pos: o.dataStart + offset}
			return &sub
		}
	}
	return nil
}

// GetFieldAtIndex returns the (key, value) of the field at idx (key-sorted). On
// error it returns ("", a zero Variant).
func (v Variant) GetFieldAtIndex(idx int) (string, Variant) {
	k, sub, err := v.fieldAtIndex(idx)
	if err != nil {
		return "", Variant{}
	}
	return k, sub
}

func (v Variant) fieldAtIndex(idx int) (string, Variant, error) {
	o, err := v.objectInfo()
	if err != nil {
		return "", Variant{}, err
	}
	id, err := readUnsignedLE(v.value, o.idStart+o.idSize*idx, o.idSize)
	if err != nil {
		return "", Variant{}, err
	}
	offset, err := readUnsignedLE(v.value, o.offsetStart+o.offsetSize*idx, o.offsetSize)
	if err != nil {
		return "", Variant{}, err
	}
	key, err := v.getMetadataKey(id)
	if err != nil {
		return "", Variant{}, err
	}
	return key, Variant{value: v.value, metadata: v.metadata, pos: o.dataStart + offset}, nil
}

// GetElementAtIndex returns the array element at index, or nil if out of bounds
// (or if this is not an array).
func (v Variant) GetElementAtIndex(index int) *Variant {
	sub, err := v.elementAtIndex(index)
	if err != nil {
		return nil
	}
	return sub
}

func (v Variant) elementAtIndex(index int) (*Variant, error) {
	a, err := v.arrayInfo()
	if err != nil {
		return nil, err
	}
	if index < 0 || index >= a.numFields {
		return nil, nil
	}
	offset, err := readUnsignedLE(v.value, a.offsetStart+a.offsetSize*index, a.offsetSize)
	if err != nil {
		return nil, err
	}
	sub := Variant{value: v.value, metadata: v.metadata, pos: a.dataStart + offset}
	return &sub, nil
}

// --- metadata dictionary ---

func (v Variant) getMetadataKey(id int) (string, error) {
	if err := checkIndex(0, len(v.metadata)); err != nil {
		return "", err
	}
	if int(v.metadata[0])&versionMask != version {
		return "", fmt.Errorf("variant: unsupported metadata version: %d", int(v.metadata[0])&versionMask)
	}
	offsetSize := ((int(v.metadata[0]) >> 6) & 0x3) + 1
	dictSize, err := readUnsignedLE(v.metadata, 1, offsetSize)
	if err != nil {
		return "", err
	}
	if id >= dictSize {
		return "", fmt.Errorf("variant: field id out of range")
	}
	stringStart := 1 + (dictSize+2)*offsetSize
	offset, err := readUnsignedLE(v.metadata, 1+(id+1)*offsetSize, offsetSize)
	if err != nil {
		return "", err
	}
	nextOffset, err := readUnsignedLE(v.metadata, 1+(id+2)*offsetSize, offsetSize)
	if err != nil {
		return "", err
	}
	if offset > nextOffset {
		return "", fmt.Errorf("variant: non-monotonic metadata offsets")
	}
	if nextOffset == offset {
		return "", nil
	}
	if err = checkIndex(stringStart+nextOffset-1, len(v.metadata)); err != nil {
		return "", err
	}
	return string(v.metadata[stringStart+offset : stringStart+nextOffset]), nil
}

// --- JSON serialization ---

// ToJSON serializes to a JSON string, matching the cross-language contract.
func (v Variant) ToJSON() (string, error) {
	var sb strings.Builder
	if err := v.writeJSON(&sb); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (v Variant) writeJSON(sb *strings.Builder) error {
	t, err := v.variantType()
	if err != nil {
		return err
	}
	switch t {
	case Object:
		sb.WriteByte('{')
		o, err := v.objectInfo()
		if err != nil {
			return err
		}
		for i := 0; i < o.numFields; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			key, field, err := v.fieldAtIndex(i)
			if err != nil {
				return err
			}
			sb.WriteString(jsonQuote(key))
			sb.WriteByte(':')
			if err := field.writeJSON(sb); err != nil {
				return err
			}
		}
		sb.WriteByte('}')
	case Array:
		sb.WriteByte('[')
		a, err := v.arrayInfo()
		if err != nil {
			return err
		}
		for i := 0; i < a.numFields; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			el, err := v.elementAtIndex(i)
			if err != nil {
				return err
			}
			if err := el.writeJSON(sb); err != nil {
				return err
			}
		}
		sb.WriteByte(']')
	case Null:
		sb.WriteString("null")
	case Boolean:
		b, err := v.GetBoolean()
		if err != nil {
			return err
		}
		if b {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case String:
		s, err := v.GetString()
		if err != nil {
			return err
		}
		sb.WriteString(jsonQuote(s))
	case Byte, Short, Int, Long:
		n, err := v.GetLong()
		if err != nil {
			return err
		}
		sb.WriteString(strconv.FormatInt(n, 10))
	case Float:
		f, err := v.GetFloat()
		if err != nil {
			return err
		}
		s, err := formatFloat(f)
		if err != nil {
			return err
		}
		sb.WriteString(s)
	case Double:
		d, err := v.GetDouble()
		if err != nil {
			return err
		}
		s, err := formatDouble(d)
		if err != nil {
			return err
		}
		sb.WriteString(s)
	case Decimal4, Decimal8, Decimal16:
		s, err := v.GetDecimalString()
		if err != nil {
			return err
		}
		sb.WriteString(s)
	case Date:
		n, err := v.GetLong()
		if err != nil {
			return err
		}
		sb.WriteByte('"')
		sb.WriteString(formatDate(n))
		sb.WriteByte('"')
	case TimestampTz:
		n, err := v.GetLong()
		if err != nil {
			return err
		}
		sb.WriteByte('"')
		sb.WriteString(formatInstant(n * 1000))
		sb.WriteByte('"')
	case TimestampNtz:
		n, err := v.GetLong()
		if err != nil {
			return err
		}
		sb.WriteByte('"')
		sb.WriteString(formatLocalDateTime(n * 1000))
		sb.WriteByte('"')
	case TimestampNanosTz:
		n, err := v.GetLong()
		if err != nil {
			return err
		}
		sb.WriteByte('"')
		sb.WriteString(formatInstant(n))
		sb.WriteByte('"')
	case TimestampNanosNtz:
		n, err := v.GetLong()
		if err != nil {
			return err
		}
		sb.WriteByte('"')
		sb.WriteString(formatLocalDateTime(n))
		sb.WriteByte('"')
	case Time:
		n, err := v.GetLong()
		if err != nil {
			return err
		}
		sb.WriteByte('"')
		sb.WriteString(formatLocalTime(n))
		sb.WriteByte('"')
	case Binary:
		b, err := v.GetBinary()
		if err != nil {
			return err
		}
		sb.WriteByte('"')
		sb.WriteString(base64.StdEncoding.EncodeToString(b))
		sb.WriteByte('"')
	case Uuid:
		s, err := v.GetUuid()
		if err != nil {
			return err
		}
		sb.WriteByte('"')
		sb.WriteString(s)
		sb.WriteByte('"')
	default:
		return fmt.Errorf("variant: unsupported type for JSON: %d", t)
	}
	return nil
}

// --- low-level byte helpers ---

func checkIndex(pos, length int) error {
	if pos < 0 || pos >= length {
		return fmt.Errorf("variant: index out of bounds")
	}
	return nil
}

func readUnsignedLE(data []byte, pos, numBytes int) (int, error) {
	if err := checkIndex(pos, len(data)); err != nil {
		return 0, err
	}
	if err := checkIndex(pos+numBytes-1, len(data)); err != nil {
		return 0, err
	}
	result := 0
	for i := numBytes - 1; i >= 0; i-- {
		result = (result << 8) | int(data[pos+i])
	}
	return result, nil
}

func readSignedLong(data []byte, pos, numBytes int) (int64, error) {
	if err := checkIndex(pos, len(data)); err != nil {
		return 0, err
	}
	if err := checkIndex(pos+numBytes-1, len(data)); err != nil {
		return 0, err
	}
	var result uint64
	for i := numBytes - 1; i >= 0; i-- {
		result = (result << 8) | uint64(data[pos+i])
	}
	if numBytes < 8 {
		signBit := uint64(1) << (numBytes*8 - 1)
		if result&signBit != 0 {
			result |= ^uint64(0) << (numBytes * 8)
		}
	}
	return int64(result), nil
}

func readFloatLE(data []byte, pos int) (float32, error) {
	if err := checkIndex(pos+3, len(data)); err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint32(data[pos : pos+4])
	return math.Float32frombits(bits), nil
}

func readDoubleLE(data []byte, pos int) (float64, error) {
	if err := checkIndex(pos+7, len(data)); err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(data[pos : pos+8])
	return math.Float64frombits(bits), nil
}

// --- calendar / temporal formatting (cross-language contract) ---

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func floorMod(a, b int64) int64 {
	r := a % b
	if r != 0 && (r < 0) != (b < 0) {
		r += b
	}
	return r
}

func frac(nano int64) string {
	if nano == 0 {
		return ""
	}
	if nano%1000000 == 0 {
		return fmt.Sprintf(".%03d", nano/1000000)
	}
	if nano%1000 == 0 {
		return fmt.Sprintf(".%06d", nano/1000)
	}
	return fmt.Sprintf(".%09d", nano)
}

func formatInstant(totalNanos int64) string {
	sec := floorDiv(totalNanos, 1000000000)
	nano := floorMod(totalNanos, 1000000000)
	dt := time.Unix(sec, 0).UTC()
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d%sZ",
		dt.Year(), int(dt.Month()), dt.Day(), dt.Hour(), dt.Minute(), dt.Second(), frac(nano))
}

func formatLocalDateTime(totalNanos int64) string {
	sec := floorDiv(totalNanos, 1000000000)
	nano := floorMod(totalNanos, 1000000000)
	dt := time.Unix(sec, 0).UTC()
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d%s",
		dt.Year(), int(dt.Month()), dt.Day(), dt.Hour(), dt.Minute(), dt.Second(), frac(nano))
}

func formatLocalTime(micros int64) string {
	nanoOfDay := micros * 1000
	secs := floorDiv(nanoOfDay, 1000000000)
	nano := floorMod(nanoOfDay, 1000000000)
	hour := secs / 3600
	rem := secs % 3600
	return fmt.Sprintf("%02d:%02d:%02d%s", hour, rem/60, rem%60, frac(nano))
}

func formatDate(days int64) string {
	dt := time.Unix(days*86400, 0).UTC()
	return fmt.Sprintf("%04d-%02d-%02d", dt.Year(), int(dt.Month()), dt.Day())
}

// nonFiniteJSON renders NaN/Infinity/-Infinity as bareword JSON tokens (matching
// Java's Double.toString / Float.toString, which the cross-language contract
// adopts deliberately - Spark quotes these, this contract does not). It returns
// ("", false) for finite values.
func nonFiniteJSON(d float64) (string, bool) {
	switch {
	case math.IsNaN(d):
		return "NaN", true
	case math.IsInf(d, 1):
		return "Infinity", true
	case math.IsInf(d, -1):
		return "-Infinity", true
	default:
		return "", false
	}
}

// formatDouble renders integral doubles as N.0 and everything else with the
// shortest round-tripping decimal representation. Non-finite values render as the
// barewords NaN/Infinity/-Infinity (the cross-language contract). (Scientific-
// notation edge cases for very large/small magnitudes are a known minor
// divergence from Java's Double.toString.)
func formatDouble(d float64) (string, error) {
	if s, ok := nonFiniteJSON(d); ok {
		return s, nil
	}
	if d == math.Floor(d) && math.Abs(d) < 1e16 {
		return strconv.FormatInt(int64(d), 10) + ".0", nil
	}
	return strconv.FormatFloat(d, 'g', -1, 64), nil
}

// formatFloat mirrors formatDouble but formats at float32 precision so that
// 32-bit floats render with their shortest round-tripping decimal (matching
// Java's Float.toString and Apache Arrow) rather than the f64-widened form.
// Non-finite values render as the barewords NaN/Infinity/-Infinity.
func formatFloat(f float32) (string, error) {
	d := float64(f)
	if s, ok := nonFiniteJSON(d); ok {
		return s, nil
	}
	if d == math.Floor(d) && math.Abs(d) < 1e16 {
		return strconv.FormatInt(int64(d), 10) + ".0", nil
	}
	return strconv.FormatFloat(d, 'g', -1, 32), nil
}

func formatUUID(data []byte, start int) string {
	const hex = "0123456789abcdef"
	var sb strings.Builder
	sb.Grow(36)
	for i := 0; i < 16; i++ {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			sb.WriteByte('-')
		}
		b := data[start+i]
		sb.WriteByte(hex[b>>4])
		sb.WriteByte(hex[b&0xF])
	}
	return sb.String()
}

// bigIntFromTwosComplementBE interprets big-endian two's-complement bytes as a
// signed big.Int.
func bigIntFromTwosComplementBE(be []byte) *big.Int {
	n := new(big.Int).SetBytes(be)
	if len(be) > 0 && be[0]&0x80 != 0 {
		// Negative: subtract 2^(8*len).
		shift := uint(8 * len(be))
		mod := new(big.Int).Lsh(big.NewInt(1), shift)
		n.Sub(n, mod)
	}
	return n
}

// decimalPlainString renders unscaled*10^-scale as an exact fixed-point string
// (never scientific), matching .NET BigDecimal.ToPlainString.
func decimalPlainString(unscaled *big.Int, scale int) string {
	negative := unscaled.Sign() < 0
	digits := new(big.Int).Abs(unscaled).String()
	sign := ""
	if negative {
		sign = "-"
	}
	if scale == 0 {
		return sign + digits
	}
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	point := len(digits) - scale
	return sign + digits[:point] + "." + digits[point:]
}

// jsonQuote returns s as a JSON string literal, without HTML-escaping (matching
// Newtonsoft JsonConvert.ToString / nlohmann dump).
func jsonQuote(s string) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	// Encode cannot fail for a string; ignore the error.
	_ = enc.Encode(s)
	out := buf.Bytes()
	// Encoder appends a trailing newline.
	if len(out) > 0 && out[len(out)-1] == '\n' {
		out = out[:len(out)-1]
	}
	return string(out)
}
