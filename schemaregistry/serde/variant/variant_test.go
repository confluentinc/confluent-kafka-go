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
	"math"
	"math/big"
	"strings"
	"testing"
)

// emptyMetadata is a valid metadata buffer for a standalone scalar value
// (version 1, zero-key dictionary).
var emptyMetadata = []byte{version, 0x00, 0x00}

func mustParse(t *testing.T, jsonStr string) Variant {
	t.Helper()
	v, err := ParseJSON(jsonStr)
	if err != nil {
		t.Fatalf("ParseJSON(%q) failed: %v", jsonStr, err)
	}
	return v
}

func mustJSON(t *testing.T, v Variant) string {
	t.Helper()
	s, err := v.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}
	return s
}

func TestVariantParseJSONRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"object simple", `{"x":1}`, `{"x":1}`},
		{"object key sort", `{"b":2,"a":1}`, `{"a":1,"b":2}`},
		{"nested", `{"a":1,"b":[1,2,3],"c":"hello"}`, `{"a":1,"b":[1,2,3],"c":"hello"}`},
		{"array mixed", `[1,"two",true,null,3.5]`, `[1,"two",true,null,3.5]`},
		{"null", `null`, `null`},
		{"boolean true", `true`, `true`},
		{"boolean false", `false`, `false`},
		{"string", `"hello world"`, `"hello world"`},
		{"string escape", `"a\"b\\c"`, `"a\"b\\c"`},
		{"int", `42`, `42`},
		{"negative int", `-7`, `-7`},
		{"short", `300`, `300`},
		{"int4", `100000`, `100000`},
		{"long", `10000000000`, `10000000000`},
		{"double", `1.5`, `1.5`},
		{"double 2.0", `2.0`, `2.0`},
		{"double pi", `3.14`, `3.14`},
		{"bigint decimal", `1000000000000000000000`, `1000000000000000000000`},
		{"empty object", `{}`, `{}`},
		{"empty array", `[]`, `[]`},
		{"long string", `"` + longStr(100) + `"`, `"` + longStr(100) + `"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := mustParse(t, c.in)
			if got := mustJSON(t, v); got != c.want {
				t.Errorf("round trip: got %q, want %q", got, c.want)
			}
		})
	}
}

func longStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

func TestVariantGetType(t *testing.T) {
	cases := []struct {
		in   string
		want Type
	}{
		{`{"a":1}`, Object},
		{`[1,2]`, Array},
		{`null`, Null},
		{`true`, Boolean},
		{`"s"`, String},
		{`1`, Byte},
		{`300`, Short},
		{`100000`, Int},
		{`10000000000`, Long},
		{`1.5`, Double},
		{`1000000000000000000000`, Decimal16},
	}
	for _, c := range cases {
		v := mustParse(t, c.in)
		if got := v.GetType(); got != c.want {
			t.Errorf("GetType(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestVariantScalarGetters(t *testing.T) {
	if b, err := mustParse(t, `true`).GetBoolean(); err != nil || b != true {
		t.Errorf("GetBoolean: %v %v", b, err)
	}
	if b, err := mustParse(t, `false`).GetBoolean(); err != nil || b != false {
		t.Errorf("GetBoolean: %v %v", b, err)
	}
	if n, err := mustParse(t, `42`).GetLong(); err != nil || n != 42 {
		t.Errorf("GetLong: %v %v", n, err)
	}
	if n, err := mustParse(t, `-123456789`).GetLong(); err != nil || n != -123456789 {
		t.Errorf("GetLong: %v %v", n, err)
	}
	if d, err := mustParse(t, `3.5`).GetDouble(); err != nil || d != 3.5 {
		t.Errorf("GetDouble: %v %v", d, err)
	}
	if s, err := mustParse(t, `"hi"`).GetString(); err != nil || s != "hi" {
		t.Errorf("GetString: %v %v", s, err)
	}
	// Wrong-type access should error, not panic.
	if _, err := mustParse(t, `42`).GetBoolean(); err == nil {
		t.Error("GetBoolean on int should error")
	}
	if _, err := mustParse(t, `"s"`).GetLong(); err == nil {
		t.Error("GetLong on string should error")
	}
}

func TestVariantObjectNavigation(t *testing.T) {
	v := mustParse(t, `{"a":1,"b":2,"c":"three"}`)
	if got := v.NumObjectFields(); got != 3 {
		t.Fatalf("NumObjectFields = %d, want 3", got)
	}
	fa := v.GetFieldByKey("a")
	if fa == nil {
		t.Fatal("GetFieldByKey(a) = nil")
	}
	if n, err := fa.GetLong(); err != nil || n != 1 {
		t.Errorf("field a = %v %v", n, err)
	}
	fc := v.GetFieldByKey("c")
	if fc == nil {
		t.Fatal("GetFieldByKey(c) = nil")
	}
	if s, err := fc.GetString(); err != nil || s != "three" {
		t.Errorf("field c = %v %v", s, err)
	}
	// Miss returns nil.
	if v.GetFieldByKey("missing") != nil {
		t.Error("GetFieldByKey(missing) should be nil")
	}
	// GetFieldAtIndex is key-sorted: index 0 -> "a".
	k0, _ := v.GetFieldAtIndex(0)
	if k0 != "a" {
		t.Errorf("GetFieldAtIndex(0) key = %q, want a", k0)
	}
	// GetFieldByKey on a non-object returns nil.
	if mustParse(t, `[1,2]`).GetFieldByKey("a") != nil {
		t.Error("GetFieldByKey on array should be nil")
	}
}

func TestVariantObjectManyFieldsBinarySearch(t *testing.T) {
	// Exceed the binary-search threshold to exercise that path.
	var sb []byte
	sb = append(sb, '{')
	for i := 0; i < 40; i++ {
		if i > 0 {
			sb = append(sb, ',')
		}
		sb = append(sb, []byte(`"k`)...)
		sb = append(sb, []byte(itoa(i))...)
		sb = append(sb, []byte(`":`)...)
		sb = append(sb, []byte(itoa(i))...)
	}
	sb = append(sb, '}')
	v := mustParse(t, string(sb))
	if v.NumObjectFields() != 40 {
		t.Fatalf("NumObjectFields = %d", v.NumObjectFields())
	}
	f := v.GetFieldByKey("k37")
	if f == nil {
		t.Fatal("GetFieldByKey(k37) = nil")
	}
	if n, err := f.GetLong(); err != nil || n != 37 {
		t.Errorf("k37 = %v %v", n, err)
	}
	if v.GetFieldByKey("nope") != nil {
		t.Error("GetFieldByKey(nope) should be nil")
	}
}

func TestVariantDuplicateKeysLastWins(t *testing.T) {
	// The streaming JSON decoder does not collapse duplicate keys, so the builder must
	// deduplicate them last-wins. The duplicate "a" has a different value size than its first
	// occurrence, exercising the value-repacking path; "b" and "c" (written before and after
	// the duplicate) must survive with correct values after compaction.
	v := mustParse(t, `{"b": 1, "a": "x", "a": "second-longer-value", "c": 3}`)
	if got := v.NumObjectFields(); got != 3 {
		t.Fatalf("NumObjectFields = %d, want 3", got)
	}
	fa := v.GetFieldByKey("a")
	if fa == nil {
		t.Fatal("GetFieldByKey(a) = nil")
	}
	if s, err := fa.GetString(); err != nil || s != "second-longer-value" {
		t.Errorf("a = %q %v, want last-wins value", s, err)
	}
	if fb := v.GetFieldByKey("b"); fb == nil {
		t.Fatal("GetFieldByKey(b) = nil")
	} else if n, err := fb.GetLong(); err != nil || n != 1 {
		t.Errorf("b = %v %v, want 1", n, err)
	}
	if fc := v.GetFieldByKey("c"); fc == nil {
		t.Fatal("GetFieldByKey(c) = nil")
	} else if n, err := fc.GetLong(); err != nil || n != 3 {
		t.Errorf("c = %v %v, want 3", n, err)
	}
}

func TestVariantBuilderDuplicateKeysLastWins(t *testing.T) {
	// The programmatic builder path (repeated AppendKey) must also deduplicate last-wins.
	vb := NewVariantBuilder()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(vb.StartObject())
	must(vb.AppendKey("a"))
	must(vb.AppendLong(1))
	must(vb.AppendKey("a"))
	must(vb.AppendLong(2))
	must(vb.EndObject())
	v, err := vb.Build()
	if err != nil {
		t.Fatal(err)
	}
	if got := v.NumObjectFields(); got != 1 {
		t.Fatalf("NumObjectFields = %d, want 1", got)
	}
	fa := v.GetFieldByKey("a")
	if fa == nil {
		t.Fatal("GetFieldByKey(a) = nil")
	}
	if n, err := fa.GetLong(); err != nil || n != 2 {
		t.Errorf("a = %v %v, want 2 (last-wins)", n, err)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestVariantArrayNavigation(t *testing.T) {
	v := mustParse(t, `[10,20,30]`)
	if got := v.NumArrayElements(); got != 3 {
		t.Fatalf("NumArrayElements = %d, want 3", got)
	}
	el := v.GetElementAtIndex(1)
	if el == nil {
		t.Fatal("GetElementAtIndex(1) = nil")
	}
	if n, err := el.GetLong(); err != nil || n != 20 {
		t.Errorf("element 1 = %v %v", n, err)
	}
	// Out-of-bounds returns nil.
	if v.GetElementAtIndex(5) != nil {
		t.Error("GetElementAtIndex(5) should be nil")
	}
	if v.GetElementAtIndex(-1) != nil {
		t.Error("GetElementAtIndex(-1) should be nil")
	}
	// GetElementAtIndex on a non-array returns nil.
	if mustParse(t, `{"a":1}`).GetElementAtIndex(0) != nil {
		t.Error("GetElementAtIndex on object should be nil")
	}
}

func TestVariantXToJSON(t *testing.T) {
	v := mustParse(t, `{"x":1}`)
	if got := mustJSON(t, v); got != `{"x":1}` {
		t.Errorf(`ToJSON = %q, want {"x":1}`, got)
	}
}

func TestVariantDecimalString(t *testing.T) {
	// Scale-0 big integer via ParseJSON.
	v := mustParse(t, `123456789012345678901234567890`)
	if v.GetType() != Decimal16 {
		t.Fatalf("type = %d, want Decimal16", v.GetType())
	}
	s, err := v.GetDecimalString()
	if err != nil {
		t.Fatalf("GetDecimalString: %v", err)
	}
	if s != "123456789012345678901234567890" {
		t.Errorf("GetDecimalString = %q", s)
	}

	// Manually-built scaled decimals exercise the scale/point insertion and sign.
	dcases := []struct {
		unscaled string
		scale    int
		width    int
		code     int
		want     string
	}{
		{"12345", 2, 4, tDecimal4, "123.45"},
		{"-12345", 2, 4, tDecimal4, "-123.45"},
		{"5", 3, 4, tDecimal4, "0.005"},
		{"-5", 3, 4, tDecimal4, "-0.005"},
		{"1", 0, 4, tDecimal4, "1"},
		{"123456789012345678", 6, 8, tDecimal8, "123456789012.345678"},
	}
	for _, c := range dcases {
		unscaled, _ := new(big.Int).SetString(c.unscaled, 10)
		val := []byte{primitiveHeader(c.code), byte(c.scale)}
		appendBigIntLE(&val, unscaled, c.width)
		dv := New(val, emptyMetadata)
		got, err := dv.GetDecimalString()
		if err != nil {
			t.Fatalf("GetDecimalString(%s): %v", c.unscaled, err)
		}
		if got != c.want {
			t.Errorf("decimal %s scale %d = %q, want %q", c.unscaled, c.scale, got, c.want)
		}
		// GetDecimalParts should round-trip the scale and big-endian bytes.
		be, sc, err := dv.GetDecimalParts()
		if err != nil || sc != c.scale {
			t.Errorf("GetDecimalParts scale = %d (%v), want %d", sc, err, c.scale)
		}
		if got := bigIntFromTwosComplementBE(be); got.Cmp(unscaled) != 0 {
			t.Errorf("GetDecimalParts unscaled = %s, want %s", got, unscaled)
		}
	}
}

func TestVariantTemporalAndBinaryToJSON(t *testing.T) {
	cases := []struct {
		name string
		val  []byte
		want string
	}{
		{"date epoch", dateBytes(0), `"1970-01-01"`},
		{"date 2021-01-01", dateBytes(18628), `"2021-01-01"`},
		{"date before epoch", dateBytes(-1), `"1969-12-31"`},
		{"timestamp tz epoch", tsBytes(tTimestamp, 0), `"1970-01-01T00:00:00Z"`},
		{"timestamp ntz epoch", tsBytes(tTimestampNtz, 0), `"1970-01-01T00:00:00"`},
		{"timestamp tz micros", tsBytes(tTimestamp, 1_000_000), `"1970-01-01T00:00:01Z"`},
		{"time", tsBytes(tTime, 3661_000_000), `"01:01:01"`},
		{"binary", binBytes([]byte{1, 2, 3}), `"AQID"`},
		{"uuid", uuidBytes(), `"00112233-4455-6677-8899-aabbccddeeff"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := New(c.val, emptyMetadata)
			got := mustJSON(t, v)
			if got != c.want {
				t.Errorf("ToJSON = %q, want %q", got, c.want)
			}
		})
	}
}

func dateBytes(days int64) []byte {
	val := []byte{primitiveHeader(tDate)}
	appendLongLE(&val, days, 4)
	return val
}

func tsBytes(code int, micros int64) []byte {
	val := []byte{primitiveHeader(code)}
	appendLongLE(&val, micros, 8)
	return val
}

func binBytes(data []byte) []byte {
	val := []byte{primitiveHeader(tBinary)}
	appendUintLE(&val, len(data), u32Size)
	val = append(val, data...)
	return val
}

func uuidBytes() []byte {
	val := []byte{primitiveHeader(tUUID)}
	val = append(val, []byte{
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
		0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
	}...)
	return val
}

func TestVariantStandaloneValueBytesRoundTrip(t *testing.T) {
	v := mustParse(t, `{"a":1,"b":[1,2,3],"c":"hello"}`)

	// Navigate to the "b" array field and re-encode it standalone.
	b := v.GetFieldByKey("b")
	if b == nil {
		t.Fatal("GetFieldByKey(b) = nil")
	}
	reencoded := New(b.StandaloneValueBytes(), b.MetadataBytes())
	if got := mustJSON(t, reencoded); got != `[1,2,3]` {
		t.Errorf("re-encoded array = %q, want [1,2,3]", got)
	}

	// Navigate to a nested scalar and re-encode.
	c := v.GetFieldByKey("c")
	if c == nil {
		t.Fatal("GetFieldByKey(c) = nil")
	}
	reC := New(c.StandaloneValueBytes(), c.MetadataBytes())
	if got := mustJSON(t, reC); got != `"hello"` {
		t.Errorf("re-encoded string = %q, want \"hello\"", got)
	}

	// For the root, StandaloneValueBytes == ValueBytes.
	if string(v.StandaloneValueBytes()) != string(v.ValueBytes()) {
		t.Error("root StandaloneValueBytes != ValueBytes")
	}
}

func TestVariantMalformedJSON(t *testing.T) {
	bad := []string{
		`{bad`,
		`{"a":}`,
		`[1,2,`,
		``,
		`{"a":1} extra`,
		`not json`,
	}
	for _, s := range bad {
		if _, err := ParseJSON(s); err == nil {
			t.Errorf("ParseJSON(%q) should have errored", s)
		}
	}
}

func TestVariantDecimalOverflowError(t *testing.T) {
	// 40 digits exceeds the maximum precision (38).
	big40 := "1234567890123456789012345678901234567890"
	if _, err := ParseJSON(big40); err == nil {
		t.Error("ParseJSON of 40-digit integer should error (exceeds precision)")
	}
}

func floatBytes(f float32) []byte {
	val := []byte{primitiveHeader(tFloat)}
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], math.Float32bits(f))
	return append(val, b[:]...)
}

func doubleBytes(f float64) []byte {
	val := []byte{primitiveHeader(tDouble)}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(f))
	return append(val, b[:]...)
}

func TestVariantIntGetters(t *testing.T) {
	// GetByte: INT8 only.
	if n, err := mustParse(t, `1`).GetByte(); err != nil || n != 1 {
		t.Errorf("GetByte(1) = %v %v", n, err)
	}
	if _, err := mustParse(t, `300`).GetByte(); err == nil {
		t.Error("GetByte on short should error")
	}
	// GetShort: <=INT16, widens.
	if n, err := mustParse(t, `1`).GetShort(); err != nil || n != 1 {
		t.Errorf("GetShort(1) = %v %v", n, err)
	}
	if n, err := mustParse(t, `300`).GetShort(); err != nil || n != 300 {
		t.Errorf("GetShort(300) = %v %v", n, err)
	}
	if _, err := mustParse(t, `100000`).GetShort(); err == nil {
		t.Error("GetShort on int should error")
	}
	// GetInt: <=INT32, widens.
	if n, err := mustParse(t, `1`).GetInt(); err != nil || n != 1 {
		t.Errorf("GetInt(1) = %v %v", n, err)
	}
	if n, err := mustParse(t, `300`).GetInt(); err != nil || n != 300 {
		t.Errorf("GetInt(300) = %v %v", n, err)
	}
	if n, err := mustParse(t, `100000`).GetInt(); err != nil || n != 100000 {
		t.Errorf("GetInt(100000) = %v %v", n, err)
	}
	if _, err := mustParse(t, `10000000000`).GetInt(); err == nil {
		t.Error("GetInt on long should error")
	}
	// GetLong: widens any int width.
	if n, err := mustParse(t, `10000000000`).GetLong(); err != nil || n != 10000000000 {
		t.Errorf("GetLong = %v %v", n, err)
	}
}

func TestVariantFloatAndDoubleExact(t *testing.T) {
	// GetFloat accepts FLOAT exactly.
	if f, err := New(floatBytes(1.5), emptyMetadata).GetFloat(); err != nil || f != 1.5 {
		t.Errorf("GetFloat(FLOAT) = %v %v", f, err)
	}
	// GetFloat rejects DOUBLE.
	if _, err := New(doubleBytes(1.5), emptyMetadata).GetFloat(); err == nil {
		t.Error("GetFloat on double should error")
	}
	// GetDouble accepts DOUBLE exactly.
	if d, err := New(doubleBytes(3.5), emptyMetadata).GetDouble(); err != nil || d != 3.5 {
		t.Errorf("GetDouble(DOUBLE) = %v %v", d, err)
	}
	// GetDouble rejects FLOAT (no longer widens).
	if _, err := New(floatBytes(1.5), emptyMetadata).GetDouble(); err == nil {
		t.Error("GetDouble on float should error (exact double only)")
	}
}

func TestVariantUuidGetter(t *testing.T) {
	v := New(uuidBytes(), emptyMetadata)
	s, err := v.GetUuid()
	if err != nil || s != "00112233-4455-6677-8899-aabbccddeeff" {
		t.Errorf("GetUuid = %q %v", s, err)
	}
}

func TestVariantBuilderNestedMatchesParseJSON(t *testing.T) {
	// Build a nested document with the flat streaming API. The int widths are
	// chosen so each value's encoding matches ParseJSON of the equivalent JSON.
	b := NewVariantBuilder()
	mustNoErr := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("builder step failed: %v", err)
		}
	}
	mustNoErr(b.StartObject())
	mustNoErr(b.AppendKey("id"))
	mustNoErr(b.AppendLong(10000000000)) // INT64 == "10000000000"
	mustNoErr(b.AppendKey("count"))
	mustNoErr(b.AppendInt(100000)) // INT32 == "100000"
	mustNoErr(b.AppendKey("tags"))
	mustNoErr(b.StartArray())
	mustNoErr(b.AppendString("x"))
	mustNoErr(b.AppendString("y"))
	mustNoErr(b.EndArray())
	mustNoErr(b.AppendKey("nested"))
	mustNoErr(b.StartObject())
	mustNoErr(b.AppendKey("flag"))
	mustNoErr(b.AppendBoolean(true))
	mustNoErr(b.AppendKey("pi"))
	mustNoErr(b.AppendDouble(3.14))
	mustNoErr(b.EndObject())
	mustNoErr(b.EndObject())
	built, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Key order in the document is chosen so both the builder (append order) and
	// ParseJSON (document order) assign the same metadata dictionary IDs.
	const equivalent = `{"id":10000000000,"count":100000,"tags":["x","y"],"nested":{"flag":true,"pi":3.14}}`
	parsed := mustParse(t, equivalent)

	if got, want := mustJSON(t, built), mustJSON(t, parsed); got != want {
		t.Errorf("ToJSON: built = %q, parsed = %q", got, want)
	}
	if !bytesEqual(built.ValueBytes(), parsed.ValueBytes()) {
		t.Errorf("value bytes differ:\n built  = %v\n parsed = %v", built.ValueBytes(), parsed.ValueBytes())
	}
	if !bytesEqual(built.MetadataBytes(), parsed.MetadataBytes()) {
		t.Errorf("metadata bytes differ:\n built  = %v\n parsed = %v", built.MetadataBytes(), parsed.MetadataBytes())
	}
}

func TestVariantBuilderRootScalar(t *testing.T) {
	b := NewVariantBuilder()
	if err := b.AppendByte(42); err != nil {
		t.Fatalf("AppendByte: %v", err)
	}
	built, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	parsed := mustParse(t, "42")
	if !bytesEqual(built.ValueBytes(), parsed.ValueBytes()) {
		t.Errorf("root scalar value bytes differ: %v vs %v", built.ValueBytes(), parsed.ValueBytes())
	}
	if n, err := built.GetByte(); err != nil || n != 42 {
		t.Errorf("GetByte = %v %v", n, err)
	}
}

func TestVariantBuilderErrors(t *testing.T) {
	// AppendKey outside an object.
	if err := NewVariantBuilder().AppendKey("k"); err == nil {
		t.Error("AppendKey outside an object should error")
	}
	// Value appended to an object without a preceding AppendKey.
	b := NewVariantBuilder()
	if err := b.StartObject(); err != nil {
		t.Fatalf("StartObject: %v", err)
	}
	if err := b.AppendLong(1); err == nil {
		t.Error("value in object without AppendKey should error")
	}
	// Build with an open container.
	b2 := NewVariantBuilder()
	if err := b2.StartArray(); err != nil {
		t.Fatalf("StartArray: %v", err)
	}
	if _, err := b2.Build(); err == nil {
		t.Error("Build with an open container should error")
	}
	// Build with nothing appended.
	if _, err := NewVariantBuilder().Build(); err == nil {
		t.Error("Build with no value should error")
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestLargeDataRegionUses4ByteOffsets is a regression test for Bug #1: the
// integerSize helper capped at 3 bytes and never returned 4, so a container
// whose data/offset region exceeds 0xFFFFFF (16777215) bytes produced a corrupt
// Variant. This builds an array holding a single string of 16777216 bytes so the
// data region exceeds 16 MiB, forcing the 4-byte offset-size path, then verifies
// it round-trips.
func TestLargeDataRegionUses4ByteOffsets(t *testing.T) {
	const size = 16777216 // 0x1000000, one byte past the 3-byte offset limit
	big := strings.Repeat("a", size)

	b := NewVariantBuilder()
	if err := b.StartArray(); err != nil {
		t.Fatalf("StartArray: %v", err)
	}
	if err := b.AppendString(big); err != nil {
		t.Fatalf("AppendString: %v", err)
	}
	if err := b.EndArray(); err != nil {
		t.Fatalf("EndArray: %v", err)
	}
	built, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if built.GetType() != Array {
		t.Fatalf("GetType = %d, want Array", built.GetType())
	}
	if n := built.NumArrayElements(); n != 1 {
		t.Fatalf("NumArrayElements = %d, want 1", n)
	}
	el := built.GetElementAtIndex(0)
	if el == nil {
		t.Fatal("GetElementAtIndex(0) = nil")
	}
	if el.GetType() != String {
		t.Fatalf("element type = %d, want String", el.GetType())
	}
	s, err := el.GetString()
	if err != nil {
		t.Fatalf("GetString: %v", err)
	}
	if len(s) != size {
		t.Fatalf("element string length = %d, want %d", len(s), size)
	}
}

func TestVariantFloatToJSON(t *testing.T) {
	// A FLOAT value renders through GetFloat in ToJSON.
	v := New(floatBytes(1.5), emptyMetadata)
	if v.GetType() != Float {
		t.Fatalf("type = %d, want Float", v.GetType())
	}
	if got := mustJSON(t, v); got != "1.5" {
		t.Errorf("ToJSON(FLOAT) = %q, want 1.5", got)
	}
}
