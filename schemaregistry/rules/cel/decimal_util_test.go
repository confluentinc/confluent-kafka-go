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
	"bytes"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/cockroachdb/apd/v3"

	prototypes "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
)

func mustDecimal(t *testing.T, s string) *apd.Decimal {
	t.Helper()
	d, _, err := apd.NewFromString(s)
	if err != nil {
		t.Fatalf("apd.NewFromString(%q): %v", s, err)
	}
	return d
}

func TestDecimalProtoRoundTrip(t *testing.T) {
	for _, s := range []string{"12.34", "0", "-7.5", "100", "1.50", "0.001", "-0.0000000001"} {
		d := mustDecimal(t, s)
		proto, err := decimalToProto(d)
		if err != nil {
			t.Fatalf("decimalToProto(%s): %v", s, err)
		}
		back, err := decimalFromProto(proto)
		if err != nil {
			t.Fatalf("decimalFromProto for %s: %v", s, err)
		}
		if back.Cmp(d) != 0 {
			t.Errorf("round-trip mismatch for %s: got %s", s, back.Text('f'))
		}
		// Scale is preserved through the proto (1.50 stays 1.50, not 1.5).
		if -proto.Scale != d.Exponent {
			t.Errorf("scale mismatch for %s: proto scale %d, apd exponent %d", s, proto.Scale, d.Exponent)
		}
	}
}

func TestDecimalProtoKnownWireForm(t *testing.T) {
	// 12.34 = unscaled 1234 (0x04D2) at scale 2.
	proto, err := decimalToProto(mustDecimal(t, "12.34"))
	if err != nil {
		t.Fatalf("decimalToProto: %v", err)
	}
	if !bytes.Equal(proto.Value, []byte{0x04, 0xd2}) || proto.Scale != 2 {
		t.Errorf("expected value=[04 d2] scale=2, got value=% x scale=%d", proto.Value, proto.Scale)
	}
}

// TestDecimalToProtoSetsPrecision covers confluent.type.Decimal's precision field, which is the
// unscaled value's digit count -- what BigDecimal.precision() reports and what the JVM's
// ProtobufResultWriter writes (m.put("precision", dec.precision())). This writer left it unset,
// so the same computed decimal serialized differently here than on the JVM, and differently
// from this client's own protobuf serde path, which has always set it.
func TestDecimalToProtoSetsPrecision(t *testing.T) {
	cases := []struct {
		in        string
		precision uint32
		scale     int32
	}{
		{"12.34", 4, 2},
		{"12.3400", 6, 4}, // trailing zeros are digits, so precision is 6 not 4
		{"1E+3", 1, -3},   // unscaled 1 at a negative scale
		{"0.00", 1, 2},    // zero has precision 1, as BigDecimal reports it
		{"100", 3, 0},
		{"-12.34", 4, 2}, // the sign is not a digit
	}
	for _, tc := range cases {
		d, _, err := apd.NewFromString(tc.in)
		if err != nil {
			t.Fatalf("parse %s: %v", tc.in, err)
		}
		got, err := decimalToProto(d)
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if got.Precision != tc.precision || got.Scale != tc.scale {
			t.Errorf("decimalToProto(%s) = (precision %d, scale %d), want (%d, %d)",
				tc.in, got.Precision, got.Scale, tc.precision, tc.scale)
		}
	}
}

// TestAvroDecimalEntersCelExactly pins the Avro decode path. An Avro decimal's denominator is
// always a power of ten, so its value is exactly representable - but decimalFromRat divided
// through the 38-digit division context, which *changed the field on the way into CEL*, before
// any arithmetic:
//
//	1234567890123456789012345678901234567890/100 -> 12345678901234567890123456789012345679
//	                                       (exact: 12345678901234567890123456789012345678.9)
//
// and padded ordinary values out to 38 significant digits (1234/100 came back as
// 12.340000000000000000000000000000000000), which is why string() on an Avro decimal did not
// match the reference's toPlainString. Java reads an Avro decimal straight into
// BigDecimal(unscaled, scale): exact, and at its own scale.
func TestAvroDecimalEntersCelExactly(t *testing.T) {
	cases := []struct {
		num, den string
		want     string
	}{
		{"1234", "100", "12.34"},
		// A trailing-zero scale cannot survive big.Rat's reduction (1990/100 is 199/10), which
		// is the limitation this path already carried - not something the exactness fix
		// introduces.
		{"1990", "100", "19.9"},
		// Denominators that are not powers of ten but still terminate: 2^a * 5^b.
		{"617", "50", "12.34"},
		{"1", "8", "0.125"},
		{"3", "1", "3"},
		{"1234567890123456789012345678901234567890", "100",
			"12345678901234567890123456789012345678.9"},
		{"99999999999999999999999999999999999999999", "1000",
			"99999999999999999999999999999999999999.999"},
		// Non-terminating: 1/3 has no finite expansion, so the 38-digit division context is
		// still the answer there and rounding is unavoidable.
		{"1", "3", "0.33333333333333333333333333333333333333"},
		{"-1234", "100", "-12.34"},
		{"7", "1", "7"},
		{"0", "100", "0"},
	}
	for _, tc := range cases {
		n, _ := new(big.Int).SetString(tc.num, 10)
		d, _ := new(big.Int).SetString(tc.den, 10)
		got, err := decimalFromRat(new(big.Rat).SetFrac(n, d))
		if err != nil {
			t.Fatalf("%s/%s: %v", tc.num, tc.den, err)
		}
		if s := got.Text('f'); s != tc.want {
			t.Errorf("decimalFromRat(%s/%s) = %s, want %s", tc.num, tc.den, s, tc.want)
		}
	}
}

// TestDecimalFromBytesScaleAtTheInt32Extremes covers the scales requireIntScale lets through.
// plainDecimalString materialised every digit of the positional form, so scale
// math.MinInt32 reached strings.Repeat with a negative count (surfacing as cel-go's
// "internal error: strings: negative Repeat count") and scale -2147483647 built a 2147483648
// byte string. Constructing the coefficient and exponent directly leaves the exponent range to
// apd, which is what the decimals design delegates to it.
func TestDecimalFromBytesScaleAtTheInt32Extremes(t *testing.T) {
	// The one scale whose negation does not fit an int32 is refused, by name.
	if _, err := decimalFromBytesScale([]byte{0x01}, math.MinInt32); err == nil {
		t.Error("scale math.MinInt32 should be refused, not panic")
	}
	// Everything else is cheap to hold: no digits are materialised.
	for _, scale := range []int32{math.MaxInt32, -2147483647, 1000000, 0, -1000000} {
		d, err := decimalFromBytesScale([]byte{0x01}, scale)
		if err != nil {
			t.Fatalf("scale %d: %v", scale, err)
		}
		if d.Exponent != -scale {
			t.Errorf("scale %d: exponent %d, want %d", scale, d.Exponent, -scale)
		}
	}
	// And ordinary values still round-trip, sign included.
	for _, tc := range []struct {
		b     []byte
		scale int32
		want  string
	}{
		{[]byte{0x04, 0xd2}, 2, "12.34"},
		{[]byte{0x07, 0xc6}, 2, "19.90"},
		{[]byte{0x0c}, -2, "1200"},
		{[]byte{0xfb, 0x2e}, 2, "-12.34"},
		{[]byte{0x00}, 0, "0"},
	} {
		d, err := decimalFromBytesScale(tc.b, tc.scale)
		if err != nil {
			t.Fatalf("%x at %d: %v", tc.b, tc.scale, err)
		}
		if s := d.Text('f'); s != tc.want {
			t.Errorf("decimalFromBytesScale(%x, %d) = %s, want %s", tc.b, tc.scale, s, tc.want)
		}
	}
}

// TestDecimalFromProtoBoundsTheWireScale pins the scale a *producer* controls. decimalFromProto
// used to render the positional form via plainDecimalString, which materialises every digit:
// measured, Scale math.MinInt32 panicked with "strings: negative Repeat count" (surfacing
// through cel-go as an internal error, not a rule error) and Scale -2147483647 was still
// allocating after 300s, since it asks for a 2147483648-byte string.
//
// decimalFromBytesScale was moved off that rendering for exactly this reason; this path was
// left behind, and it is the more exposed of the two because its input comes off the wire.
func TestDecimalFromProtoBoundsTheWireScale(t *testing.T) {
	// Refused, not panicked: apd cannot represent an exponent of +2147483648.
	if _, err := decimalFromProto(&prototypes.Decimal{
		Value: []byte{0x01}, Scale: math.MinInt32,
	}); err == nil {
		t.Error("scale math.MinInt32 should be refused")
	} else if !strings.Contains(err.Error(), "cannot be represented") {
		t.Errorf("error should say the scale cannot be represented, got %v", err)
	}

	// And the extremes that *are* representable answer immediately rather than allocating a
	// multi-gigabyte string.
	for _, scale := range []int32{-2147483647, 2147483647, 2, 0} {
		if _, err := decimalFromProto(&prototypes.Decimal{
			Value: []byte{0x01}, Scale: scale,
		}); err != nil {
			t.Errorf("scale %d: unexpected error %v", scale, err)
		}
	}
}

// The Avro write-back converts a decimal through its plain text form, because hamba encodes a
// decimal logical type from a *big.Rat and a big.Rat has no exponent. That makes the exponent a
// width: `decimal(b"\x01", 2147483647)` is built from a coefficient and an exponent at no cost,
// and only turning it back into Avro expands it - measured, ~2.1e9 characters, then a
// 10^2147483647 denominator on top.
//
// The reference never renders: it hands its BigDecimal to Avro's own DecimalConversion, which
// writes unscaledValue() plus the schema's scale. Python, JavaScript, C#, C++ and Rust all do
// the same. Go is the only client whose Avro decimal boundary is a big.Rat, which is why it is
// the only one that needs a width bound here - the same 10^7 the protobuf serde's big.Rat
// conversion uses.
func TestAvroDecimalWidthIsBounded(t *testing.T) {
	for _, exp := range []int32{
		math.MinInt32, math.MaxInt32,
		-(maxAvroDecimalWidth + 1), maxAvroDecimalWidth + 1,
	} {
		d := apd.New(1, exp)
		if _, err := ratFromDecimal(d); err == nil {
			t.Errorf("exponent %d: expected an error, got none", exp)
		}
	}
}

// ...and the ordinary decimals still convert, so the bound is not refusing everything.
func TestAvroDecimalOrdinaryValuesStillConvert(t *testing.T) {
	cases := []struct {
		text string
		want string
	}{
		{"12.34", "617/50"},
		{"0", "0"},
		{"-1.5", "-3/2"},
		{"1E+3", "1000"}, // a positive exponent, which widens the other way
	}
	for _, c := range cases {
		d, _, err := apd.NewFromString(c.text)
		if err != nil {
			t.Fatalf("%s: %v", c.text, err)
		}
		rat, err := ratFromDecimal(d)
		if err != nil {
			t.Fatalf("%s: %v", c.text, err)
		}
		if rat.RatString() != c.want {
			t.Errorf("%s: got %s, want %s", c.text, rat.RatString(), c.want)
		}
	}
}
