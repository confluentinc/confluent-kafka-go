/**
 * Copyright 2025 Confluent Inc.
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

package protobuf

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/apd/v3"
	typepb "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/type"
)

func TestDecimalConversion(t *testing.T) {
	tests := []struct {
		name  string
		input *big.Rat
		scale int32
	}{
		{
			name:  "1st value",
			input: big.NewRat(0, 1),
			scale: 0,
		},
		{
			name:  "2nd value",
			input: big.NewRat(101, 100),
			scale: 2,
		},
		{
			name:  "3rd value",
			input: big.NewRat(123456789123456789, 100),
			scale: 2,
		},
		{
			name:  "4th value",
			input: big.NewRat(1234, 1),
			scale: 0,
		},
		{
			name:  "5h value",
			input: big.NewRat(12345, 10),
			scale: 1,
		},
		{
			name:  "6th value",
			input: big.NewRat(-0, 1),
			scale: 0,
		},
		{
			name:  "7th value",
			input: big.NewRat(-101, 100),
			scale: 2,
		},
		{
			name:  "8th value",
			input: big.NewRat(-123456789123456789, 100),
			scale: 2,
		},
		{
			name:  "9th value",
			input: big.NewRat(-1234, 1),
			scale: 0,
		},
		{
			name:  "10th value",
			input: big.NewRat(-12345, 10),
			scale: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted, err := BigRatToDecimal(test.input, test.scale)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			result, err := DecimalToBigRat(converted)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(test.input, result) {
				t.Fatalf("not equal: input %v, output %v", test.input, result)
			}
		})
	}
}

// TestSignedBytesIsMinimal pins the encoding against java.math.BigInteger.toByteArray(), which
// is what confluent.type.Decimal.value carries. Sizing the buffer from BitLen() over-allocated a
// byte at every exact signed boundary (a negative power of 256/2), because BitLen ignores the
// sign: -128 needs one byte (0x80) but reports a bit length of 8, so the encoder emitted ff80.
func TestSignedBytesIsMinimal(t *testing.T) {
	cases := map[int64]string{
		0: "00", 1: "01", 127: "7f", 128: "0080", 255: "00ff", 256: "0100",
		-1: "ff", -127: "81", -128: "80", -129: "ff7f", -255: "ff01", -256: "ff00",
		-32768: "8000", -32769: "ff7fff",
	}
	for n, want := range cases {
		if got := fmt.Sprintf("%x", signedBytes(big.NewInt(n))); got != want {
			t.Errorf("signedBytes(%d) = %s, want %s", n, got, want)
		}
	}
	// Every negative signed-range minimum must fit in exactly k bytes.
	for k := 1; k <= 11; k++ {
		n := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), uint(8*k-1)))
		if got := len(signedBytes(n)); got != k {
			t.Errorf("signedBytes(-2^%d) used %d bytes, want %d", 8*k-1, got, k)
		}
	}
}

// TestDecimalToProtoPartsPreservesNegativeScale covers a decimal with a positive exponent.
// BigDecimal("1E+3") reports unscaled 1 with scale -3 and the proto's scale field is a signed
// int32, so the JVM writes it as-is. Normalising it into the digits wrote (1000, 0) instead --
// a different representation of the same number, and one whose precision field (1) no longer
// described the unscaled value it was stored with.
func TestDecimalToProtoPartsPreservesNegativeScale(t *testing.T) {
	cases := []struct {
		in        string
		unscaled  int64
		scale     int32
		precision uint32
	}{
		{"1E+3", 1, -3, 1},
		{"1000", 1000, 0, 4},
		{"12.34", 1234, 2, 4},
		{"1E-3", 1, 3, 1},
		{"1E+0", 1, 0, 1},
	}
	for _, tc := range cases {
		d, _, err := apd.NewFromString(tc.in)
		if err != nil {
			t.Fatalf("parse %s: %v", tc.in, err)
		}
		got, err := decimalToProtoParts(d)
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		un := new(big.Int).SetBytes(got.unscaled).Int64()
		if un != tc.unscaled || got.scale != tc.scale || got.precision != tc.precision {
			t.Errorf("decimalToProtoParts(%s) = (unscaled %d, scale %d, precision %d), want (%d, %d, %d)",
				tc.in, un, got.scale, got.precision, tc.unscaled, tc.scale, tc.precision)
		}
	}
}

// TestBigRatToDecimalWritesThePrecision pins the digit count on the one write path that used to
// leave it at 0. precision() is never less than 1 on the JVM -- BigDecimal.ZERO.precision() is 1
// -- so 0 is a value the reference cannot produce, and a JVM consumer normalises it away: a Java
// identity transform over such a message rewrites the field and the bytes stop matching. The
// count comes from the unscaled integer actually written, not from the operand, because
// BigRatToDecimal scales the rational by 10^scale first.
func TestBigRatToDecimalWritesThePrecision(t *testing.T) {
	cases := []struct {
		num, den  int64
		scale     int32
		unscaled  int64
		precision uint32
	}{
		{1234, 100, 2, 1234, 4},
		// The scale is applied before the count: 12.34 at scale 4 is written as 123400, six
		// digits, not four. Deriving precision from the operand would report 4 here.
		{1234, 100, 4, 123400, 6},
		{3, 2, 1, 15, 2},
		{0, 1, 0, 0, 1}, // zero is precision 1, as BigDecimal.ZERO is
		{0, 1, 3, 0, 1},
		{-1234, 100, 2, -1234, 4},
		{1, 1, 0, 1, 1},
	}
	for _, tc := range cases {
		got, err := BigRatToDecimal(big.NewRat(tc.num, tc.den), tc.scale)
		if err != nil {
			t.Fatalf("%d/%d at scale %d: %v", tc.num, tc.den, tc.scale, err)
		}
		un := new(big.Int).SetBytes(got.Value)
		if got.Value[0]&0x80 != 0 {
			un.Sub(un, new(big.Int).Lsh(big.NewInt(1), uint(8*len(got.Value))))
		}
		if un.Int64() != tc.unscaled || got.Scale != tc.scale || got.Precision != tc.precision {
			t.Errorf("BigRatToDecimal(%d/%d, %d) = (unscaled %d, scale %d, precision %d), want (%d, %d, %d)",
				tc.num, tc.den, tc.scale, un.Int64(), got.Scale, got.Precision,
				tc.unscaled, tc.scale, tc.precision)
		}
	}
}

// TestDecimalToBigRatIgnoresPrecision is the read half of the same decision. Java applies
// precision as a MathContext and rounds; every client here reads the value at its own digits, so
// a declared precision that disagrees with the coefficient does not silently reshape data the
// producer sent exactly.
func TestDecimalToBigRatIgnoresPrecision(t *testing.T) {
	for _, precision := range []uint32{0, 1, 2, 4, 38} {
		d := &typepb.Decimal{Value: []byte{0x04, 0xd2}, Scale: 2, Precision: precision} // 12.34
		got, err := DecimalToBigRat(d)
		if err != nil {
			t.Fatalf("precision %d: %v", precision, err)
		}
		if want := big.NewRat(1234, 100); got.Cmp(want) != 0 {
			t.Errorf("DecimalToBigRat(precision %d) = %s, want %s", precision,
				got.RatString(), want.RatString())
		}
	}
}

// TestNegativeScaleRoundTripsOnTheWire pins both directions of the negative-scale conversion.
//
// `big.Int.Exp(10, negative, nil)` returns **1**, so the sign was silently ignored in both
// BigRatToDecimal and ratFromBytes - and because the two errors were mirror images, a Go->Go
// round-trip looked correct while the wire bytes were wrong for every other reader. Measured on
// the JDK, which is what the wire form has to mean:
//
//	new BigDecimal("1000").setScale(-3)              -> unscaled 1, scale -3, precision 1
//	new BigDecimal(BigInteger.valueOf(1000), -3)     -> 1000000
//
// So storing unscaled 1000 at scale -3, as this did, is off by a factor of 1000 on the JVM.
func TestNegativeScaleRoundTripsOnTheWire(t *testing.T) {
	cases := []struct {
		num, den  int64
		scale     int32
		unscaled  int64
		precision uint32
	}{
		{1000, 1, -3, 1, 1}, // the reported case
		{1200, 1, -2, 12, 2},
		{100, 1, -2, 1, 1},
		{1234, 100, 2, 1234, 4}, // a positive scale is unaffected
		{0, 1, -3, 0, 1},
		{-1000, 1, -3, -1, 1},
	}
	for _, tc := range cases {
		m, err := BigRatToDecimal(big.NewRat(tc.num, tc.den), tc.scale)
		if err != nil {
			t.Fatalf("%d/%d at %d: %v", tc.num, tc.den, tc.scale, err)
		}
		un := new(big.Int).SetBytes(m.Value)
		if len(m.Value) > 0 && m.Value[0]&0x80 != 0 {
			un.Sub(un, new(big.Int).Lsh(big.NewInt(1), uint(8*len(m.Value))))
		}
		if un.Int64() != tc.unscaled || m.Scale != tc.scale || m.Precision != tc.precision {
			t.Errorf("BigRatToDecimal(%d/%d, %d) = (unscaled %d, scale %d, precision %d), want (%d, %d, %d)",
				tc.num, tc.den, tc.scale, un.Int64(), m.Scale, m.Precision,
				tc.unscaled, tc.scale, tc.precision)
		}
		// And reading it back gives the value we started from, so the two halves agree.
		back, err := DecimalToBigRat(m)
		if err != nil {
			t.Fatalf("DecimalToBigRat: %v", err)
		}
		if want := big.NewRat(tc.num, tc.den); back.Cmp(want) != 0 {
			t.Errorf("round-trip of %d/%d at scale %d = %s, want %s",
				tc.num, tc.den, tc.scale, back.RatString(), want.RatString())
		}
	}
}

// TestDecimalToBigRatReadsANegativeScale is the reading half on its own, against the byte form
// the reference writes: unscaled 1 at scale -3 is 1000, not 1.
func TestDecimalToBigRatReadsANegativeScale(t *testing.T) {
	for _, tc := range []struct {
		unscaled []byte
		scale    int32
		want     *big.Rat
	}{
		{[]byte{0x01}, -3, big.NewRat(1000, 1)},
		{[]byte{0x0c}, -2, big.NewRat(1200, 1)},
		{[]byte{0x04, 0xd2}, 2, big.NewRat(1234, 100)},
		{[]byte{0x01}, 0, big.NewRat(1, 1)},
	} {
		got, err := DecimalToBigRat(&typepb.Decimal{Value: tc.unscaled, Scale: tc.scale})
		if err != nil {
			t.Fatalf("%x at %d: %v", tc.unscaled, tc.scale, err)
		}
		if got.Cmp(tc.want) != 0 {
			t.Errorf("DecimalToBigRat(%x, %d) = %s, want %s",
				tc.unscaled, tc.scale, got.RatString(), tc.want.RatString())
		}
	}
}

// A wire scale is producer-controlled int32 data, and a big.Rat has no exponent to store it in -
// the power of ten has to be built. Both signs cost the same, so both are bounded: 10^2147483648
// is ~890MB, and even the bound itself (10^10000000, 33219281 bits) takes 663ms to build.
//
// The reference needs no such check - `new BigDecimal(unscaled, scale)` just stores the int - and
// neither does this client's CEL path, which builds an apd.Decimal from coefficient and exponent.
// This is a limitation of the big.Rat representation, not a parity choice.
func TestBigRatScaleIsBounded(t *testing.T) {
	for _, scale := range []int32{
		math.MinInt32, math.MaxInt32, -2000000000, 2000000000,
		-(maxRatScale + 1), maxRatScale + 1,
	} {
		d := &typepb.Decimal{Value: []byte{0x01}, Scale: scale}
		if _, err := DecimalToBigRat(d); err == nil {
			t.Errorf("DecimalToBigRat(scale %d): expected an error, got none", scale)
		}
		if _, err := BigRatToDecimal(big.NewRat(1, 1), scale); err == nil {
			t.Errorf("BigRatToDecimal(scale %d): expected an error, got none", scale)
		}
	}
}

// ...and the ordinary scales still work, so the bound is not simply refusing everything.
func TestBigRatOrdinaryScalesStillConvert(t *testing.T) {
	cases := []struct {
		scale int32
		want  string
	}{
		{2, "1234/100"},  // 12.34
		{0, "1234/1"},    // 1234
		{-2, "123400/1"}, // 1234 * 10^2
	}
	for _, c := range cases {
		got, err := DecimalToBigRat(&typepb.Decimal{Value: []byte{0x04, 0xD2}, Scale: c.scale})
		if err != nil {
			t.Fatalf("scale %d: %v", c.scale, err)
		}
		if got.RatString() != new(big.Rat).SetFrac(
			mustBigInt(c.want, 0), mustBigInt(c.want, 1)).RatString() {
			t.Errorf("scale %d: got %s, want %s", c.scale, got.RatString(), c.want)
		}
	}
}

func mustBigInt(frac string, which int) *big.Int {
	parts := strings.Split(frac, "/")
	v, _ := new(big.Int).SetString(parts[which], 10)
	return v
}

// An inexact rescale truncates toward zero, symmetrically. big.Int.Div is Euclidean and floors
// toward negative infinity, so only the negative branch was off: -1.25 at scale 1 wrote unscaled
// -13 where +1.25 wrote 12. Flooring matches neither contract in this family - the reference's
// BigInteger.divide truncates toward zero, as does decimals.trunc's ROUND_DOWN - and -1.21 shows
// it is not HALF_UP either, which would also give -12.
//
// The pairs are the point: a sign-symmetric contract cannot be asserted from one side.
func TestBigRatToDecimalTruncatesTowardZero(t *testing.T) {
	cases := []struct {
		rat      string
		scale    int32
		unscaled int64
	}{
		{"5/4", 1, 12},   // 1.25 -> 1.2
		{"-5/4", 1, -12}, // -1.25 -> -1.2, not -1.3
		{"121/100", 1, 12},
		{"-121/100", 1, -12}, // HALF_UP would also be -12; floor gave -13
		{"1/3", 2, 33},
		{"-1/3", 2, -33},
		{"2/3", 2, 66},
		{"-2/3", 2, -66},
		// Exact values are unaffected by the rounding direction at all.
		{"617/50", 2, 1234}, // 12.34
		{"-617/50", 2, -1234},
	}
	for _, c := range cases {
		r, ok := new(big.Rat).SetString(c.rat)
		if !ok {
			t.Fatalf("bad rational %q", c.rat)
		}
		d, err := BigRatToDecimal(r, c.scale)
		if err != nil {
			t.Fatalf("%s: %v", c.rat, err)
		}
		got := new(big.Int).SetBytes(d.Value)
		if len(d.Value) > 0 && d.Value[0]&0x80 != 0 {
			got.Sub(got, new(big.Int).Lsh(big.NewInt(1), uint(len(d.Value))*8))
		}
		if got.Int64() != c.unscaled {
			t.Errorf("%s at scale %d: unscaled %s, want %d", c.rat, c.scale, got, c.unscaled)
		}
	}
}
