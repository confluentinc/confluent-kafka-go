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
	"math/big"
	"reflect"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
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
		d := &types.Decimal{Value: []byte{0x04, 0xd2}, Scale: 2, Precision: precision} // 12.34
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
