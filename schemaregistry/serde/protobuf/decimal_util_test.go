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
