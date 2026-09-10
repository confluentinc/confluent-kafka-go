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
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"math/big"
)

var one = big.NewInt(1)

// BigRatToDecimal converts a big.Rat to a Decimal protobuf message.
func BigRatToDecimal(value *big.Rat, scale int32) (*types.Decimal, error) {
	if value == nil {
		return nil, nil
	}

	// A negative scale means the value is `unscaled * 10^-scale`, so the unscaled integer is
	// num/(den * 10^-scale) - not num * 10^scale. `big.Int.Exp` returns **1** for a negative
	// exponent, so the sign was silently ignored: `BigRatToDecimal(1000, -3)` stored unscaled
	// 1000 at scale -3, which the reference reads as 1000 * 10^3 = 1000000. Measured on the
	// JDK: `new BigDecimal("1000").setScale(-3)` is unscaled 1, precision 1, and
	// `new BigDecimal(BigInteger.valueOf(1000), -3)` is 1000000.
	//
	// It round-tripped inside this client only because ratFromBytes had the mirror-image bug,
	// so the two cancelled and the wire bytes were wrong for every other reader.
	i := new(big.Int).Set(value.Num())
	den := new(big.Int).Set(value.Denom())
	if scale >= 0 {
		i.Mul(i, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
	} else {
		den.Mul(den, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-int64(scale))), nil))
	}
	i = i.Div(i, den)

	return &types.Decimal{
		Value: signedBytes(i),
		// The unscaled value's digit count, which is what BigDecimal.precision() reports and
		// what every other write path in this client family carries. Left at 0, this was one
		// of three paths whose output a JVM consumer rewrites on its next touch:
		// precision() is never less than 1 - zero's precision is 1 - so 0 is a value the
		// reference cannot produce, and its reader normalises it away.
		//
		// Taken from `i`, the integer actually being written, so the multiplication above
		// cannot leave it stale.
		Precision: uint32(len(new(big.Int).Abs(i).String())),
		Scale:     scale,
	}, nil
}

// DecimalToBigRat converts a Decimal protobuf message to a big.Rat.
func DecimalToBigRat(value *types.Decimal) (*big.Rat, error) {
	if value == nil {
		return nil, nil
	}

	return ratFromBytes(value.Value, int(value.Scale)), nil
}

func ratFromBytes(b []byte, scale int) *big.Rat {
	num := (&big.Int{}).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 > 0 {
		num.Sub(num, new(big.Int).Lsh(one, uint(len(b))*8))
	}
	// The reading half of the same asymmetry: at a negative scale the value is
	// `unscaled * 10^-scale`, so the power of ten multiplies the numerator rather than the
	// denominator. `big.Int.Exp` returning 1 for a negative exponent made this treat scale -3
	// as scale 0.
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs64(int64(scale)))), nil)
	if scale < 0 {
		return new(big.Rat).SetFrac(new(big.Int).Mul(num, pow), big.NewInt(1))
	}
	return new(big.Rat).SetFrac(num, pow)
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// signedBytes encodes an integer as minimal big-endian two's-complement bytes, which is how
// confluent.type.Decimal stores an unscaled value.
func signedBytes(i *big.Int) []byte {
	switch i.Sign() {
	case 0:
		return []byte{0}
	case 1:
		b := i.Bytes()
		if b[0]&0x80 > 0 {
			// The high bit would read as a sign bit, so pad to keep the value positive.
			b = append([]byte{0}, b...)
		}
		return b
	default:
		// A negative value's magnitude is Not(i) == -i-1, so its bit length is one less at
		// every exact signed boundary. Sizing from i.BitLen() emitted ff80 for -128 where
		// BigInteger.toByteArray gives 80. Mirrors signedBytesFromBigInt in rules/cel.
		bits := new(big.Int).Not(i).BitLen() + 1
		byteLen := (bits + 7) / 8
		if byteLen < 1 {
			byteLen = 1
		}
		shifted := new(big.Int).Add(i, new(big.Int).Lsh(one, uint(byteLen)*8))
		out := make([]byte, byteLen)
		shifted.FillBytes(out)
		return out
	}
}
