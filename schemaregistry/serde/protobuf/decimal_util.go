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

	exp := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	i := (&big.Int{}).Mul(value.Num(), exp)
	i = i.Div(i, value.Denom())

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
	denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return new(big.Rat).SetFrac(num, denom)
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
