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

	var b []byte
	switch i.Sign() {
	case 0:
		b = []byte{0}

	case 1:
		b = i.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}

	case -1:
		length := uint(i.BitLen()/8+1) * 8
		b = i.Add(i, (&big.Int{}).Lsh(one, length)).Bytes()
	}

	return &types.Decimal{
		Value:     b,
		Precision: 0,
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
