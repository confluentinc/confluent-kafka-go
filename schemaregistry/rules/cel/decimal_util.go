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
	"fmt"
	"math/big"
	"strings"

	"github.com/cockroachdb/apd/v3"
	prototypes "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
)

// Conversions between the CEL decimal backing type (apd.Decimal) and the shapes decoders
// produce — the CEL counterpart of Java's DecimalUtils and the confluent.type.Decimal
// helpers in the other clients. The serde's own *big.Rat <-> confluent.type.Decimal helper
// lives separately in schemaregistry/serde/protobuf/decimal_util.go; the CEL layer uses
// apd.Decimal (which, unlike *big.Rat, carries a scale and supports 38-significant-digit
// rounding), so it keeps its own conversions here.

// decimalFromProto converts a confluent.type.Decimal message (unscaled big-endian two's
// complement bytes + scale) to an apd.Decimal, preserving the message's scale exactly.
func decimalFromProto(d *prototypes.Decimal) (*apd.Decimal, error) {
	res, _, err := apd.NewFromString(plainDecimalString(bigIntFromSignedBytes(d.Value), d.Scale))
	return res, err
}

// decimalToProto converts an apd.Decimal to a confluent.type.Decimal message, mirroring
// Java's BigDecimal.unscaledValue()/scale(): the scale is the number of fractional digits
// and the value is the unscaled integer as big-endian two's-complement bytes.
func decimalToProto(d *apd.Decimal) (*prototypes.Decimal, error) {
	if d.Form != apd.Finite {
		return nil, fmt.Errorf("cannot convert non-finite decimal %q to confluent.type.Decimal", d.Text('f'))
	}
	unscaled := new(big.Int).Set(d.Coeff.MathBigInt())
	if d.Negative {
		unscaled.Neg(unscaled)
	}
	return &prototypes.Decimal{
		Value: signedBytesFromBigInt(unscaled),
		Scale: -d.Exponent,
	}, nil
}

// decimalFromRat converts an exact rational (the Avro decimal decode from hamba) to an
// apd.Decimal. The rational carries no scale, so trailing-zero scale is not preserved.
func decimalFromRat(r *big.Rat) (*apd.Decimal, error) {
	num, _, err := apd.NewFromString(r.Num().String())
	if err != nil {
		return nil, err
	}
	den, _, err := apd.NewFromString(r.Denom().String())
	if err != nil {
		return nil, err
	}
	res := new(apd.Decimal)
	if _, err := divContext.Quo(res, num, den); err != nil {
		return nil, err
	}
	return res, nil
}

// decimalFromBytesScale builds an apd.Decimal from raw two's-complement bytes plus a scale.
func decimalFromBytesScale(b []byte, scale int32) (*apd.Decimal, error) {
	res, _, err := apd.NewFromString(plainDecimalString(bigIntFromSignedBytes(b), scale))
	return res, err
}

// bigIntFromSignedBytes decodes big-endian two's-complement bytes (BigInteger.toByteArray
// form) into a *big.Int, mirroring the protobuf serde's ratFromBytes.
func bigIntFromSignedBytes(b []byte) *big.Int {
	num := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 > 0 {
		num.Sub(num, new(big.Int).Lsh(big.NewInt(1), uint(len(b))*8))
	}
	return num
}

// signedBytesFromBigInt encodes a *big.Int as big-endian two's-complement bytes, mirroring
// the protobuf serde's BigRatToDecimal (the inverse of bigIntFromSignedBytes).
func signedBytesFromBigInt(i *big.Int) []byte {
	switch i.Sign() {
	case 0:
		return []byte{0}
	case 1:
		b := i.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}
		return b
	default:
		length := uint(i.BitLen()/8+1) * 8
		return new(big.Int).Add(i, new(big.Int).Lsh(big.NewInt(1), length)).Bytes()
	}
}

// plainDecimalString renders unscaled × 10^-scale as a plain (never scientific) string.
func plainDecimalString(unscaled *big.Int, scale int32) string {
	negative := unscaled.Sign() < 0
	digits := new(big.Int).Abs(unscaled).String()

	var sb strings.Builder
	switch {
	case scale <= 0:
		sb.WriteString(digits)
		sb.WriteString(strings.Repeat("0", int(-scale)))
	case len(digits) > int(scale):
		point := len(digits) - int(scale)
		sb.WriteString(digits[:point])
		sb.WriteByte('.')
		sb.WriteString(digits[point:])
	default:
		sb.WriteString("0.")
		sb.WriteString(strings.Repeat("0", int(scale)-len(digits)))
		sb.WriteString(digits)
	}
	if negative {
		return "-" + sb.String()
	}
	return sb.String()
}
