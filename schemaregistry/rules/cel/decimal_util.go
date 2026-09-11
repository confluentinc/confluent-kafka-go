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
	"math"
	"math/big"
	"strings"

	"github.com/cockroachdb/apd/v3"
	typepb "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/type"
)

// Conversions between the CEL decimal backing type (apd.Decimal) and the shapes decoders
// produce — the CEL counterpart of Java's DecimalUtils and the confluent.type.Decimal
// helpers in the other clients. The serde's own *big.Rat <-> confluent.type.Decimal helper
// lives separately in schemaregistry/serde/protobuf/decimal_util.go; the CEL layer uses
// apd.Decimal (which, unlike *big.Rat, carries a scale and supports 38-significant-digit
// rounding), so it keeps its own conversions here.

// decimalFromProto converts a confluent.type.Decimal message (unscaled big-endian two's
// complement bytes + scale) to an apd.Decimal, preserving the message's scale exactly.
func decimalFromProto(d *typepb.Decimal) (*apd.Decimal, error) {
	// Built from the coefficient and exponent, not from plainDecimalString, for the reason
	// decimalFromBytesScale below records: that rendering materialises every digit of the
	// positional form, and the scale here arrives off the wire. Measured on this very path -
	// Scale math.MinInt32 panicked with "strings: negative Repeat count", and -2147483647 was
	// still allocating after 300s. decimalFromBytesScale was moved off it and this was left
	// behind, which is the more exposed of the two: this one reads producer-controlled input.
	return decimalFromCoefficient(bigIntFromSignedBytes(d.Value), d.Scale)
}

// decimalToProto converts an apd.Decimal to a confluent.type.Decimal message, mirroring
// Java's BigDecimal.unscaledValue()/scale(): the scale is the number of fractional digits
// and the value is the unscaled integer as big-endian two's-complement bytes.
func decimalToProto(d *apd.Decimal) (*typepb.Decimal, error) {
	if d.Form != apd.Finite {
		return nil, fmt.Errorf("cannot convert non-finite decimal %q to confluent.type.Decimal", d.Text('f'))
	}
	unscaled := new(big.Int).Set(d.Coeff.MathBigInt())
	if d.Negative {
		unscaled.Neg(unscaled)
	}
	return &typepb.Decimal{
		Value: signedBytesFromBigInt(unscaled),
		// Precision is the unscaled value's digit count, as BigDecimal.precision() reports it
		// and as Java's ProtobufResultWriter sets it. Safe here because this writer does not
		// rescale, so the coefficient's digits are the digits actually written.
		Precision: uint32(len(d.Coeff.MathBigInt().String())),
		Scale:     -d.Exponent,
	}, nil
}

// decimalFromRat converts an exact rational (the Avro decimal decode from hamba) to an
// apd.Decimal. The rational carries no scale, so trailing-zero scale is not preserved.
//
// An Avro decimal's denominator is always a power of ten, which makes the value exactly
// representable - so the coefficient and exponent are taken directly rather than divided out.
// Going through divContext.Quo rounded every such field to 38 significant digits *on the way
// into CEL*, before any arithmetic:
//
//	1234567890123456789012345678901234567890/100  ->  12345678901234567890123456789012345679
//	                                        (exact: 12345678901234567890123456789012345678.9)
//
// Note that big.Rat has already reduced the fraction by the time it gets here, so the scale is
// recovered from the reduced denominator (see terminatingScale) rather than read off a literal
// power of ten. A trailing-zero scale cannot survive that reduction - 1990/100 is 199/10, i.e.
// 19.9 rather than 19.90 - which is the pre-existing limitation this path already documented.
//
// which silently changes a field a rule only meant to read. It also padded ordinary values to
// the full 38 digits - 1234/100 came back as 12.340000000000000000000000000000000000 - which is
// why string() on an Avro decimal did not match the reference's toPlainString. Java reads an
// Avro decimal straight into BigDecimal(unscaled, scale), exactly and at its own scale.
//
// A non-terminating rational cannot come from an Avro decimal, but if one ever arrives the
// division is still there as a fallback, and rounding is then unavoidable.
func decimalFromRat(r *big.Rat) (*apd.Decimal, error) {
	if scale, ok := terminatingScale(r.Denom()); ok {
		// coeff = num * 10^scale / den, exact by construction of scale.
		coeff := new(big.Int).Mul(r.Num(), new(big.Int).Exp(big.NewInt(10),
			big.NewInt(int64(scale)), nil))
		coeff.Quo(coeff, r.Denom())
		return decimalFromCoefficient(coeff, scale)
	}
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

// terminatingScale reports the smallest k for which num/den == c/10^k exactly, i.e. the scale
// of the value's finite decimal expansion, or false when it has none.
//
// The test is on the *reduced* denominator, because big.Rat normalises: an Avro decimal read as
// 1234/100 arrives as 617/50, so looking for a literal power of ten found almost nothing. A
// fraction in lowest terms terminates exactly when its denominator is 2^a * 5^b, and the scale
// is then max(a, b).
func terminatingScale(den *big.Int) (int32, bool) {
	if den.Sign() <= 0 {
		return 0, false
	}
	cur := new(big.Int).Set(den)
	rem := new(big.Int)
	var twos, fives int32
	for _, f := range []struct {
		p *big.Int
		n *int32
	}{{big.NewInt(2), &twos}, {big.NewInt(5), &fives}} {
		for {
			q := new(big.Int)
			q.QuoRem(cur, f.p, rem)
			if rem.Sign() != 0 {
				break
			}
			cur = q
			*f.n++
			if *f.n < 0 { // int32 wrap on an absurd denominator
				return 0, false
			}
		}
	}
	if cur.Cmp(big.NewInt(1)) != 0 {
		return 0, false // a factor other than 2 or 5 remains: non-terminating
	}
	if fives > twos {
		return fives, true
	}
	return twos, true
}

// decimalFromCoefficient builds an apd.Decimal from an unscaled integer and a scale, with no
// intermediate text. apd stores a magnitude plus a sign, and an int32 exponent.
// decimalFromRatAtScale is decimalFromRat with the field's declared scale supplied rather than
// derived from the value. big.Rat normalises, so 12.3400 at scale 4 arrives as 617/50 and only
// the schema still knows the trailing zeros the reference renders.
//
// Falls back to the derived scale when the declared one cannot hold the value exactly, or is
// wide enough to be a denial of service on its own - the same bound the write side uses.
func decimalFromRatAtScale(r *big.Rat, scale int) (*apd.Decimal, error) {
	if scale < 0 || scale > maxAvroDecimalWidth {
		return decimalFromRat(r)
	}
	coeff := new(big.Int).Mul(r.Num(), new(big.Int).Exp(big.NewInt(10),
		big.NewInt(int64(scale)), nil))
	if new(big.Int).Rem(coeff, r.Denom()).Sign() != 0 {
		return decimalFromRat(r)
	}
	return decimalFromCoefficient(coeff.Quo(coeff, r.Denom()), int32(scale))
}

func decimalFromCoefficient(unscaled *big.Int, scale int32) (*apd.Decimal, error) {
	// exponent = -scale, computed in int64 because -math.MinInt32 does not fit an int32: it
	// wrapped back to math.MinInt32 and reached strings.Repeat with a negative count.
	exponent := -int64(scale)
	if exponent < math.MinInt32 || exponent > math.MaxInt32 {
		return nil, fmt.Errorf("decimal scale %d cannot be represented", scale)
	}
	res := new(apd.Decimal)
	res.Negative = unscaled.Sign() < 0
	res.Coeff.SetMathBigInt(new(big.Int).Abs(unscaled))
	res.Exponent = int32(exponent)
	res.Form = apd.Finite
	return res, nil
}

// decimalFromBytesScale builds an apd.Decimal from raw two's-complement bytes plus a scale.
//
// Built from the coefficient and exponent rather than from plainDecimalString: that rendering
// materialises every digit of the positional form, so an accepted int32 scale could ask for a
// multi-gigabyte string (scale -2147483647 produced 2147483648 bytes) or panic outright
// (scale math.MinInt32 reached strings.Repeat with a negative count, surfacing as cel-go's
// "internal error: strings: negative Repeat count" rather than a rule error). This way the
// exponent range is apd's to enforce, which is what the decimals design delegates to it.
func decimalFromBytesScale(b []byte, scale int32) (*apd.Decimal, error) {
	return decimalFromCoefficient(bigIntFromSignedBytes(b), scale)
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
		// A negative value's magnitude is Not(i) == -i-1, so its bit length is one less at every
		// exact signed boundary. Sizing from i.BitLen() emitted ff80 for -128 where
		// BigInteger.toByteArray gives 80.
		bits := new(big.Int).Not(i).BitLen() + 1
		byteLen := (bits + 7) / 8
		if byteLen < 1 {
			byteLen = 1
		}
		shifted := new(big.Int).Add(i, new(big.Int).Lsh(big.NewInt(1), uint(byteLen)*8))
		out := make([]byte, byteLen)
		shifted.FillBytes(out)
		return out
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
