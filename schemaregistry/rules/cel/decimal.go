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
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/apd/v3"
	prototypes "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// decimalTypeName is the cross-language CEL type label for a decimal. Every client
// (Java/JS/Python/Rust/C#/Go) uses this exact string; the backing value here is an
// apd.Decimal, mirroring Java's BigDecimal.
const decimalTypeName = "confluent.type.Decimal"

// decimalType is the opaque CEL type used in function declarations. cel-go supports opaque
// types natively (unlike @bufbuild/cel or cel.net), so this mirrors the Java client's
// OpaqueType approach directly.
var decimalType = cel.OpaqueType(decimalTypeName)

// divContext rounds division and square root to 38 significant digits, HALF_UP — matching
// Java's MathContext(38, HALF_UP) used across the other clients. exactContext (Precision 0)
// performs unrounded add/sub/mul/remainder, matching BigDecimal's exact operators.
var (
	divContext   = &apd.Context{Precision: 38, Rounding: apd.RoundHalfUp, MaxExponent: apd.MaxExponent, MinExponent: apd.MinExponent}
	exactContext = &apd.Context{Precision: 0, Rounding: apd.RoundHalfUp, MaxExponent: apd.MaxExponent, MinExponent: apd.MinExponent}
)

// decimalVal is the CEL runtime value for a decimal, wrapping an apd.Decimal.
type decimalVal struct {
	d *apd.Decimal
}

func newDecimal(d *apd.Decimal) decimalVal { return decimalVal{d: d} }

func (v decimalVal) ConvertToNative(typeDesc reflect.Type) (any, error) {
	switch typeDesc.Kind() {
	case reflect.String:
		return v.d.Text('f'), nil
	case reflect.Interface:
		return v.d, nil
	}
	if typeDesc == reflect.TypeOf((*apd.Decimal)(nil)) {
		return v.d, nil
	}
	return nil, fmt.Errorf("type conversion error from '%s' to '%v'", decimalTypeName, typeDesc)
}

func (v decimalVal) ConvertToType(typeValue ref.Type) ref.Val {
	switch typeValue.TypeName() {
	case "string":
		return types.String(v.d.Text('f'))
	case "double":
		f, _ := v.d.Float64()
		return types.Double(f)
	case "type":
		return decimalType
	case decimalTypeName:
		return v
	}
	return types.NewErr("type conversion error from '%s' to '%s'", decimalTypeName, typeValue.TypeName())
}

func (v decimalVal) Equal(other ref.Val) ref.Val {
	o, ok := other.(decimalVal)
	if !ok {
		return types.False
	}
	return types.Bool(v.d.Cmp(o.d) == 0)
}

func (v decimalVal) Type() ref.Type { return decimalType }

func (v decimalVal) Value() any { return v.d }

// asDecimal unwraps an argument the checker has already typed as a Decimal.
func asDecimal(v ref.Val) (*apd.Decimal, ref.Val) {
	d, ok := v.(decimalVal)
	if !ok {
		return nil, types.NewErr("expected a decimal, got %s", v.Type().TypeName())
	}
	return d.d, nil
}

// toDecimal is the runtime dispatch backing decimal(dyn). It accepts whatever shape an
// Avro (hamba *big.Rat) or Protobuf (confluent.type.Decimal, numbers) decoder produces.
func toDecimal(v ref.Val) ref.Val {
	if d, ok := v.(decimalVal); ok {
		return d
	}
	switch x := v.Value().(type) {
	case *apd.Decimal:
		return newDecimal(x)
	case *big.Rat:
		// Avro's decimal logical type decodes to *big.Rat, which carries no scale, so the
		// exact terminating decimal is recovered by dividing at full precision. (Trailing-zero
		// scale is not preserved — Go matches the JS client here.)
		d, err := decimalFromRat(x)
		if err != nil {
			return types.NewErr("decimal: %v", err)
		}
		return newDecimal(d)
	case *prototypes.Decimal:
		d, err := decimalFromProto(x)
		if err != nil {
			return types.NewErr("decimal: %v", err)
		}
		return newDecimal(d)
	case int64:
		return newDecimal(apd.New(x, 0))
	case uint64:
		d, _, err := apd.NewFromString(strconv.FormatUint(x, 10))
		if err != nil {
			return types.NewErr("decimal: %v", err)
		}
		return newDecimal(d)
	case float64:
		// Shortest round-tripping form, matching Java's BigDecimal.valueOf(double).
		d, _, err := apd.NewFromString(strconv.FormatFloat(x, 'g', -1, 64))
		if err != nil {
			return types.NewErr("decimal: %v", err)
		}
		return newDecimal(d)
	case string:
		d, _, err := apd.NewFromString(x)
		if err != nil {
			return types.NewErr("decimal: cannot parse %q: %v", x, err)
		}
		return newDecimal(d)
	case []byte:
		return types.NewErr("decimal: raw bytes need a scale; use decimal(bytes, scale)")
	default:
		return types.NewErr("decimal: cannot convert %s to Decimal", v.Type().TypeName())
	}
}

// decimalOptions returns the cel.EnvOption declarations for the decimal(...) constructor and
// the decimals.* operator family, mirroring the Java/JS/Python/Rust/C# clients.
func decimalOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("decimal",
			cel.Overload("dyn_to_decimal", []*cel.Type{cel.DynType}, decimalType,
				cel.UnaryBinding(toDecimal)),
			cel.Overload("bytes_int_to_decimal", []*cel.Type{cel.BytesType, cel.IntType}, decimalType,
				cel.BinaryBinding(func(b, s ref.Val) ref.Val {
					bytes, ok := b.Value().([]byte)
					if !ok {
						return types.NewErr("decimal: first argument must be bytes")
					}
					scale, ok := s.Value().(int64)
					if !ok {
						return types.NewErr("decimal: scale must be an int")
					}
					sc, errv := requireIntScale(scale, "decimal(bytes, scale)")
					if errv != nil {
						return errv
					}
					d, err := decimalFromBytesScale(bytes, sc)
					if err != nil {
						return types.NewErr("decimal: %v", err)
					}
					return newDecimal(d)
				})),
		),

		decimalsCompare("decimals.eq", func(c int) bool { return c == 0 }),
		decimalsCompare("decimals.lt", func(c int) bool { return c < 0 }),
		decimalsCompare("decimals.le", func(c int) bool { return c <= 0 }),
		decimalsCompare("decimals.gt", func(c int) bool { return c > 0 }),
		decimalsCompare("decimals.ge", func(c int) bool { return c >= 0 }),

		decimalsArith("decimals.add", func(res, a, b *apd.Decimal) error { _, err := exactContext.Add(res, a, b); return err }),
		decimalsArith("decimals.sub", func(res, a, b *apd.Decimal) error { _, err := exactContext.Sub(res, a, b); return err }),
		decimalsArith("decimals.mul", func(res, a, b *apd.Decimal) error { _, err := exactContext.Mul(res, a, b); return err }),
		decimalsArith("decimals.div", func(res, a, b *apd.Decimal) error {
			if b.Sign() == 0 {
				return fmt.Errorf("division by zero")
			}
			cond, err := divContext.Quo(res, a, b)
			if err != nil {
				return err
			}
			// An exact quotient adopts Java BigDecimal.divide's preferred scale
			// (dividend.scale - divisor.scale), so 6.0/3 -> "2.0" and 10.00/2 -> "5.00";
			// a non-terminating one keeps all 38 significant digits.
			if !cond.Inexact() {
				return applyPreferredScale(res, a.Exponent-b.Exponent)
			}
			return nil
		}),
		decimalsArith("decimals.mod", func(res, a, b *apd.Decimal) error {
			if b.Sign() == 0 {
				return fmt.Errorf("division by zero")
			}
			// Rem requires a nonzero precision; the remainder itself is exact.
			_, err := divContext.Rem(res, a, b)
			return err
		}),
		decimalsArith("decimals.greatest", func(res, a, b *apd.Decimal) error {
			if a.Cmp(b) >= 0 {
				res.Set(a)
			} else {
				res.Set(b)
			}
			return nil
		}),
		decimalsArith("decimals.least", func(res, a, b *apd.Decimal) error {
			if a.Cmp(b) <= 0 {
				res.Set(a)
			} else {
				res.Set(b)
			}
			return nil
		}),

		decimalsUnary("decimals.sqrt", func(res, a *apd.Decimal) error {
			if a.Sign() < 0 {
				return fmt.Errorf("square root of negative number")
			}
			cond, err := divContext.Sqrt(res, a)
			if err != nil {
				return err
			}
			// An exact root adopts Java BigDecimal.sqrt's preferred scale (this.scale/2),
			// so sqrt(4.00) -> "2.0" and sqrt(100.0000) -> "10.00"; a non-terminating one
			// keeps all 38 significant digits.
			if !cond.Inexact() {
				return applyPreferredScale(res, -((-a.Exponent) / 2))
			}
			return nil
		}),
		decimalsUnary("decimals.neg", func(res, a *apd.Decimal) error { res.Neg(a); return nil }),
		decimalsUnary("decimals.abs", func(res, a *apd.Decimal) error { res.Abs(a); return nil }),
		decimalsUnary("decimals.floor", func(res, a *apd.Decimal) error { return quantize(res, a, 0, apd.RoundFloor) }),
		decimalsUnary("decimals.ceil", func(res, a *apd.Decimal) error { return quantize(res, a, 0, apd.RoundCeiling) }),

		cel.Function("decimals.sign",
			cel.Overload("decimals_sign_decimal", []*cel.Type{decimalType}, cel.IntType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					d, err := asDecimal(v)
					if err != nil {
						return err
					}
					return types.Int(d.Sign())
				})),
		),

		cel.Function("decimals.round",
			cel.Overload("decimals_round_unary", []*cel.Type{decimalType}, decimalType,
				cel.UnaryBinding(roundBinding(0))),
			cel.Overload("decimals_round_scale", []*cel.Type{decimalType, cel.IntType}, decimalType,
				cel.BinaryBinding(roundScaleBinding(apd.RoundHalfUp))),
		),
		cel.Function("decimals.trunc",
			cel.Overload("decimals_trunc_unary", []*cel.Type{decimalType}, decimalType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					d, errv := asDecimal(v)
					if errv != nil {
						return errv
					}
					return truncTo(d, 0)
				})),
			cel.Overload("decimals_trunc_scale", []*cel.Type{decimalType, cel.IntType}, decimalType,
				cel.BinaryBinding(func(a, s ref.Val) ref.Val {
					d, errv := asDecimal(a)
					if errv != nil {
						return errv
					}
					scale, ok := s.Value().(int64)
					if !ok {
						return types.NewErr("decimals.trunc: scale must be an int")
					}
					sc, errv := requireIntScale(scale, "decimals.trunc")
					if errv != nil {
						return errv
					}
					return truncTo(d, sc)
				})),
		),

		// string(Decimal) and double(Decimal) extend the stdlib conversions; cel-go merges
		// these overloads with the built-in string/double functions.
		cel.Function("string",
			cel.Overload("decimal_to_string", []*cel.Type{decimalType}, cel.StringType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					d, err := asDecimal(v)
					if err != nil {
						return err
					}
					return types.String(d.Text('f'))
				})),
		),
		cel.Function("double",
			cel.Overload("decimal_to_double", []*cel.Type{decimalType}, cel.DoubleType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					d, err := asDecimal(v)
					if err != nil {
						return err
					}
					f, _ := d.Float64()
					return types.Double(f)
				})),
		),
	}
}

func decimalsCompare(name string, cmp func(int) bool) cel.EnvOption {
	return cel.Function(name,
		cel.Overload(strings.ReplaceAll(name, ".", "_")+"_decimal_decimal",
			[]*cel.Type{decimalType, decimalType}, cel.BoolType,
			cel.BinaryBinding(func(a, b ref.Val) ref.Val {
				x, errv := asDecimal(a)
				if errv != nil {
					return errv
				}
				y, errv := asDecimal(b)
				if errv != nil {
					return errv
				}
				return types.Bool(cmp(x.Cmp(y)))
			})),
	)
}

func decimalsArith(name string, fn func(res, a, b *apd.Decimal) error) cel.EnvOption {
	return cel.Function(name,
		cel.Overload(strings.ReplaceAll(name, ".", "_")+"_decimal_decimal",
			[]*cel.Type{decimalType, decimalType}, decimalType,
			cel.BinaryBinding(func(a, b ref.Val) ref.Val {
				x, errv := asDecimal(a)
				if errv != nil {
					return errv
				}
				y, errv := asDecimal(b)
				if errv != nil {
					return errv
				}
				res := new(apd.Decimal)
				if err := fn(res, x, y); err != nil {
					return types.NewErr("%s: %v", name, err)
				}
				return newDecimal(res)
			})),
	)
}

func decimalsUnary(name string, fn func(res, a *apd.Decimal) error) cel.EnvOption {
	return cel.Function(name,
		cel.Overload(strings.ReplaceAll(name, ".", "_")+"_decimal",
			[]*cel.Type{decimalType}, decimalType,
			cel.UnaryBinding(func(v ref.Val) ref.Val {
				d, errv := asDecimal(v)
				if errv != nil {
					return errv
				}
				res := new(apd.Decimal)
				if err := fn(res, d); err != nil {
					return types.NewErr("%s: %v", name, err)
				}
				return newDecimal(res)
			})),
	)
}

func roundBinding(scale int32) functionUnary {
	return func(v ref.Val) ref.Val {
		d, errv := asDecimal(v)
		if errv != nil {
			return errv
		}
		res := new(apd.Decimal)
		if err := quantize(res, d, scale, apd.RoundHalfUp); err != nil {
			return types.NewErr("decimals.round: %v", err)
		}
		return newDecimal(res)
	}
}

func roundScaleBinding(rounder apd.Rounder) functionBinary {
	return func(a, s ref.Val) ref.Val {
		d, errv := asDecimal(a)
		if errv != nil {
			return errv
		}
		scale, ok := s.Value().(int64)
		if !ok {
			return types.NewErr("decimals.round: scale must be an int")
		}
		sc, errv := requireIntScale(scale, "decimals.round")
		if errv != nil {
			return errv
		}
		res := new(apd.Decimal)
		if err := quantize(res, d, sc, rounder); err != nil {
			return types.NewErr("decimals.round: %v", err)
		}
		return newDecimal(res)
	}
}

type functionUnary = func(ref.Val) ref.Val
type functionBinary = func(ref.Val, ref.Val) ref.Val

// truncTo mirrors Flink TRUNCATE: a no-op when the target scale is at or finer than the
// current scale, otherwise round toward zero.
func truncTo(d *apd.Decimal, scale int32) ref.Val {
	currentScale := -d.Exponent
	if scale >= currentScale {
		return newDecimal(new(apd.Decimal).Set(d))
	}
	res := new(apd.Decimal)
	if err := quantize(res, d, scale, apd.RoundDown); err != nil {
		return types.NewErr("decimals.trunc: %v", err)
	}
	return newDecimal(res)
}

// quantize sets res to d rounded to the given scale (number of fractional digits) with the
// given rounder — the apd analog of BigDecimal.setScale(scale, mode).
func quantize(res, d *apd.Decimal, scale int32, rounder apd.Rounder) error {
	ctx := &apd.Context{Precision: divContext.Precision, Rounding: rounder, MaxExponent: apd.MaxExponent, MinExponent: apd.MinExponent}
	_, err := ctx.Quantize(res, d, -scale)
	return err
}

// applyPreferredScale rewrites an exact div/sqrt result to Java BigDecimal's preferred
// scale: strip to the minimal representation, then, when that is coarser than the preferred
// exponent, pad with trailing zeros back out to it. apd's Quo/Sqrt otherwise pad exact
// results out to the full 38-digit precision, and a bare Reduce would over-strip (6.0/3 ->
// "2" instead of Java's "2.0"). The preferred exponent is dividend.exp - divisor.exp for
// division and -(this.scale/2) for square root, mirroring BigDecimal.
func applyPreferredScale(res *apd.Decimal, preferredExp int32) error {
	res.Reduce(res)
	if res.Exponent > preferredExp {
		ctx := &apd.Context{Precision: divContext.Precision, Rounding: apd.RoundHalfUp, MaxExponent: apd.MaxExponent, MinExponent: apd.MinExponent}
		if _, err := ctx.Quantize(res, res, preferredExp); err != nil {
			return err
		}
	}
	return nil
}

// requireIntScale narrows a CEL int (i64) scale argument to the i32 that apd (and Java's
// BigDecimal) scales use. It returns a CEL error when the value does not fit rather than
// silently taking the low 32 bits (e.g. 2^32 -> 0), mirroring Java's requireIntScale
// (Math.toIntExact).
func requireIntScale(scale int64, fn string) (int32, ref.Val) {
	if scale < math.MinInt32 || scale > math.MaxInt32 {
		return 0, types.NewErr("%s: scale out of int range: %d", fn, scale)
	}
	return int32(scale), nil
}
