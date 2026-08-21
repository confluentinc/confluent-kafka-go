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
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	prototypes "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/variant"
)

// variantTypeName is the cross-language CEL type label for a Variant. Every client uses the
// same string; the backing type is this client's implementation detail.
const variantTypeName = "confluent.type.Variant"

// variantType is the opaque CEL type used in function declarations. cel-go supports opaque
// types directly, so a Variant is a first-class opaque value (mirroring decimal).
var variantType = cel.OpaqueType(variantTypeName)

// variantVal is the CEL runtime value for a Variant, wrapping the codec type.
type variantVal struct {
	v variant.Variant
}

func newVariant(v variant.Variant) variantVal { return variantVal{v: v} }

func (val variantVal) ConvertToNative(typeDesc reflect.Type) (any, error) {
	if typeDesc == reflect.TypeOf(variant.Variant{}) || typeDesc.Kind() == reflect.Interface {
		return val.v, nil
	}
	if typeDesc.Kind() == reflect.String {
		s, err := val.v.ToJSON()
		if err != nil {
			return nil, err
		}
		return s, nil
	}
	return nil, fmt.Errorf("type conversion error from '%s' to '%v'", variantTypeName, typeDesc)
}

func (val variantVal) ConvertToType(typeValue ref.Type) ref.Val {
	switch typeValue.TypeName() {
	case "string":
		s, err := val.v.ToJSON()
		if err != nil {
			return types.NewErr("variant: %v", err)
		}
		return types.String(s)
	case "type":
		return variantType
	case variantTypeName:
		return val
	}
	return types.NewErr("type conversion error from '%s' to '%s'", variantTypeName, typeValue.TypeName())
}

func (val variantVal) Equal(other ref.Val) ref.Val {
	o, ok := other.(variantVal)
	if !ok {
		return types.False
	}
	return types.Bool(bytes.Equal(val.v.StandaloneValueBytes(), o.v.StandaloneValueBytes()) &&
		bytes.Equal(val.v.MetadataBytes(), o.v.MetadataBytes()))
}

func (val variantVal) Type() ref.Type { return variantType }

func (val variantVal) Value() any { return val.v }

// variantTypeLabel is the coarse label variants.type returns: integer widths collapse to
// "int", float/double to "double", decimal widths to "decimal", all timestamp variants to
// "timestamp".
func variantTypeLabel(t variant.Type) string {
	switch t {
	case variant.Object:
		return "object"
	case variant.Array:
		return "array"
	case variant.Null:
		return "null"
	case variant.Boolean:
		return "boolean"
	case variant.Byte, variant.Short, variant.Int, variant.Long:
		return "int"
	case variant.Float, variant.Double:
		return "double"
	case variant.Decimal4, variant.Decimal8, variant.Decimal16:
		return "decimal"
	case variant.Date:
		return "date"
	case variant.Time:
		return "time"
	case variant.TimestampTz, variant.TimestampNtz, variant.TimestampNanosTz, variant.TimestampNanosNtz:
		return "timestamp"
	case variant.String:
		return "string"
	case variant.Binary:
		return "bytes"
	case variant.Uuid:
		return "uuid"
	}
	return "unknown"
}

// asVariantFromValue converts the shapes an Avro/Protobuf decoder produces into a Variant:
// a codec Variant, a confluent.type.Variant proto message, or a map with metadata/value
// byte entries. Returns (zero, false) if the value isn't a variant.
func asVariantFromValue(v ref.Val) (variant.Variant, bool) {
	if vv, ok := v.(variantVal); ok {
		return vv.v, true
	}
	switch x := v.Value().(type) {
	case variant.Variant:
		return x, true
	case *prototypes.Variant:
		return variant.New(x.Value, x.Metadata), true
	case map[string]interface{}:
		md, mok := x["metadata"].([]byte)
		val, vok := x["value"].([]byte)
		if mok && vok {
			return variant.New(val, md), true
		}
	}
	return variant.Variant{}, false
}

// receiverVariant is a variants.* navigation receiver: CEL null passes through (returns
// types.NullValue as the second result), a variant is returned, anything else is a hard
// error. The caller returns the second result directly when it is non-nil.
func receiverVariant(v ref.Val, fn string) (variant.Variant, ref.Val) {
	if v == nil || v.Type() == types.NullType {
		return variant.Variant{}, types.NullValue
	}
	if vv, ok := asVariantFromValue(v); ok {
		return vv, nil
	}
	return variant.Variant{}, types.NewErr("%s: expected a Variant, got %s", fn, v.Type().TypeName())
}

// toVariant is the runtime dispatch backing variant(dyn). Rejects strings (use parseJson)
// and null.
func toVariant(v ref.Val) ref.Val {
	if vv, ok := v.(variantVal); ok {
		return vv
	}
	switch x := v.Value().(type) {
	case variant.Variant:
		return newVariant(x)
	case *prototypes.Variant:
		return newVariant(variant.New(x.Value, x.Metadata))
	case map[string]interface{}:
		md, mok := x["metadata"].([]byte)
		val, vok := x["value"].([]byte)
		if mok && vok {
			return newVariant(variant.New(val, md))
		}
		return types.NewErr("variant: map missing 'metadata'/'value' byte entries")
	case string:
		return types.NewErr("variant: cannot convert string to Variant; use variants.parseJson(s)")
	}
	if v == nil || v.Type() == types.NullType {
		return types.NewErr("variant: cannot convert null to Variant")
	}
	return types.NewErr("variant: cannot convert %s to Variant", v.Type().TypeName())
}

// variantAs backs variants.as (strict) and variants.tryAs (soft). On a type mismatch the
// strict form errors and the soft form returns CEL null; types with no CEL scalar
// extraction (object/array/null/date/time/uuid) always error.
func variantAs(a, b ref.Val, nullOnError bool) ref.Val {
	vv, status := receiverVariant(a, "variants.as")
	if status != nil {
		return status
	}
	t, _ := b.Value().(string)
	vt := vv.GetType()
	switch t {
	case "string":
		if vt == variant.String {
			s, err := vv.GetString()
			if err != nil {
				return types.NewErr("variants.as: %v", err)
			}
			return types.String(s)
		}
	case "int":
		if vt == variant.Byte || vt == variant.Short || vt == variant.Int || vt == variant.Long {
			n, err := vv.GetLong()
			if err != nil {
				return types.NewErr("variants.as: %v", err)
			}
			return types.Int(n)
		}
	case "double":
		if vt == variant.Float || vt == variant.Double {
			d, err := vv.GetDouble()
			if err != nil {
				return types.NewErr("variants.as: %v", err)
			}
			return types.Double(d)
		}
	case "boolean":
		if vt == variant.Boolean {
			bl, err := vv.GetBoolean()
			if err != nil {
				return types.NewErr("variants.as: %v", err)
			}
			return types.Bool(bl)
		}
	case "decimal":
		if vt == variant.Decimal4 || vt == variant.Decimal8 || vt == variant.Decimal16 {
			parts, scale, err := vv.GetDecimalParts()
			if err != nil {
				return types.NewErr("variants.as: %v", err)
			}
			d, err := decimalFromBytesScale(parts, int32(scale))
			if err != nil {
				return types.NewErr("variants.as: %v", err)
			}
			return newDecimal(d)
		}
	case "timestamp":
		if vt == variant.TimestampTz || vt == variant.TimestampNtz ||
			vt == variant.TimestampNanosTz || vt == variant.TimestampNanosNtz {
			raw, err := vv.GetLong()
			if err != nil {
				return types.NewErr("variants.as: %v", err)
			}
			micros := raw
			if vt == variant.TimestampNanosTz || vt == variant.TimestampNanosNtz {
				micros = raw / 1000
			}
			return types.Timestamp{Time: time.UnixMicro(micros).UTC()}
		}
	case "bytes":
		if vt == variant.Binary {
			bin, err := vv.GetBinary()
			if err != nil {
				return types.NewErr("variants.as: %v", err)
			}
			return types.Bytes(bin)
		}
	case "object", "array", "null", "date", "time", "uuid":
		return types.NewErr("variants.as: type '%s' is not supported for extraction "+
			"(use variants.type/variants.path/variants.field/variants.index instead)", t)
	default:
		if nullOnError {
			return types.NullValue
		}
		return types.NewErr("variants.as: unknown type '%s' (expected one of: string, int, "+
			"double, boolean, decimal, timestamp, bytes)", t)
	}
	// Recognized type string, but the variant's actual type does not match.
	if nullOnError {
		return types.NullValue
	}
	return types.NewErr("variants.as: variant is not %s-typed", t)
}

// variantOptions returns the cel.EnvOption declarations for the variant(...) constructor and
// the variants.* accessor family, mirroring the Java/Python/JS/C#/C++ clients. Navigation
// functions take a dyn receiver and return dyn so a CEL-null (a miss) passes through and
// `... == null` type-checks.
func variantOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("variant",
			cel.Overload("dyn_to_variant", []*cel.Type{cel.DynType}, variantType,
				cel.UnaryBinding(toVariant)),
			cel.Overload("bytes_bytes_to_variant", []*cel.Type{cel.BytesType, cel.BytesType}, variantType,
				cel.BinaryBinding(func(a, b ref.Val) ref.Val {
					value, ok := a.Value().([]byte)
					if !ok {
						return types.NewErr("variant: first argument must be bytes")
					}
					md, ok := b.Value().([]byte)
					if !ok {
						return types.NewErr("variant: second argument must be bytes")
					}
					return newVariant(variant.New(value, md))
				})),
		),

		cel.Function("variants.parseJson",
			cel.Overload("variants_parse_json", []*cel.Type{cel.StringType}, variantType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					s, ok := v.Value().(string)
					if !ok {
						return types.NewErr("variants.parseJson: expected a string")
					}
					pv, err := variant.ParseJSON(s)
					if err != nil {
						return types.NewErr("variants.parseJson: %v", err)
					}
					return newVariant(pv)
				}))),

		cel.Function("variants.tryParseJson",
			cel.Overload("variants_try_parse_json", []*cel.Type{cel.StringType}, cel.DynType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					s, ok := v.Value().(string)
					if !ok {
						return types.NullValue
					}
					pv, err := variant.ParseJSON(s)
					if err != nil {
						return types.NullValue
					}
					return newVariant(pv)
				}))),

		cel.Function("variants.type",
			cel.Overload("variants_type", []*cel.Type{cel.DynType}, cel.StringType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					vv, status := receiverVariant(v, "variants.type")
					if status != nil {
						return status
					}
					return types.String(variantTypeLabel(vv.GetType()))
				}))),

		cel.Function("variants.isNull",
			cel.Overload("variants_is_null", []*cel.Type{cel.DynType}, cel.BoolType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					vv, ok := asVariantFromValue(v)
					return types.Bool(ok && vv.GetType() == variant.Null)
				}))),

		cel.Function("variants.path",
			cel.Overload("variants_path", []*cel.Type{cel.DynType, cel.StringType}, cel.DynType,
				cel.BinaryBinding(func(a, b ref.Val) ref.Val {
					vv, status := receiverVariant(a, "variants.path")
					if status != nil {
						return status
					}
					path, _ := b.Value().(string)
					r, err := walkVariantPath(vv, path)
					if err != nil {
						return types.NewErr("variants.path: %v", err)
					}
					if r == nil {
						return types.NullValue
					}
					return newVariant(*r)
				}))),

		cel.Function("variants.field",
			cel.Overload("variants_field", []*cel.Type{cel.DynType, cel.StringType}, cel.DynType,
				cel.BinaryBinding(func(a, b ref.Val) ref.Val {
					vv, status := receiverVariant(a, "variants.field")
					if status != nil {
						return status
					}
					if vv.GetType() != variant.Object {
						return types.NullValue
					}
					key, _ := b.Value().(string)
					r := vv.GetFieldByKey(key)
					if r == nil {
						return types.NullValue
					}
					return newVariant(*r)
				}))),

		cel.Function("variants.index",
			cel.Overload("variants_index", []*cel.Type{cel.DynType, cel.IntType}, cel.DynType,
				cel.BinaryBinding(func(a, b ref.Val) ref.Val {
					vv, status := receiverVariant(a, "variants.index")
					if status != nil {
						return status
					}
					if vv.GetType() != variant.Array {
						return types.NullValue
					}
					idx, _ := b.Value().(int64)
					if idx < 0 || idx > math.MaxInt32 {
						return types.NullValue
					}
					r := vv.GetElementAtIndex(int(idx))
					if r == nil {
						return types.NullValue
					}
					return newVariant(*r)
				}))),

		cel.Function("variants.as",
			cel.Overload("variants_as", []*cel.Type{cel.DynType, cel.StringType}, cel.DynType,
				cel.BinaryBinding(func(a, b ref.Val) ref.Val {
					return variantAs(a, b, false)
				}))),

		cel.Function("variants.tryAs",
			cel.Overload("variants_try_as", []*cel.Type{cel.DynType, cel.StringType}, cel.DynType,
				cel.BinaryBinding(func(a, b ref.Val) ref.Val {
					return variantAs(a, b, true)
				}))),

		cel.Function("variants.toJson",
			cel.Overload("variants_to_json", []*cel.Type{cel.DynType}, cel.StringType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					vv, status := receiverVariant(v, "variants.toJson")
					if status != nil {
						return status
					}
					s, err := vv.ToJSON()
					if err != nil {
						return types.NewErr("variants.toJson: %v", err)
					}
					return types.String(s)
				}))),
	}
}
