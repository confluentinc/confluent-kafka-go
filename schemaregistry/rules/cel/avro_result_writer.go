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
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/variant"
)

// writeBackAvro shapes a CEL result into what hamba's Avro encoder accepts.
//
// Two separate problems, one fix:
//
//   - cel-go's native conversion produces map[interface{}]interface{}, while hamba writes a
//     record from map[string]interface{};
//   - the CEL value types have no Avro encoding of their own. A decimal is an *apd.Decimal,
//     which hamba rejects outright ("avro: *apd.Decimal is unsupported for Avro bytes") - so a
//     decimal field broke serialization for *any* rule touching it, including an identity one
//     . A variant is a variant.Variant, which hamba has never seen.
//
// hamba applies the field's own scale when encoding a *big.Rat, and an Avro variant is just a
// record of two bytes fields, so the decimal and variant shapes need no schema. The numeric
// widths do: CEL has one integer type and one floating one, so an int or float field receives
// an int64 or a float64 and hamba refuses both ("int64 is unsupported for Avro int"). The
// reference narrows against the schema in narrowToInt and narrowToFloat, so schema is threaded
// through here for the same purpose. A nil schema simply skips the narrowing.
func writeBackAvro(schema avro.Schema, result interface{}) (interface{}, error) {
	return avroValue(schema, result)
}

func avroValue(schema avro.Schema, value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(v))
		for k, item := range v {
			key, ok := k.(string)
			if !ok {
				// A non-string key cannot name an Avro record field or a map key; leaving
				// the value untouched is safer than guessing at a conversion.
				return value, nil
			}
			converted, err := avroValue(childSchema(schema, key), item)
			if err != nil {
				return nil, err
			}
			out[key] = converted
		}
		return out, nil
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for k, item := range v {
			converted, err := avroValue(childSchema(schema, k), item)
			if err != nil {
				return nil, err
			}
			out[k] = converted
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, 0, len(v))
		for _, item := range v {
			converted, err := avroValue(itemSchema(schema), item)
			if err != nil {
				return nil, err
			}
			out = append(out, converted)
		}
		return out, nil
	case *apd.Decimal:
		// hamba encodes a decimal logical type from *big.Rat, applying the schema's scale.
		return ratFromDecimal(v)
	case variant.Variant:
		// An Avro variant is a record of two bytes fields; hamba writes it from this map.
		return map[string]interface{}{
			"metadata": v.MetadataBytes(),
			// Slice from this node's offset, not from 0: a Variant from
			// variants.field/path/index is a view, and ValueBytes would write the
			// entire source variant. Trailing sibling bytes are kept deliberately -
			// the Java reference emits ByteBuffer position..limit, so this matches
			// it byte for byte.
			"value": v.StandaloneValueBytes(),
		}, nil
	case time.Time:
		// hamba encodes the timestamp logical types straight from time.Time.
		return v, nil
	case structpb.NullValue:
		// CEL null. hamba writes an Avro union's null branch from a plain nil; cel-go's own
		// representation is structpb.NullValue, a protobuf enum, which means nothing to it -
		// so a rule returning null for a field, or echoing one that was already null, failed
		// with "avro: unable to resolve type structpb.NullValue". That hit the two forms a
		// rule author is most likely to write: an identity pass-through over a nullable field,
		// and the `has(x) ? x : null` guard that is the only way to preserve absence.
		return nil, nil
	case int64:
		return narrowAvroInt(schema, v)
	case uint64:
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("value %d is out of range for an Avro long field", v)
		}
		return narrowAvroInt(schema, int64(v))
	case float64:
		if numericBranch(schema) == avro.Float {
			return float32(v), nil
		}
		return v, nil
	default:
		return value, nil
	}
}

// childSchema is the schema of a record field or map value named key, or nil when the schema
// does not describe one.
func childSchema(schema avro.Schema, key string) avro.Schema {
	switch s := resolveAvroSchema(schema).(type) {
	case *avro.RecordSchema:
		for _, field := range s.Fields() {
			if field.Name() == key {
				return field.Type()
			}
		}
	case *avro.MapSchema:
		return s.Values()
	}
	return nil
}

// itemSchema is the schema of an array element, or nil when the schema does not describe one.
func itemSchema(schema avro.Schema) avro.Schema {
	if s, ok := resolveAvroSchema(schema).(*avro.ArraySchema); ok {
		return s.Items()
	}
	return nil
}

func resolveAvroSchema(schema avro.Schema) avro.Schema {
	if ref, ok := schema.(*avro.RefSchema); ok {
		return ref.Schema()
	}
	return schema
}

// numericBranch is the Avro numeric type an integer or floating result will be written as, or
// Null when the schema names none. A union is resolved by value the way the reference's
// branchAccepts does, since a union is transparent in CEL and hamba picks the branch from the
// Go type it is handed.
func numericBranch(schema avro.Schema, value ...int64) avro.Type {
	resolved := resolveAvroSchema(schema)
	if resolved == nil {
		return avro.Null
	}
	if union, ok := resolved.(*avro.UnionSchema); ok {
		for _, branch := range union.Types() {
			switch t := numericBranch(branch, value...); t {
			case avro.Int:
				if len(value) == 0 || (value[0] >= math.MinInt32 && value[0] <= math.MaxInt32) {
					return t
				}
			case avro.Long, avro.Float, avro.Double:
				return t
			}
		}
		return avro.Null
	}
	switch resolved.Type() {
	case avro.Int, avro.Long, avro.Float, avro.Double:
		return resolved.Type()
	}
	return avro.Null
}

// narrowAvroInt is the reference's narrowToInt: an int field takes an integer that fits in one
// and refuses anything else, rather than truncating it into the slot.
func narrowAvroInt(schema avro.Schema, v int64) (interface{}, error) {
	switch numericBranch(schema, v) {
	case avro.Int:
		if v < math.MinInt32 || v > math.MaxInt32 {
			return nil, fmt.Errorf("value %d is out of range for an Avro int field", v)
		}
		return int32(v), nil
	case avro.Float:
		return float32(v), nil
	case avro.Double:
		return float64(v), nil
	default:
		return v, nil
	}
}

// maxAvroDecimalWidth bounds the positional form this writer will build, the same 10^7 digits
// the family uses for a plain form too wide to materialise (decimals.md 4b) and the same bound
// as the protobuf serde's big.Rat conversion.
//
// It is needed for the same reason: hamba encodes a decimal logical type from a *big.Rat, which
// has no exponent, so the value has to be rendered digit by digit to get there. The reference
// hands its BigDecimal to Avro's own DecimalConversion, which writes unscaledValue() plus the
// schema's scale and never materialises anything - and so do the Python, JavaScript, C#, C++
// and Rust clients. Go is the only one whose Avro decimal boundary is a big.Rat.
//
// The scale reaching here is producer- or rule-controlled: `decimal(b"\x01", 2147483647)` is
// built from a coefficient and an exponent at no cost, and only turning it back into Avro
// expands it. Measured: 10^7 renders 10MB, so an int32 scale is ~2.1e9 characters, and
// big.Rat.SetString then builds a 10^2147483647 denominator on top of that.
const maxAvroDecimalWidth = 10000000

// ratFromDecimal converts through the decimal's own text form, which is exact for any finite
// decimal: the unscaled digits over the power of ten its exponent names.
func ratFromDecimal(d *apd.Decimal) (*big.Rat, error) {
	if err := checkAvroDecimalWidth(d); err != nil {
		return nil, err
	}
	text := d.Text('f')
	rat, ok := new(big.Rat).SetString(text)
	if !ok {
		// Reusing the rendered text rather than rendering a second time, which doubled the
		// allocation on the one path that had already produced a large one.
		return nil, &decimalConversionError{text: text}
	}
	return rat, nil
}

// checkAvroDecimalWidth refuses a decimal whose positional form is too wide to build. The
// exponent dominates it: a negative exponent puts that many digits after the point and a
// positive one that many zeros before it.
// plainFormWidth is the digit count d's positional form would need, which is what bounds
// anything that has to materialise it.
func plainFormWidth(d *apd.Decimal) int64 {
	exp := int64(d.Exponent)
	width := d.NumDigits()
	if exp < 0 {
		if -exp > width {
			return -exp
		}
		return width
	}
	return width + exp
}

func checkAvroDecimalWidth(d *apd.Decimal) error {
	width := plainFormWidth(d)
	if width > maxAvroDecimalWidth {
		return fmt.Errorf(
			"cannot encode a decimal of exponent %d for Avro: its plain form needs %d digits, past this client's %d-digit limit",
			d.Exponent, width, maxAvroDecimalWidth)
	}
	return nil
}

type decimalConversionError struct {
	text string
}

func (e *decimalConversionError) Error() string {
	return "cannot convert decimal " + e.text + " to a rational for Avro encoding"
}
