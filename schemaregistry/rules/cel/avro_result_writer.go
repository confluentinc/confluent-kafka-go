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
	"time"

	"github.com/cockroachdb/apd/v3"
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
// The conversion needs no schema: hamba applies the field's own scale when encoding a
// *big.Rat, and an Avro variant is just a record of two bytes fields, so emitting the map
// shape is enough. That is why this is not the schema-driven writer the protobuf side needs.
func writeBackAvro(result interface{}) (interface{}, error) {
	return avroValue(result)
}

func avroValue(value interface{}) (interface{}, error) {
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
			converted, err := avroValue(item)
			if err != nil {
				return nil, err
			}
			out[key] = converted
		}
		return out, nil
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for k, item := range v {
			converted, err := avroValue(item)
			if err != nil {
				return nil, err
			}
			out[k] = converted
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, 0, len(v))
		for _, item := range v {
			converted, err := avroValue(item)
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
	default:
		return value, nil
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
