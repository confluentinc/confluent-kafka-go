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

// A message-level CEL transform over a record with an int or float field.
//
// CEL has one integer width and one floating one, so the result map carries int64 and float64
// whatever the field declares, and hamba refuses both ("int64 is unsupported for Avro int").
// The write-back therefore narrowed nothing and *every* rule over such a schema failed - the
// identity transform included, since replace semantics make the rule name every field. The
// reference narrows against the schema in narrowToInt / narrowToFloat, and refuses an
// out-of-range value rather than truncating it into the slot.

package avrov2

import (
	"strings"
	"testing"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

const celNestedSchema = `{
  "type": "record",
  "name": "Nested",
  "fields": [
    {"name": "inner", "type": {"type":"record","name":"Inner","fields":[{"name":"count","type":"int"}]}},
    {"name": "nullableInner", "type": ["null", "Inner"], "default": null},
    {"name": "nullableMap", "type": ["null", {"type":"map","values":"int"}], "default": null},
    {"name": "label", "type": "string"}
  ]
}`

type celNestedInner struct {
	Count int32 `avro:"count"`
}

type celNested struct {
	Inner         celNestedInner    `avro:"inner"`
	NullableInner *celNestedInner   `avro:"nullableInner"`
	NullableMap   *map[string]int32 `avro:"nullableMap"`
	Label         string            `avro:"label"`
}

const celNumericSchema = `{
  "type": "record",
  "name": "Numeric",
  "fields": [
    {"name": "count", "type": "int"},
    {"name": "ratio", "type": "float"},
    {"name": "nullableCount", "type": ["null", "int"], "default": null},
    {"name": "label", "type": "string"}
  ]
}`

type celNumeric struct {
	Count         int32   `avro:"count"`
	Ratio         float32 `avro:"ratio"`
	NullableCount *int32  `avro:"nullableCount"`
	Label         string  `avro:"label"`
}

func celNumericRoundTrip(t *testing.T, topic string, expr string) (celNumeric, error) {
	t.Helper()
	cel.Register()

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatal(err)
	}
	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	if err != nil {
		t.Fatal(err)
	}
	rule := schemaregistry.Rule{Name: "r", Kind: "TRANSFORM", Mode: "WRITE",
		Type: "CEL", Expr: expr}
	info := schemaregistry.SchemaInfo{
		Schema: celNumericSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, err := client.Register(topic+"-value", info, false); err != nil {
		t.Fatal(err)
	}

	nullable := int32(3)
	obj := celNumeric{Count: 7, Ratio: 1.5, NullableCount: &nullable, Label: "hi"}
	bytes, err := ser.Serialize(topic, &obj)
	if err != nil {
		return celNumeric{}, err
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	if err != nil {
		t.Fatal(err)
	}
	deser.Client = client
	var out celNumeric
	if err := deser.DeserializeInto(topic, bytes, &out); err != nil {
		return celNumeric{}, err
	}
	return out, nil
}

const celNumericAll = `{"count": message.Count, "ratio": message.Ratio, ` +
	`"nullableCount": message.NullableCount, "label": message.Label}`

func TestCelAvroIdentityOverNumericFieldsRoundTrips(t *testing.T) {
	out, err := celNumericRoundTrip(t, "celnum", celNumericAll)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}

	if out.Count != 7 {
		t.Errorf("count = %d, want 7", out.Count)
	}
	if out.Ratio != 1.5 {
		t.Errorf("ratio = %v, want 1.5", out.Ratio)
	}
	if out.NullableCount == nil || *out.NullableCount != 3 {
		t.Errorf("nullableCount = %v, want 3", out.NullableCount)
	}
}

// The discriminator: a *computed* int proves the rule's result was written, so the test above
// cannot be passing because the transform stopped running.
func TestCelAvroComputedIntIsWritten(t *testing.T) {
	out, err := celNumericRoundTrip(t, "celnumcalc",
		`{"count": message.Count + 1, "ratio": message.Ratio, `+
			`"nullableCount": message.NullableCount, "label": message.Label}`)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}

	if out.Count != 8 {
		t.Errorf("count = %d, want 8", out.Count)
	}
}

// narrowToInt's range check: truncating would have written -2147483648.
func TestCelAvroOutOfRangeIntIsRefused(t *testing.T) {
	_, err := celNumericRoundTrip(t, "celnumrange",
		`{"count": 2147483648, "ratio": message.Ratio, `+
			`"nullableCount": message.NullableCount, "label": message.Label}`)
	if err == nil {
		t.Fatal("an out-of-range int was accepted for an Avro int field")
	}
	if !strings.Contains(err.Error(), "out of range for an Avro int field") {
		t.Errorf("refused for the wrong reason: %v", err)
	}
}

// A record or map computed *under a union* is written to the branch the schema names.
//
// hamba resolves a union from the Go type, and a map is ambiguous there - a record branch and a
// map branch have the same shape - so it refused a computed nested record in a `["null", T]`
// field with "unknown union type count", naming the record's first field. Schema propagation
// also stopped at the union, leaving the int inside unnarrowed. One resolution fixes both.
func TestCelAvroComputedRecordUnderAUnionIsWritten(t *testing.T) {
	cel.Register()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatal(err)
	}
	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	if err != nil {
		t.Fatal(err)
	}
	expr := `{"nullableInner": {"count": message.Inner.Count + 1}, ` +
		`"nullableMap": {"a": message.Inner.Count + 2}, ` +
		`"inner": message.Inner, "label": message.Label}`
	rule := schemaregistry.Rule{Name: "r", Kind: "TRANSFORM", Mode: "WRITE", Type: "CEL", Expr: expr}
	info := schemaregistry.SchemaInfo{Schema: celNestedSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}}}
	if _, err := client.Register("celnested-value", info, false); err != nil {
		t.Fatal(err)
	}
	obj := celNested{Inner: celNestedInner{Count: 7}, Label: "hi"}
	bytes, err := ser.Serialize("celnested", &obj)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	if err != nil {
		t.Fatal(err)
	}
	deser.Client = client
	var out celNested
	if err := deser.DeserializeInto("celnested", bytes, &out); err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	if out.NullableInner == nil || out.NullableInner.Count != 8 {
		t.Errorf("nullableInner = %v, want count 8", out.NullableInner)
	}
	if out.NullableMap == nil || (*out.NullableMap)["a"] != 9 {
		t.Errorf("nullableMap = %v, want a=9", out.NullableMap)
	}
}
