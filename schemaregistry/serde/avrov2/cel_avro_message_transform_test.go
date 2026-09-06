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

// Message-level CEL transforms over Avro, and specifically that they *replace* rather than
// merge: the rule's map is the whole new record, so a field the rule does not name takes the
// schema's declared default rather than the value it had on the way in.
//
// This case existed only on the protobuf side, and its absence hid a real defect elsewhere -
// the C++ client seeded its result record from the input before applying the map, so it
// merged. Every other C6/C7 case names *all* of the record's fields, which makes merge and
// replace indistinguishable.
//
// Driven end to end through the serializer rather than through the executor alone. That is
// not incidental: the executor hands back a plain map, and whether that map is something
// hamba can actually encode is precisely what the naming rule below decides. An
// executor-level test passes either way.
//
// **The naming rule.** A domain rule *reads* an Avro record through its Go field names
// (`message.Kept`) - see schemaFieldName in rules/cel/cel_executor.go, where only validation
// rules use the schema's names - but the *keys of the result map* are the Avro record's own
// field names (`"kept"`), because that map is the record and hamba encodes it against the
// schema. Mixing the two spellings in one expression looks odd and is correct.

package avrov2

import (
	"testing"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

const celDefaultsSchema = `{
  "type": "record",
  "name": "Defaults",
  "fields": [
    {"name": "kept", "type": "string"},
    {"name": "withDefault", "type": "string", "default": "fallback"},
    {"name": "nullable", "type": ["null", "string"], "default": null}
  ]
}`

type celDefaults struct {
	Kept        string  `avro:"kept"`
	WithDefault string  `avro:"withDefault"`
	Nullable    *string `avro:"nullable"`
}

// Serializes the fixture under one message-level CEL transform and reads it back.
func celTransformRoundTrip(t *testing.T, topic string, expr string) (celDefaults, error) {
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
		Schema: celDefaultsSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, err := client.Register(topic+"-value", info, false); err != nil {
		t.Fatal(err)
	}

	nullable := "original-nullable"
	obj := celDefaults{Kept: "original-kept", WithDefault: "original-withDefault",
		Nullable: &nullable}
	bytes, err := ser.Serialize(topic, &obj)
	if err != nil {
		return celDefaults{}, err
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	if err != nil {
		t.Fatal(err)
	}
	deser.Client = client
	var out celDefaults
	if err := deser.DeserializeInto(topic, bytes, &out); err != nil {
		return celDefaults{}, err
	}
	return out, nil
}

// The case this file exists for. Under merge, withDefault would still read
// "original-withDefault"; under replace it takes the schema's declared default.
func TestCelAvroMessageTransformDropsUnnamedFields(t *testing.T) {
	out, err := celTransformRoundTrip(t, "celdrop", `{"kept": message.Kept}`)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}

	if out.Kept != "original-kept" {
		t.Errorf("kept = %q, want original-kept", out.Kept)
	}
	if out.WithDefault != "fallback" {
		t.Errorf("withDefault = %q, want the declared default \"fallback\"", out.WithDefault)
	}
	if out.WithDefault == "original-withDefault" {
		t.Error("withDefault kept its input value - this is merge, not replace")
	}
	if out.Nullable != nil {
		t.Errorf("nullable = %q, want the declared default null", *out.Nullable)
	}
}

// The must-fail twin. Without it, "the other fields took their defaults" is equally consistent
// with the transform having stopped working altogether.
func TestCelAvroMessageTransformNamingEveryFieldRoundTrips(t *testing.T) {
	out, err := celTransformRoundTrip(t, "celall",
		`{"kept": message.Kept, "withDefault": message.WithDefault, "nullable": message.Nullable}`)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}

	if out.Kept != "original-kept" || out.WithDefault != "original-withDefault" {
		t.Errorf("kept/withDefault = %q/%q", out.Kept, out.WithDefault)
	}
	if out.Nullable == nil || *out.Nullable != "original-nullable" {
		t.Errorf("nullable = %v, want original-nullable", out.Nullable)
	}
}
