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

package avrov2

// A CEL_FIELD transform that returns null.
//
// The reference states the contract in CelFieldExecutor: CEL null is normalized to a Java null
// "so a nullable target sees null and a non-nullable target surfaces the contract violation
// directly". Every other client builds a *new* container as it walks and so just stores the
// null; this client writes back in place with reflect, where the invalid reflect.Value that
// stands for null is indistinguishable from the one an untouched branch returns. That
// conflation produced three different wrong answers, none of them an error:
//
//   - a record field silently kept its old value, nullable or not;
//   - an array element panicked, "reflect: call of reflect.Value.Set on zero Value";
//   - a map value silently *deleted its entry*, because SetMapIndex with an invalid Value
//     removes the key.

import (
	"strings"
	"testing"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

const celNullSchema = `{
  "type": "record",
  "name": "N1",
  "fields": [
    {"name": "plain", "type": "string", "confluent:tags": ["PLAIN"]},
    {"name": "nullable", "type": ["null","string"], "confluent:tags": ["NULLABLE"]},
    {"name": "nullableItems",
     "type": {"type": "array", "items": ["null","string"]},
     "confluent:tags": ["ITEMS"]},
    {"name": "nullableValues",
     "type": {"type": "map", "values": ["null","string"]},
     "confluent:tags": ["VALUES"]}
  ]
}`

type celNullRec struct {
	Plain          string             `avro:"plain"`
	Nullable       *string            `avro:"nullable"`
	NullableItems  []*string          `avro:"nullableItems"`
	NullableValues map[string]*string `avro:"nullableValues"`
}

func celNullRun(t *testing.T, subject, tag, expr string) (*celNullRec, error) {
	t.Helper()
	cel.Register()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatal(err)
	}
	rule := schemaregistry.Rule{
		Name: "r", Kind: "TRANSFORM", Mode: "WRITE", Type: "CEL_FIELD",
		Tags: []string{tag}, Expr: expr,
	}
	info := schemaregistry.SchemaInfo{
		Schema: celNullSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, err := client.Register(subject+"-value", info, false); err != nil {
		t.Fatal(err)
	}
	sc := NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, err := NewSerializer(client, serde.ValueSerde, sc)
	if err != nil {
		t.Fatal(err)
	}
	orig, other := "orig", "other"
	payload, err := ser.Serialize(subject, &celNullRec{
		Plain:          "orig",
		Nullable:       &orig,
		NullableItems:  []*string{&orig, &other},
		NullableValues: map[string]*string{"a": &orig},
	})
	if err != nil {
		return nil, err
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	if err != nil {
		t.Fatal(err)
	}
	deser.Client = client
	var got celNullRec
	if err := deser.DeserializeInto(subject, payload, &got); err != nil {
		t.Fatal(err)
	}
	return &got, nil
}

// A nullable field sees the null, as the reference's comment requires.
func TestCelFieldNullOnANullableFieldSetsNull(t *testing.T) {
	got, err := celNullRun(t, "celnullfield", "NULLABLE", `null`)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if got.Nullable != nil {
		t.Errorf("nullable = %q, want null", *got.Nullable)
	}
	if got.Plain != "orig" {
		t.Errorf("plain = %q, want it untouched", got.Plain)
	}
}

// A non-nullable field surfaces the violation instead. Before this it silently kept "orig",
// which is the one outcome the reference's comment rules out.
func TestCelFieldNullOnANonNullableFieldIsAnError(t *testing.T) {
	_, err := celNullRun(t, "celnullplain", "PLAIN", `null`)
	if err == nil {
		t.Fatal("expected an error for a null on a non-nullable field")
	}
	if !strings.Contains(err.Error(), "N1.plain") || !strings.Contains(err.Error(), "not nullable") {
		t.Errorf("error should name the field and the reason, got %v", err)
	}
}

// An array of a nullable item type takes the null per element. This panicked.
func TestCelFieldNullOnArrayElementsSetsNull(t *testing.T) {
	got, err := celNullRun(t, "celnullarr", "ITEMS", `null`)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if len(got.NullableItems) != 2 {
		t.Fatalf("items = %v, want 2 of them", got.NullableItems)
	}
	for i, it := range got.NullableItems {
		if it != nil {
			t.Errorf("item %d = %q, want null", i, *it)
		}
	}
}

// A map of a nullable value type keeps its keys and nulls the values. This deleted the entry:
// the discriminator is the key surviving at all, not just the value being null.
func TestCelFieldNullOnMapValuesKeepsTheKey(t *testing.T) {
	got, err := celNullRun(t, "celnullmap", "VALUES", `null`)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	v, ok := got.NullableValues["a"]
	if !ok {
		t.Fatalf(`key "a" was dropped from the map: %v`, got.NullableValues)
	}
	if v != nil {
		t.Errorf(`map["a"] = %q, want null`, *v)
	}
}
