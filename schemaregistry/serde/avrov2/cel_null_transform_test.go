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
	"fmt"
	"strings"
	"testing"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/hamba/avro/v2"
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

// celNullRunGeneric is celNullRun's counterpart for a *generically* decoded value. hamba
// represents a union in a map[string]interface{} as a single entry keyed by the branch name,
// so the branch value handed down the walk is a bare `string` for ["null","string"] - a type
// that cannot hold nil even though the field plainly can. Deriving nullability from it
// rejected a legal transform; on the null branch the value is the zero reflect.Value, where
// Type() panics outright. The struct harness above cannot reach either: its fields are
// *string, which can hold a typed nil.
func celNullRunGeneric(t *testing.T, subject, tag, expr string,
	nullable interface{}) (out map[string]interface{}, err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	cel.Register()
	client, cerr := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if cerr != nil {
		t.Fatal(cerr)
	}
	rule := schemaregistry.Rule{
		Name: "r", Kind: "TRANSFORM", Mode: "WRITE", Type: "CEL_FIELD",
		Tags: []string{tag}, Expr: expr,
	}
	info := schemaregistry.SchemaInfo{
		Schema: celNullSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, rerr := client.Register(subject+"-value", info, false); rerr != nil {
		t.Fatal(rerr)
	}
	sc := NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, serr := NewSerializer(client, serde.ValueSerde, sc)
	if serr != nil {
		t.Fatal(serr)
	}
	payload, err := ser.Serialize(subject, map[string]interface{}{
		"plain":          "orig",
		"nullable":       nullable,
		"nullableItems":  []interface{}{},
		"nullableValues": map[string]interface{}{},
	})
	if err != nil {
		return nil, err
	}
	// Decoded with hamba directly rather than through the deserializer, which needs a
	// registered type: what matters here is what the transform wrote.
	sch, perr := avro.Parse(celNullSchema)
	if perr != nil {
		t.Fatal(perr)
	}
	if uerr := avro.Unmarshal(sch, payload[5:], &out); uerr != nil {
		return nil, uerr
	}
	return out, nil
}

// Every shape a generic nullable union takes, against a rule that returns CEL null. The
// reference normalizes CEL null to a native null and lets the field setter decide, so a
// nullable field takes it whatever the branch's own type is.
func TestCelFieldNullOnAGenericUnionSetsNull(t *testing.T) {
	for name, nullable := range map[string]interface{}{
		"value branch": map[string]interface{}{"string": "x"},
		"null branch":  map[string]interface{}{"null": nil},
		"bare nil":     nil,
	} {
		out, err := celNullRunGeneric(t, "celnullgen"+strings.ReplaceAll(name, " ", ""),
			"NULLABLE", "true ; null", nullable)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if out["nullable"] != nil {
			t.Errorf("%s: nullable = %#v, want nil", name, out["nullable"])
		}
	}
}

// The non-nullable half of the same contract, now decided by the schema rather than by the
// branch value's Go type - which for a generic value is `string` either way, so the old test
// (a struct, whose plain field is also a string) could not tell the two rules apart.
func TestCelFieldNullOnAGenericNonNullableFieldIsAnError(t *testing.T) {
	_, err := celNullRunGeneric(t, "celnullgenplain", "PLAIN", "true ; null",
		map[string]interface{}{"string": "x"})
	if err == nil {
		t.Fatal("expected an error for a null written to a non-nullable field")
	}
	if !strings.Contains(err.Error(), "not nullable") {
		t.Errorf("error should name the contract violation, got %v", err)
	}
}

// A null branch the rule does not touch. resolveUnion hands the branch value down as the
// invalid reflect.Value and the walk gives it back unchanged; rewrapping it called
// Interface() on that, which panics.
func TestGenericNullBranchSurvivesANonMatchingRule(t *testing.T) {
	out, err := celNullRunGeneric(t, "celnullgenuntouched", "ITEMS", "true ; value",
		map[string]interface{}{"null": nil})
	if err != nil {
		t.Fatalf("a rule matching another field should leave this one alone: %v", err)
	}
	if out["nullable"] != nil {
		t.Errorf("nullable = %#v, want nil", out["nullable"])
	}
}

// A ["null", T] element is held as *T and the leaf hands back a bare T, which the array and map
// write-backs assigned straight into the slot: "value of type string is not assignable to type
// *string". The null case passed because a typed nil is already a pointer, and the scalar case
// because setField re-wraps for a struct field - so only containers, and only a non-null result.
func TestCelFieldTransformOnNullableContainerElements(t *testing.T) {
	got, err := celNullRun(t, "celunionitems", "ITEMS", `true ; value + "!"`)
	if err != nil {
		t.Fatalf("array of [null, string]: %v", err)
	}
	if len(got.NullableItems) != 2 || got.NullableItems[0] == nil ||
		*got.NullableItems[0] != "orig!" || *got.NullableItems[1] != "other!" {
		t.Errorf("items = %v", derefAll(got.NullableItems))
	}

	got, err = celNullRun(t, "celunionvalues", "VALUES", `true ; value + "!"`)
	if err != nil {
		t.Fatalf("map of [null, string]: %v", err)
	}
	if v, ok := got.NullableValues["a"]; !ok || v == nil || *v != "orig!" {
		t.Errorf("values[a] = %v", v)
	}
}

func derefAll(ps []*string) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		if p == nil {
			out = append(out, "<nil>")
			continue
		}
		out = append(out, *p)
	}
	return out
}
