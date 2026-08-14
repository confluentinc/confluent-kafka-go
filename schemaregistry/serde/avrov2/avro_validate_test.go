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

import (
	"reflect"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/hamba/avro/v2"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
)

// Record-level rule plus two field-level rules, matching the JVM client's test layout.
const validationSchema = `
{
  "name": "Person",
  "type": "record",
  "confluent:rules": [
    { "name": "ageNotInsane", "expr": "this.age <= 150" }
  ],
  "fields": [
    {
      "name": "age",
      "type": "int",
      "confluent:rules": [ { "name": "agePositive", "expr": "this >= 0" } ]
    },
    {
      "name": "name",
      "type": "string",
      "confluent:rules": [
        { "name": "nameNotEmpty", "doc": "name must not be empty", "expr": "size(this) > 0" }
      ]
    }
  ]
}
`

// Person is the struct form of validationSchema. The avro tags rename the fields, so
// inline rules address them by their schema names.
type Person struct {
	Age  int    `avro:"age"`
	Name string `avro:"name"`
}

func newValidationSerializer(t *testing.T, execution serde.ValidationRulesExecution, failFast bool) *Serializer {
	t.Helper()
	conf := schemaregistry.NewConfig("mock://")
	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	info := schemaregistry.SchemaInfo{
		Schema:     validationSchema,
		SchemaType: "AVRO",
	}
	_, err = client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.ValidationRulesExecution = execution
	serConfig.ValidationRulesFailFast = failFast
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)
	return ser
}

func TestAvroValidationPassesWhenAllRulesPass(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	bytes, err := ser.Serialize("topic1", &Person{Age: 30, Name: "Alice"})
	serde.MaybeFail("serialization", err)
	if len(bytes) == 0 {
		t.Error("expected a non-empty payload")
	}
}

func TestAvroValidationDisabledByDefault(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	// Age -5 would fail agePositive, but validation is disabled by default.
	ser := newValidationSerializer(t, "", false)
	_, err := ser.Serialize("topic1", &Person{Age: -5, Name: "Alice"})
	serde.MaybeFail("serialization", err)
}

func TestAvroValidationFailsOnFieldRule(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	_, err := ser.Serialize("topic1", &Person{Age: -5, Name: "Alice"})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	if !strings.Contains(err.Error(), "age: agePositive: this >= 0") {
		t.Errorf("expected the field rule violation, got %q", err.Error())
	}
}

func TestAvroValidationFailsOnRecordRule(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	_, err := ser.Serialize("topic1", &Person{Age: 200, Name: "Alice"})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	if !strings.Contains(err.Error(), "<root>: ageNotInsane") {
		t.Errorf("expected the record rule violation at the root, got %q", err.Error())
	}
}

func TestAvroValidationReportsEveryViolation(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	_, err := ser.Serialize("topic1", &Person{Age: -5, Name: ""})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	msg := err.Error()
	for _, want := range []string{"2 violations", "agePositive", "name must not be empty"} {
		if !strings.Contains(msg, want) {
			t.Errorf("expected %q in %q", want, msg)
		}
	}
}

func TestAvroValidationFailFastReportsOneViolation(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, true)
	_, err := ser.Serialize("topic1", &Person{Age: -5, Name: ""})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "1 violation)") {
		t.Errorf("expected a single violation, got %q", msg)
	}
	if strings.Contains(msg, "nameNotEmpty") {
		t.Errorf("expected fail-fast to stop before the second rule, got %q", msg)
	}
}

func TestAvroValidationBeforeDomainRules(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	// No domain rules exist, so before and after collapse to the same single point.
	ser := newValidationSerializer(t, serde.ValidationRulesBeforeDomainRules, false)
	_, err := ser.Serialize("topic1", &Person{Age: -5, Name: "Alice"})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	if !strings.Contains(err.Error(), "agePositive") {
		t.Errorf("expected the field rule violation, got %q", err.Error())
	}
}

// A record can be used by name after being defined inline, and a record from a referenced
// subject is always used by name. hamba parses both into a RefSchema, whose rules and
// inline tags live on the definition it points at - so both walks have to unwrap it.
const namedReferenceSchema = `
{
  "name": "Outer",
  "type": "record",
  "namespace": "test",
  "fields": [
    {
      "name": "a",
      "type": {
        "type": "record",
        "name": "Inner",
        "fields": [
          {
            "name": "x",
            "type": "int",
            "confluent:tags": [ "PII" ],
            "confluent:rules": [ { "name": "xPositive", "expr": "this > 0" } ]
          }
        ]
      }
    },
    { "name": "b", "type": "test.Inner" }
  ]
}
`

type namedRefInner struct {
	X int `avro:"x"`
}

type namedRefOuter struct {
	A namedRefInner `avro:"a"`
	B namedRefInner `avro:"b"`
}

func TestAvroValidationFollowsNamedReferences(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	schema, err := avro.Parse(namedReferenceSchema)
	serde.MaybeFail("schema parse", err)

	value := reflect.ValueOf(namedRefOuter{A: namedRefInner{X: -1}, B: namedRefInner{X: -1}})
	violations, err := validateMessage(cel.NewValidator(), avro.NewTypeResolver(), schema,
		&value, false)
	serde.MaybeFail("validation", err)

	var paths []string
	for _, violation := range violations {
		paths = append(paths, violation.FieldPath)
	}
	if len(paths) != 2 || paths[0] != "a.x" || paths[1] != "b.x" {
		t.Errorf("expected violations at a.x and b.x, got %v", paths)
	}
}

func TestAvroInlineTagsFollowNamedReferences(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	schema, err := avro.Parse(namedReferenceSchema)
	serde.MaybeFail("schema parse", err)

	// The transform walk drives field encryption: a field reached through a named
	// reference has to be visited with its tags, or it is silently left in the clear.
	recorder := &taggedFieldRecorder{}
	value := reflect.ValueOf(&namedRefOuter{A: namedRefInner{X: 1}, B: namedRefInner{X: 2}})
	rule := schemaregistry.Rule{Name: "t", Type: "TEST", Tags: []string{"PII"}}
	ctx := serde.RuleContext{
		Target:  &schemaregistry.SchemaInfo{Schema: namedReferenceSchema, SchemaType: "AVRO"},
		Subject: "topic1-value",
		Topic:   "topic1",
		Rule:    &rule,
		Rules:   []schemaregistry.Rule{rule},
	}
	_, err = transform(ctx, avro.NewTypeResolver(), schema, &value, recorder)
	serde.MaybeFail("transform", err)

	if len(recorder.visited) != 2 {
		t.Errorf("expected the tagged field to be visited under both a and b, got %v",
			recorder.visited)
	}
}

// taggedFieldRecorder records every field the transform walk visits that carries the PII
// tag, which is what an encryption executor would act on.
type taggedFieldRecorder struct {
	visited []string
}

func (r *taggedFieldRecorder) Transform(ctx serde.RuleContext, fieldCtx serde.FieldContext,
	fieldValue interface{}) (interface{}, error) {
	if fieldCtx.Tags["PII"] {
		r.visited = append(r.visited, fieldCtx.Name)
	}
	return fieldValue, nil
}

// A nested record whose value the caller holds as a pointer inside an interface - the
// shape a generic map[string]interface{} record produces. deref has to unwrap both the
// interface and the pointer to reach the struct; stopping at one leaves a reflect.Pointer,
// and every rule inside the child is silently skipped while the record still serializes.
const nestedValidationSchema = `
{
  "name": "Parent",
  "type": "record",
  "fields": [
    {
      "name": "child",
      "type": {
        "name": "Child",
        "type": "record",
        "fields": [
          {
            "name": "code",
            "type": "string",
            "confluent:rules": [ { "name": "codeNotEmpty", "expr": "size(this) > 0" } ]
          }
        ]
      }
    }
  ]
}
`

type NestedChild struct {
	Code string `avro:"code"`
}

type NestedParent struct {
	Child NestedChild `avro:"child"`
}

func newNestedValidationSerializer(t *testing.T) *Serializer {
	t.Helper()
	conf := schemaregistry.NewConfig("mock://")
	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)
	_, err = client.Register("topic1-value", schemaregistry.SchemaInfo{
		Schema:     nestedValidationSchema,
		SchemaType: "AVRO",
	}, false)
	serde.MaybeFail("Schema registration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.ValidationRulesExecution = serde.ValidationRulesAfterDomainRules
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)
	return ser
}

func TestAvroValidationReachesNestedRecordsThroughEveryIndirection(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newNestedValidationSerializer(t)

	empty := NestedChild{Code: ""}
	shapes := map[string]interface{}{
		"typed struct":     &NestedParent{Child: empty},
		"pointer in field": &map[string]interface{}{"child": &empty},
		"value in field":   &map[string]interface{}{"child": empty},
		"map in field":     &map[string]interface{}{"child": map[string]interface{}{"code": ""}},
	}
	for name, msg := range shapes {
		_, err := ser.Serialize("topic1", msg)
		if err == nil {
			t.Errorf("%s: codeNotEmpty was not evaluated; the nested record was skipped", name)
			continue
		}
		if !strings.Contains(err.Error(), "codeNotEmpty") {
			t.Errorf("%s: expected codeNotEmpty to fire, got: %v", name, err)
		}
	}
}

func TestAvroValidationPassesForNestedRecordsThroughEveryIndirection(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newNestedValidationSerializer(t)

	ok := NestedChild{Code: "abc"}
	shapes := map[string]interface{}{
		"typed struct":     &NestedParent{Child: ok},
		"pointer in field": &map[string]interface{}{"child": &ok},
		"value in field":   &map[string]interface{}{"child": ok},
		"map in field":     &map[string]interface{}{"child": map[string]interface{}{"code": "abc"}},
	}
	for name, msg := range shapes {
		if _, err := ser.Serialize("topic1", msg); err != nil {
			t.Errorf("%s: expected no violation, got: %v", name, err)
		}
	}
}
