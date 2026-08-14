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

package jsonschema

import (
	"reflect"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	jsonschema2 "github.com/santhosh-tekuri/jsonschema/v5"

	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
)

// Object-level rule plus two property-level rules, matching the JVM client's test layout.
const validationSchema = `
{
  "type": "object",
  "title": "Person",
  "confluent:rules": [
    { "name": "ageNotInsane", "expr": "this.age <= 150" }
  ],
  "properties": {
    "age": {
      "type": "integer",
      "confluent:rules": [ { "name": "agePositive", "expr": "this >= 0" } ]
    },
    "name": {
      "type": "string",
      "confluent:rules": [
        { "name": "nameNotEmpty", "doc": "name must not be empty", "expr": "size(this) > 0" }
      ]
    }
  }
}
`

// ValidationPerson is the struct form of validationSchema. The json tags rename the
// fields, so inline rules address them by their schema names.
type ValidationPerson struct {
	Age  int    `json:"age"`
	Name string `json:"name"`
}

func newValidationSerializer(t *testing.T, execution serde.ValidationRulesExecution, failFast bool) *Serializer {
	t.Helper()
	conf := schemaregistry.NewConfig("mock://")
	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	info := schemaregistry.SchemaInfo{
		Schema:     validationSchema,
		SchemaType: "JSON",
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

func TestJSONValidationPassesWhenAllRulesPass(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	bytes, err := ser.Serialize("topic1", &ValidationPerson{Age: 30, Name: "Alice"})
	serde.MaybeFail("serialization", err)
	if len(bytes) == 0 {
		t.Error("expected a non-empty payload")
	}
}

func TestJSONValidationDisabledByDefault(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, "", false)
	_, err := ser.Serialize("topic1", &ValidationPerson{Age: -5, Name: "Alice"})
	serde.MaybeFail("serialization", err)
}

func TestJSONValidationFailsOnPropertyRule(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	_, err := ser.Serialize("topic1", &ValidationPerson{Age: -5, Name: "Alice"})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	// JSON paths are rooted at $, matching the JVM client.
	if !strings.Contains(err.Error(), "$.age: agePositive") {
		t.Errorf("expected the property rule violation, got %q", err.Error())
	}
}

func TestJSONValidationFailsOnObjectRule(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	_, err := ser.Serialize("topic1", &ValidationPerson{Age: 200, Name: "Alice"})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	if !strings.Contains(err.Error(), "$: ageNotInsane") {
		t.Errorf("expected the object rule violation at the root, got %q", err.Error())
	}
}

func TestJSONValidationReportsEveryViolation(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	_, err := ser.Serialize("topic1", &ValidationPerson{Age: -5, Name: ""})
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

func TestJSONValidationFailFastReportsOneViolation(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, true)
	_, err := ser.Serialize("topic1", &ValidationPerson{Age: -5, Name: ""})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	if !strings.Contains(err.Error(), "1 violation)") {
		t.Errorf("expected a single violation, got %q", err.Error())
	}
}

// A property whose declared type is a union of scalars. The walker has to narrow to the
// matching type before descending, and must do so without mutating the shared compiled
// schema.
const multiTypeValidationSchema = `
{
  "type": "object",
  "title": "Flexible",
  "properties": {
    "value": {
      "type": ["string", "integer"],
      "confluent:rules": [ { "name": "notForbidden", "expr": "string(this) != 'forbidden'" } ]
    }
  }
}
`

// ValidationFlexible exercises a multi-type property.
type ValidationFlexible struct {
	Value string `json:"value"`
}

func newMultiTypeSerializer(t *testing.T) *Serializer {
	t.Helper()
	conf := schemaregistry.NewConfig("mock://")
	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	info := schemaregistry.SchemaInfo{
		Schema:     multiTypeValidationSchema,
		SchemaType: "JSON",
	}
	_, err = client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.ValidationRulesExecution = serde.ValidationRulesAfterDomainRules
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)
	return ser
}

func TestJSONValidationHandlesMultiTypeProperties(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newMultiTypeSerializer(t)

	bytes, err := ser.Serialize("topic1", &ValidationFlexible{Value: "allowed"})
	serde.MaybeFail("serialization", err)
	if len(bytes) == 0 {
		t.Error("expected a non-empty payload")
	}

	// Serializing twice exercises the cached compiled schema: narrowing the declared
	// types must not leave the cached schema modified.
	_, err = ser.Serialize("topic1", &ValidationFlexible{Value: "forbidden"})
	if err == nil {
		t.Fatal("expected the rule on the multi-type property to fail")
	}
	if !strings.Contains(err.Error(), "notForbidden") {
		t.Errorf("unexpected error: %v", err)
	}

	bytes, err = ser.Serialize("topic1", &ValidationFlexible{Value: "allowed"})
	serde.MaybeFail("serialization after a failure", err)
	if len(bytes) == 0 {
		t.Error("expected a non-empty payload on the second pass")
	}
}

// ValidationTagged carries json tag options, which the property lookup has to strip before
// matching schema property names.
type ValidationTagged struct {
	Age  int    `json:"age,omitempty"`
	Name string `json:"name,omitempty"`
}

func TestJSONValidationHonorsJSONTagOptions(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)

	bytes, err := ser.Serialize("topic1", &ValidationTagged{Age: 30, Name: "Alice"})
	serde.MaybeFail("serialization", err)
	if len(bytes) == 0 {
		t.Error("expected a non-empty payload")
	}

	// A tag of `age,omitempty` must still match the `age` property, otherwise the rule is
	// silently skipped.
	_, err = ser.Serialize("topic1", &ValidationTagged{Age: -5, Name: "Alice"})
	if err == nil {
		t.Fatal("expected agePositive to fail for a field tagged with omitempty")
	}
	if !strings.Contains(err.Error(), "agePositive") {
		t.Errorf("unexpected error: %v", err)
	}
}

// Narrowing a type array must not touch the compiled schema, which is cached and shared
// across serializations - in either walk. Restoring it afterwards is not enough: a
// concurrent serialization observes the narrowing while the walk is still running, so both
// observers below sample the schema mid-walk.
func TestJsonSchemaWalksDoNotMutateTheSharedSchema(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	schemaText := `{
	  "type": ["object", "null"],
	  "properties": { "x": { "type": "integer", "confluent:rules": [{"name": "r", "expr": "true"}] } }
	}`
	compiler := jsonschema2.NewCompiler()
	// The rules keyword is an extension, so the compiler has to know it - as the serde's
	// own compiler does.
	compiler.RegisterExtension("confluent:tags", tagsMeta, tagsCompiler{})
	compiler.RegisterExtension(serde.ValidationRulesProp, validationRulesMeta,
		validationRulesCompiler{})
	if err := compiler.AddResource("main.json", strings.NewReader(schemaText)); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile("main.json")
	if err != nil {
		t.Fatal(err)
	}
	expected := append([]string(nil), schema.Types...)

	value := reflect.ValueOf(&sharedSchemaValue{X: 5})
	validator := &typesObserver{schema: schema}
	if _, err := validateMessage(validator, schema, &value, false); err != nil {
		t.Fatalf("validation: %v", err)
	}
	if len(validator.observed) == 0 {
		t.Fatal("the validation walk never reached the property rule")
	}
	for _, observed := range validator.observed {
		if !reflect.DeepEqual(observed, expected) {
			t.Errorf("the validation walk narrowed the shared schema to %v", observed)
		}
	}

	rule := schemaregistry.Rule{Name: "t", Type: "TEST"}
	ctx := serde.RuleContext{
		Target:  &schemaregistry.SchemaInfo{Schema: schemaText, SchemaType: "JSON"},
		Subject: "topic1-value",
		Topic:   "topic1",
		Rule:    &rule,
		Rules:   []schemaregistry.Rule{rule},
	}
	transformer := &typesObserver{schema: schema}
	if _, err := transform(ctx, schema, "$", &value, transformer); err != nil {
		t.Fatalf("transform: %v", err)
	}
	if len(transformer.observed) == 0 {
		t.Fatal("the transform walk never reached the property")
	}
	for _, observed := range transformer.observed {
		if !reflect.DeepEqual(observed, expected) {
			t.Errorf("the transform walk narrowed the shared schema to %v", observed)
		}
	}
}

// sharedSchemaValue is the struct form of the schema above.
type sharedSchemaValue struct {
	X int `json:"x"`
}

// typesObserver samples the root schema's declared types every time a walk hands it a
// value, which is what a concurrent serialization would see.
type typesObserver struct {
	schema   *jsonschema2.Schema
	observed [][]string
}

func (o *typesObserver) sample() {
	o.observed = append(o.observed, append([]string(nil), o.schema.Types...))
}

func (o *typesObserver) Execute(rule serde.ValidationRule, schema interface{},
	msg interface{}) (interface{}, error) {
	o.sample()
	return true, nil
}

func (o *typesObserver) Transform(ctx serde.RuleContext, fieldCtx serde.FieldContext,
	fieldValue interface{}) (interface{}, error) {
	o.sample()
	return fieldValue, nil
}

// An integer property is visited by both walks. The type mapping used to name a type JSON
// Schema does not have ("int"), which left integer fields typed NULL and skipped by the
// transform walk while the validation walk still visited them.
func TestJsonSchemaTransformVisitsIntegerProperties(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	schemaText := `{
	  "type": "object",
	  "properties": { "x": { "type": "integer" } }
	}`
	compiler := jsonschema2.NewCompiler()
	compiler.RegisterExtension("confluent:tags", tagsMeta, tagsCompiler{})
	compiler.RegisterExtension(serde.ValidationRulesProp, validationRulesMeta,
		validationRulesCompiler{})
	if err := compiler.AddResource("main.json", strings.NewReader(schemaText)); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile("main.json")
	if err != nil {
		t.Fatal(err)
	}

	value := reflect.ValueOf(&sharedSchemaValue{X: 5})
	rule := schemaregistry.Rule{Name: "t", Type: "TEST"}
	ctx := serde.RuleContext{
		Target:  &schemaregistry.SchemaInfo{Schema: schemaText, SchemaType: "JSON"},
		Subject: "topic1-value",
		Topic:   "topic1",
		Rule:    &rule,
		Rules:   []schemaregistry.Rule{rule},
	}
	observer := &typesObserver{schema: schema}
	if _, err := transform(ctx, schema, "$", &value, observer); err != nil {
		t.Fatalf("transform: %v", err)
	}
	if len(observer.observed) != 1 {
		t.Errorf("expected the integer property to be visited once, got %d visits",
			len(observer.observed))
	}
}

// A root whose text is the same whichever schema it references. What its "$ref"
// resolves to is decided entirely by the reference list on the SchemaInfo.
const sharedRootSchema = `
{
  "type": "object",
  "properties": { "payload": { "$ref": "ref" } }
}
`

// Two referenced schemas differing only in the rule they declare.
func refSchemaRequiring(prefix string, ruleName string) string {
	return `
{
  "type": "object",
  "properties": {
    "code": {
      "type": "string",
      "confluent:rules": [
        { "name": "` + ruleName + `", "expr": "this.startsWith('` + prefix + `')" }
      ]
    }
  }
}
`
}

func rootReferencing(subject string) schemaregistry.SchemaInfo {
	return schemaregistry.SchemaInfo{
		Schema:     sharedRootSchema,
		SchemaType: "JSON",
		References: []schemaregistry.Reference{
			{Name: "ref", Subject: subject, Version: 1},
		},
	}
}

func TestJsonSchemaCacheSeparatesSchemasByTheirReferences(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	conf := schemaregistry.NewConfig("mock://")
	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	for _, ref := range []struct{ subject, prefix, rule string }{
		{"ref-a", "A", "code_a"},
		{"ref-b", "B", "code_b"},
	} {
		_, err = client.Register(ref.subject, schemaregistry.SchemaInfo{
			Schema:     refSchemaRequiring(ref.prefix, ref.rule),
			SchemaType: "JSON",
		}, false)
		serde.MaybeFail("reference registration", err)
	}

	// One serde, so both lookups go through the same cache. The two schemas
	// agree on every byte of their text and differ only in what they reference,
	// which is exactly the pair a text-keyed cache would conflate.
	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	typeA, err := ser.toJSONSchema(client, rootReferencing("ref-a"))
	serde.MaybeFail("compiling the schema referencing ref-a", err)
	typeB, err := ser.toJSONSchema(client, rootReferencing("ref-b"))
	serde.MaybeFail("compiling the schema referencing ref-b", err)

	if typeA == typeB {
		t.Fatal("the second schema was served the first one's compiled schema")
	}
	if got := ruleNamesOf(typeA); got != "code_a" {
		t.Errorf("schema referencing ref-a carries rule %q, want code_a", got)
	}
	if got := ruleNamesOf(typeB); got != "code_b" {
		t.Errorf("schema referencing ref-b carries rule %q, want code_b", got)
	}

	// The same schema still hits the cache rather than recompiling.
	again, err := ser.toJSONSchema(client, rootReferencing("ref-a"))
	serde.MaybeFail("recompiling the schema referencing ref-a", err)
	if again != typeA {
		t.Error("an identical schema missed the cache")
	}
}

// Name of the single rule reachable under the root's "payload" property.
func ruleNamesOf(schema *jsonschema2.Schema) string {
	payload := schema.Properties["payload"]
	if payload == nil {
		return "<no payload property>"
	}
	// A "$ref" compiles to a schema that points at the resolved one, which is
	// where the referenced schema's rules live.
	if payload.Ref != nil {
		payload = payload.Ref
	}
	code := payload.Properties["code"]
	if code == nil {
		return "<no code property>"
	}
	rules, ok := code.Extensions[serde.ValidationRulesProp].(validationRulesSchema)
	if !ok || len(rules) != 1 {
		return "<no rules>"
	}
	return rules[0].Name
}
