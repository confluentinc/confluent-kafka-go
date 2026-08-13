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

package protobuf

import (
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/test"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
)

// Message-level rule plus two field-level rules, matching the JVM client's test layout.
// The schema text has to match schemaregistry/test/proto/validation_widget.proto.
const validationPersonSchema = `
syntax = "proto3";

package test;
option go_package="../test";

import "confluent/meta.proto";

message ValidationPerson {
  option (.confluent.message_meta) = {
    rules: [{name: "ageNotInsane", expr: "this.age <= 150"}]
  };

  int32 age = 1 [(.confluent.field_meta) = {
    rules: [{name: "agePositive", doc: "age must not be negative", expr: "this >= 0"}]
  }];
  string name = 2 [(.confluent.field_meta) = {
    rules: [{name: "nameNotEmpty", expr: "size(this) > 0"}]
  }];
}
`

func newValidationSerializer(t *testing.T, execution serde.ValidationRulesExecution, failFast bool) *Serializer {
	t.Helper()
	conf := schemaregistry.NewConfig("mock://")
	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	info := schemaregistry.SchemaInfo{
		Schema:     validationPersonSchema,
		SchemaType: "PROTOBUF",
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

func TestProtobufValidationPassesWhenAllRulesPass(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	msg := &test.ValidationPerson{Age: 30, Name: "Alice"}
	bytes, err := ser.Serialize("topic1", msg)
	serde.MaybeFail("serialization", err)
	if len(bytes) == 0 {
		t.Error("expected a non-empty payload")
	}
}

func TestProtobufValidationDisabledByDefault(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, "", false)
	_, err := ser.Serialize("topic1", &test.ValidationPerson{Age: -5, Name: "Alice"})
	serde.MaybeFail("serialization", err)
}

func TestProtobufValidationFailsOnFieldRule(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	_, err := ser.Serialize("topic1", &test.ValidationPerson{Age: -5, Name: "Alice"})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	// The rule's doc is preferred over its expression in the failure text.
	if !strings.Contains(err.Error(), "age must not be negative") {
		t.Errorf("expected the field rule doc, got %q", err.Error())
	}
}

func TestProtobufValidationFailsOnMessageRule(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	_, err := ser.Serialize("topic1", &test.ValidationPerson{Age: 200, Name: "Alice"})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	if !strings.Contains(err.Error(), "<root>: ageNotInsane") {
		t.Errorf("expected the message rule violation at the root, got %q", err.Error())
	}
}

func TestProtobufValidationReportsEveryViolation(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, false)
	_, err := ser.Serialize("topic1", &test.ValidationPerson{Age: 200, Name: ""})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	msg := err.Error()
	for _, want := range []string{"2 violations", "ageNotInsane", "nameNotEmpty"} {
		if !strings.Contains(msg, want) {
			t.Errorf("expected %q in %q", want, msg)
		}
	}
}

func TestProtobufValidationFailFastReportsOneViolation(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	ser := newValidationSerializer(t, serde.ValidationRulesAfterDomainRules, true)
	_, err := ser.Serialize("topic1", &test.ValidationPerson{Age: 200, Name: ""})
	if err == nil {
		t.Fatal("expected a validation error")
	}
	if !strings.Contains(err.Error(), "1 violation)") {
		t.Errorf("expected a single violation, got %q", err.Error())
	}
}

// Walker-level coverage: an always-failing executor shows exactly which rules the walker
// fired and at which paths.
type alwaysFail struct{}

func (alwaysFail) Execute(rule serde.ValidationRule, schema interface{}, msg interface{}) (interface{}, error) {
	return false, nil
}

func firedRules(t *testing.T, msg proto.Message) []string {
	t.Helper()
	desc := msg.ProtoReflect().Descriptor()
	violations, err := validateMessage(alwaysFail{}, desc, msg, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fired := make([]string, 0, len(violations))
	for _, v := range violations {
		fired = append(fired, v.Rule.Name+"@"+v.FieldPath)
	}
	return fired
}

func TestProtobufWalkerDispatch(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	cases := []struct {
		name     string
		msg      proto.Message
		expected []string
	}{
		{
			"message and field rules both fire",
			&test.ValidationPerson{Age: 30, Name: "Alice"},
			[]string{"ageNotInsane@", "agePositive@age", "nameNotEmpty@name"},
		},
		{
			"nested message recurses with a dotted path",
			&test.ValidationOuter{Inner: &test.ValidationInner{X: 5}},
			[]string{"r@inner.x", "tagsNotEmpty@tags"},
		},
		{
			"repeated message fires the element rule per element",
			&test.ValidationOuter{Items: []*test.ValidationItem{{V: 1}, {V: 2}}},
			[]string{"itemRule@items[0]", "itemRule@items[1]", "tagsNotEmpty@tags"},
		},
		{
			// maybe and inner are unset, so their rules are skipped; the repeated tags
			// field has no presence so its rule always fires, matching the JVM client.
			"unset fields with presence are skipped",
			&test.ValidationOuter{},
			[]string{"tagsNotEmpty@tags"},
		},
		{
			"set optional field fires its rule",
			&test.ValidationOuter{Maybe: proto.String("hi")},
			[]string{"maybeNotEmpty@maybe", "tagsNotEmpty@tags"},
		},
		{
			"map values are descended with a keyed path",
			&test.ValidationOuter{Labels: map[string]*test.ValidationItem{"a": {V: 1}}},
			[]string{`itemRule@labels["a"]`, "tagsNotEmpty@tags"},
		},
	}
	for _, c := range cases {
		got := firedRules(t, c.msg)
		if strings.Join(got, ",") != strings.Join(c.expected, ",") {
			t.Errorf("%s: expected %v, got %v", c.name, c.expected, got)
		}
	}
}

func TestProtobufDynamicFailureMessage(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	msg := &test.ValidationDynamicMessage{Age: -5}
	desc := msg.ProtoReflect().Descriptor()
	violations, err := validateMessage(serde.GetValidationRuleExecutor(), desc, msg, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(violations))
	}
	if violations[0].Message != "age must be positive, got -5" {
		t.Errorf("expected the dynamic message, got %q", violations[0].Message)
	}
	if violations[0].Error() != "age: ageMsg: age must be positive, got -5" {
		t.Errorf("unexpected rendering: %q", violations[0].Error())
	}
}

// Field-level rules on message, list and map fields bind a protobuf value to `this`.
// Both halves have to hold for those to work: the walker has to hand CEL a Go value
// rather than a protoreflect wrapper, and the validator has to know the types the
// schema declares so that a message reached through a collection resolves its fields.
func TestProtobufFieldRulesOnCollectionsAndMessages(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	validator := cel.NewValidator()
	outer := &test.ValidationOuter{
		Inner:  &test.ValidationInner{X: 5},
		Items:  []*test.ValidationItem{{V: 1}},
		Labels: map[string]*test.ValidationItem{"a": {V: 2}},
		Tags:   []string{"t"},
	}
	reflectMsg := outer.ProtoReflect()
	cases := []struct {
		field string
		expr  string
	}{
		{"inner", "this.x > 0"},
		{"items", "this[0].v > 0"},
		{"labels", "this['a'].v > 0"},
		{"tags", "size(this) > 0"},
	}
	for _, c := range cases {
		fd := reflectMsg.Descriptor().Fields().ByName(protoreflect.Name(c.field))
		if fd == nil {
			t.Fatalf("no field %q", c.field)
		}
		value := celFieldValue(fd, reflectMsg.Get(fd))
		result, err := validator.Execute(serde.ValidationRule{Name: "r", Expr: c.expr}, fd, value)
		if err != nil {
			t.Errorf("%s: %v", c.field, err)
			continue
		}
		if result != true {
			t.Errorf("%s: expected true, got %v", c.field, result)
		}
	}
}

// countingTransform records the path of every field value handed to it, so a test can see
// exactly which fields the transform walk reached.
type countingTransform struct {
	visited []string
}

func (c *countingTransform) Transform(ctx serde.RuleContext, fieldCtx serde.FieldContext,
	fieldValue interface{}) (interface{}, error) {
	c.visited = append(c.visited, fieldCtx.Name)
	if s, ok := fieldValue.(string); ok {
		return s + "-suffix", nil
	}
	return fieldValue, nil
}

// The transform walk drives field-level rules such as CSFLE, and has to descend the same
// way the validation walk does: into a message-valued field with that field's own
// descriptor, into every element of a repeated field, and into every value of a
// message-valued map.
func TestProtobufTransformDescendsLikeTheValidationWalk(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	msg := &test.ValidationOuter{
		Inner:  &test.ValidationInner{X: 1},
		Items:  []*test.ValidationItem{{V: 1}, {V: 2}},
		Labels: map[string]*test.ValidationItem{"a": {V: 3}},
		Maybe:  proto.String("hi"),
		Tags:   []string{"t1", "t2"},
	}
	transformer := &countingTransform{}
	rule := schemaregistry.Rule{Name: "t", Type: "TEST"}
	ctx := serde.RuleContext{
		Target:  &schemaregistry.SchemaInfo{Schema: validationPersonSchema, SchemaType: "PROTOBUF"},
		Subject: "topic1-value",
		Topic:   "topic1",
		Rule:    &rule,
		Rules:   []schemaregistry.Rule{rule},
	}
	desc := msg.ProtoReflect().Descriptor()
	result, err := transform(ctx, desc, msg, transformer)
	serde.MaybeFail("transform", err)

	// Fields are walked in declaration order - inner, items, maybe, labels, tags - so:
	// x under inner, v under each item, maybe, v under the map value, and both tags.
	expected := []string{"x", "v", "v", "maybe", "v", "tags", "tags"}
	if len(transformer.visited) != len(expected) {
		t.Fatalf("expected to visit %v, got %v", expected, transformer.visited)
	}
	for i, name := range expected {
		if transformer.visited[i] != name {
			t.Errorf("visit %d: expected %q, got %q", i, name, transformer.visited[i])
		}
	}

	// The transformed values have to be written back through every shape.
	out, ok := result.(*test.ValidationOuter)
	if !ok {
		t.Fatalf("expected a ValidationOuter, got %T", result)
	}
	if out.GetMaybe() != "hi-suffix" {
		t.Errorf("expected the scalar field to be transformed, got %q", out.GetMaybe())
	}
	if len(out.GetTags()) != 2 || out.GetTags()[0] != "t1-suffix" || out.GetTags()[1] != "t2-suffix" {
		t.Errorf("expected both repeated scalars to be transformed, got %v", out.GetTags())
	}
	if out.GetInner().GetX() != 1 || len(out.GetItems()) != 2 || out.GetLabels()["a"].GetV() != 3 {
		t.Errorf("expected nested values to be preserved, got %v", out)
	}
}

// A field with explicit presence that is unset has nothing to transform, and writing a
// value back would materialize it: an absent message or unset optional scalar would become
// present, carrying a transformed default.
func TestProtobufTransformLeavesAbsentFieldsAbsent(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	msg := &test.ValidationOuter{Tags: []string{"t"}}
	transformer := &countingTransform{}
	rule := schemaregistry.Rule{Name: "t", Type: "TEST"}
	ctx := serde.RuleContext{
		Target:  &schemaregistry.SchemaInfo{Schema: validationPersonSchema, SchemaType: "PROTOBUF"},
		Subject: "topic1-value",
		Topic:   "topic1",
		Rule:    &rule,
		Rules:   []schemaregistry.Rule{rule},
	}
	result, err := transform(ctx, msg.ProtoReflect().Descriptor(), msg, transformer)
	serde.MaybeFail("transform", err)

	out, ok := result.(*test.ValidationOuter)
	if !ok {
		t.Fatalf("expected a ValidationOuter, got %T", result)
	}
	if out.Inner != nil {
		t.Errorf("the absent message was materialized: %v", out.Inner)
	}
	if out.Maybe != nil {
		t.Errorf("the unset optional scalar was materialized: %v", out.GetMaybe())
	}
	// The field that is present is still transformed.
	if len(out.GetTags()) != 1 || out.GetTags()[0] != "t-suffix" {
		t.Errorf("expected the present field to be transformed, got %v", out.GetTags())
	}
}

// Protobuf identifies a field by its number, and renaming a field at the same number is a
// compatible change, so with use.latest.version the registered schema's name for a field can
// differ from the message's. Resolving the schema-side field by name would find nothing and
// silently skip that field's rules and tags - for the transform walk, leaving a tagged field
// untouched.
func TestProtobufWalksResolveRenamedFieldsByNumber(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	renamedSchema := `syntax = "proto3";
package test;
import "confluent/meta.proto";
message ValidationPerson {
  int32 age = 1 [(.confluent.field_meta) = {
    rules: [{name: "agePositive", expr: "this >= 0"}]
  }];
  string renamed = 2 [(.confluent.field_meta) = {
    tags: [ "PII" ]
    rules: [{name: "renamedNotEmpty", expr: "size(this) > 0"}]
  }];
}
`
	// The registered schema calls field 2 "renamed"; the generated message calls it "name".
	schemaDesc := parseMessageDescriptor(t, renamedSchema, "test.ValidationPerson")
	msg := &test.ValidationPerson{Age: 30, Name: "Alice"}

	violations, err := validateMessage(alwaysFail{}, schemaDesc, msg, false)
	serde.MaybeFail("validation", err)
	var fired []string
	for _, v := range violations {
		fired = append(fired, v.Rule.Name+"@"+v.FieldPath)
	}
	// The rule on field 2 fires, and reports the registered schema's name for it.
	if len(fired) != 2 || fired[0] != "agePositive@age" || fired[1] != "renamedNotEmpty@renamed" {
		t.Errorf("expected both rules to fire with registered names, got %v", fired)
	}

	// The transform walk has to find the tag on field 2 through the same resolution.
	transformer := &countingTransform{}
	rule := schemaregistry.Rule{Name: "t", Type: "TEST", Tags: []string{"PII"}}
	ctx := serde.RuleContext{
		Target:  &schemaregistry.SchemaInfo{Schema: renamedSchema, SchemaType: "PROTOBUF"},
		Subject: "topic1-value",
		Topic:   "topic1",
		Rule:    &rule,
		Rules:   []schemaregistry.Rule{rule},
	}
	result, err := transform(ctx, schemaDesc, msg, transformer)
	serde.MaybeFail("transform", err)
	out, ok := result.(*test.ValidationPerson)
	if !ok {
		t.Fatalf("expected a ValidationPerson, got %T", result)
	}
	if out.GetName() != "Alice-suffix" {
		t.Errorf("expected the tagged field to be transformed, got %q", out.GetName())
	}
	if len(transformer.visited) != 1 || transformer.visited[0] != "renamed" {
		t.Errorf("expected the registered name to be reported, got %v", transformer.visited)
	}
}

// parseMessageDescriptor parses schema text the way the serde does and returns one message's
// descriptor, so a test can pair a registered schema against a differently-shaped message.
func parseMessageDescriptor(t *testing.T, schema string,
	messageName string) protoreflect.MessageDescriptor {
	t.Helper()
	fd, err := parseFileDesc(nil, schemaregistry.SchemaInfo{Schema: schema, SchemaType: "PROTOBUF"})
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	md := fd.UnwrapFile().Messages().ByName(protoreflect.Name(
		messageName[strings.LastIndex(messageName, ".")+1:]))
	if md == nil {
		t.Fatalf("message %s not found", messageName)
	}
	return md
}

// A message-level rule binds `this` to the message and its CEL environment is built from the
// registered schema, so the message it evaluates has to be in the schema's terms too.
// Otherwise a rule written against a renamed field reads a missing field and rejects a valid
// message.
func TestProtobufMessageLevelRuleSeesSchemaNames(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	renamedSchema := `syntax = "proto3";
package test;
import "confluent/meta.proto";
message ValidationPerson {
  option (.confluent.message_meta) = {
    rules: [{name: "nameIsAlice", expr: "this.renamed == 'Alice'"}]
  };

  int32 age = 1;
  string renamed = 2;
}
`
	// The registered schema calls field 2 "renamed"; the generated message calls it "name".
	schemaDesc := parseMessageDescriptor(t, renamedSchema, "test.ValidationPerson")
	msg := &test.ValidationPerson{Age: 30, Name: "Alice"}

	violations, err := validateMessage(cel.NewValidator(), schemaDesc, msg, false)
	serde.MaybeFail("validation", err)

	if len(violations) != 0 {
		t.Errorf("expected the rule to hold against the renamed field, got %v", violations)
	}
}

// A rule that binds `this` to a nested message needs that message in the schema's terms, not
// just the top-level one - on the singular, repeated and map paths alike. The repeated
// elements also have to be paired positionally, and map values by key.
func TestProtobufNestedMessageRulesSeeSchemaNamesUnderARename(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	renamedSchema := `syntax = "proto3";
package test;
import "confluent/meta.proto";
message ValidationInner {
  option (.confluent.message_meta) = {
    rules: [{name: "innerRule", expr: "this.renamed_x > 0"}]
  };

  int32 renamed_x = 1;
}
message ValidationItem {
  option (.confluent.message_meta) = {
    rules: [{name: "itemRule", expr: "this.renamed_v > 0"}]
  };

  int32 renamed_v = 1;
}
message ValidationOuter {
  ValidationInner inner = 1;
  repeated ValidationItem items = 2;
  map<string, ValidationItem> labels = 4;
}
`
	// The registered schema renames the nested types' fields; the generated types still call
	// them x and v. Renaming a field at the same number is a compatible change.
	schemaDesc := parseMessageDescriptor(t, renamedSchema, "test.ValidationOuter")
	msg := &test.ValidationOuter{
		Inner: &test.ValidationInner{X: 5},
		Items: []*test.ValidationItem{{V: 1}, {V: -5}},
		Labels: map[string]*test.ValidationItem{
			"a": {V: 2},
		},
	}

	violations, err := validateMessage(cel.NewValidator(), schemaDesc, msg, false)
	serde.MaybeFail("validation", err)

	var fired []string
	for _, v := range violations {
		fired = append(fired, v.Rule.Name+"@"+v.FieldPath)
	}
	if len(fired) != 1 || fired[0] != "itemRule@items[1]" {
		t.Errorf("expected only the second item to violate, got %v", fired)
	}
}

// A generated type's descriptor is never the same object as the one built from the registered
// schema, so an identity check alone would re-read every record. When the two describe the
// same fields there is nothing to gain from it - and something to lose: re-reading has to
// marshal the message first, which a proto2 message missing a required field cannot do.
func TestProtobufDescriptorsThatAgreeAreNotReRead(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	proto2Schema := `syntax = "proto2";
package test;
import "confluent/meta.proto";
message Proto2Required {
  required string a = 1;
  optional string b = 2 [(.confluent.field_meta) = {
    rules: [{name: "r", expr: "size(this) > 0"}]
  }];
}
`
	// Two parses of the same text: distinct descriptors describing the same fields.
	schemaDesc := parseMessageDescriptor(t, proto2Schema, "test.Proto2Required")
	producerDesc := parseMessageDescriptor(t, proto2Schema, "test.Proto2Required")
	msg := dynamicpb.NewMessage(producerDesc)
	msg.Set(producerDesc.Fields().ByName("b"), protoreflect.ValueOfString("set"))
	if _, err := proto.Marshal(msg); err == nil {
		t.Fatal("the fixture must be missing its required field")
	}

	violations, err := validateMessage(alwaysFail{}, schemaDesc, msg, false)
	serde.MaybeFail("validation", err)

	var fired []string
	for _, v := range violations {
		fired = append(fired, v.Rule.Name+"@"+v.FieldPath)
	}
	if len(fired) != 1 || fired[0] != "r@b" {
		t.Errorf("expected the rule on b to fire, got %v", fired)
	}
}

// bytes and string are interchangeable at the same number - a compatible change - so a
// producer can write bytes against a schema that declares a string. The rule is authored
// against the schema, so it has to be handed the string: naming is not the only thing the
// schema's view fixes.
func TestProtobufScalarFieldRuleSeesTheSchemasRepresentation(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	registered := `syntax = "proto3";
package test;
import "confluent/meta.proto";
message Payload {
  string payload = 1 [(.confluent.field_meta) = {
    rules: [{name: "r", expr: "this == 'hello'"}]
  }];
}
`
	producer := `syntax = "proto3";
package test;
message Payload {
  bytes payload = 1;
}
`
	schemaDesc := parseMessageDescriptor(t, registered, "test.Payload")
	producerDesc := parseMessageDescriptor(t, producer, "test.Payload")
	msg := dynamicpb.NewMessage(producerDesc)
	msg.Set(producerDesc.Fields().ByName("payload"), protoreflect.ValueOfBytes([]byte("hello")))

	violations, err := validateMessage(cel.NewValidator(), schemaDesc, msg, false)
	serde.MaybeFail("validation", err)

	if len(violations) != 0 {
		t.Errorf("expected the rule to match the schema's string value, got %v", violations)
	}
}

// Adding a field is the most ordinary compatible change there is, so the registered schema
// can declare one the producer's type has never heard of - and a message-level rule can
// reference it, expecting the schema's default. The rule's environment comes from the value it
// is handed, so that only works if the message is read through the schema: a field with no
// counterpart is itself a reason to re-read, even when every shared field agrees.
func TestProtobufFieldOnlyTheSchemaDeclaresIsVisibleToMessageRules(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	registered := `syntax = "proto3";
package test;
import "confluent/meta.proto";
message ValidationPerson {
  option (.confluent.message_meta) = {
    rules: [{name: "m", expr: "this.added == ''"}]
  };

  int32 age = 1;
  string name = 2;
  string added = 99;
}
`
	// The generated ValidationPerson has no `added` field.
	schemaDesc := parseMessageDescriptor(t, registered, "test.ValidationPerson")
	msg := &test.ValidationPerson{Age: 30, Name: "Alice"}

	violations, err := validateMessage(cel.NewValidator(), schemaDesc, msg, false)
	serde.MaybeFail("validation", err)

	if len(violations) != 0 {
		t.Errorf("expected the rule to read the schema's default for the added field, got %v", violations)
	}
}
