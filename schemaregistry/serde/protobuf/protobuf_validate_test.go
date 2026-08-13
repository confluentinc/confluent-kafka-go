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
