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
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"

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
