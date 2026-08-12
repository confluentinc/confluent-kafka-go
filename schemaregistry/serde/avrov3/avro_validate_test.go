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

package avrov3

import (
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"

	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
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
