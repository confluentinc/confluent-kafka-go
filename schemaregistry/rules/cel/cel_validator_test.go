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

package cel

import (
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

type person struct {
	Age  int    `avro:"age"`
	Name string `avro:"name"`
}

func rule(expr string) serde.ValidationRule {
	return serde.ValidationRule{Name: "r", Expr: expr}
}

func TestValidatorBooleanResults(t *testing.T) {
	v := NewValidator()
	cases := []struct {
		expr     string
		value    interface{}
		expected bool
	}{
		{"this >= 0", 30, true},
		{"this >= 0", -5, false},
		{"size(this) > 0", "alice", true},
		{"size(this) > 0", "", false},
		{"this.age <= 150", person{Age: 30, Name: "Alice"}, true}, // struct fields resolve by their schema names, via the avro tag
		{"this.age <= 150", person{Age: 200, Name: "Alice"}, false},
		{"this.startsWith('a')", "alice", true},
		{"this in ['a', 'b']", "a", true},
		{"this['age'] <= 150", map[string]interface{}{"age": 30}, true},
		{"this['age'] <= 150", map[string]interface{}{"age": 200}, false},
	}
	for _, c := range cases {
		result, err := v.Execute(rule(c.expr), nil, c.value)
		if err != nil {
			t.Errorf("expr %q on %v: unexpected error: %v", c.expr, c.value, err)
			continue
		}
		if result != c.expected {
			t.Errorf("expr %q on %v: expected %v, got %v", c.expr, c.value, c.expected, result)
		}
	}
}

func TestValidatorStringResultIsFailureMessage(t *testing.T) {
	v := NewValidator()
	expr := "this >= 0 ? '' : 'age must be positive, got ' + string(this)"
	// An empty string means the rule passed.
	result, err := v.Execute(rule(expr), nil, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "" {
		t.Errorf("expected empty string, got %v", result)
	}
	result, err = v.Execute(rule(expr), nil, -5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "age must be positive, got -5" {
		t.Errorf("expected dynamic message, got %v", result)
	}
}

func TestValidatorBindsNow(t *testing.T) {
	v := NewValidator()
	result, err := v.Execute(rule("now > timestamp('2000-01-01T00:00:00Z')"), nil, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != true {
		t.Errorf("expected now to be after 2000, got %v", result)
	}
}

func TestValidatorPointerIsDereferenced(t *testing.T) {
	v := NewValidator()
	result, err := v.Execute(rule("this.age <= 150"), nil, &person{Age: 30, Name: "Alice"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != true {
		t.Errorf("expected true, got %v", result)
	}
}

func TestValidatorErrorSurfaces(t *testing.T) {
	v := NewValidator()
	cases := []struct {
		name  string
		rule  serde.ValidationRule
		value interface{}
		match string
	}{
		{"nil value", rule("this > 0"), nil, "received a null value"},
		{"no expression", serde.ValidationRule{Name: "r"}, 1, "has no expression"},
		{"uncompilable", rule("this >= "), 1, "could not compile validation rule 'r'"},
		{"unnamed rule", serde.ValidationRule{}, 1, "validation rule 'unnamed' has no expression"},
		{"non bool or string", rule("1 + 1"), 1, "must return bool or string"},
	}
	for _, c := range cases {
		_, err := v.Execute(c.rule, nil, c.value)
		if err == nil {
			t.Errorf("%s: expected an error", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.match) {
			t.Errorf("%s: expected error containing %q, got %q", c.name, c.match, err.Error())
		}
	}
}

func TestValidatorEvaluationErrorIncludesDoc(t *testing.T) {
	v := NewValidator()
	r := serde.ValidationRule{Name: "r", Doc: "some doc", Expr: "this.nope > 0"}
	_, err := v.Execute(r, nil, person{Age: 1})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "some doc") {
		t.Errorf("expected error to include the rule doc, got %q", err.Error())
	}
}

func TestValidatorCachesOneProgramPerExpressionAndType(t *testing.T) {
	v := NewValidator().(*Validator)
	for i := 0; i < 5; i++ {
		if _, err := v.Execute(rule("this >= 0"), nil, i); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if _, err := v.Execute(rule("this <= 100"), nil, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(v.cache) != 2 {
		t.Errorf("expected 2 cached programs, got %d", len(v.cache))
	}
}
