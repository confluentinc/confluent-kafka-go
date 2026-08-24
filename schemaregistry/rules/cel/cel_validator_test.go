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
	"cel.dev/cel-go/cel"
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

// Two record types that share a rule expression. Native structs are all declared as CEL's
// dyn type, so a cache keyed only on the expression and declaration type would hand the
// second type a program compiled against the first.
type sharedExprA struct {
	Name string `json:"name"`
}

type sharedExprB struct {
	Name string `json:"name"`
}

func TestValidatorDistinguishesTypesSharingAnExpression(t *testing.T) {
	v := NewValidator()
	rule := serde.ValidationRule{Name: "r", Expr: "size(this.name) > 0"}

	first, err := v.Execute(rule, nil, sharedExprA{Name: "x"})
	if err != nil {
		t.Fatalf("first type: %v", err)
	}
	if first != true {
		t.Errorf("first type: expected true, got %v", first)
	}

	second, err := v.Execute(rule, nil, sharedExprB{Name: "y"})
	if err != nil {
		t.Fatalf("second type reused the first type's program: %v", err)
	}
	if second != true {
		t.Errorf("second type: expected true, got %v", second)
	}
}

// A validation rule comes from the schema, so it addresses a Go struct's fields by their
// schema names. A domain rule is written by the user against the Go type, so it keeps
// addressing them by their Go names - the two must not be conflated, or existing rule sets
// break as soon as a struct's tag differs from its field name.
func TestValidatorUsesSchemaFieldNamesWithoutAffectingDomainRules(t *testing.T) {
	v := NewValidator()
	value := person{Age: 30, Name: "Alice"}

	result, err := v.Execute(rule("this.age > 0"), nil, value)
	if err != nil {
		t.Fatalf("validation rule with the schema field name: %v", err)
	}
	if result != true {
		t.Errorf("expected true, got %v", result)
	}
	if _, err := v.Execute(rule("this.Age > 0"), nil, value); err == nil {
		t.Error("expected the Go field name to be unavailable to a validation rule")
	}

	// Through the domain-rule executor's own path, so that its choice of field naming is
	// what is under test.
	executor := NewExecutor().(*Executor)
	decls := []cel.EnvOption{cel.Variable("message", findType(value))}
	program, err := executor.newProgram("message.Age > 0", value, decls)
	if err != nil {
		t.Fatalf("domain rule with the Go field name: %v", err)
	}
	domainResult, err := evalProgram("message.Age > 0", program,
		map[string]interface{}{"message": value})
	if err != nil {
		t.Fatalf("domain rule with the Go field name: %v", err)
	}
	if domainResult != true {
		t.Errorf("expected true, got %v", domainResult)
	}
}

// An unsigned value has to be declared unsigned: comparisons survive a signed declaration
// through cross-type numeric comparison, but arithmetic on it does not.
func TestValidatorHandlesUnsignedValues(t *testing.T) {
	v := NewValidator()
	var big uint64 = 1 << 63 // above math.MaxInt64
	result, err := v.Execute(rule("this > 0"), nil, big)
	if err != nil {
		t.Fatalf("comparison on a uint64: %v", err)
	}
	if result != true {
		t.Errorf("expected a uint64 above int64 max to compare as positive, got %v", result)
	}
	if result, err = v.Execute(rule("this % 2u == 0u"), nil, big); err != nil {
		t.Fatalf("arithmetic on a uint64: %v", err)
	}
	if result != true {
		t.Errorf("expected the modulo to hold, got %v", result)
	}
}

// The JVM client registers both CEL extensions - strings and math - so a rule written
// against either resolves there. The string extension was already registered here; the math
// one supplies math.greatest/least, the rounding and sign functions, and the bit operations.
func TestValidatorResolvesBothCelExtensions(t *testing.T) {
	v := NewValidator()
	cases := []struct {
		expr     string
		expected bool
	}{
		// math
		{"math.greatest(1, 5, 3) == 5", true},
		{"math.least(1, 5, 3) == 1", true},
		{"math.abs(-4) == 4", true},
		{"math.ceil(1.2) == 2.0", true},
		{"math.floor(1.8) == 1.0", true},
		{"math.round(1.5) == 2.0", true},
		{"math.trunc(1.9) == 1.0", true},
		{"math.sign(-3) == -1", true},
		{"math.isNaN(0.0/0.0)", true},
		{"math.bitAnd(12, 10) == 8", true},
		{"math.bitOr(12, 10) == 14", true},
		{"math.bitXor(12, 10) == 6", true},
		{"math.bitShiftLeft(1, 3) == 8", true},
		{"math.bitShiftRight(8, 3) == 1", true},
		// strings, unchanged
		{"'abc'.charAt(1) == 'b'", true},
		{"'a-b'.split('-') == ['a', 'b']", true},
		{"'AbC'.lowerAscii() == 'abc'", true},
	}
	for _, c := range cases {
		result, err := v.Execute(rule(c.expr), nil, "ignored")
		if err != nil {
			t.Errorf("expr %q: unexpected error: %v", c.expr, err)
			continue
		}
		if result != c.expected {
			t.Errorf("expr %q: expected %v, got %v", c.expr, c.expected, result)
		}
	}
}
