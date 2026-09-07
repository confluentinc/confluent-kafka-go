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

// Reading and echoing a container field from a CEL rule.
//
// Three separate defects met here, all invisible while every fixture used scalar fields:
//
//   1. The null-aware wrapper claimed containers. A repeated protobuf field arrives as a
//      protoreflect.List and an Avro one comes back as an already-adapted *types.baseList -
//      both struct pointers, so the wrapper's Kind test admitted them. Its Get understands
//      only a string key, so `amounts[0]` answered "no such overload" and Size was lost,
//      taking `size(amounts)` with it.
//   2. Container element types were never registered. An inline *field* rule binds `this` to
//      the field's own value, so a rule on an array of decimals compiled with
//      this = []*big.Rat and nothing declared big.Rat; `this[0]` failed to adapt.
//   3. A map echoed by a message transform reached the write-back as cel-go's own *pb.Map,
//      which it did not recognise and **dropped silently** - the identity transform lost
//      amount_map with no error at all.
//
// Every positive below is paired with something that distinguishes "the rule answered" from
// "the rule never ran": a false twin, or an omitting transform.

import (
	"math/big"
	"testing"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	prototypes "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/test"
)

func containerDecimal(t *testing.T, text string) *prototypes.Decimal {
	t.Helper()
	r, ok := new(big.Rat).SetString(text)
	if !ok {
		t.Fatalf("bad decimal %q", text)
	}
	scaled := new(big.Int).Div(new(big.Int).Mul(r.Num(), big.NewInt(100)), r.Denom())
	// Two's-complement, not magnitude: Bytes() drops the sign, so 222 would go in as -34.
	raw := scaled.Bytes()
	if len(raw) > 0 && raw[0]&0x80 != 0 {
		raw = append([]byte{0}, raw...)
	}
	return &prototypes.Decimal{Value: raw, Precision: 8, Scale: 2}
}

func containerMsg(t *testing.T) *test.ValueTypeContainers {
	t.Helper()
	return &test.ValueTypeContainers{
		Amounts:   []*prototypes.Decimal{containerDecimal(t, "1.11"), containerDecimal(t, "2.22")},
		AmountMap: map[string]*prototypes.Decimal{"a": containerDecimal(t, "3.33")},
		Nested:    &test.ValueTypeNested{Inner: containerDecimal(t, "4.44")},
		Label:     "hi",
	}
}

// containerEval evaluates a message-level CEL rule and returns its raw result.
func containerEval(t *testing.T, expr string, msg interface{}) interface{} {
	t.Helper()
	rule := schemaregistry.Rule{Name: "r", Kind: "CONDITION", Mode: "WRITE",
		Type: "CEL", Expr: expr}
	ctx := serde.RuleContext{
		Target: &schemaregistry.SchemaInfo{}, Subject: "t-value", Topic: "t",
		RuleMode: schemaregistry.Write, Rule: &rule, Index: 0,
		Rules: []schemaregistry.Rule{rule},
	}
	result, err := NewExecutor().Transform(ctx, msg)
	if err != nil {
		t.Fatalf("eval %q: %v", expr, err)
	}
	return result
}

func containerBool(t *testing.T, expr string, msg interface{}) bool {
	t.Helper()
	b, ok := containerEval(t, expr, msg).(bool)
	if !ok {
		t.Fatalf("eval %q: expected a bool", expr)
	}
	return b
}

// --- 1. the wrapper must not claim a container -------------------------------------------

func TestProtobufRepeatedFieldReportsItsSize(t *testing.T) {
	if !containerBool(t, `size(message.amounts) == 2`, containerMsg(t)) {
		t.Error("size(message.amounts) != 2")
	}
	// The twin: a wrong size must answer false rather than error, which is what tells a
	// working Size from a lost one.
	if containerBool(t, `size(message.amounts) == 3`, containerMsg(t)) {
		t.Error("size(message.amounts) == 3 answered true")
	}
}

func TestProtobufRepeatedDecimalIsIndexable(t *testing.T) {
	if !containerBool(t, `decimals.gt(message.amounts[0], decimal("1.00"))`, containerMsg(t)) {
		t.Error("amounts[0] > 1.00 answered false")
	}
	if containerBool(t, `decimals.gt(message.amounts[0], decimal("100.00"))`, containerMsg(t)) {
		t.Error("amounts[0] > 100.00 answered true")
	}
}

// A map is the same shape of mistake and was reached through a different route - it happened
// to survive only because its key is a string, which the wrapper's Get did accept.
func TestProtobufMapValueIsIndexable(t *testing.T) {
	if !containerBool(t, `decimals.gt(message.amount_map["a"], decimal("1.00"))`,
		containerMsg(t)) {
		t.Error(`amount_map["a"] > 1.00 answered false`)
	}
	if containerBool(t, `decimals.gt(message.amount_map["a"], decimal("100.00"))`,
		containerMsg(t)) {
		t.Error(`amount_map["a"] > 100.00 answered true`)
	}
}

// --- 2. an inline field rule binds `this` to the container itself -------------------------

func TestInlineFieldRuleIndexesAnArrayOfDecimals(t *testing.T) {
	rat := func(s string) *big.Rat {
		r, ok := new(big.Rat).SetString(s)
		if !ok {
			t.Fatalf("bad decimal %q", s)
		}
		return r
	}
	arr := []*big.Rat{rat("1.11"), rat("2.22")}
	v := NewValidator()
	for _, c := range []struct {
		expr string
		want bool
	}{
		{`decimals.gt(this[0], decimal('1.00'))`, true},
		{`decimals.gt(this[0], decimal('100.00'))`, false},
	} {
		got, err := v.Execute(serde.ValidationRule{Name: "fldArr", Expr: c.expr}, nil, arr)
		if err != nil {
			t.Fatalf("%s: %v", c.expr, err)
		}
		if got != c.want {
			t.Errorf("%s = %v, want %v", c.expr, got, c.want)
		}
	}
}

// --- 3. a message transform must not drop a container it echoes ---------------------------

func containerTransform(t *testing.T, expr string) *test.ValueTypeContainers {
	t.Helper()
	rule := schemaregistry.Rule{Name: "r", Kind: "TRANSFORM", Mode: "WRITE",
		Type: "CEL", Expr: expr}
	ctx := serde.RuleContext{
		Target: &schemaregistry.SchemaInfo{}, Subject: "t-value", Topic: "t",
		RuleMode: schemaregistry.Write, Rule: &rule, Index: 0,
		Rules: []schemaregistry.Rule{rule},
	}
	result, err := NewExecutor().Transform(ctx, containerMsg(t))
	if err != nil {
		t.Fatalf("transform %q: %v", expr, err)
	}
	out, ok := result.(*test.ValueTypeContainers)
	if !ok {
		t.Fatalf("expected the message to be rebuilt, got %T", result)
	}
	return out
}

const containerIdentity = `{"amounts": message.amounts, "amount_map": message.amount_map, ` +
	`"nested": message.nested, "label": message.label}`

func TestMessageTransformKeepsEveryContainerItEchoes(t *testing.T) {
	out := containerTransform(t, containerIdentity)

	if len(out.Amounts) != 2 {
		t.Errorf("amounts = %d elements, want 2", len(out.Amounts))
	}
	// The map was the one that vanished, and it vanished without an error.
	if got := out.AmountMap["a"]; got == nil {
		t.Error(`amount_map["a"] was dropped`)
	}
	if out.Nested == nil || out.Nested.Inner == nil {
		t.Error("nested was dropped")
	}
	if out.Label != "hi" {
		t.Errorf("label = %q, want hi", out.Label)
	}
}

// The discriminator: a rule that does not name the map must still leave it empty. Without
// this, "the map survived" could mean the transform never replaced the message at all.
func TestMessageTransformOmittingAContainerLeavesItEmpty(t *testing.T) {
	out := containerTransform(t,
		`{"amounts": message.amounts, "nested": message.nested, "label": message.label}`)

	if len(out.AmountMap) != 0 {
		t.Errorf("amount_map = %v, want empty", out.AmountMap)
	}
	if len(out.Amounts) != 2 {
		t.Errorf("amounts = %d elements, want 2", len(out.Amounts))
	}
}
