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

// CEL_FIELD rules over protobuf decimal and timestamp fields .
//
// Avro carries these two as logical types on a primitive, so the field is a leaf and a field
// rule reaches it. Protobuf carries them as messages, so the walk used to descend *past* the
// field and transform value/scale or seconds/nanos one at a time - meaning a rule tagged for
// the field never fired at all, and the message came back unchanged with no error. A silent
// no-op is the worst of the three possible outcomes: the rule author gets no signal.
//
// This is the port of the JVM client's #4538 (isCelLeafMessage). Variant is deliberately not a
// leaf - it is a record in Avro too, so skipping it is the behaviour that matches, and a
// variant is reached with a message-level CEL rule instead.

package protobuf

import (
	"math/big"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	prototypes "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/variant"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/test"
)

// 0x04D2 = 1234 unscaled, i.e. 12.34 at scale 2.
var vtUnscaled1234 = []byte{0x04, 0xD2}

func vtMessage(t *testing.T) *test.ValueTypes {
	t.Helper()
	v, err := variant.ParseJSON(`{"name":"alice"}`)
	if err != nil {
		t.Fatal(err)
	}
	return &test.ValueTypes{
		Amount: &prototypes.Decimal{Value: vtUnscaled1234, Precision: 8, Scale: 2},
		Ts:     &timestamppb.Timestamp{Seconds: 1700000000, Nanos: 123000000},
		Data:   &prototypes.Variant{Metadata: v.MetadataBytes(), Value: v.ValueBytes()},
		Label:  "hi",
	}
}

// vtRun drives the client's own walker with one tagged CEL_FIELD rule.
func vtRun(t *testing.T, expr string, kind string, tag string,
	msg *test.ValueTypes) (*test.ValueTypes, error) {
	t.Helper()
	rule := schemaregistry.Rule{Name: "r", Kind: kind, Mode: "WRITE", Type: "CEL_FIELD",
		Expr: expr, Tags: []string{tag}}
	ctx := serde.RuleContext{
		Target: &schemaregistry.SchemaInfo{}, Subject: "t-value", Topic: "t",
		RuleMode: schemaregistry.Write, Rule: &rule, Index: 0,
		Rules: []schemaregistry.Rule{rule},
	}
	ft, err := cel.NewFieldExecutor().(serde.FieldRuleExecutor).NewTransform(ctx)
	if err != nil {
		t.Fatal(err)
	}
	out, err := transform(ctx, msg.ProtoReflect().Descriptor(), msg, ft)
	if err != nil {
		return nil, err
	}
	result, ok := out.(*test.ValueTypes)
	if !ok {
		t.Fatalf("expected the message back, got %T", out)
	}
	return result, nil
}

func vtDecimal(t *testing.T, d *prototypes.Decimal) string {
	t.Helper()
	if d == nil {
		return "<nil>"
	}
	return new(big.Int).SetBytes(d.Value).String()
}

// The declared type is what makes CEL_FIELD apply at all: a RECORD is skipped outright.
func TestValueTypeFieldTypesMatchAvro(t *testing.T) {
	fields := (&test.ValueTypes{}).ProtoReflect().Descriptor().Fields()

	for _, tc := range []struct {
		name string
		want serde.FieldType
	}{
		{"amount", serde.TypeBytes},
		{"ts", serde.TypeLong},
		// Variant stays a record, as in Avro - not a leaf.
		{"data", serde.TypeRecord},
		{"label", serde.TypeString},
	} {
		if got := getType(fields.ByName(protoreflect.Name(tc.name))); got != tc.want {
			t.Errorf("getType(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Before the port this reported nothing because the rule never ran.
func TestValueTypeDecimalCondition(t *testing.T) {
	if _, err := vtRun(t, `decimals.gt(decimal(value), decimal("10.00"))`,
		"CONDITION", "AMOUNT", vtMessage(t)); err != nil {
		t.Errorf("expected the condition to pass, got %v", err)
	}
}

// The must-fail twin. Without it the test above would also pass if no rule ran at all - which
// is exactly how the defect hid.
func TestValueTypeDecimalConditionFails(t *testing.T) {
	if _, err := vtRun(t, `decimals.gt(decimal(value), decimal("1000.00"))`,
		"CONDITION", "AMOUNT", vtMessage(t)); err == nil {
		t.Error("expected a violation; the rule did not fire")
	}
}

func TestValueTypeTimestampCondition(t *testing.T) {
	if _, err := vtRun(t, `value > timestamp("2000-01-01T00:00:00Z")`,
		"CONDITION", "TS", vtMessage(t)); err != nil {
		t.Errorf("expected the condition to pass, got %v", err)
	}
}

func TestValueTypeTimestampConditionFails(t *testing.T) {
	if _, err := vtRun(t, `value > timestamp("2050-01-01T00:00:00Z")`,
		"CONDITION", "TS", vtMessage(t)); err == nil {
		t.Error("expected a violation; the rule did not fire")
	}
}

// The rule returns an *apd.Decimal; it has to be encoded back into the message.
func TestValueTypeDecimalTransform(t *testing.T) {
	out, err := vtRun(t, `decimals.add(decimal(value), decimal("1.00"))`,
		"TRANSFORM", "AMOUNT", vtMessage(t))
	if err != nil {
		t.Fatal(err)
	}

	if got := vtDecimal(t, out.Amount); got != "1334" {
		t.Errorf("amount unscaled = %s, want 1334 (13.34)", got)
	}
	if out.Amount.Scale != 2 {
		t.Errorf("scale = %d, want 2", out.Amount.Scale)
	}
}

func TestValueTypeTimestampTransform(t *testing.T) {
	out, err := vtRun(t, `value + duration("60s")`, "TRANSFORM", "TS", vtMessage(t))
	if err != nil {
		t.Fatal(err)
	}

	if out.Ts.Seconds != 1700000060 || out.Ts.Nanos != 123000000 {
		t.Errorf("ts = %d.%09d, want 1700000060.123000000", out.Ts.Seconds, out.Ts.Nanos)
	}
}

// The pass-through: the cheapest check that the encode inverts the decode exactly.
func TestValueTypeIdentityTransform(t *testing.T) {
	out, err := vtRun(t, "value", "TRANSFORM", "AMOUNT", vtMessage(t))
	if err != nil {
		t.Fatal(err)
	}

	if got := vtDecimal(t, out.Amount); got != "1234" {
		t.Errorf("amount unscaled = %s, want 1234 (12.34)", got)
	}
	if out.Amount.Scale != 2 {
		t.Errorf("scale = %d, want 2", out.Amount.Scale)
	}
}

// Variant is a record in both formats, so a field rule must not reach it. The rule below would
// raise if it ran, so a clean return means it was skipped.
func TestValueTypeVariantIsStillSkipped(t *testing.T) {
	out, err := vtRun(t, `variants.type(value) == "not-a-type"`,
		"CONDITION", "DATA", vtMessage(t))
	if err != nil {
		t.Fatalf("a variant field must be skipped, not evaluated: %v", err)
	}
	if out.Data == nil || len(out.Data.Metadata) == 0 {
		t.Error("the variant should be untouched")
	}
}

// A rule returning something that is neither a decimal nor the message is a rule-authoring
// mistake; it must be named rather than written back as a default.
func TestValueTypeWrongResultTypeIsReported(t *testing.T) {
	_, err := vtRun(t, `"not a decimal"`, "TRANSFORM", "AMOUNT", vtMessage(t))
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "expected a decimal") {
		t.Errorf("error should name what was expected, got %v", err)
	}
}

// A *repeated* value-type field is a list of single values, not a list of records. The list
// branch of the walk descended into each element as a message, so the walk reached value/scale
// one at a time, the rule tagged for the field never fired, and the field came back **unchanged
// with no error** — a silent no-op, and the worst of the three possible outcomes. #4538 gave the
// scalar case its leaf handling; a list never reached it. The reference answers `[2.11, 3.22]`.

// c9Containers: amounts = [1.11, 2.22], nested.inner = 4.44, one map entry a = 3.33.
func vtContainerMessage(t *testing.T) *test.ValueTypeContainers {
	t.Helper()
	d := func(unscaled int64) *prototypes.Decimal {
		// Two's-complement, not magnitude: big.Int.Bytes() drops the sign, so 222 (0xDE)
		// would be read back as -34 and every arithmetic assertion below would be measuring
		// the wrong input. `decimals.add(-0.34, 1.00)` is 0.66, which looks indistinguishable
		// from a client dropping a byte.
		raw := new(big.Int).SetInt64(unscaled).Bytes()
		if len(raw) > 0 && raw[0]&0x80 != 0 {
			raw = append([]byte{0}, raw...)
		}
		return &prototypes.Decimal{Value: raw, Precision: 8, Scale: 2}
	}
	return &test.ValueTypeContainers{
		Amounts:   []*prototypes.Decimal{d(111), d(222)},
		AmountMap: map[string]*prototypes.Decimal{"a": d(333)},
		Nested:    &test.ValueTypeNested{Inner: d(444)},
		Label:     "hi",
		// A repeated *scalar*, tagged CODES: the element type decides which arm of the list
		// branch runs, and no client's fixture had one.
		Codes: []string{"a", "b"},
	}
}

func vtRunContainer(t *testing.T, expr, kind, tag string) (*test.ValueTypeContainers, error) {
	t.Helper()
	msg := vtContainerMessage(t)
	rule := schemaregistry.Rule{Name: "r", Kind: kind, Mode: "WRITE", Type: "CEL_FIELD",
		Expr: expr, Tags: []string{tag}}
	ctx := serde.RuleContext{
		Target: &schemaregistry.SchemaInfo{}, Subject: "t-value", Topic: "t",
		RuleMode: schemaregistry.Write, Rule: &rule, Index: 0,
		Rules: []schemaregistry.Rule{rule},
	}
	ft, err := cel.NewFieldExecutor().(serde.FieldRuleExecutor).NewTransform(ctx)
	if err != nil {
		t.Fatal(err)
	}
	out, err := transform(ctx, msg.ProtoReflect().Descriptor(), msg, ft)
	if err != nil {
		return nil, err
	}
	result, ok := out.(*test.ValueTypeContainers)
	if !ok {
		t.Fatalf("expected the message back, got %T", out)
	}
	return result, nil
}

func vtAmounts(t *testing.T, m *test.ValueTypeContainers) []string {
	t.Helper()
	var out []string
	for _, a := range m.Amounts {
		out = append(out, vtDecimal(t, a))
	}
	return out
}

func TestValueTypeRepeatedDecimalTransform(t *testing.T) {
	out, err := vtRunContainer(t, `decimals.add(decimal(value), decimal("1.00"))`,
		"TRANSFORM", "AMOUNTS")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := vtAmounts(t, out); len(got) != 2 || got[0] != "211" || got[1] != "322" {
		t.Errorf("amounts = %v, want [211 322] (2.11, 3.22 at scale 2)", got)
	}
}

// The must-pass twin: an identity rule hands back the message it was given, and the per-element
// path has to accept that as readily as a computed decimal. Without it, "the values changed"
// could be satisfied by a rebuild that mangles an untouched element.
func TestValueTypeRepeatedIdentityTransform(t *testing.T) {
	out, err := vtRunContainer(t, "value", "TRANSFORM", "AMOUNTS")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := vtAmounts(t, out); len(got) != 2 || got[0] != "111" || got[1] != "222" {
		t.Errorf("amounts = %v, want [111 222] unchanged", got)
	}
}

// A scalar *condition* over a repeated field passes even when it is false for every element:
// the reference collects the per-element verdicts into a list and tests
// `Boolean.FALSE.equals(list)`, which a list never satisfies. The
// verdict must also not be written into the list.
func TestValueTypeRepeatedConditionVerdictIsDiscarded(t *testing.T) {
	out, err := vtRunContainer(t, `decimals.gt(decimal(value), decimal("100.00"))`,
		"CONDITION", "AMOUNTS")
	if err != nil {
		t.Fatalf("a false condition over a repeated field must not fail: %v", err)
	}
	if got := vtAmounts(t, out); len(got) != 2 || got[0] != "111" || got[1] != "222" {
		t.Errorf("amounts = %v, want [111 222] untouched by a condition", got)
	}
}

// A condition over a repeated *scalar* field is the same contract as over a repeated decimal,
// and it used to differ: the decimal path drops the verdict (transformValueTypeLeafInList) while
// the scalar path raised on it.
//
// Settled from the reference's own source rather than a probe: its protobuf walk maps a repeated
// field to a new List of the per-element results and then tests `Boolean.FALSE.equals(newValue)`
// on it, which a List never is - so the verdict is dropped for *every* element type, not just the
// ones a fixture happens to cover.
func TestRepeatedScalarConditionVerdictIsDiscarded(t *testing.T) {
	out, err := vtRunContainer(t, `value == "zzz"`, "CONDITION", "CODES")
	if err != nil {
		t.Fatalf("a false condition over a repeated scalar must not raise: %v", err)
	}
	if got := out.Codes; len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("codes = %v, want [a b] unchanged", got)
	}
}

// The must-pass twin, so the test above cannot be satisfied by a walk that skipped the field.
func TestRepeatedScalarConditionThatHoldsAlsoPasses(t *testing.T) {
	out, err := vtRunContainer(t, `value == "a" || value == "b"`, "CONDITION", "CODES")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out.Codes; len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("codes = %v, want [a b] unchanged", got)
	}
}

// The discriminator that keeps the fix from being too broad: dropping the verdict is a property
// of being *inside a container*, not of being a scalar. A singular field must still raise.
func TestSingularScalarConditionStillRaises(t *testing.T) {
	if _, err := vtRunContainer(t, `value == "zzz"`, "CONDITION", "LABEL"); err == nil {
		t.Error("a false condition on a singular scalar field must raise")
	}
}

// And a transform over the same repeated scalar still writes every element - the verdict is what
// is dropped, not the walk.
func TestRepeatedScalarTransformWritesEveryElement(t *testing.T) {
	out, err := vtRunContainer(t, `value + "!"`, "TRANSFORM", "CODES")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out.Codes; len(got) != 2 || got[0] != "a!" || got[1] != "b!" {
		t.Errorf("codes = %v, want [a! b!]", got)
	}
}

// A tag scopes a rule to its own field, on the container path as much as anywhere else.
//
// This is the property the drifted copy of transformValueTypeLeaf broke: its condition branch
// had no tag check, so a rule tagged CODES was also evaluated against the repeated *decimal*
// field and failed to compile. Asserted here on the transform path, where the damage would be
// silent rather than loud - a wrong field quietly rewritten.
func TestARepeatedFieldTagOnlyReachesItsOwnField(t *testing.T) {
	out, err := vtRunContainer(t, `value + "!"`, "TRANSFORM", "CODES")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out.Codes; len(got) != 2 || got[0] != "a!" || got[1] != "b!" {
		t.Errorf("codes = %v, want [a! b!]", got)
	}
	if got := vtAmounts(t, out); len(got) != 2 || got[0] != "111" || got[1] != "222" {
		t.Errorf("amounts = %v, want [111 222] untouched by a CODES-tagged rule", got)
	}
	if out.Label != "hi" {
		t.Errorf("label = %q, want hi untouched", out.Label)
	}
}
