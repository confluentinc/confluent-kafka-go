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

// CEL_FIELD rules over protobuf decimal and timestamp fields (capabilities C4 and C5).
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

// C4. Before the port this reported nothing because the rule never ran.
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

// C5. The rule returns an *apd.Decimal; it has to be encoded back into the message.
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
