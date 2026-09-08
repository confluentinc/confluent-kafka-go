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

// Message-level CEL transforms over protobuf: the rule returns a map and the message is
// rebuilt from it.
//
// Before this the executor returned that map raw, which the protobuf serializer cannot
// write, so every message-level transform failed - including an identity one.
//
// The transform has replace semantics: the map is the new message, so a field the rule does
// not name is dropped and a null clears its field. Both are covered here, because they are
// the part a rule author is most likely to be surprised by.

package cel

import (
	"math/big"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	prototypes "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/variant"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/test"
)

// 0x04D2 = 1234 unscaled, i.e. 12.34 at scale 2.
var unscaled1234 = []byte{0x04, 0xD2}

func valueTypesMsg(t *testing.T) *test.ValueTypes {
	t.Helper()
	v, err := variant.ParseJSON(`{"name":"alice"}`)
	if err != nil {
		t.Fatal(err)
	}
	return &test.ValueTypes{
		Amount: &prototypes.Decimal{Value: unscaled1234, Precision: 8, Scale: 2},
		Ts:     &timestamppb.Timestamp{Seconds: 1700000000, Nanos: 123000000},
		Data:   &prototypes.Variant{Metadata: v.MetadataBytes(), Value: v.ValueBytes()},
		Label:  "hi",
		Count:  7,
	}
}

func transformMsg(t *testing.T, expr string, msg interface{}) *test.ValueTypes {
	t.Helper()
	rule := schemaregistry.Rule{Name: "r", Kind: "TRANSFORM", Mode: "WRITE",
		Type: "CEL", Expr: expr}
	ctx := serde.RuleContext{
		Target: &schemaregistry.SchemaInfo{}, Subject: "t-value", Topic: "t",
		RuleMode: schemaregistry.Write, Rule: &rule, Index: 0,
		Rules: []schemaregistry.Rule{rule},
	}
	result, err := NewExecutor().Transform(ctx, msg)
	if err != nil {
		t.Fatalf("transform %q: %v", expr, err)
	}
	out, ok := result.(*test.ValueTypes)
	if !ok {
		t.Fatalf("expected the message to be rebuilt, got %T", result)
	}
	return out
}

func unscaledOf(t *testing.T, d *prototypes.Decimal) string {
	t.Helper()
	if d == nil {
		return "<nil>"
	}
	return new(big.Int).SetBytes(d.Value).String()
}

func variantJSON(t *testing.T, v *prototypes.Variant) string {
	t.Helper()
	j, err := variant.New(v.Value, v.Metadata).ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	return j
}

const allFields = `"amount": message.amount, "ts": message.ts, ` +
	`"data": message.data, "label": message.label, "count": message.count`

// An identity transform is the cheapest regression test for a write-back path: it fails for
// any breakage in the plumbing, without depending on the computation.
func TestMessageTransformPassThrough(t *testing.T) {
	out := transformMsg(t, "{"+allFields+"}", valueTypesMsg(t))

	if got := unscaledOf(t, out.Amount); got != "1234" {
		t.Errorf("amount unscaled = %s, want 1234", got)
	}
	if out.Amount.Scale != 2 {
		t.Errorf("amount scale = %d, want 2", out.Amount.Scale)
	}
	if out.Ts.Seconds != 1700000000 || out.Ts.Nanos != 123000000 {
		t.Errorf("ts = %d.%09d, want 1700000000.123000000", out.Ts.Seconds, out.Ts.Nanos)
	}
	if got := variantJSON(t, out.Data); got != `{"name":"alice"}` {
		t.Errorf("data = %s", got)
	}
	if out.Label != "hi" || out.Count != 7 {
		t.Errorf("label/count = %q/%d", out.Label, out.Count)
	}
}

func TestMessageTransformComputedDecimal(t *testing.T) {
	out := transformMsg(t,
		`{"amount": decimals.add(decimal(message.amount), decimal("1.00")), `+
			`"ts": message.ts, "data": message.data, "label": message.label}`,
		valueTypesMsg(t))

	if got := unscaledOf(t, out.Amount); got != "1334" {
		t.Errorf("amount unscaled = %s, want 1334 (13.34)", got)
	}
	if out.Amount.Scale != 2 {
		t.Errorf("amount scale = %d, want 2", out.Amount.Scale)
	}
}

func TestMessageTransformComputedTimestamp(t *testing.T) {
	out := transformMsg(t,
		`{"amount": message.amount, "ts": message.ts + duration("60s"), `+
			`"data": message.data, "label": message.label}`,
		valueTypesMsg(t))

	if out.Ts.Seconds != 1700000060 || out.Ts.Nanos != 123000000 {
		t.Errorf("ts = %d.%09d, want 1700000060.123000000", out.Ts.Seconds, out.Ts.Nanos)
	}
}

// Asserted through the decoded JSON rather than the metadata bytes: metadata holds the field
// names, so {"name":"alice"} and {"name":"bob"} share it and comparing metadata would prove
// nothing.
func TestMessageTransformComputedVariant(t *testing.T) {
	out := transformMsg(t,
		`{"amount": message.amount, "ts": message.ts, `+
			`"data": variants.parseJson("{\"name\":\"bob\"}"), "label": message.label}`,
		valueTypesMsg(t))

	if got := variantJSON(t, out.Data); got != `{"name":"bob"}` {
		t.Errorf("data = %s, want {\"name\":\"bob\"}", got)
	}
}

// Replace semantics, and the consequence most likely to surprise: a rule naming only the
// field it changes discards everything else. Intended, but silent on protobuf - proto3 has
// no required fields, so nothing catches it.
func TestMessageTransformDropsUnnamedFields(t *testing.T) {
	out := transformMsg(t, `{"label": "changed"}`, valueTypesMsg(t))

	if out.Label != "changed" {
		t.Errorf("label = %q, want changed", out.Label)
	}
	if out.Amount != nil || out.Ts != nil || out.Data != nil {
		t.Errorf("expected the unnamed fields to be dropped, got amount=%v ts=%v data=%v",
			out.Amount, out.Ts, out.Data)
	}
	if out.Count != 0 {
		t.Errorf("count = %d, want 0", out.Count)
	}
}

// The idiom for preserving absence across a transform that echoes a field is
// `has(x) ? x : null`; without a null arm there would be no way to express it.
func TestMessageTransformNullClearsAField(t *testing.T) {
	out := transformMsg(t,
		`{"amount": null, "ts": message.ts, "data": message.data, "label": message.label}`,
		valueTypesMsg(t))

	if out.Amount != nil {
		t.Errorf("amount = %v, want cleared", out.Amount)
	}
	if out.Ts == nil || out.Label != "hi" {
		t.Errorf("the other fields should survive: ts=%v label=%q", out.Ts, out.Label)
	}
}

// The other face of replace: reading an absent field produces its default, so echoing it
// writes that default back. Documents the behaviour rather than endorsing it.
func TestMessageTransformEchoingAnAbsentFieldMaterialisesIt(t *testing.T) {
	absent := &test.ValueTypes{Label: "hi"}

	echoed := transformMsg(t, `{"amount": message.amount, "label": message.label}`, absent)
	if echoed.Amount == nil {
		t.Errorf("echoing an absent field is expected to materialise it")
	}

	guarded := transformMsg(t,
		`{"amount": has(message.amount) ? message.amount : null, "label": message.label}`,
		absent)
	if guarded.Amount != nil {
		t.Errorf("has(x) ? x : null must preserve absence, got %v", guarded.Amount)
	}
}

// A CONDITION answers with a bool, which must not be run through the message rebuild.
func TestMessageTransformLeavesConditionsAlone(t *testing.T) {
	rule := schemaregistry.Rule{Name: "r", Kind: "CONDITION", Mode: "WRITE", Type: "CEL",
		Expr: `decimals.gt(message.amount, decimal("10.00"))`}
	ctx := serde.RuleContext{
		Target: &schemaregistry.SchemaInfo{}, Subject: "t-value", Topic: "t",
		RuleMode: schemaregistry.Write, Rule: &rule, Index: 0,
		Rules: []schemaregistry.Rule{rule},
	}
	result, err := NewExecutor().Transform(ctx, valueTypesMsg(t))
	if err != nil {
		t.Fatal(err)
	}
	if b, ok := result.(bool); !ok || !b {
		t.Errorf("expected true, got %#v", result)
	}
}
