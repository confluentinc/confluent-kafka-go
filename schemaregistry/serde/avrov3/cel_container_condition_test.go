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

// A CEL_FIELD rule on an array or a map field.
//
// A condition is evaluated once per element and the verdicts are then dropped: the reference
// collects them into an untyped list, and the field-level check that raises tests for `false`,
// which a list never is. A condition therefore does not apply to a container field - decided as
// the intended contract.
//
// This walk assigned each verdict back into the container as it went, and reflect.Set panics
// because a []*big.Rat element cannot hold a bool - so the field failed whatever the rule
// answered. The transform cases below are the other half of the contract: dropping a verdict
// must not become skipping the field.

import (
	"math/big"
	"strings"
	"testing"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

const celContainerSchema = `{
  "type": "record",
  "name": "C9",
  "fields": [
    {"name": "amounts",
     "type": {"type": "array", "items": {"type": "bytes", "logicalType": "decimal", "precision": 8, "scale": 2}},
     "confluent:tags": ["AMOUNTS"]},
    {"name": "amountMap",
     "type": {"type": "map", "values": {"type": "bytes", "logicalType": "decimal", "precision": 8, "scale": 2}},
     "confluent:tags": ["AMOUNTMAP"]},
    {"name": "label", "type": "string"}
  ]
}`

type celContainerRec struct {
	Amounts   []*big.Rat          `avro:"amounts"`
	AmountMap map[string]*big.Rat `avro:"amountMap"`
	Label     string              `avro:"label"`
}

func celContainerRat(t *testing.T, s string) *big.Rat {
	t.Helper()
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		t.Fatalf("bad decimal %q", s)
	}
	return r
}

func celContainerRecord(t *testing.T) *celContainerRec {
	return &celContainerRec{
		Amounts:   []*big.Rat{celContainerRat(t, "1.11"), celContainerRat(t, "2.22")},
		AmountMap: map[string]*big.Rat{"a": celContainerRat(t, "3.33")},
		Label:     "hi",
	}
}

// celContainerRun serializes the record under one CEL_FIELD rule and reads it back, so what is
// asserted is what actually went on the wire.
func celContainerRun(t *testing.T, subject, kind, tag, expr string) (*celContainerRec, error) {
	t.Helper()
	cel.Register()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatal(err)
	}
	rule := schemaregistry.Rule{
		Name: "r", Kind: kind, Mode: "WRITE", Type: "CEL_FIELD",
		Tags: []string{tag}, Expr: expr,
	}
	info := schemaregistry.SchemaInfo{
		Schema: celContainerSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, err := client.Register(subject+"-value", info, false); err != nil {
		t.Fatal(err)
	}
	sc := NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, err := NewSerializer(client, serde.ValueSerde, sc)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := ser.Serialize(subject, celContainerRecord(t))
	if err != nil {
		return nil, err
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	if err != nil {
		t.Fatal(err)
	}
	deser.Client = client
	var got celContainerRec
	if err := deser.DeserializeInto(subject, payload, &got); err != nil {
		t.Fatal(err)
	}
	return &got, nil
}

func celContainerAmounts(r *celContainerRec) []string {
	out := make([]string, 0, len(r.Amounts))
	for _, a := range r.Amounts {
		out = append(out, a.FloatString(2))
	}
	return out
}

func TestCelFieldConditionOnAnArrayHoldsAndLeavesItAlone(t *testing.T) {
	got, err := celContainerRun(t, "celcontarrpos", "CONDITION", "AMOUNTS",
		`decimals.gt(decimal(value), decimal("1.00"))`)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if want := []string{"1.11", "2.22"}; strings.Join(celContainerAmounts(got), ",") !=
		strings.Join(want, ",") {
		t.Errorf("amounts = %v, want %v", celContainerAmounts(got), want)
	}
}

// The twin: the verdict is false for both elements and the walk
// still has to pass, because a condition does not apply to a container field. Before the fix
// this panicked, exactly as the twin above did.
func TestCelFieldConditionOnAnArrayIsDroppedRatherThanRaised(t *testing.T) {
	got, err := celContainerRun(t, "celcontarrneg", "CONDITION", "AMOUNTS",
		`decimals.gt(decimal(value), decimal("100.00"))`)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if want := []string{"1.11", "2.22"}; strings.Join(celContainerAmounts(got), ",") !=
		strings.Join(want, ",") {
		t.Errorf("amounts = %v, want %v", celContainerAmounts(got), want)
	}
}

// A map is the same contract, and reachable in Avro in a way it is not in protobuf: the Avro
// walk descends into map values, so a verdict landed in one of those too.
func TestCelFieldConditionOnAMapIsDroppedRatherThanRaised(t *testing.T) {
	got, err := celContainerRun(t, "celcontmapneg", "CONDITION", "AMOUNTMAP",
		`decimals.gt(decimal(value), decimal("100.00"))`)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if got.AmountMap["a"].FloatString(2) != "3.33" {
		t.Errorf(`map["a"] = %v, want 3.33`, got.AmountMap["a"].FloatString(2))
	}
}

// The discriminator for the three above: dropping the verdict must not become skipping the
// field. A rule that cannot evaluate on a decimal has to surface, which it can only do if
// every element was handed to it.
func TestCelFieldConditionInAContainerIsStillEvaluatedPerElement(t *testing.T) {
	if _, err := celContainerRun(t, "celcontraise", "CONDITION", "AMOUNTS",
		`variants.type(value) == "object"`); err == nil {
		t.Error("expected the rule to fail on a decimal element, got no error")
	}
}

// And a transform over the same fields still writes every element - the verdict is what is
// dropped, not the walk.
func TestCelFieldTransformOnAContainerStillWritesEveryElement(t *testing.T) {
	got, err := celContainerRun(t, "celcontarrxf", "TRANSFORM", "AMOUNTS",
		`decimals.add(decimal(value), decimal("1.00"))`)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if want := []string{"2.11", "3.22"}; strings.Join(celContainerAmounts(got), ",") !=
		strings.Join(want, ",") {
		t.Errorf("amounts = %v, want %v", celContainerAmounts(got), want)
	}
}

func TestCelFieldTransformOnAMapStillWritesEveryValue(t *testing.T) {
	got, err := celContainerRun(t, "celcontmapxf", "TRANSFORM", "AMOUNTMAP",
		`decimals.add(decimal(value), decimal("1.00"))`)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if got.AmountMap["a"].FloatString(2) != "4.33" {
		t.Errorf(`map["a"] = %v, want 4.33`, got.AmountMap["a"].FloatString(2))
	}
}
