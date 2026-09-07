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

// A rule over a PRESENT value in a ["null", T] union.
//
// Picking the union branch means asking hamba's type resolver to name the value's Go type, and
// its registrations are not consistent about pointers: a nullable decimal is known as *big.Rat
// and not as big.Rat, while a nullable string is known as string and not as *string. The walk
// dereferenced before asking, so every present decimal failed serialization outright with
// "avro: unable to resolve type big.Rat" - and asking with the pointer instead would have failed
// every nullable primitive. Both shapes are tried now.
//
// An absent value is tested for separately rather than left to the resolver: a nil pointer still
// has a nameable type, so the resolver would pick the value branch and encode a zero.

import (
	"math/big"
	"strings"
	"testing"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/hamba/avro/v2"
)

func nullableRat(t *testing.T, s string) *big.Rat {
	t.Helper()
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		t.Fatalf("bad decimal %q", s)
	}
	return r
}

// runBothPresent serializes with the nullable decimal AND the nullable string both present,
// under a CEL_FIELD rule tagged for the decimal.
//
// The rule targets one field but the walk visits every field, so the nullable string's union has
// to be resolved too. That is what makes this a guard on the *direction* of the fix: the
// resolver knows string and not *string, so resolving only the pointer form fails here while
// the decimal above passes.
func runBothPresent(t *testing.T, subject, expr string, amount *big.Rat, note *string) string {
	t.Helper()
	Register()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatal(err)
	}
	rule := schemaregistry.Rule{
		Name: "r", Kind: "CONDITION", Mode: "WRITE", Type: "CEL_FIELD",
		Tags: []string{"AMOUNT"}, Expr: expr,
	}
	info := schemaregistry.SchemaInfo{
		Schema: nullFieldSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, err := client.Register(subject+"-value", info, false); err != nil {
		t.Fatal(err)
	}
	sc := avrov2.NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, err := avrov2.NewSerializer(client, serde.ValueSerde, sc)
	if err != nil {
		t.Fatal(err)
	}
	obj := nullFieldRec{Amount: amount, Note: note, Plain: "hi"}
	if _, err := ser.Serialize(subject, &obj); err != nil {
		return err.Error()
	}
	return ""
}

func TestPresentDecimalInANullableUnionEvaluates(t *testing.T) {
	if got := runNullField(t, "nu1", `decimals.gt(decimal(value), decimal("10.00"))`,
		nullableRat(t, "12.34")); got != "" {
		t.Fatalf("a present decimal in a nullable field must evaluate, got %q", got)
	}
}

// The twin: the rule must be able to answer no, or the test above is satisfied by a walk that
// never reached the field.
func TestPresentDecimalInANullableUnionCanFail(t *testing.T) {
	got := runNullField(t, "nu2", `decimals.gt(decimal(value), decimal("100.00"))`,
		nullableRat(t, "12.34"))
	if !strings.Contains(got, "Expr failed") {
		t.Fatalf("expected the condition to fail, got %q", got)
	}
}

// The other pointer shape, and the guard on the fix's direction: a present nullable *string*
// alongside the decimal. Resolving only the pointer form fixes the decimal and breaks this.
func TestPresentStringInANullableUnionAlsoResolves(t *testing.T) {
	note := "hello"
	if got := runBothPresent(t, "nu3", `decimals.gt(decimal(value), decimal("10.00"))`,
		nullableRat(t, "12.34"), &note); got != "" {
		t.Fatalf("a present nullable string must resolve alongside the decimal, got %q", got)
	}
}

// An absent value still takes the null branch rather than encoding a zero.
//
// Asserted on the wire, and it has to be: picking the value branch for a nil pointer encodes a
// zero *successfully*, and the rule still sees nil either way because the leaf dereferences. A
// test that only checked for the absence of an error would pass while the null was being
// silently replaced.
func TestAbsentDecimalStillTakesTheNullBranch(t *testing.T) {
	rule := schemaregistry.Rule{
		Name: "r", Kind: "CONDITION", Mode: "WRITE", Type: "CEL_FIELD",
		Tags: []string{"AMOUNT"}, Expr: "value == null",
	}
	got := wireOf(t, "nu4", rule, &nullFieldRec{Plain: "hi"})
	if got["amount"] != nil {
		t.Errorf("amount = %#v, want nil - the null branch, not a zero", got["amount"])
	}
}

// wireOf serializes obj under one rule and decodes the payload as a map, so what is asserted is
// what was written rather than what hamba's struct mapping makes of it.
func wireOf(t *testing.T, subject string, rule schemaregistry.Rule,
	obj *nullFieldRec) map[string]interface{} {
	t.Helper()
	Register()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatal(err)
	}
	info := schemaregistry.SchemaInfo{
		Schema: nullFieldSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, err := client.Register(subject+"-value", info, false); err != nil {
		t.Fatal(err)
	}
	sc := avrov2.NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, err := avrov2.NewSerializer(client, serde.ValueSerde, sc)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := ser.Serialize(subject, obj)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	// Past the 5-byte schema-id framing.
	var got map[string]interface{}
	if err := avro.Unmarshal(avro.MustParse(nullFieldSchema), payload[5:], &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return got
}

// A computed decimal written into the union reaches the wire correctly.
//
// Asserted on the wire for a second reason: hamba cannot decode a ["null", decimal] union into a
// *big.Rat struct field. It yields a zero with no error, on 2.24 and on 2.31, and reproduces with
// a plain struct round trip and no rules involved - which is why this was first written up as a
// broken *write*.
func TestComputedDecimalReachesTheWire(t *testing.T) {
	rule := schemaregistry.Rule{
		Name: "r", Kind: "TRANSFORM", Mode: "WRITE", Type: "CEL",
		Expr: `{"amount": decimal("2.34"), "note": message.Note, "plain": message.Plain}`,
	}
	got := wireOf(t, "nu5", rule, &nullFieldRec{Plain: "hi"})

	branch, ok := got["amount"].(map[string]interface{})
	if !ok {
		t.Fatalf("amount = %#v, want the union's branch map", got["amount"])
	}
	rat, ok := branch["bytes.decimal"].(*big.Rat)
	if !ok {
		t.Fatalf("amount branch = %#v, want a *big.Rat", branch)
	}
	if rat.FloatString(2) != "2.34" {
		t.Errorf("amount = %s, want 2.34", rat.FloatString(2))
	}
}

func TestPresentDecimalInANullableUnionEvaluatesInV3(t *testing.T) {
	if got := runNullFieldV3(t, "nu6", `decimals.gt(decimal(value), decimal("10.00"))`,
		nullableRat(t, "12.34")); got != "" {
		t.Fatalf("a present decimal in a nullable field must evaluate in v3, got %q", got)
	}
	got := runNullFieldV3(t, "nu7", `decimals.gt(decimal(value), decimal("100.00"))`,
		nullableRat(t, "12.34"))
	if !strings.Contains(got, "Expr failed") {
		t.Fatalf("expected the v3 condition to fail, got %q", got)
	}
}
