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

// An Avro decimal reaches a CEL_FIELD rule at the scale its schema declares.
//
// hamba decodes one into a *big.Rat, which normalises: 12.3400 at scale 4 and 12.34 at scale 2
// both arrive as 617/50, so a scale recovered from the value alone renders 12.34 either way.
// The reference reads BigDecimal(unscaled, scale) straight off the schema and renders 12.3400.
// This client is the only one whose decimal boundary carries no scale, which is why it needs
// the field's schema from the rule context.

import (
	"math/big"
	"strings"
	"testing"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov3"
)

const decimalScaleSchema = `{
  "type": "record",
  "name": "Scaled",
  "fields": [
    {"name": "amount",
     "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 4},
     "confluent:tags": ["AMOUNT"]},
    {"name": "plain", "type": "string"}
  ]
}`

type decimalScaleRec struct {
	Amount *big.Rat `avro:"amount"`
	Plain  string   `avro:"plain"`
}

// serializeFn is one serde's Serialize, so each case runs against both. avrov2 and avrov3 are
// built on *different* Avro libraries, and the slot the scale is read from is one of that
// library's types - so a check that names one silently passes for the other.
type serializeFn func(t *testing.T, client schemaregistry.Client, subject string,
	amount *big.Rat) error

func serializeV2(t *testing.T, client schemaregistry.Client, subject string,
	amount *big.Rat) error {
	t.Helper()
	sc := avrov2.NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, err := avrov2.NewSerializer(client, serde.ValueSerde, sc)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ser.Serialize(subject, &decimalScaleRec{Amount: amount, Plain: "hi"})
	return err
}

func serializeV3(t *testing.T, client schemaregistry.Client, subject string,
	amount *big.Rat) error {
	t.Helper()
	sc := avrov3.NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, err := avrov3.NewSerializer(client, serde.ValueSerde, sc)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ser.Serialize(subject, &decimalScaleRec{Amount: amount, Plain: "hi"})
	return err
}

func bothSerdes() map[string]serializeFn {
	return map[string]serializeFn{"avrov2": serializeV2, "avrov3": serializeV3}
}

func runDecimalScale(t *testing.T, subject, expr string, serialize serializeFn) string {
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
		Schema: decimalScaleSchema, SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{rule}},
	}
	if _, err := client.Register(subject+"-value", info, false); err != nil {
		t.Fatal(err)
	}
	if err := serialize(t, client, subject, new(big.Rat).SetFrac64(123400, 10000)); err != nil {
		return err.Error()
	}
	return ""
}

func TestAvroDecimalReachesARuleAtItsDeclaredScale(t *testing.T) {
	for name, serialize := range bothSerdes() {
		if got := runDecimalScale(t, "ds1"+name,
			`string(value) == "12.3400"`, serialize); got != "" {
			t.Errorf("%s: expected the declared scale of 4, got %q", name, got)
		}
		// The discriminator: the scale derived from the reduced big.Rat, which is what this
		// used to render.
		got := runDecimalScale(t, "ds2"+name, `string(value) == "12.34"`, serialize)
		if !strings.Contains(got, "Expr failed") {
			t.Errorf("%s: expected the value-derived scale to be gone, got %q", name, got)
		}
	}
}

// The must-pass twin: comparisons are numeric, so the added scale must not disturb them.
func TestDeclaredScaleLeavesComparisonsAlone(t *testing.T) {
	for name, serialize := range bothSerdes() {
		if got := runDecimalScale(t, "ds3"+name,
			`decimals.gt(decimal(value), decimal("10.00"))`, serialize); got != "" {
			t.Errorf("%s: expected 12.3400 > 10.00 to pass, got %q", name, got)
		}
		if got := runDecimalScale(t, "ds4"+name,
			`value == decimal("12.34")`, serialize); got != "" {
			t.Errorf("%s: expected 12.3400 == 12.34 numerically, got %q", name, got)
		}
	}
}
