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

func runDecimalScale(t *testing.T, subject, expr string) string {
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
	sc := avrov2.NewSerializerConfig()
	sc.AutoRegisterSchemas = false
	sc.UseLatestVersion = true
	ser, err := avrov2.NewSerializer(client, serde.ValueSerde, sc)
	if err != nil {
		t.Fatal(err)
	}
	obj := decimalScaleRec{Amount: new(big.Rat).SetFrac64(123400, 10000), Plain: "hi"}
	if _, err := ser.Serialize(subject, &obj); err != nil {
		return err.Error()
	}
	return ""
}

func TestAvroDecimalReachesARuleAtItsDeclaredScale(t *testing.T) {
	if got := runDecimalScale(t, "ds1", `string(value) == "12.3400"`); got != "" {
		t.Fatalf("expected the declared scale of 4, got %q", got)
	}
	// The discriminator: the scale derived from the reduced big.Rat, which is what this used
	// to render.
	got := runDecimalScale(t, "ds2", `string(value) == "12.34"`)
	if !strings.Contains(got, "Expr failed") {
		t.Fatalf("expected the value-derived scale to be gone, got %q", got)
	}
}

// The must-pass twin: comparisons are numeric, so the added scale must not disturb them.
func TestDeclaredScaleLeavesComparisonsAlone(t *testing.T) {
	if got := runDecimalScale(t, "ds3",
		`decimals.gt(decimal(value), decimal("10.00"))`); got != "" {
		t.Fatalf("expected 12.3400 > 10.00 to pass, got %q", got)
	}
	if got := runDecimalScale(t, "ds4", `value == decimal("12.34")`); got != "" {
		t.Fatalf("expected 12.3400 == 12.34 numerically, got %q", got)
	}
}
