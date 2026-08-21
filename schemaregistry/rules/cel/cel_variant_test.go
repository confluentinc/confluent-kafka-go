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
	"testing"

	prototypes "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/variant"
)

// A JSON document exercising objects, arrays, an explicit null, and nesting.
const variantDoc = `{"name":"alice","age":30,"explicit":null,"nested":{"x":1},"scores":[10,20,30]}`

func TestVariantFunctions(t *testing.T) {
	cases := []struct {
		expr     string
		expected bool
	}{
		{"variants.type(variants.parseJson(this)) == 'object'", true},
		{"variants.as(variants.field(variants.parseJson(this), 'name'), 'string') == 'alice'", true},
		{"variants.as(variants.field(variants.parseJson(this), 'age'), 'int') == 30", true},
		// A missing field is CEL null (absent); an explicit JSON null is a present variant-null.
		{"variants.field(variants.parseJson(this), 'missing') == null", true},
		{"variants.isNull(variants.field(variants.parseJson(this), 'explicit'))", true},
		{"!variants.isNull(variants.field(variants.parseJson(this), 'missing'))", true},
		{"variants.as(variants.path(variants.parseJson(this), '$.nested.x'), 'int') == 1", true},
		{"variants.as(variants.index(variants.field(variants.parseJson(this), 'scores'), 2), 'int') == 30", true},
		// tryAs returns CEL null on a type mismatch (age is an int, not a string).
		{"variants.tryAs(variants.field(variants.parseJson(this), 'age'), 'string') == null", true},
		{`variants.toJson(variants.field(variants.parseJson(this), 'nested')) == '{"x":1}'`, true},
	}
	for _, tc := range cases {
		if got := evalBool(t, tc.expr, variantDoc); got != tc.expected {
			t.Errorf("expr %q = %v, want %v", tc.expr, got, tc.expected)
		}
	}
}

// A confluent.type.Variant proto message bound as `this`; variant(dyn) unwraps it.
func TestProtoConfluentTypeVariantIntoCel(t *testing.T) {
	pv, err := variant.ParseJSON(variantDoc)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	msg := &prototypes.Variant{Metadata: pv.MetadataBytes(), Value: pv.ValueBytes()}
	if !evalBool(t, "variants.as(variants.field(variant(this), 'age'), 'int') == 30", msg) {
		t.Errorf("proto confluent.type.Variant did not marshal into CEL correctly")
	}
}

// An Avro variant field decodes (generically) to a map with metadata/value byte entries;
// variant(this.data) accepts it, mirroring the Python client's map handling.
func TestAvroVariantMapIntoCel(t *testing.T) {
	pv, err := variant.ParseJSON(`{"name":"alice","age":30}`)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	type holder struct {
		Data map[string]interface{} `avro:"data"`
	}
	rec := holder{Data: map[string]interface{}{
		"metadata": pv.MetadataBytes(),
		"value":    pv.ValueBytes(),
	}}
	if !evalBool(t, "variants.as(variants.field(variant(this.data), 'name'), 'string') == 'alice'", rec) {
		t.Errorf("avro variant map did not marshal into CEL correctly")
	}
}
