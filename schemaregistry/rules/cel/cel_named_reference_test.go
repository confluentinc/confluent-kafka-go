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

// A named record referenced BY NAME rather than defined in place.
//
// Every such reference parses into a RefSchema, and its tags and inline rules live on the
// definition it points at. A walk that does not unwrap it treats the reference as a leaf and
// silently skips whatever the definition declared - so a field tagged for encryption behind a
// reference goes out in plaintext, and an inline rule on it never runs.
//
// Asserted through avrov2 and avrov3 together, which is the point: avrov3 had the case in neither
// of its two walks while avrov2 had it in both, and a table-driven pair is what makes that kind of
// drift fail rather than pass quietly.

import (
	"strings"
	"testing"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov3"
)

// `first` defines Inner; `second` references it by name. The tag and the inline rule are declared
// once, on Inner's field, so both sites have to see them.
const namedRefSchema = `{
  "type": "record", "name": "Outer", "fields": [
    {"name": "first", "type": {"type": "record", "name": "Inner", "fields": [
        {"name": "secret", "type": "string", "confluent:tags": ["PII"],
         "confluent:rules": [{"name": "notEmpty", "expr": "this != \"\""}]}]}},
    {"name": "second", "type": "Inner"}
  ]
}`

type namedRefInner struct {
	Secret string `avro:"secret"`
}

type namedRefOuter struct {
	First  namedRefInner `avro:"first"`
	Second namedRefInner `avro:"second"`
}

// serdePair names one of the two Avro serdes, so each behaviour below is asserted through both.
type serdePair struct {
	name      string
	serialize func(t *testing.T, client schemaregistry.Client, subject string,
		obj *namedRefOuter, validate bool) ([]byte, error)
	deserialize func(t *testing.T, client schemaregistry.Client, subject string,
		payload []byte, into *namedRefOuter) error
}

func serdePairs() []serdePair {
	return []serdePair{
		{
			name: "avrov2",
			serialize: func(t *testing.T, client schemaregistry.Client, subject string,
				obj *namedRefOuter, validate bool) ([]byte, error) {
				sc := avrov2.NewSerializerConfig()
				sc.AutoRegisterSchemas = false
				sc.UseLatestVersion = true
				if validate {
					sc.ValidationRulesExecution = "AFTER_DOMAIN_RULES"
				}
				ser, err := avrov2.NewSerializer(client, serde.ValueSerde, sc)
				if err != nil {
					t.Fatal(err)
				}
				return ser.Serialize(subject, obj)
			},
			deserialize: func(t *testing.T, client schemaregistry.Client, subject string,
				payload []byte, into *namedRefOuter) error {
				deser, err := avrov2.NewDeserializer(client, serde.ValueSerde,
					avrov2.NewDeserializerConfig())
				if err != nil {
					t.Fatal(err)
				}
				deser.Client = client
				return deser.DeserializeInto(subject, payload, into)
			},
		},
		{
			name: "avrov3",
			serialize: func(t *testing.T, client schemaregistry.Client, subject string,
				obj *namedRefOuter, validate bool) ([]byte, error) {
				sc := avrov3.NewSerializerConfig()
				sc.AutoRegisterSchemas = false
				sc.UseLatestVersion = true
				if validate {
					sc.ValidationRulesExecution = "AFTER_DOMAIN_RULES"
				}
				ser, err := avrov3.NewSerializer(client, serde.ValueSerde, sc)
				if err != nil {
					t.Fatal(err)
				}
				return ser.Serialize(subject, obj)
			},
			deserialize: func(t *testing.T, client schemaregistry.Client, subject string,
				payload []byte, into *namedRefOuter) error {
				deser, err := avrov3.NewDeserializer(client, serde.ValueSerde,
					avrov3.NewDeserializerConfig())
				if err != nil {
					t.Fatal(err)
				}
				deser.Client = client
				return deser.DeserializeInto(subject, payload, into)
			},
		},
	}
}

func namedRefClient(t *testing.T, subject string, rule *schemaregistry.Rule) schemaregistry.Client {
	t.Helper()
	Register()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatal(err)
	}
	info := schemaregistry.SchemaInfo{Schema: namedRefSchema, SchemaType: "AVRO"}
	if rule != nil {
		info.RuleSet = &schemaregistry.RuleSet{DomainRules: []schemaregistry.Rule{*rule}}
	}
	if _, err := client.Register(subject+"-value", info, false); err != nil {
		t.Fatal(err)
	}
	return client
}

// A tagged CEL_FIELD transform must reach the field at both sites.
func TestTaggedFieldBehindANamedReferenceIsTransformed(t *testing.T) {
	for _, p := range serdePairs() {
		t.Run(p.name, func(t *testing.T) {
			subject := "namedref" + p.name
			rule := schemaregistry.Rule{
				Name: "r", Kind: "TRANSFORM", Mode: "WRITE", Type: "CEL_FIELD",
				Tags: []string{"PII"}, Expr: `value + "-x"`,
			}
			client := namedRefClient(t, subject, &rule)
			obj := namedRefOuter{
				First:  namedRefInner{Secret: "a"},
				Second: namedRefInner{Secret: "b"},
			}
			payload, err := p.serialize(t, client, subject, &obj, false)
			if err != nil {
				t.Fatalf("serialize: %v", err)
			}
			var got namedRefOuter
			if err := p.deserialize(t, client, subject, payload, &got); err != nil {
				t.Fatalf("deserialize: %v", err)
			}
			// The definition site is the control: it worked before, so a failure there means
			// the fix broke the case that was already fine.
			if got.First.Secret != "a-x" {
				t.Errorf("first.secret = %q, want a-x", got.First.Secret)
			}
			if got.Second.Secret != "b-x" {
				t.Errorf("second.secret = %q, want b-x - the reference must be unwrapped",
					got.Second.Secret)
			}
		})
	}
}

// And an inline rule declared on the definition must run at both sites.
func TestInlineRuleBehindANamedReferenceRuns(t *testing.T) {
	for _, p := range serdePairs() {
		t.Run(p.name, func(t *testing.T) {
			subject := "namedrefval" + p.name
			client := namedRefClient(t, subject, nil)
			// Only `second` is empty, so only the reference site can report it.
			obj := namedRefOuter{
				First:  namedRefInner{Secret: "a"},
				Second: namedRefInner{Secret: ""},
			}
			_, err := p.serialize(t, client, subject, &obj, true)
			if err == nil {
				t.Fatal("expected a violation from the rule behind the reference")
			}
			if !strings.Contains(err.Error(), "notEmpty") {
				t.Errorf("error = %v, want the notEmpty rule named", err)
			}
		})
	}
}

// The twin: a record that satisfies the rule at both sites must serialize cleanly, or the test
// above would also pass on a walk that reports a violation for the wrong reason.
func TestInlineRuleBehindANamedReferencePassesWhenSatisfied(t *testing.T) {
	for _, p := range serdePairs() {
		t.Run(p.name, func(t *testing.T) {
			subject := "namedrefok" + p.name
			client := namedRefClient(t, subject, nil)
			obj := namedRefOuter{
				First:  namedRefInner{Secret: "a"},
				Second: namedRefInner{Secret: "b"},
			}
			if _, err := p.serialize(t, client, subject, &obj, true); err != nil {
				t.Fatalf("expected no violation, got %v", err)
			}
		})
	}
}
