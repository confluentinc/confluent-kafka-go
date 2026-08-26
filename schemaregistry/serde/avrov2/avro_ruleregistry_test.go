/**
 * Copyright 2024 Confluent Inc.
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

package avrov2

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

// countingExecutor is a no-op rule executor that records how many times Configure was
// called on it, used to observe which registry a serializer draws its executors from.
type countingExecutor struct {
	ruleType   string
	configured int
}

func (e *countingExecutor) Configure(*schemaregistry.Config, map[string]string) error {
	e.configured++
	return nil
}
func (e *countingExecutor) Type() string { return e.ruleType }
func (e *countingExecutor) Transform(_ serde.RuleContext, msg interface{}) (interface{}, error) {
	return msg, nil
}
func (e *countingExecutor) Close() error { return nil }

// TestSerializerDefaultsToGlobalRuleRegistry verifies that leaving SerializerConfig.RuleRegistry
// nil preserves the pre-existing behavior of using the process-wide global registry.
func TestSerializerDefaultsToGlobalRuleRegistry(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	conf := schemaregistry.NewConfig("mock://")
	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	if ser.RuleRegistry != serde.GlobalRuleRegistry() {
		t.Fatalf("expected a serializer with a nil RuleRegistry to use the global registry")
	}
}

// TestSerializerRuleRegistryIsolation verifies that a serializer built with an injected
// RuleRegistry configures only that registry's executors, and that a separately-built
// default serializer does not reach into (and reconfigure) the injected registry - i.e.
// the two serializers do not share rule-executor state. This is what makes it safe to
// construct CSFLE-enabled serializers concurrently.
func TestSerializerRuleRegistryIsolation(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	conf := schemaregistry.NewConfig("mock://")
	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	reg := serde.NewRuleRegistry()
	isolated := &countingExecutor{ruleType: "ISOLATED_TEST_RULE"}
	reg.RegisterExecutor(isolated)

	serConf := NewSerializerConfig()
	serConf.RuleRegistry = &reg
	ser, err := NewSerializer(client, serde.ValueSerde, serConf)
	serde.MaybeFail("Serializer configuration", err)

	if ser.RuleRegistry != &reg {
		t.Fatalf("expected serializer to use the injected RuleRegistry")
	}
	if ser.RuleRegistry == serde.GlobalRuleRegistry() {
		t.Fatalf("injected RuleRegistry must not be the global registry")
	}
	if isolated.configured != 1 {
		t.Fatalf("expected the injected registry's executor to be configured exactly once, got %d", isolated.configured)
	}

	// A serializer using the default (global) registry must not touch the injected
	// registry's executor - proving the two do not share executor state.
	def, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Default serializer configuration", err)
	if def.RuleRegistry != serde.GlobalRuleRegistry() {
		t.Fatalf("expected the default serializer to use the global registry")
	}
	if isolated.configured != 1 {
		t.Fatalf("a default-registry serializer must not reconfigure the injected registry's executor; configured count is now %d", isolated.configured)
	}
}
