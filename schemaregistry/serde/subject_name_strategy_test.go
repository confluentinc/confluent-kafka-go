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

package serde

import (
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

const testSchema = `{"type":"record","name":"DemoSchema","fields":[{"name":"IntField","type":"int"}]}`

func testSchemaInfo() schemaregistry.SchemaInfo {
	return schemaregistry.SchemaInfo{Schema: testSchema, SchemaType: "AVRO"}
}

// newTestAssociation registers subject and associates it with topic in the
// given namespace.
func newTestAssociation(t *testing.T, client schemaregistry.Client,
	topic string, namespace string, subject string, associationType string) {
	t.Helper()

	_, err := client.Register(subject, testSchemaInfo(), false)
	if err != nil {
		t.Fatalf("Failed to register %s: %s", subject, err)
	}

	_, err = client.CreateAssociation(schemaregistry.AssociationCreateOrUpdateRequest{
		ResourceName:      topic,
		ResourceNamespace: namespace,
		ResourceID:        namespace + ":" + topic,
		ResourceType:      "topic",
		Associations: []schemaregistry.AssociationCreateOrUpdateInfo{
			{Subject: subject, AssociationType: associationType, Lifecycle: "STRONG"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to associate %s with %s: %s", subject, topic, err)
	}
}

func newTestClient(t *testing.T) schemaregistry.Client {
	t.Helper()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatalf("Failed to create the Schema Registry client: %s", err)
	}
	return client
}

// TestAssociatedNameStrategyClusterID covers the cluster ID handling of the
// associated name strategy, which decides whether the Kafka cluster ID has to
// be fetched from the cluster.
func TestAssociatedNameStrategyClusterID(t *testing.T) {
	client := newTestClient(t)

	// Without a configured cluster ID, the strategy needs one and defaults to
	// the wildcard namespace until it is set.
	strategy, err := newAssociatedNameStrategy(client, map[string]string{}, nil)
	if err != nil {
		t.Fatalf("Failed to create the strategy: %s", err)
	}
	if !strategy.needsClusterID() {
		t.Errorf("Expected the strategy to need the cluster ID")
	}
	if strategy.kafkaClusterID != NamespaceWildcard {
		t.Errorf("Expected the wildcard namespace, got %s", strategy.kafkaClusterID)
	}

	strategy.setClusterID("lkc-123")
	if strategy.kafkaClusterID != "lkc-123" {
		t.Errorf("Expected lkc-123, got %s", strategy.kafkaClusterID)
	}
	if strategy.needsClusterID() {
		t.Errorf("Expected the strategy not to need the cluster ID anymore")
	}

	// Once set, the cluster ID is not overwritten.
	strategy.setClusterID("lkc-456")
	if strategy.kafkaClusterID != "lkc-123" {
		t.Errorf("Expected lkc-123 to be kept, got %s", strategy.kafkaClusterID)
	}

	// An explicitly configured cluster ID needs no lookup.
	strategy, err = newAssociatedNameStrategy(client,
		map[string]string{KafkaClusterIDConfig: "lkc-789"}, nil)
	if err != nil {
		t.Fatalf("Failed to create the strategy: %s", err)
	}
	if strategy.needsClusterID() {
		t.Errorf("Expected a configured cluster ID not to be fetched")
	}
	if strategy.kafkaClusterID != "lkc-789" {
		t.Errorf("Expected lkc-789, got %s", strategy.kafkaClusterID)
	}
	strategy.setClusterID("lkc-000")
	if strategy.kafkaClusterID != "lkc-789" {
		t.Errorf("Expected the configured cluster ID to be kept, got %s", strategy.kafkaClusterID)
	}

	// An empty cluster ID is treated as not configured.
	strategy, err = newAssociatedNameStrategy(client,
		map[string]string{KafkaClusterIDConfig: ""}, nil)
	if err != nil {
		t.Fatalf("Failed to create the strategy: %s", err)
	}
	if !strategy.needsClusterID() {
		t.Errorf("Expected an empty cluster ID to be fetched")
	}
}

func TestAssociatedNameStrategyConfigErrors(t *testing.T) {
	client := newTestClient(t)

	_, err := newAssociatedNameStrategy(client,
		map[string]string{FallbackTypeConfig: "BOGUS"}, nil)
	if err == nil {
		t.Errorf("Expected an unrecognized fallback type to fail")
	}

	_, err = newAssociatedNameStrategy(client,
		map[string]string{FallbackTypeConfig: "RECORD"}, nil)
	if err == nil {
		t.Errorf("Expected RECORD without a record name function to fail")
	}

	// The exported constructor reports the same errors.
	_, err = AssociatedNameStrategy(client, map[string]string{FallbackTypeConfig: "BOGUS"}, nil)
	if err == nil {
		t.Errorf("Expected an unrecognized fallback type to fail")
	}
}

// TestAssociatedNameStrategySubjectName covers the subject name lookup, its
// cache, and the namespace the cluster ID is used for.
func TestAssociatedNameStrategySubjectName(t *testing.T) {
	client := newTestClient(t)
	newTestAssociation(t, client, "topic1", NamespaceWildcard, "my-value-subject", "value")
	newTestAssociation(t, client, "topic1", NamespaceWildcard, "my-key-subject", "key")
	newTestAssociation(t, client, "topic2", "lkc-123", "cluster-value-subject", "value")

	strategy, err := newAssociatedNameStrategy(client, map[string]string{}, nil)
	if err != nil {
		t.Fatalf("Failed to create the strategy: %s", err)
	}

	// An empty topic has no subject.
	subject, err := strategy.subjectNameStrategy("", ValueSerde, testSchemaInfo())
	if err != nil || subject != "" {
		t.Errorf("Expected an empty subject, got %q (%v)", subject, err)
	}

	subject, err = strategy.subjectNameStrategy("topic1", ValueSerde, testSchemaInfo())
	if err != nil || subject != "my-value-subject" {
		t.Errorf("Expected my-value-subject, got %q (%v)", subject, err)
	}

	subject, err = strategy.subjectNameStrategy("topic1", KeySerde, testSchemaInfo())
	if err != nil || subject != "my-key-subject" {
		t.Errorf("Expected my-key-subject, got %q (%v)", subject, err)
	}

	// The association of another namespace is not visible with the wildcard.
	subject, err = strategy.subjectNameStrategy("topic2", ValueSerde, testSchemaInfo())
	if err != nil || subject != "topic2-value" {
		t.Errorf("Expected the topic name fallback, got %q (%v)", subject, err)
	}

	// The subject name is cached: deleting the association does not change
	// the result for an already resolved topic and schema.
	err = client.DeleteAssociations(NamespaceWildcard+":topic1", "topic", []string{"value"}, true)
	if err != nil {
		t.Fatalf("Failed to delete the association: %s", err)
	}
	subject, err = strategy.subjectNameStrategy("topic1", ValueSerde, testSchemaInfo())
	if err != nil || subject != "my-value-subject" {
		t.Errorf("Expected the cached my-value-subject, got %q (%v)", subject, err)
	}

	// A different schema is a different cache entry, and now falls back.
	otherSchema := schemaregistry.SchemaInfo{
		Schema:     strings.Replace(testSchema, "DemoSchema", "OtherSchema", 1),
		SchemaType: "AVRO",
	}
	subject, err = strategy.subjectNameStrategy("topic1", ValueSerde, otherSchema)
	if err != nil || subject != "topic1-value" {
		t.Errorf("Expected the topic name fallback, got %q (%v)", subject, err)
	}

	// With the cluster ID set, the association of that namespace is used.
	strategy, err = newAssociatedNameStrategy(client, map[string]string{}, nil)
	if err != nil {
		t.Fatalf("Failed to create the strategy: %s", err)
	}
	strategy.setClusterID("lkc-123")
	subject, err = strategy.subjectNameStrategy("topic2", ValueSerde, testSchemaInfo())
	if err != nil || subject != "cluster-value-subject" {
		t.Errorf("Expected cluster-value-subject, got %q (%v)", subject, err)
	}
}

func TestAssociatedNameStrategyFallbacks(t *testing.T) {
	client := newTestClient(t)
	getRecordName := func(schema schemaregistry.SchemaInfo) (string, error) {
		return "DemoSchema", nil
	}

	for _, test := range []struct {
		fallbackType string
		expected     string
	}{
		{"", "topic1-value"},
		{"TOPIC", "topic1-value"},
		{"RECORD", "DemoSchema"},
		{"TOPIC_RECORD", "topic1-DemoSchema"},
	} {
		strategy, err := newAssociatedNameStrategy(client,
			map[string]string{FallbackTypeConfig: test.fallbackType}, getRecordName)
		if err != nil {
			t.Fatalf("Failed to create the strategy for %q: %s", test.fallbackType, err)
		}
		subject, err := strategy.subjectNameStrategy("topic1", ValueSerde, testSchemaInfo())
		if err != nil || subject != test.expected {
			t.Errorf("Expected %q for fallback %q, got %q (%v)",
				test.expected, test.fallbackType, subject, err)
		}
	}

	// Without a fallback, an unassociated topic is an error.
	strategy, err := newAssociatedNameStrategy(client,
		map[string]string{FallbackTypeConfig: "NONE"}, getRecordName)
	if err != nil {
		t.Fatalf("Failed to create the strategy: %s", err)
	}
	_, err = strategy.subjectNameStrategy("topic1", ValueSerde, testSchemaInfo())
	if err == nil || !strings.Contains(err.Error(), "no associated subject found") {
		t.Errorf("Expected no associated subject to be found, got %v", err)
	}
}

// TestAssociatedNameStrategyFunc covers the exported wrapper returning the
// strategy as a function.
func TestAssociatedNameStrategyFunc(t *testing.T) {
	client := newTestClient(t)
	newTestAssociation(t, client, "topic3", "lkc-123", "func-value-subject", "value")

	strategyFunc, err := AssociatedNameStrategy(client,
		map[string]string{KafkaClusterIDConfig: "lkc-123"}, nil)
	if err != nil {
		t.Fatalf("Failed to create the strategy: %s", err)
	}
	subject, err := strategyFunc("topic3", ValueSerde, testSchemaInfo())
	if err != nil || subject != "func-value-subject" {
		t.Errorf("Expected func-value-subject, got %q (%v)", subject, err)
	}
}

// TestSerdeConfigureSubjectNameStrategy covers the cluster ID plumbing of the
// Serde, which only applies to the associated name strategy.
func TestSerdeConfigureSubjectNameStrategy(t *testing.T) {
	client := newTestClient(t)
	newTestAssociation(t, client, "topic4", "lkc-42", "serde-value-subject", "value")

	// No strategy type means the associated name strategy, which needs the
	// cluster ID of the cluster the serde is used with.
	s := &Serde{Client: client, SerdeType: ValueSerde}
	err := s.ConfigureSubjectNameStrategy(NoStrategyType, nil, nil)
	if err != nil {
		t.Fatalf("Failed to configure the subject name strategy: %s", err)
	}
	if !s.NeedsClusterID() {
		t.Errorf("Expected the serde to need the cluster ID")
	}
	if s.SubjectNameStrategy == nil {
		t.Fatalf("Expected a subject name strategy to be set")
	}

	s.SetClusterID("lkc-42")
	if s.NeedsClusterID() {
		t.Errorf("Expected the serde not to need the cluster ID anymore")
	}
	subject, err := s.SubjectNameStrategy("topic4", ValueSerde, testSchemaInfo())
	if err != nil || subject != "serde-value-subject" {
		t.Errorf("Expected serde-value-subject, got %q (%v)", subject, err)
	}

	// An explicitly configured cluster ID is used as-is.
	s = &Serde{Client: client, SerdeType: ValueSerde}
	err = s.ConfigureSubjectNameStrategy(AssociatedNameStrategyType,
		map[string]string{KafkaClusterIDConfig: "lkc-42"}, nil)
	if err != nil {
		t.Fatalf("Failed to configure the subject name strategy: %s", err)
	}
	if s.NeedsClusterID() {
		t.Errorf("Expected a configured cluster ID not to be fetched")
	}
	subject, err = s.SubjectNameStrategy("topic4", ValueSerde, testSchemaInfo())
	if err != nil || subject != "serde-value-subject" {
		t.Errorf("Expected serde-value-subject, got %q (%v)", subject, err)
	}

	// Other strategies never need the cluster ID, and setting one is a no-op.
	s = &Serde{Client: client, SerdeType: ValueSerde}
	err = s.ConfigureSubjectNameStrategy(TopicNameStrategyType, nil, nil)
	if err != nil {
		t.Fatalf("Failed to configure the subject name strategy: %s", err)
	}
	if s.NeedsClusterID() {
		t.Errorf("Expected the topic name strategy not to need the cluster ID")
	}
	s.SetClusterID("lkc-42")
	subject, err = s.SubjectNameStrategy("topic4", ValueSerde, testSchemaInfo())
	if err != nil || subject != "topic4-value" {
		t.Errorf("Expected topic4-value, got %q (%v)", subject, err)
	}

	// A zero value Serde, without a configured strategy, needs nothing.
	s = &Serde{}
	if s.NeedsClusterID() {
		t.Errorf("Expected an unconfigured serde not to need the cluster ID")
	}
	s.SetClusterID("lkc-42")

	// Configuration errors are propagated.
	s = &Serde{Client: client, SerdeType: ValueSerde}
	err = s.ConfigureSubjectNameStrategy(RecordNameStrategyType, nil, nil)
	if err == nil {
		t.Errorf("Expected RECORD without a record name function to fail")
	}
	s = &Serde{Client: client, SerdeType: ValueSerde}
	err = s.ConfigureSubjectNameStrategy(AssociatedNameStrategyType,
		map[string]string{FallbackTypeConfig: "BOGUS"}, nil)
	if err == nil {
		t.Errorf("Expected an unrecognized fallback type to fail")
	}
}
