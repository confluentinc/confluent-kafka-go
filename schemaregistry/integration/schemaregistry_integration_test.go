package integration

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

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/invopop/jsonschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/compose"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rest"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	jsonschemaserde "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

// subjectNameStrategyTestRecord is the payload the associated subject name
// tests produce.
type subjectNameStrategyTestRecord struct {
	Value string `json:"Value"`
}

// stringSerializer serializes a string key as its UTF-8 bytes. The Schema
// Registry serializers cover the value side; this only exists so that the tests
// can produce a non-empty key, which SerializingProducer.Produce rejects when no
// key serializer is set.
type stringSerializer struct {
	closed bool
}

func (s *stringSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	str, ok := msg.(string)
	if !ok {
		return nil, fmt.Errorf("stringSerializer: expected a string, got %T", msg)
	}
	return []byte(str), nil
}

func (s *stringSerializer) SerializeWithHeaders(topic string, msg interface{}) ([]kafka.Header, []byte, error) {
	payload, err := s.Serialize(topic, msg)
	return nil, payload, err
}

// The string serializer resolves nothing from the Kafka cluster.
func (s *stringSerializer) SetClusterIDResolver(resolve func() (string, error)) {}

func (s *stringSerializer) Close() error { s.closed = true; return nil }

type stringSerializerBuilder struct {
	serializer *stringSerializer
}

func (b *stringSerializerBuilder) Build(conf *kafka.ConfigMap, isKey bool) (kafka.Serializer, *kafka.ConfigMap, error) {
	b.serializer = &stringSerializer{}
	return b.serializer, conf, nil
}

func newStringSerializerBuilder() *stringSerializerBuilder {
	return &stringSerializerBuilder{}
}

// schemaRegistryClientForTest returns a client for the configured Schema
// Registry. A registry that is configured but unreachable is a failure, not a
// skip; only an unconfigured one is skipped.
func schemaRegistryClientForTest(t *testing.T) schemaregistry.Client {
	t.Helper()

	if testconf.SchemaRegistryURL == "" {
		t.Skip("No SchemaRegistryURL configured, skipping Schema Registry tests")
	}

	client, err := schemaregistry.NewClient(
		schemaregistry.NewConfig(testconf.SchemaRegistryURL))
	require.NoError(t, err, "failed to create the Schema Registry client")
	return client
}

// clusterIDForTest returns the cluster ID the broker reports, which is what the
// associated subject name strategy uses as the resource namespace.
func clusterIDForTest(t *testing.T) string {
	t.Helper()

	a := createAdminClient(t)
	defer a.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clusterID, err := a.ClusterID(ctx)
	require.NoError(t, err, "failed to retrieve the cluster ID")
	require.NotEmpty(t, clusterID, "the broker did not report a cluster ID")
	return clusterID
}

// skipUnlessAssociationsAreSupported skips the calling test when the Schema
// Registry under test does not implement the associations API.
func skipUnlessAssociationsAreSupported(t *testing.T, client schemaregistry.Client,
	topic string, clusterID string) {
	t.Helper()

	_, err := client.GetAssociationsByResourceName(topic, clusterID, "topic", nil, "", 0, -1)
	if err == nil {
		return
	}

	var restErr *rest.Error
	if errors.As(err, &restErr) {
		for _, status := range []int{404, 405, 501} {
			if restErr.HasStatus(status) {
				t.Skipf("the Schema Registry under test does not support the "+
					"association API (HTTP %d)", status)
			}
		}
	}
	require.NoError(t, err, "failed to query associations")
}

// testRecordSchemaInfo returns the JSON schema the JSON Schema serializer
// derives for subjectNameStrategyTestRecord, so that the schema registered up
// front is the one the serializer would otherwise have registered itself.
func testRecordSchemaInfo(t *testing.T) schemaregistry.SchemaInfo {
	t.Helper()

	raw, err := json.Marshal(jsonschema.Reflect(&subjectNameStrategyTestRecord{}))
	require.NoError(t, err, "failed to derive the JSON schema")
	return schemaregistry.SchemaInfo{Schema: string(raw), SchemaType: "JSON"}
}

// deleteAssociations removes the associations a test created, so that reruns
// against a long-lived registry start from a clean state.
func deleteAssociations(t *testing.T, client schemaregistry.Client,
	topic string, namespace string) {
	t.Helper()

	associations, err := client.GetAssociationsByResourceName(
		topic, namespace, "topic", nil, "", 0, -1)
	if err != nil || len(associations) == 0 {
		return
	}
	if err := client.DeleteAssociations(
		associations[0].ResourceID, "topic", []string{"value"}, true); err != nil {
		t.Logf("failed to clean up associations for topic %s: %s", topic, err)
	}
}

// produceOne produces a single message and waits for its delivery report.
func produceOne(t *testing.T, p *kafka.SerializingProducer[string, *subjectNameStrategyTestRecord],
	topic string, key string, value *subjectNameStrategyTestRecord) {
	t.Helper()

	deliveryChan := make(chan kafka.Event, 1)
	err := p.Produce(&kafka.SerializableMessage[string, *subjectNameStrategyTestRecord]{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}, deliveryChan)
	require.NoError(t, err, "Produce should not fail")

	select {
	case ev := <-deliveryChan:
		msg, ok := ev.(*kafka.SerializableMessage[string, *subjectNameStrategyTestRecord])
		require.True(t, ok, "expected a SerializableMessage delivery report, got %T", ev)
		require.NoError(t, msg.TopicPartition.Error, "message delivery failed")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the delivery report")
	}
}

// TestAssociatedClusterIDJSON verifies that the associated subject name strategy
// resolves the subject from an association registered under the broker's own
// cluster ID, without that cluster ID ever being configured explicitly.
//
// The association deliberately points at a subject name the topic name fallback
// could never produce. Without that, the test would pass even where associations
// are unsupported: an association lookup that 404s is swallowed and the strategy
// quietly falls back to <topic>-value.
func (its *IntegrationTestSuite) TestAssociatedClusterIDJSON() {
	t := its.T()

	client := schemaRegistryClientForTest(t)
	clusterID := clusterIDForTest(t)
	topic := createTestTopic(t, "assoc", 1, 1)

	skipUnlessAssociationsAreSupported(t, client, topic, clusterID)

	associatedSubject := fmt.Sprintf("assoc-%s-subject",
		strings.ReplaceAll(uuid.NewString(), "-", ""))

	_, err := client.Register(associatedSubject, testRecordSchemaInfo(t), true)
	require.NoError(t, err, "failed to register the schema")

	_, err = client.CreateAssociation(schemaregistry.AssociationCreateOrUpdateRequest{
		ResourceName:      topic,
		ResourceNamespace: clusterID,
		ResourceID:        clusterID + ":" + topic,
		ResourceType:      "topic",
		Associations: []schemaregistry.AssociationCreateOrUpdateInfo{{
			Subject:         associatedSubject,
			AssociationType: "value",
			Lifecycle:       schemaregistry.STRONG,
		}},
	})
	require.NoError(t, err, "failed to create the association")
	defer deleteAssociations(t, client, topic, clusterID)

	// No subject.name.strategy.kafka.cluster.id is configured, so the producer
	// has to fetch the cluster ID from the broker while it is being built.
	serializerConf := jsonschemaserde.NewSerializerConfig()
	serializerConf.AutoRegisterSchemas = false
	serializerConf.UseLatestVersion = true
	serializerConf.SubjectNameStrategyType = serde.AssociatedNameStrategyType

	p, err := kafka.NewSerializingProducer[string, *subjectNameStrategyTestRecord](
		&kafka.ConfigMap{"bootstrap.servers": testconf.Brokers},
		newStringSerializerBuilder(),
		jsonschemaserde.NewKafkaSerializerBuilder().
			SetSchemaRegistryClient(client).
			SetSerializerConfig(serializerConf))
	require.NoError(t, err, "failed to create the serializing producer")
	defer p.Close()

	produceOne(t, p, topic, "test1", &subjectNameStrategyTestRecord{Value: "test-string"})

	subjects, err := client.GetAllSubjects()
	require.NoError(t, err, "failed to list subjects")
	assert.Contains(t, subjects, associatedSubject,
		"the associated subject should have been used")
	assert.NotContains(t, subjects, topic+"-value",
		"the topic name fallback should not have been used")
}

// TestAssociatedClusterIDIsScopedJSON verifies that an association registered
// under the namespace wildcard is out of scope for a producer that resolved a
// real cluster ID, so the strategy falls back to the topic name.
func (its *IntegrationTestSuite) TestAssociatedClusterIDIsScopedJSON() {
	t := its.T()

	client := schemaRegistryClientForTest(t)
	clusterID := clusterIDForTest(t)
	topic := createTestTopic(t, "assoc-scoped", 1, 1)

	skipUnlessAssociationsAreSupported(t, client, topic, clusterID)

	associatedSubject := fmt.Sprintf("assoc-%s-subject",
		strings.ReplaceAll(uuid.NewString(), "-", ""))

	_, err := client.Register(associatedSubject, testRecordSchemaInfo(t), true)
	require.NoError(t, err, "failed to register the schema")

	_, err = client.CreateAssociation(schemaregistry.AssociationCreateOrUpdateRequest{
		ResourceName:      topic,
		ResourceNamespace: serde.NamespaceWildcard,
		ResourceID:        serde.NamespaceWildcard + ":" + topic,
		ResourceType:      "topic",
		Associations: []schemaregistry.AssociationCreateOrUpdateInfo{{
			Subject:         associatedSubject,
			AssociationType: "value",
			Lifecycle:       schemaregistry.STRONG,
		}},
	})
	require.NoError(t, err, "failed to create the association")
	defer deleteAssociations(t, client, topic, serde.NamespaceWildcard)

	serializerConf := jsonschemaserde.NewSerializerConfig()
	serializerConf.SubjectNameStrategyType = serde.AssociatedNameStrategyType

	p, err := kafka.NewSerializingProducer[string, *subjectNameStrategyTestRecord](
		&kafka.ConfigMap{"bootstrap.servers": testconf.Brokers},
		newStringSerializerBuilder(),
		jsonschemaserde.NewKafkaSerializerBuilder().
			SetSchemaRegistryClient(client).
			SetSerializerConfig(serializerConf))
	require.NoError(t, err, "failed to create the serializing producer")
	defer p.Close()

	produceOne(t, p, topic, "test1", &subjectNameStrategyTestRecord{Value: "test-string"})

	subjects, err := client.GetAllSubjects()
	require.NoError(t, err, "failed to list subjects")
	assert.Contains(t, subjects, topic+"-value",
		"the wildcard association is out of scope, so the topic name should have been used")
}

type IntegrationTestSuite struct {
	suite.Suite
	compose *compose.LocalDockerCompose
}

func (its *IntegrationTestSuite) TearDownSuite() {
	if testconf.DockerNeeded && its.compose != nil {
		its.compose.Down()
	}
}

func TestIntegration(t *testing.T) {
	its := new(IntegrationTestSuite)
	testconfInit()
	if !testconfRead() {
		t.Skipf("testconf not provided or not usable\n")
		return
	}

	if testconf.DockerNeeded && !testconf.DockerExists {
		its.compose = compose.NewLocalDockerCompose(
			[]string{"./testresources/docker-compose.yaml"}, "sr-integration-docker")
		execErr := its.compose.WithCommand([]string{"up", "-d"}).Invoke()
		if err := execErr.Error; err != nil {
			t.Fatalf("up -d command failed with the error message %s\n", err)
		}
	}

	// Outside the block above on purpose: under -docker.exists the stack is
	// brought up by hand, but the tests still need a registry that answers.
	if testconf.SchemaRegistryURL != "" {
		if err := waitSchemaRegistryReady(testconf.SchemaRegistryURL,
			180*time.Second); err != nil {
			t.Fatalf("%s", err)
		}
	}

	suite.Run(t, its)
}
