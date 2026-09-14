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

package avrov2

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

// The builders are meant to be used with a SerializingProducer and a
// DeserializingConsumer.
var _ kafka.SerializerBuilder = NewKafkaSerializerBuilder()
var _ kafka.DeserializerBuilder = NewKafkaDeserializerBuilder()

func newBuilderTestClient(t *testing.T) schemaregistry.Client {
	t.Helper()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatalf("Failed to create the Schema Registry client: %s", err)
	}
	return client
}

func newBuilderTestConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{"bootstrap.servers": "localhost:9092"}
}

// TestKafkaSerializerBuilderWithClient verifies that a Schema Registry client
// passed to the builder is used as-is, leaving the Kafka ConfigMap untouched.
func TestKafkaSerializerBuilderWithClient(t *testing.T) {
	client := newBuilderTestClient(t)
	conf := newBuilderTestConfigMap()

	serializerConf := NewSerializerConfig()
	serializerConf.AutoRegisterSchemas = false
	serializerConf.UseLatestVersion = true

	initialized := 0
	builder := NewKafkaSerializerBuilder().
		SetSchemaRegistryClient(client).
		SetSerializerConfig(serializerConf).
		SetSerializerInit(func(s *Serializer) { initialized++ })

	kafkaSerializer, filteredConf, err := builder.Build(conf, true)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if filteredConf != conf {
		t.Errorf("Expected the Kafka ConfigMap to be passed through, got %v", filteredConf)
	}
	if initialized != 1 {
		t.Errorf("Expected the serializer init function to be called once, got %d", initialized)
	}

	ser, ok := kafkaSerializer.(*Serializer)
	if !ok {
		t.Fatalf("Expected a *Serializer, got %T", kafkaSerializer)
	}
	if ser.Client != client {
		t.Errorf("Expected the given Schema Registry client to be used")
	}
	if ser.SerdeType != serde.KeySerde {
		t.Errorf("Expected a key serializer, got %v", ser.SerdeType)
	}
	if ser.Conf.AutoRegisterSchemas || !ser.Conf.UseLatestVersion {
		t.Errorf("Expected the given serializer config to be used")
	}

	// isKey=false builds a value serializer.
	kafkaSerializer, _, err = NewKafkaSerializerBuilder().
		SetSchemaRegistryClient(client).Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	ser = kafkaSerializer.(*Serializer)
	if ser.SerdeType != serde.ValueSerde {
		t.Errorf("Expected a value serializer, got %v", ser.SerdeType)
	}
	// Without a serializer config, the defaults are used.
	if !ser.Conf.AutoRegisterSchemas {
		t.Errorf("Expected the default serializer config to be used")
	}
}

// TestKafkaSerializerBuilderWithConfig verifies that, without a client, one is
// created from the Schema Registry config and the Kafka ConfigMap.
func TestKafkaSerializerBuilderWithConfig(t *testing.T) {
	conf := newBuilderTestConfigMap()

	kafkaSerializer, filteredConf, err := NewKafkaSerializerBuilder().
		SetSchemaRegistryConfig(schemaregistry.NewConfig("mock://")).
		Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if filteredConf == conf {
		t.Errorf("Expected a filtered copy of the Kafka ConfigMap")
	}
	if len(*filteredConf) != len(*conf) {
		t.Errorf("Expected the Kafka properties to be kept, got %v", filteredConf)
	}
	if kafkaSerializer.(*Serializer).Client == nil {
		t.Errorf("Expected a Schema Registry client to be created")
	}

	// An invalid Schema Registry config is reported.
	_, _, err = NewKafkaSerializerBuilder().
		SetSchemaRegistryConfig(schemaregistry.NewConfig("://invalid")).
		Build(conf, false)
	if err == nil {
		t.Errorf("Expected an invalid Schema Registry URL to fail")
	}

	// A serializer configuration error is reported as well.
	serializerConf := NewSerializerConfig()
	serializerConf.SubjectNameStrategyConfig = map[string]string{
		serde.FallbackTypeConfig: "BOGUS",
	}
	_, _, err = NewKafkaSerializerBuilder().
		SetSchemaRegistryClient(newBuilderTestClient(t)).
		SetSerializerConfig(serializerConf).
		Build(conf, false)
	if err == nil {
		t.Errorf("Expected an invalid serializer configuration to fail")
	}
}

// TestKafkaSerializerBuilderClusterID verifies whether the built serializer
// asks for the Kafka cluster ID, which depends on the subject name strategy.
func TestKafkaSerializerBuilderClusterID(t *testing.T) {
	client := newBuilderTestClient(t)
	conf := newBuilderTestConfigMap()

	// The default subject name strategy resolves the subject through the
	// associations of the Kafka cluster, so it needs its ID.
	kafkaSerializer, _, err := NewKafkaSerializerBuilder().
		SetSchemaRegistryClient(client).Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if !kafkaSerializer.NeedsClusterID() {
		t.Errorf("Expected the serializer to need the Kafka cluster ID")
	}

	// A configured cluster ID needs no lookup.
	serializerConf := NewSerializerConfig()
	serializerConf.SubjectNameStrategyConfig = map[string]string{
		serde.KafkaClusterIDConfig: "lkc-123",
	}
	kafkaSerializer, _, err = NewKafkaSerializerBuilder().
		SetSchemaRegistryClient(client).
		SetSerializerConfig(serializerConf).
		Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if kafkaSerializer.NeedsClusterID() {
		t.Errorf("Expected a configured cluster ID not to be fetched")
	}

	// Neither does another subject name strategy.
	serializerConf = NewSerializerConfig()
	serializerConf.SubjectNameStrategyType = serde.TopicNameStrategyType
	kafkaSerializer, _, err = NewKafkaSerializerBuilder().
		SetSchemaRegistryClient(client).
		SetSerializerConfig(serializerConf).
		Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if kafkaSerializer.NeedsClusterID() {
		t.Errorf("Expected the topic name strategy not to need the cluster ID")
	}
}

// TestKafkaDeserializerBuilderWithClient verifies that a Schema Registry
// client passed to the builder is used as-is, leaving the Kafka ConfigMap
// untouched.
func TestKafkaDeserializerBuilderWithClient(t *testing.T) {
	client := newBuilderTestClient(t)
	conf := newBuilderTestConfigMap()

	deserializerConf := NewDeserializerConfig()
	deserializerConf.UseLatestVersion = true

	initialized := 0
	builder := NewKafkaDeserializerBuilder().
		SetSchemaRegistryClient(client).
		SetDeserializerConfig(deserializerConf).
		SetDeserializerInit(func(d *Deserializer) { initialized++ })

	kafkaDeserializer, filteredConf, err := builder.Build(conf, true)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if filteredConf != conf {
		t.Errorf("Expected the Kafka ConfigMap to be passed through, got %v", filteredConf)
	}
	if initialized != 1 {
		t.Errorf("Expected the deserializer init function to be called once, got %d", initialized)
	}

	deser, ok := kafkaDeserializer.(*Deserializer)
	if !ok {
		t.Fatalf("Expected a *Deserializer, got %T", kafkaDeserializer)
	}
	if deser.Client != client {
		t.Errorf("Expected the given Schema Registry client to be used")
	}
	if deser.SerdeType != serde.KeySerde {
		t.Errorf("Expected a key deserializer, got %v", deser.SerdeType)
	}
	if !deser.Conf.UseLatestVersion {
		t.Errorf("Expected the given deserializer config to be used")
	}

	// isKey=false builds a value deserializer.
	kafkaDeserializer, _, err = NewKafkaDeserializerBuilder().
		SetSchemaRegistryClient(client).Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	deser = kafkaDeserializer.(*Deserializer)
	if deser.SerdeType != serde.ValueSerde {
		t.Errorf("Expected a value deserializer, got %v", deser.SerdeType)
	}
	// Without a deserializer config, the defaults are used.
	if deser.Conf.UseLatestVersion {
		t.Errorf("Expected the default deserializer config to be used")
	}
}

// TestKafkaDeserializerBuilderWithConfig verifies that, without a client, one
// is created from the Schema Registry config and the Kafka ConfigMap.
func TestKafkaDeserializerBuilderWithConfig(t *testing.T) {
	conf := newBuilderTestConfigMap()

	kafkaDeserializer, filteredConf, err := NewKafkaDeserializerBuilder().
		SetSchemaRegistryConfig(schemaregistry.NewConfig("mock://")).
		Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if filteredConf == conf {
		t.Errorf("Expected a filtered copy of the Kafka ConfigMap")
	}
	if len(*filteredConf) != len(*conf) {
		t.Errorf("Expected the Kafka properties to be kept, got %v", filteredConf)
	}
	if kafkaDeserializer.(*Deserializer).Client == nil {
		t.Errorf("Expected a Schema Registry client to be created")
	}

	// An invalid Schema Registry config is reported.
	_, _, err = NewKafkaDeserializerBuilder().
		SetSchemaRegistryConfig(schemaregistry.NewConfig("://invalid")).
		Build(conf, false)
	if err == nil {
		t.Errorf("Expected an invalid Schema Registry URL to fail")
	}

	// A deserializer configuration error is reported as well.
	deserializerConf := NewDeserializerConfig()
	deserializerConf.SubjectNameStrategyConfig = map[string]string{
		serde.FallbackTypeConfig: "BOGUS",
	}
	_, _, err = NewKafkaDeserializerBuilder().
		SetSchemaRegistryClient(newBuilderTestClient(t)).
		SetDeserializerConfig(deserializerConf).
		Build(conf, false)
	if err == nil {
		t.Errorf("Expected an invalid deserializer configuration to fail")
	}
}

// TestKafkaDeserializerBuilderClusterID verifies whether the built
// deserializer asks for the Kafka cluster ID, which depends on the subject
// name strategy.
func TestKafkaDeserializerBuilderClusterID(t *testing.T) {
	client := newBuilderTestClient(t)
	conf := newBuilderTestConfigMap()

	kafkaDeserializer, _, err := NewKafkaDeserializerBuilder().
		SetSchemaRegistryClient(client).Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if !kafkaDeserializer.NeedsClusterID() {
		t.Errorf("Expected the deserializer to need the Kafka cluster ID")
	}

	deserializerConf := NewDeserializerConfig()
	deserializerConf.SubjectNameStrategyConfig = map[string]string{
		serde.KafkaClusterIDConfig: "lkc-123",
	}
	kafkaDeserializer, _, err = NewKafkaDeserializerBuilder().
		SetSchemaRegistryClient(client).
		SetDeserializerConfig(deserializerConf).
		Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if kafkaDeserializer.NeedsClusterID() {
		t.Errorf("Expected a configured cluster ID not to be fetched")
	}

	deserializerConf = NewDeserializerConfig()
	deserializerConf.SubjectNameStrategyType = serde.TopicNameStrategyType
	kafkaDeserializer, _, err = NewKafkaDeserializerBuilder().
		SetSchemaRegistryClient(client).
		SetDeserializerConfig(deserializerConf).
		Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if kafkaDeserializer.NeedsClusterID() {
		t.Errorf("Expected the topic name strategy not to need the cluster ID")
	}
}
