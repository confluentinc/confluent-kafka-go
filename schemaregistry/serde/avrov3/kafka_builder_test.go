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

func builderTestSchemaInfo() schemaregistry.SchemaInfo {
	return schemaregistry.SchemaInfo{
		Schema:     `{"type":"record","name":"DemoSchema","fields":[]}`,
		SchemaType: "AVRO",
	}
}

// countingClient counts how often the Schema Registry client it wraps is
// closed, so that a test can tell whether a serde closed a client it was
// given.
type countingClient struct {
	schemaregistry.Client
	closed int
}

func (c *countingClient) Close() error {
	c.closed++
	return c.Client.Close()
}

// bogusFallbackConfig makes NewSerializer and NewDeserializer fail, by
// configuring a subject name strategy fallback that does not exist.
func bogusFallbackConfig() map[string]string {
	return map[string]string{serde.FallbackTypeConfig: "BOGUS"}
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
// resolves the Kafka cluster ID, which depends on the subject name strategy,
// and that it does so when it looks a subject up rather than when it is built.
func TestKafkaSerializerBuilderClusterID(t *testing.T) {
	client := newBuilderTestClient(t)
	conf := newBuilderTestConfigMap()

	// resolverCalls counts the resolutions the serializer built with
	// serializerConf performs while looking a subject up.
	resolverCalls := func(t *testing.T, serializerConf *SerializerConfig) int {
		t.Helper()
		builder := NewKafkaSerializerBuilder().SetSchemaRegistryClient(client)
		if serializerConf != nil {
			builder = builder.SetSerializerConfig(serializerConf)
		}
		kafkaSerializer, _, err := builder.Build(conf, false)
		if err != nil {
			t.Fatalf("Build failed: %s", err)
		}

		calls := 0
		kafkaSerializer.SetClusterIDResolver(func() (string, error) {
			calls++
			return "lkc-123", nil
		})
		if calls != 0 {
			t.Errorf("Expected the resolver not to be invoked on hand-over, got %d calls", calls)
		}

		ser, ok := kafkaSerializer.(*Serializer)
		if !ok {
			t.Fatalf("Expected a *Serializer, got %T", kafkaSerializer)
		}
		if _, err = ser.SubjectNameStrategy("topic1", serde.ValueSerde, builderTestSchemaInfo()); err != nil {
			t.Fatalf("Subject name lookup failed: %s", err)
		}
		return calls
	}

	// The default subject name strategy resolves the subject through the
	// associations of the Kafka cluster, so it resolves its ID.
	if calls := resolverCalls(t, nil); calls != 1 {
		t.Errorf("Expected the serializer to resolve the Kafka cluster ID once, got %d", calls)
	}

	// A configured cluster ID needs no resolution.
	serializerConf := NewSerializerConfig()
	serializerConf.SubjectNameStrategyConfig = map[string]string{
		serde.KafkaClusterIDConfig: "lkc-123",
	}
	if calls := resolverCalls(t, serializerConf); calls != 0 {
		t.Errorf("Expected a configured cluster ID not to be resolved, got %d calls", calls)
	}

	// Neither does another subject name strategy.
	serializerConf = NewSerializerConfig()
	serializerConf.SubjectNameStrategyType = serde.TopicNameStrategyType
	if calls := resolverCalls(t, serializerConf); calls != 0 {
		t.Errorf("Expected the topic name strategy not to resolve the cluster ID, got %d calls", calls)
	}
}

// TestKafkaSerializerBuilderClientOwnership verifies that a serializer closes
// the Schema Registry client the builder created for it, and never one the
// application supplied.
func TestKafkaSerializerBuilderClientOwnership(t *testing.T) {
	conf := newBuilderTestConfigMap()

	// A client the application supplied is left open, however often the
	// serializer is closed.
	injected := &countingClient{Client: newBuilderTestClient(t)}
	kafkaSerializer, _, err := NewKafkaSerializerBuilder().
		SetSchemaRegistryClient(injected).Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if err = kafkaSerializer.Close(); err != nil {
		t.Errorf("Close failed: %s", err)
	}
	if err = kafkaSerializer.Close(); err != nil {
		t.Errorf("The second Close failed: %s", err)
	}
	if injected.closed != 0 {
		t.Errorf("Expected the injected client to be left open, got %d Close calls", injected.closed)
	}

	// Nor is it closed when the serializer cannot be constructed.
	injected = &countingClient{Client: newBuilderTestClient(t)}
	serializerConf := NewSerializerConfig()
	serializerConf.SubjectNameStrategyConfig = bogusFallbackConfig()
	_, _, err = NewKafkaSerializerBuilder().
		SetSchemaRegistryClient(injected).
		SetSerializerConfig(serializerConf).
		Build(conf, false)
	if err == nil {
		t.Fatal("Expected an invalid subject name strategy fallback to fail the build")
	}
	if injected.closed != 0 {
		t.Errorf("Expected the injected client to be left open, got %d Close calls", injected.closed)
	}

	// A serializer around a client the builder created closes it, and is safe
	// to close more than once.
	kafkaSerializer, _, err = NewKafkaSerializerBuilder().
		SetSchemaRegistryConfig(schemaregistry.NewConfig("mock://")).
		Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if err = kafkaSerializer.Close(); err != nil {
		t.Errorf("Close failed: %s", err)
	}
	if err = kafkaSerializer.Close(); err != nil {
		t.Errorf("The second Close failed: %s", err)
	}

	// The ownership the builder hands over is the Serde's, promoted into the
	// serializer: owning a client makes Close close it, exactly once.
	owned := &countingClient{Client: newBuilderTestClient(t)}
	kafkaSerializer, _, err = NewKafkaSerializerBuilder().
		SetSchemaRegistryClient(owned).Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	ser, ok := kafkaSerializer.(*Serializer)
	if !ok {
		t.Fatalf("Expected a *Serializer, got %T", kafkaSerializer)
	}
	ser.OwnSchemaRegistryClient()
	if err = ser.Close(); err != nil {
		t.Errorf("Close failed: %s", err)
	}
	if err = ser.Close(); err != nil {
		t.Errorf("The second Close failed: %s", err)
	}
	if owned.closed != 1 {
		t.Errorf("Expected the owned client to be closed exactly once, got %d", owned.closed)
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
// deserializer resolves the Kafka cluster ID, which depends on the subject
// name strategy, and that it does so when it looks a subject up rather than
// when it is built.
func TestKafkaDeserializerBuilderClusterID(t *testing.T) {
	client := newBuilderTestClient(t)
	conf := newBuilderTestConfigMap()

	// resolverCalls counts the resolutions the deserializer built with
	// deserializerConf performs while looking a subject up.
	resolverCalls := func(t *testing.T, deserializerConf *DeserializerConfig) int {
		t.Helper()
		builder := NewKafkaDeserializerBuilder().SetSchemaRegistryClient(client)
		if deserializerConf != nil {
			builder = builder.SetDeserializerConfig(deserializerConf)
		}
		kafkaDeserializer, _, err := builder.Build(conf, false)
		if err != nil {
			t.Fatalf("Build failed: %s", err)
		}

		calls := 0
		kafkaDeserializer.SetClusterIDResolver(func() (string, error) {
			calls++
			return "lkc-123", nil
		})
		if calls != 0 {
			t.Errorf("Expected the resolver not to be invoked on hand-over, got %d calls", calls)
		}

		des, ok := kafkaDeserializer.(*Deserializer)
		if !ok {
			t.Fatalf("Expected a *Deserializer, got %T", kafkaDeserializer)
		}
		if _, err = des.SubjectNameStrategy("topic1", serde.ValueSerde, builderTestSchemaInfo()); err != nil {
			t.Fatalf("Subject name lookup failed: %s", err)
		}
		return calls
	}

	// The default subject name strategy resolves the subject through the
	// associations of the Kafka cluster, so it resolves its ID.
	if calls := resolverCalls(t, nil); calls != 1 {
		t.Errorf("Expected the deserializer to resolve the Kafka cluster ID once, got %d", calls)
	}

	// A configured cluster ID needs no resolution.
	deserializerConf := NewDeserializerConfig()
	deserializerConf.SubjectNameStrategyConfig = map[string]string{
		serde.KafkaClusterIDConfig: "lkc-123",
	}
	if calls := resolverCalls(t, deserializerConf); calls != 0 {
		t.Errorf("Expected a configured cluster ID not to be resolved, got %d calls", calls)
	}

	// Neither does another subject name strategy.
	deserializerConf = NewDeserializerConfig()
	deserializerConf.SubjectNameStrategyType = serde.TopicNameStrategyType
	if calls := resolverCalls(t, deserializerConf); calls != 0 {
		t.Errorf("Expected the topic name strategy not to resolve the cluster ID, got %d calls", calls)
	}
}

// TestKafkaDeserializerBuilderClientOwnership verifies that a deserializer
// closes the Schema Registry client the builder created for it, and never one
// the application supplied.
func TestKafkaDeserializerBuilderClientOwnership(t *testing.T) {
	conf := newBuilderTestConfigMap()

	// A client the application supplied is left open, however often the
	// deserializer is closed.
	injected := &countingClient{Client: newBuilderTestClient(t)}
	kafkaDeserializer, _, err := NewKafkaDeserializerBuilder().
		SetSchemaRegistryClient(injected).Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if err = kafkaDeserializer.Close(); err != nil {
		t.Errorf("Close failed: %s", err)
	}
	if err = kafkaDeserializer.Close(); err != nil {
		t.Errorf("The second Close failed: %s", err)
	}
	if injected.closed != 0 {
		t.Errorf("Expected the injected client to be left open, got %d Close calls", injected.closed)
	}

	// Nor is it closed when the deserializer cannot be constructed.
	injected = &countingClient{Client: newBuilderTestClient(t)}
	deserializerConf := NewDeserializerConfig()
	deserializerConf.SubjectNameStrategyConfig = bogusFallbackConfig()
	_, _, err = NewKafkaDeserializerBuilder().
		SetSchemaRegistryClient(injected).
		SetDeserializerConfig(deserializerConf).
		Build(conf, false)
	if err == nil {
		t.Fatal("Expected an invalid subject name strategy fallback to fail the build")
	}
	if injected.closed != 0 {
		t.Errorf("Expected the injected client to be left open, got %d Close calls", injected.closed)
	}

	// A deserializer around a client the builder created closes it, and is
	// safe to close more than once.
	kafkaDeserializer, _, err = NewKafkaDeserializerBuilder().
		SetSchemaRegistryConfig(schemaregistry.NewConfig("mock://")).
		Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	if err = kafkaDeserializer.Close(); err != nil {
		t.Errorf("Close failed: %s", err)
	}
	if err = kafkaDeserializer.Close(); err != nil {
		t.Errorf("The second Close failed: %s", err)
	}

	// The ownership the builder hands over is the Serde's, promoted into the
	// deserializer: owning a client makes Close close it, exactly once.
	owned := &countingClient{Client: newBuilderTestClient(t)}
	kafkaDeserializer, _, err = NewKafkaDeserializerBuilder().
		SetSchemaRegistryClient(owned).Build(conf, false)
	if err != nil {
		t.Fatalf("Build failed: %s", err)
	}
	des, ok := kafkaDeserializer.(*Deserializer)
	if !ok {
		t.Fatalf("Expected a *Deserializer, got %T", kafkaDeserializer)
	}
	des.OwnSchemaRegistryClient()
	if err = des.Close(); err != nil {
		t.Errorf("Close failed: %s", err)
	}
	if err = des.Close(); err != nil {
		t.Errorf("The second Close failed: %s", err)
	}
	if owned.closed != 1 {
		t.Errorf("Expected the owned client to be closed exactly once, got %d", owned.closed)
	}
}
