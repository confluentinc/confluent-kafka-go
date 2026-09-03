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

package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// mockSerializer is a Serializer that records what it was asked to serialize.
type mockSerializer struct {
	prefix         string
	err            error
	needsClusterID bool
	clusterID      string
	topics         []string
	messages       []interface{}
	closed         int
}

func (s *mockSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	s.topics = append(s.topics, topic)
	s.messages = append(s.messages, msg)
	if s.err != nil {
		return nil, s.err
	}
	return []byte(fmt.Sprintf("%s%v", s.prefix, msg)), nil
}

func (s *mockSerializer) SerializeWithHeaders(topic string, msg interface{}) ([]Header, []byte, error) {
	payload, err := s.Serialize(topic, msg)
	if err != nil {
		return nil, nil, err
	}
	return []Header{{Key: "mock", Value: []byte(s.prefix)}}, payload, nil
}

func (s *mockSerializer) NeedsClusterID() bool {
	return s.needsClusterID
}

func (s *mockSerializer) SetClusterID(clusterID string) {
	s.clusterID = clusterID
}

func (s *mockSerializer) Close() error {
	s.closed++
	return nil
}

// mockSerializerBuilder builds a mockSerializer and records the arguments it
// was built with. removeConfigKey, when set, is removed from the returned
// ConfigMap, emulating a builder that consumes its own configuration
// properties.
type mockSerializerBuilder struct {
	serializer      *mockSerializer
	err             error
	removeConfigKey string
	built           int
	isKey           []bool
}

func (b *mockSerializerBuilder) Build(conf *ConfigMap, isKey bool) (Serializer, *ConfigMap, error) {
	b.built++
	b.isKey = append(b.isKey, isKey)
	if b.err != nil {
		return nil, nil, b.err
	}
	filtered := ConfigMap{}
	for k, v := range *conf {
		if k == b.removeConfigKey {
			continue
		}
		filtered[k] = v
	}
	return b.serializer, &filtered, nil
}

func newTestSerializingProducer[K, V any](t *testing.T,
	keyBuilder, valueBuilder SerializerBuilder) *SerializingProducer[K, V] {
	t.Helper()
	p, err := NewSerializingProducer[K, V](&ConfigMap{
		"bootstrap.servers":  "127.0.0.1:65533",
		"socket.timeout.ms":  10,
		"message.timeout.ms": 10,
	}, keyBuilder, valueBuilder)
	if err != nil {
		t.Fatalf("Failed to create SerializingProducer: %s", err)
	}
	return p
}

// TestSerializingProducerBuilders verifies that the serializers are built for
// the right serde type and that the filtered configuration is the one handed
// to the underlying producer.
func TestSerializingProducerBuilders(t *testing.T) {
	keyBuilder := &mockSerializerBuilder{serializer: &mockSerializer{prefix: "k:"}}
	valueBuilder := &mockSerializerBuilder{serializer: &mockSerializer{prefix: "v:"}}

	p := newTestSerializingProducer[string, string](t, keyBuilder, valueBuilder)
	defer p.Close()

	if keyBuilder.built != 1 || valueBuilder.built != 1 {
		t.Errorf("Expected both builders to be called once, got %d and %d",
			keyBuilder.built, valueBuilder.built)
	}
	if len(keyBuilder.isKey) != 1 || !keyBuilder.isKey[0] {
		t.Errorf("Expected the key builder to be called with isKey=true, got %v", keyBuilder.isKey)
	}
	if len(valueBuilder.isKey) != 1 || valueBuilder.isKey[0] {
		t.Errorf("Expected the value builder to be called with isKey=false, got %v", valueBuilder.isKey)
	}
	if p.producer.handle.sendMessageToChannel == nil {
		t.Errorf("Expected the delivery channel hook to be installed on the producer")
	}
}

// TestSerializingProducerBuilderFiltersConfig verifies that a property removed
// by a builder is not passed to the underlying producer, which would reject it.
func TestSerializingProducerBuilderFiltersConfig(t *testing.T) {
	for _, isKey := range []bool{true, false} {
		builder := &mockSerializerBuilder{
			serializer:      &mockSerializer{},
			removeConfigKey: "not.a.kafka.property",
		}
		var keyBuilder, valueBuilder SerializerBuilder
		if isKey {
			keyBuilder = builder
		} else {
			valueBuilder = builder
		}

		p, err := NewSerializingProducer[string, string](&ConfigMap{
			"bootstrap.servers":    "127.0.0.1:65533",
			"message.timeout.ms":   10,
			"not.a.kafka.property": "value",
		}, keyBuilder, valueBuilder)
		if err != nil {
			t.Fatalf("Failed to create SerializingProducer (isKey=%v): %s", isKey, err)
		}
		p.Close()
	}

	// Without a builder to filter it out, the unknown property is rejected.
	_, err := NewSerializingProducer[string, string](&ConfigMap{
		"bootstrap.servers":    "127.0.0.1:65533",
		"not.a.kafka.property": "value",
	}, nil, nil)
	if err == nil {
		t.Errorf("Expected an unfiltered unknown property to fail producer creation")
	}
}

func TestSerializingProducerBuilderError(t *testing.T) {
	buildErr := errors.New("build failed")

	_, err := NewSerializingProducer[string, string](&ConfigMap{},
		&mockSerializerBuilder{err: buildErr}, nil)
	if !errors.Is(err, buildErr) {
		t.Errorf("Expected the key builder error, got %v", err)
	}

	_, err = NewSerializingProducer[string, string](&ConfigMap{},
		nil, &mockSerializerBuilder{err: buildErr})
	if !errors.Is(err, buildErr) {
		t.Errorf("Expected the value builder error, got %v", err)
	}
}

// TestSerializingProducerClusterIDNotNeeded verifies that no cluster ID is
// looked up, nor set, when neither serializer needs it.
func TestSerializingProducerClusterIDNotNeeded(t *testing.T) {
	keySerializer := &mockSerializer{needsClusterID: false}
	valueSerializer := &mockSerializer{needsClusterID: false}

	p := newTestSerializingProducer[string, string](t,
		&mockSerializerBuilder{serializer: keySerializer},
		&mockSerializerBuilder{serializer: valueSerializer})
	defer p.Close()

	if keySerializer.clusterID != "" || valueSerializer.clusterID != "" {
		t.Errorf("Expected no cluster ID to be set, got %q and %q",
			keySerializer.clusterID, valueSerializer.clusterID)
	}
}

// TestProducerGetClusterID verifies the cluster ID lookup fails, rather than
// blocking forever, when no broker answers.
func TestProducerGetClusterID(t *testing.T) {
	p, err := NewProducer(&ConfigMap{"bootstrap.servers": "127.0.0.1:65533"})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	clusterID, err := p.getClusterID(100)
	if err == nil {
		t.Errorf("Expected an error without a broker, got cluster ID %q", clusterID)
	}
	if clusterID != "" {
		t.Errorf("Expected an empty cluster ID, got %q", clusterID)
	}
}

// TestSerializingProducerProduce produces a message and verifies that the key
// and the value are serialized, and that the delivery report is delivered as
// the SerializableMessage that was produced.
func TestSerializingProducerProduce(t *testing.T) {
	keySerializer := &mockSerializer{prefix: "k:"}
	valueSerializer := &mockSerializer{prefix: "v:"}
	p := newTestSerializingProducer[string, int](t,
		&mockSerializerBuilder{serializer: keySerializer},
		&mockSerializerBuilder{serializer: valueSerializer})
	defer p.Close()

	topic := "gotest"
	msg := &SerializableMessage[string, int]{
		TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
		Key:            "mykey",
		Value:          42,
		Headers:        []Header{{Key: "hkey", Value: []byte("hvalue")}},
	}

	drChan := make(chan Event, 1)
	if err := p.Produce(msg, drChan); err != nil {
		t.Fatalf("Produce failed: %s", err)
	}

	if len(keySerializer.topics) != 1 || keySerializer.topics[0] != topic {
		t.Errorf("Expected the key serializer to be called with %q, got %v", topic, keySerializer.topics)
	}
	if len(keySerializer.messages) != 1 || keySerializer.messages[0] != "mykey" {
		t.Errorf("Expected the key serializer to be called with the key, got %v", keySerializer.messages)
	}
	if len(valueSerializer.messages) != 1 || valueSerializer.messages[0] != 42 {
		t.Errorf("Expected the value serializer to be called with the value, got %v", valueSerializer.messages)
	}
	if string(msg.keyBytes) != "k:mykey" {
		t.Errorf("Expected the serialized key on the message, got %q", string(msg.keyBytes))
	}
	if string(msg.valueBytes) != "v:42" {
		t.Errorf("Expected the serialized value on the message, got %q", string(msg.valueBytes))
	}
	if msg.SerializedKeySize() != len("k:mykey") || msg.SerializedValueSize() != len("v:42") {
		t.Errorf("Unexpected serialized sizes %d and %d",
			msg.SerializedKeySize(), msg.SerializedValueSize())
	}

	select {
	case ev := <-drChan:
		// The delivery report is the produced SerializableMessage itself,
		// not the serialized Message.
		dr, ok := ev.(*SerializableMessage[string, int])
		if !ok {
			t.Fatalf("Expected a *SerializableMessage delivery report, got %T: %v", ev, ev)
		}
		if dr != msg {
			t.Errorf("Expected the produced message in the delivery report")
		}
		if dr.Key != "mykey" || dr.Value != 42 {
			t.Errorf("Expected the key and the value to be kept, got %v and %v", dr.Key, dr.Value)
		}
		if dr.TopicPartition.Topic == nil || *dr.TopicPartition.Topic != topic {
			t.Errorf("Expected the topic to be set on the delivery report, got %v", dr.TopicPartition)
		}
		// No broker is running, so delivery is expected to have failed.
		if dr.TopicPartition.Error == nil {
			t.Errorf("Expected a delivery error without a broker")
		}
	case <-time.After(10 * time.Second):
		t.Errorf("Timed out waiting for the delivery report")
	}
}

func TestSerializingProducerProduceSerializerError(t *testing.T) {
	serializeErr := errors.New("serialization failed")
	topic := "gotest"

	keySerializer := &mockSerializer{err: serializeErr}
	p := newTestSerializingProducer[string, string](t,
		&mockSerializerBuilder{serializer: keySerializer}, nil)
	err := p.Produce(&SerializableMessage[string, string]{
		TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
		Key:            "mykey",
	}, nil)
	if !errors.Is(err, serializeErr) {
		t.Errorf("Expected the key serialization error, got %v", err)
	}
	p.Close()

	valueSerializer := &mockSerializer{err: serializeErr}
	p = newTestSerializingProducer[string, string](t,
		nil, &mockSerializerBuilder{serializer: valueSerializer})
	err = p.Produce(&SerializableMessage[string, string]{
		TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
		Value:          "myvalue",
	}, nil)
	if !errors.Is(err, serializeErr) {
		t.Errorf("Expected the value serialization error, got %v", err)
	}
	p.Close()
}

// TestSerializingProducerProduceWithoutSerializers verifies that producing a
// non-empty key or value without the corresponding serializer is rejected,
// while empty ones are passed through.
func TestSerializingProducerProduceWithoutSerializers(t *testing.T) {
	topic := "gotest"

	p := newTestSerializingProducer[string, string](t, nil, nil)
	defer p.Close()

	err := p.Produce(&SerializableMessage[string, string]{
		TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
		Key:            "mykey",
	}, nil)
	if err == nil || err.(Error).Code() != ErrInvalidArg {
		t.Errorf("Expected ErrInvalidArg for a key without a key serializer, got %v", err)
	}

	err = p.Produce(&SerializableMessage[string, string]{
		TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
		Value:          "myvalue",
	}, nil)
	if err == nil || err.(Error).Code() != ErrInvalidArg {
		t.Errorf("Expected ErrInvalidArg for a value without a value serializer, got %v", err)
	}

	// Empty strings need no serializer.
	err = p.Produce(&SerializableMessage[string, string]{
		TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
	}, nil)
	if err != nil {
		t.Errorf("Expected empty key and value to be produced, got %v", err)
	}

	// Neither do nil interface values.
	pAny := newTestSerializingProducer[any, any](t, nil, nil)
	defer pAny.Close()
	err = pAny.Produce(&SerializableMessage[any, any]{
		TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
	}, nil)
	if err != nil {
		t.Errorf("Expected nil key and value to be produced, got %v", err)
	}

	// A non-string type is always rejected without a serializer.
	pInt := newTestSerializingProducer[int, int](t, nil, nil)
	defer pInt.Close()
	err = pInt.Produce(&SerializableMessage[int, int]{
		TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
	}, nil)
	if err == nil || err.(Error).Code() != ErrInvalidArg {
		t.Errorf("Expected ErrInvalidArg for a typed key without a key serializer, got %v", err)
	}
}

// TestSerializingProducerProduceWithoutTopic verifies that a message without a
// topic is rejected the same way [Producer.Produce] rejects it.
func TestSerializingProducerProduceWithoutTopic(t *testing.T) {
	keySerializer := &mockSerializer{prefix: "k:"}
	p := newTestSerializingProducer[string, string](t,
		&mockSerializerBuilder{serializer: keySerializer}, nil)
	defer p.Close()

	err := p.Produce(&SerializableMessage[string, string]{Key: "mykey"}, nil)
	if err == nil || err.(Error).Code() != ErrInvalidArg {
		t.Errorf("Expected ErrInvalidArg for a message without a topic, got %v", err)
	}

	emptyTopic := ""
	err = p.Produce(&SerializableMessage[string, string]{
		TopicPartition: TopicPartition{Topic: &emptyTopic},
	}, nil)
	if err == nil || err.(Error).Code() != ErrInvalidArg {
		t.Errorf("Expected ErrInvalidArg for an empty topic, got %v", err)
	}

	err = p.Produce(nil, nil)
	if err == nil || err.(Error).Code() != ErrInvalidArg {
		t.Errorf("Expected ErrInvalidArg for a nil message, got %v", err)
	}

	// The message is rejected before anything is serialized.
	if len(keySerializer.messages) != 0 {
		t.Errorf("Expected nothing to be serialized, got %v", keySerializer.messages)
	}
}

// TestSerializingProducerCloseClosesSerializers verifies that closing the
// producer also closes the serializers it built.
func TestSerializingProducerCloseClosesSerializers(t *testing.T) {
	keySerializer := &mockSerializer{}
	valueSerializer := &mockSerializer{}
	p := newTestSerializingProducer[string, string](t,
		&mockSerializerBuilder{serializer: keySerializer},
		&mockSerializerBuilder{serializer: valueSerializer})

	p.Close()

	if keySerializer.closed != 1 {
		t.Errorf("Expected the key serializer to be closed once, got %d", keySerializer.closed)
	}
	if valueSerializer.closed != 1 {
		t.Errorf("Expected the value serializer to be closed once, got %d", valueSerializer.closed)
	}

	// A producer without serializers closes just as well.
	newTestSerializingProducer[string, string](t, nil, nil).Close()
}

// TestSerializingProducerSendToChannel exercises the delivery report hook
// directly, including a message that was not produced through the
// SerializingProducer.
func TestSerializingProducerSendToChannel(t *testing.T) {
	p := newTestSerializingProducer[string, string](t, nil, nil)
	defer p.Close()

	topic := "gotest"
	serializableMessage := &SerializableMessage[string, string]{}
	timestamp := time.Now().Truncate(time.Millisecond)
	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 1, Offset: 5},
		Timestamp:      timestamp,
		TimestampType:  TimestampCreateTime,
		Opaque:         serializableMessage,
	}

	deliveryChan := make(chan Event, 1)
	termChan := make(chan bool)
	if term := p.sendToChannel(msg, &deliveryChan, termChan); term {
		t.Fatalf("Expected the message to be sent, not terminated")
	}

	ev := <-deliveryChan
	if ev != any(serializableMessage) {
		t.Errorf("Expected the SerializableMessage on the channel, got %v", ev)
	}
	if serializableMessage.TopicPartition.Topic == nil ||
		*serializableMessage.TopicPartition.Topic != topic ||
		serializableMessage.TopicPartition.Offset != 5 {
		t.Errorf("Expected the topic partition to be copied, got %v",
			serializableMessage.TopicPartition)
	}
	if !serializableMessage.Timestamp.Equal(timestamp) ||
		serializableMessage.TimestampType != TimestampCreateTime {
		t.Errorf("Expected the timestamp to be copied, got %v (%v)",
			serializableMessage.Timestamp, serializableMessage.TimestampType)
	}

	// A delivery report that does not carry a SerializableMessage cannot be
	// mapped back to the message it belongs to, which is a bug rather than a
	// runtime condition.
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("Expected a panic for a delivery report without a SerializableMessage")
				return
			}
			if !strings.Contains(fmt.Sprint(r), "*SerializableMessage") {
				t.Errorf("Unexpected panic message: %v", r)
			}
		}()
		p.sendToChannel(&Message{TopicPartition: TopicPartition{Topic: &topic}},
			&deliveryChan, termChan)
	}()
	select {
	case ev := <-deliveryChan:
		t.Errorf("Expected nothing to be sent to the channel, got %v", ev)
	default:
	}

	// A terminating channel is reported back to the caller.
	fullChan := make(chan Event)
	close(termChan)
	if term := p.sendToChannel(msg, &fullChan, termChan); !term {
		t.Errorf("Expected termination to be reported")
	}
}

// TestSerializingProducerAPIs dry-tests the methods delegating to the
// underlying producer, no broker is needed.
func TestSerializingProducerAPIs(t *testing.T) {
	// No bootstrap.servers, so that no connection error events end up on the
	// events channel and make the queue length unpredictable.
	p, err := NewSerializingProducer[string, string](&ConfigMap{
		"socket.timeout.ms":  10,
		"message.timeout.ms": 10,
	}, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create SerializingProducer: %s", err)
	}

	if p.String() == "" {
		t.Errorf("Expected a producer name")
	}
	if p.IsClosed() {
		t.Errorf("Expected the producer not to be closed")
	}
	if p.Len() != 0 {
		t.Errorf("Expected an empty queue, got %d", p.Len())
	}
	if p.Events() == nil {
		t.Errorf("Expected an events channel")
	}
	if p.Logs() != nil {
		t.Errorf("Expected no logs channel when log queueing is not enabled")
	}
	if remaining := p.Flush(100); remaining != 0 {
		t.Errorf("Expected nothing left to flush, got %d", remaining)
	}
	if err := p.Purge(PurgeInFlight | PurgeQueue); err != nil {
		t.Errorf("Purge failed: %s", err)
	}
	if err := p.GetFatalError(); err != nil {
		t.Errorf("Expected no fatal error, got %s", err)
	}
	if code := p.TestFatalError(ErrOutOfOrderSequenceNumber, "test"); code != ErrNoError {
		t.Errorf("Expected ErrNoError, got %s", code)
	}
	if err := p.GetFatalError(); err == nil {
		t.Errorf("Expected a fatal error after TestFatalError")
	}

	topic := "gotest"
	if _, err := p.GetMetadata(&topic, false, 100); err == nil {
		t.Errorf("Expected GetMetadata to fail without a broker")
	}
	if _, _, err := p.QueryWatermarkOffsets(topic, 0, 100); err == nil {
		t.Errorf("Expected QueryWatermarkOffsets to fail without a broker")
	}
	if _, err := p.OffsetsForTimes([]TopicPartition{{Topic: &topic, Offset: 12345}}, 100); err == nil {
		t.Errorf("Expected OffsetsForTimes to fail without a broker")
	}
	if err := p.SetOAuthBearerToken(OAuthBearerToken{
		TokenValue: "token", Expiration: time.Now().Add(time.Hour), Principal: "gotest",
	}); err == nil {
		t.Errorf("Expected SetOAuthBearerToken to fail when SASL OAUTHBEARER is not configured")
	}
	if err := p.SetOAuthBearerTokenFailure("failure"); err == nil {
		t.Errorf("Expected SetOAuthBearerTokenFailure to fail when SASL OAUTHBEARER is not configured")
	}
	if err := p.SetSaslCredentials("user", "pass"); err != nil {
		t.Errorf("SetSaslCredentials failed: %s", err)
	}

	p.Close()
	if !p.IsClosed() {
		t.Errorf("Expected the producer to be closed")
	}
}

// TestSerializingProducerTransactionalAPIs dry-tests the transactional methods
// delegating to the underlying producer, no broker is needed.
func TestSerializingProducerTransactionalAPIs(t *testing.T) {
	p, err := NewSerializingProducer[string, string](&ConfigMap{
		"bootstrap.servers":      "127.0.0.1:65533",
		"transactional.id":       "gotest",
		"transaction.timeout.ms": "4000",
	}, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create transactional SerializingProducer: %s", err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.InitTransactions(ctx); err == nil || err.(Error).Code() != ErrTimedOut {
		t.Errorf("Expected InitTransactions to time out without a broker, got %v", err)
	}

	// The remaining APIs fail because InitTransactions() did not succeed.
	if err := p.BeginTransaction(); err == nil {
		t.Errorf("Expected BeginTransaction to fail due to state")
	}
	cgmd, err := NewTestConsumerGroupMetadata("gotestgroup")
	if err != nil {
		t.Fatalf("Failed to create group metadata: %s", err)
	}
	topic := "gotest"
	if err := p.SendOffsetsToTransaction(context.TODO(),
		[]TopicPartition{{Topic: &topic, Partition: 0, Offset: 1}}, cgmd); err == nil {
		t.Errorf("Expected SendOffsetsToTransaction to fail due to state")
	}
	if err := p.CommitTransaction(context.TODO()); err == nil {
		t.Errorf("Expected CommitTransaction to fail due to state")
	}
	if err := p.AbortTransaction(context.TODO()); err == nil {
		t.Errorf("Expected AbortTransaction to fail due to state")
	}
}
