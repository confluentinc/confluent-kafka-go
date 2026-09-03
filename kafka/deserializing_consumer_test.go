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
	"errors"
	"strings"
	"testing"
	"time"
)

// mockDeserializer is a Deserializer that returns a canned value and records
// what it was asked to deserialize.
type mockDeserializer struct {
	value          interface{}
	err            error
	needsClusterID bool
	clusterID      string
	topics         []string
	headers        [][]Header
	payloads       [][]byte
	closeErr       error
	closed         int
}

func (d *mockDeserializer) DeserializeWithHeaders(topic string, headers []Header, payload []byte) (interface{}, error) {
	d.topics = append(d.topics, topic)
	d.headers = append(d.headers, headers)
	d.payloads = append(d.payloads, payload)
	if d.err != nil {
		return nil, d.err
	}
	return d.value, nil
}

func (d *mockDeserializer) NeedsClusterID() bool {
	return d.needsClusterID
}

func (d *mockDeserializer) SetClusterID(clusterID string) {
	d.clusterID = clusterID
}

func (d *mockDeserializer) Close() error {
	d.closed++
	return d.closeErr
}

// mockDeserializerBuilder builds a mockDeserializer and records the arguments
// it was built with. removeConfigKey, when set, is removed from the returned
// ConfigMap, emulating a builder that consumes its own configuration
// properties.
type mockDeserializerBuilder struct {
	deserializer    *mockDeserializer
	err             error
	removeConfigKey string
	built           int
	isKey           []bool
}

func (b *mockDeserializerBuilder) Build(conf *ConfigMap, isKey bool) (Deserializer, *ConfigMap, error) {
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
	return b.deserializer, &filtered, nil
}

func newTestDeserializingConsumer[K, V any](t *testing.T,
	keyBuilder, valueBuilder DeserializerBuilder) *DeserializingConsumer[K, V] {
	t.Helper()
	c, err := NewDeserializingConsumer[K, V](&ConfigMap{
		"group.id":           "gotest",
		"socket.timeout.ms":  10,
		"session.timeout.ms": 10,
	}, keyBuilder, valueBuilder)
	if err != nil {
		t.Fatalf("Failed to create DeserializingConsumer: %s", err)
	}
	return c
}

// TestDeserializingConsumerBuilders verifies that the deserializers are built
// for the right serde type and that the filtered configuration is the one
// handed to the underlying consumer.
func TestDeserializingConsumerBuilders(t *testing.T) {
	keyBuilder := &mockDeserializerBuilder{deserializer: &mockDeserializer{value: "key"}}
	valueBuilder := &mockDeserializerBuilder{deserializer: &mockDeserializer{value: "value"}}

	c := newTestDeserializingConsumer[string, string](t, keyBuilder, valueBuilder)
	defer c.Close()

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
}

// TestDeserializingConsumerBuilderFiltersConfig verifies that a property
// removed by a builder is not passed to the underlying consumer, which would
// reject it.
func TestDeserializingConsumerBuilderFiltersConfig(t *testing.T) {
	for _, isKey := range []bool{true, false} {
		builder := &mockDeserializerBuilder{
			deserializer:    &mockDeserializer{},
			removeConfigKey: "not.a.kafka.property",
		}
		var keyBuilder, valueBuilder DeserializerBuilder
		if isKey {
			keyBuilder = builder
		} else {
			valueBuilder = builder
		}

		c, err := NewDeserializingConsumer[string, string](&ConfigMap{
			"group.id":             "gotest",
			"not.a.kafka.property": "value",
		}, keyBuilder, valueBuilder)
		if err != nil {
			t.Fatalf("Failed to create DeserializingConsumer (isKey=%v): %s", isKey, err)
		}
		c.Close()
	}

	// Without a builder to filter it out, the unknown property is rejected.
	_, err := NewDeserializingConsumer[string, string](&ConfigMap{
		"group.id":             "gotest",
		"not.a.kafka.property": "value",
	}, nil, nil)
	if err == nil {
		t.Errorf("Expected an unfiltered unknown property to fail consumer creation")
	}
}

func TestDeserializingConsumerBuilderError(t *testing.T) {
	buildErr := errors.New("build failed")

	_, err := NewDeserializingConsumer[string, string](&ConfigMap{"group.id": "gotest"},
		&mockDeserializerBuilder{err: buildErr}, nil)
	if !errors.Is(err, buildErr) {
		t.Errorf("Expected the key builder error, got %v", err)
	}

	_, err = NewDeserializingConsumer[string, string](&ConfigMap{"group.id": "gotest"},
		nil, &mockDeserializerBuilder{err: buildErr})
	if !errors.Is(err, buildErr) {
		t.Errorf("Expected the value builder error, got %v", err)
	}

	// A consumer configuration error is returned as well.
	_, err = NewDeserializingConsumer[string, string](&ConfigMap{}, nil, nil)
	if err == nil {
		t.Errorf("Expected NewDeserializingConsumer() to fail without group.id")
	}
}

// TestDeserializingConsumerClusterIDNotNeeded verifies that no cluster ID is
// looked up, nor set, when neither deserializer needs it.
func TestDeserializingConsumerClusterIDNotNeeded(t *testing.T) {
	keyDeserializer := &mockDeserializer{needsClusterID: false}
	valueDeserializer := &mockDeserializer{needsClusterID: false}

	c := newTestDeserializingConsumer[string, string](t,
		&mockDeserializerBuilder{deserializer: keyDeserializer},
		&mockDeserializerBuilder{deserializer: valueDeserializer})
	defer c.Close()

	if keyDeserializer.clusterID != "" || valueDeserializer.clusterID != "" {
		t.Errorf("Expected no cluster ID to be set, got %q and %q",
			keyDeserializer.clusterID, valueDeserializer.clusterID)
	}
}

// TestConsumerGetClusterID verifies the cluster ID lookup fails, rather than
// blocking forever, when no broker answers.
func TestConsumerGetClusterID(t *testing.T) {
	c, err := NewConsumer(&ConfigMap{
		"group.id":          "gotest",
		"bootstrap.servers": "127.0.0.1:65533",
	})
	if err != nil {
		t.Fatalf("Failed to create consumer: %s", err)
	}
	defer c.Close()

	clusterID, err := c.getClusterID(100)
	if err == nil {
		t.Errorf("Expected an error without a broker, got cluster ID %q", clusterID)
	}
	if clusterID != "" {
		t.Errorf("Expected an empty cluster ID, got %q", clusterID)
	}
}

func TestDeserializationErrors(t *testing.T) {
	topic := "gotest"
	tp := TopicPartition{Topic: &topic, Partition: 3, Offset: 42}
	cause := errors.New("bad payload")

	keyErr := NewKeyDeserializationError(tp, cause)
	if keyErr.TopicPartition != tp {
		t.Errorf("Expected the topic partition to be kept, got %v", keyErr.TopicPartition)
	}
	for _, s := range []string{keyErr.Error(), keyErr.String()} {
		for _, expected := range []string{"key", "gotest-3", "offset 42", "bad payload"} {
			if !strings.Contains(s, expected) {
				t.Errorf("Expected %q in %q", expected, s)
			}
		}
	}

	valueErr := NewValueDeserializationError(tp, cause)
	if valueErr.TopicPartition != tp {
		t.Errorf("Expected the topic partition to be kept, got %v", valueErr.TopicPartition)
	}
	for _, s := range []string{valueErr.Error(), valueErr.String()} {
		for _, expected := range []string{"value", "gotest-3", "offset 42", "bad payload"} {
			if !strings.Contains(s, expected) {
				t.Errorf("Expected %q in %q", expected, s)
			}
		}
	}

	// Both are usable as Events and as errors.
	var _ Event = keyErr
	var _ Event = valueErr
	var _ error = keyErr
	var _ error = valueErr
}

// TestDeserializingConsumerDeserializeMessage covers the message conversion
// done by Poll().
func TestDeserializingConsumerDeserializeMessage(t *testing.T) {
	topic := "gotest"
	headers := []Header{{Key: "hkey", Value: []byte("hvalue")}}
	newMessage := func() *Message {
		return &Message{
			TopicPartition: TopicPartition{Topic: &topic, Partition: 1, Offset: 5},
			Key:            []byte("serialized-key"),
			Value:          []byte("serialized-value"),
			Headers:        headers,
			Timestamp:      time.Now().Truncate(time.Millisecond),
			TimestampType:  TimestampCreateTime,
		}
	}

	t.Run("key and value", func(t *testing.T) {
		keyDeserializer := &mockDeserializer{value: "mykey"}
		valueDeserializer := &mockDeserializer{value: 42}
		dc := &DeserializingConsumer[string, int]{
			keyDeserializer:   keyDeserializer,
			valueDeserializer: valueDeserializer,
		}

		msg := newMessage()
		ev := dc.deserializeMessage(msg)
		deserialized, ok := ev.(*DeserializedMessage[string, int])
		if !ok {
			t.Fatalf("Expected a *DeserializedMessage, got %T: %v", ev, ev)
		}
		if deserialized.Key != "mykey" || deserialized.Value != 42 {
			t.Errorf("Expected mykey/42, got %v/%v", deserialized.Key, deserialized.Value)
		}
		if deserialized.SerializedKeySize() != len(msg.Key) ||
			deserialized.SerializedValueSize() != len(msg.Value) {
			t.Errorf("Expected the serialized sizes to be kept, got %d and %d",
				deserialized.SerializedKeySize(), deserialized.SerializedValueSize())
		}
		if len(keyDeserializer.topics) != 1 || keyDeserializer.topics[0] != topic {
			t.Errorf("Expected the key deserializer to be called with %q, got %v",
				topic, keyDeserializer.topics)
		}
		if len(keyDeserializer.headers) != 1 || len(keyDeserializer.headers[0]) != 1 {
			t.Errorf("Expected the headers to be passed to the key deserializer, got %v",
				keyDeserializer.headers)
		}
		if len(valueDeserializer.payloads) != 1 ||
			string(valueDeserializer.payloads[0]) != "serialized-value" {
			t.Errorf("Expected the value payload to be passed to the value deserializer, got %v",
				valueDeserializer.payloads)
		}
	})

	t.Run("no deserializers", func(t *testing.T) {
		dc := &DeserializingConsumer[string, string]{}
		ev := dc.deserializeMessage(newMessage())
		deserialized, ok := ev.(*DeserializedMessage[string, string])
		if !ok {
			t.Fatalf("Expected a *DeserializedMessage, got %T: %v", ev, ev)
		}
		if deserialized.Key != "" || deserialized.Value != "" {
			t.Errorf("Expected zero values, got %q/%q", deserialized.Key, deserialized.Value)
		}
	})

	t.Run("nil key and value", func(t *testing.T) {
		keyDeserializer := &mockDeserializer{value: "mykey"}
		valueDeserializer := &mockDeserializer{value: "myvalue"}
		dc := &DeserializingConsumer[string, string]{
			keyDeserializer:   keyDeserializer,
			valueDeserializer: valueDeserializer,
		}

		msg := newMessage()
		msg.Key = nil
		msg.Value = nil
		ev := dc.deserializeMessage(msg)
		deserialized, ok := ev.(*DeserializedMessage[string, string])
		if !ok {
			t.Fatalf("Expected a *DeserializedMessage, got %T: %v", ev, ev)
		}
		if deserialized.Key != "" || deserialized.Value != "" {
			t.Errorf("Expected zero values, got %q/%q", deserialized.Key, deserialized.Value)
		}
		if len(keyDeserializer.topics) != 0 || len(valueDeserializer.topics) != 0 {
			t.Errorf("Expected the deserializers not to be called for nil key and value")
		}
	})

	t.Run("message without topic", func(t *testing.T) {
		dc := &DeserializingConsumer[string, string]{
			keyDeserializer: &mockDeserializer{value: "mykey"},
		}
		msg := newMessage()
		msg.TopicPartition.Topic = nil
		if ev := dc.deserializeMessage(msg); ev != any(msg) {
			t.Errorf("Expected the message to be returned as-is, got %T: %v", ev, ev)
		}
	})

	t.Run("key deserialization error", func(t *testing.T) {
		cause := errors.New("bad key")
		dc := &DeserializingConsumer[string, string]{
			keyDeserializer:   &mockDeserializer{err: cause},
			valueDeserializer: &mockDeserializer{value: "myvalue"},
		}
		ev := dc.deserializeMessage(newMessage())
		keyErr, ok := ev.(KeyDeserializationError)
		if !ok {
			t.Fatalf("Expected a KeyDeserializationError, got %T: %v", ev, ev)
		}
		if !errors.Is(keyErr.err, cause) {
			t.Errorf("Expected the cause to be kept, got %v", keyErr.err)
		}
	})

	t.Run("value deserialization error", func(t *testing.T) {
		cause := errors.New("bad value")
		dc := &DeserializingConsumer[string, string]{
			keyDeserializer:   &mockDeserializer{value: "mykey"},
			valueDeserializer: &mockDeserializer{err: cause},
		}
		ev := dc.deserializeMessage(newMessage())
		valueErr, ok := ev.(ValueDeserializationError)
		if !ok {
			t.Fatalf("Expected a ValueDeserializationError, got %T: %v", ev, ev)
		}
		if !errors.Is(valueErr.err, cause) {
			t.Errorf("Expected the cause to be kept, got %v", valueErr.err)
		}
	})

	t.Run("wrong key type", func(t *testing.T) {
		dc := &DeserializingConsumer[string, string]{
			keyDeserializer: &mockDeserializer{value: 42},
		}
		ev := dc.deserializeMessage(newMessage())
		keyErr, ok := ev.(KeyDeserializationError)
		if !ok {
			t.Fatalf("Expected a KeyDeserializationError, got %T: %v", ev, ev)
		}
		if !strings.Contains(keyErr.Error(), "Wrong deserialized key type: int") {
			t.Errorf("Expected the actual type in %q", keyErr.Error())
		}
	})

	t.Run("wrong value type", func(t *testing.T) {
		dc := &DeserializingConsumer[string, string]{
			valueDeserializer: &mockDeserializer{value: 42},
		}
		ev := dc.deserializeMessage(newMessage())
		valueErr, ok := ev.(ValueDeserializationError)
		if !ok {
			t.Fatalf("Expected a ValueDeserializationError, got %T: %v", ev, ev)
		}
		if !strings.Contains(valueErr.Error(), "Wrong deserialized value type: int") {
			t.Errorf("Expected the actual type in %q", valueErr.Error())
		}
	})
}

// TestDeserializingConsumerCloseClosesDeserializers verifies that closing the
// consumer also closes the deserializers it built, and reports their errors.
func TestDeserializingConsumerCloseClosesDeserializers(t *testing.T) {
	keyDeserializer := &mockDeserializer{}
	valueDeserializer := &mockDeserializer{}
	c := newTestDeserializingConsumer[string, string](t,
		&mockDeserializerBuilder{deserializer: keyDeserializer},
		&mockDeserializerBuilder{deserializer: valueDeserializer})

	if err := c.Close(); err != nil {
		t.Errorf("Close() failed: %s", err)
	}
	if keyDeserializer.closed != 1 {
		t.Errorf("Expected the key deserializer to be closed once, got %d", keyDeserializer.closed)
	}
	if valueDeserializer.closed != 1 {
		t.Errorf("Expected the value deserializer to be closed once, got %d", valueDeserializer.closed)
	}

	// The errors of the deserializers are reported.
	keyCloseErr := errors.New("key close failed")
	valueCloseErr := errors.New("value close failed")
	c = newTestDeserializingConsumer[string, string](t,
		&mockDeserializerBuilder{deserializer: &mockDeserializer{closeErr: keyCloseErr}},
		&mockDeserializerBuilder{deserializer: &mockDeserializer{closeErr: valueCloseErr}})
	err := c.Close()
	if !errors.Is(err, keyCloseErr) || !errors.Is(err, valueCloseErr) {
		t.Errorf("Expected both close errors, got %v", err)
	}

	// A consumer without deserializers closes just as well.
	if err := newTestDeserializingConsumer[string, string](t, nil, nil).Close(); err != nil {
		t.Errorf("Close() failed: %s", err)
	}
}

// TestDeserializingConsumerAPIs dry-tests the methods delegating to the
// underlying consumer, no broker is needed.
func TestDeserializingConsumerAPIs(t *testing.T) {
	c, err := NewDeserializingConsumer[string, string](&ConfigMap{
		"group.id":                 "gotest",
		"socket.timeout.ms":        10,
		"session.timeout.ms":       10,
		"enable.auto.offset.store": false, // permit StoreOffsets()
	}, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create DeserializingConsumer: %s", err)
	}

	if c.String() == "" {
		t.Errorf("Expected a consumer name")
	}
	if c.IsClosed() {
		t.Errorf("Expected the consumer not to be closed")
	}
	if c.Logs() != nil {
		t.Errorf("Expected no logs channel when log queueing is not enabled")
	}

	topic := "gotest"
	if err := c.Subscribe(topic, nil); err != nil {
		t.Errorf("Subscribe() failed: %s", err)
	}
	topics, err := c.Subscription()
	if err != nil || len(topics) != 1 || topics[0] != topic {
		t.Errorf("Expected a subscription to %s, got %v (%v)", topic, topics, err)
	}
	if err := c.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe() failed: %s", err)
	}
	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		t.Errorf("SubscribeTopics() failed: %s", err)
	}
	if err := c.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe() failed: %s", err)
	}

	if err := c.Assign([]TopicPartition{{Topic: &topic, Partition: 0}}); err != nil {
		t.Errorf("Assign() failed: %s", err)
	}
	assignment, err := c.Assignment()
	if err != nil || len(assignment) != 1 {
		t.Errorf("Expected one assigned partition, got %v (%v)", assignment, err)
	}
	if _, err := c.SeekPartitions([]TopicPartition{
		{Topic: &topic, Partition: 0, Offset: OffsetBeginning}}); err != nil {
		t.Errorf("SeekPartitions() failed: %s", err)
	}
	if err := c.Pause([]TopicPartition{{Topic: &topic, Partition: 0}}); err != nil {
		t.Errorf("Pause() failed: %s", err)
	}
	if err := c.Resume([]TopicPartition{{Topic: &topic, Partition: 0}}); err != nil {
		t.Errorf("Resume() failed: %s", err)
	}
	if _, err := c.Position([]TopicPartition{{Topic: &topic, Partition: 0}}); err != nil {
		t.Errorf("Position() failed: %s", err)
	}
	if err := c.Unassign(); err != nil {
		t.Errorf("Unassign() failed: %s", err)
	}
	if err := c.IncrementalAssign([]TopicPartition{{Topic: &topic, Partition: 1}}); err != nil {
		t.Errorf("IncrementalAssign() failed: %s", err)
	}
	if err := c.IncrementalUnassign([]TopicPartition{{Topic: &topic, Partition: 1}}); err != nil {
		t.Errorf("IncrementalUnassign() failed: %s", err)
	}
	if protocol := c.GetRebalanceProtocol(); protocol != "NONE" && protocol != "" {
		t.Errorf("Expected no rebalance protocol without an assignment, got %s", protocol)
	}
	if c.AssignmentLost() {
		t.Errorf("Expected the assignment not to be lost")
	}

	// Offset management, including the DeserializedMessage overloads.
	deserializedMessage := newDeserializedMessage(
		&Message{TopicPartition: TopicPartition{Topic: &topic, Partition: 0, Offset: 1}},
		"key", "value")
	// Nothing is assigned at this point, so storing an offset is rejected,
	// but the call must still reach the underlying consumer.
	if _, err := c.StoreMessage(deserializedMessage); err == nil {
		t.Errorf("Expected StoreMessage() to fail without an assignment")
	}
	if _, err := c.StoreOffsets([]TopicPartition{
		{Topic: &topic, Partition: 0, Offset: 1}}); err == nil {
		t.Errorf("Expected StoreOffsets() to fail without an assignment")
	}
	if _, err := c.Commit(); err == nil || err.(Error).Code() != ErrNoOffset {
		t.Errorf("Expected ErrNoOffset, got %v", err)
	}
	if _, err := c.CommitMessage(deserializedMessage); err == nil {
		t.Errorf("Expected CommitMessage() to fail without a broker")
	}
	if _, err := c.CommitOffsets([]TopicPartition{
		{Topic: &topic, Partition: 0, Offset: 1}}); err == nil {
		t.Errorf("Expected CommitOffsets() to fail without a broker")
	}
	if _, err := c.Committed([]TopicPartition{{Topic: &topic, Partition: 0}}, 100); err == nil {
		t.Errorf("Expected Committed() to fail without a broker")
	}
	if _, err := c.GetConsumerGroupMetadata(); err != nil {
		t.Errorf("GetConsumerGroupMetadata() failed: %s", err)
	}

	if _, err := c.GetMetadata(&topic, false, 100); err == nil {
		t.Errorf("Expected GetMetadata() to fail without a broker")
	}
	if _, _, err := c.QueryWatermarkOffsets(topic, 0, 100); err == nil {
		t.Errorf("Expected QueryWatermarkOffsets() to fail without a broker")
	}
	if _, _, err := c.GetWatermarkOffsets(topic, 0); err != nil {
		t.Errorf("GetWatermarkOffsets() failed: %s", err)
	}
	if _, err := c.OffsetsForTimes([]TopicPartition{
		{Topic: &topic, Offset: 12345}}, 100); err == nil {
		t.Errorf("Expected OffsetsForTimes() to fail without a broker")
	}
	if err := c.SetOAuthBearerToken(OAuthBearerToken{
		TokenValue: "token", Expiration: time.Now().Add(time.Hour), Principal: "gotest",
	}); err == nil {
		t.Errorf("Expected SetOAuthBearerToken() to fail when SASL OAUTHBEARER is not configured")
	}
	if err := c.SetOAuthBearerTokenFailure("failure"); err == nil {
		t.Errorf("Expected SetOAuthBearerTokenFailure() to fail when SASL OAUTHBEARER is not configured")
	}
	if err := c.SetSaslCredentials("user", "pass"); err != nil {
		t.Errorf("SetSaslCredentials() failed: %s", err)
	}

	// Nothing to poll without a broker.
	if ev := c.Poll(100); ev != nil {
		t.Errorf("Expected no event, got %T: %v", ev, ev)
	}

	if err := c.Close(); err != nil {
		t.Errorf("Close() failed: %s", err)
	}
	if !c.IsClosed() {
		t.Errorf("Expected the consumer to be closed")
	}
}

// TestDeserializingConsumerPollPassthrough verifies that events which are not
// messages are returned unchanged by Poll().
func TestDeserializingConsumerPollPassthrough(t *testing.T) {
	c, err := NewDeserializingConsumer[string, string](&ConfigMap{
		"group.id":          "gotest",
		"bootstrap.servers": "127.0.0.1:65533",
		"socket.timeout.ms": 10,
	}, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create DeserializingConsumer: %s", err)
	}
	defer c.Close()

	if err := c.Subscribe("gotest", nil); err != nil {
		t.Fatalf("Subscribe() failed: %s", err)
	}

	// The connection to the broker fails, which surfaces as an error event.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		switch ev := c.Poll(100).(type) {
		case nil:
			continue
		case Error:
			return
		default:
			t.Fatalf("Expected an Error event, got %T: %v", ev, ev)
		}
	}
	t.Errorf("Timed out waiting for an error event")
}
