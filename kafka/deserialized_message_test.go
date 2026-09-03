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
	"reflect"
	"testing"
	"time"
)

func TestDeserializedMessageString(t *testing.T) {
	topic := "mytopic"
	msg := &DeserializedMessage[string, string]{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 3, Offset: 42},
	}
	if msg.String() != "mytopic[3]@42" {
		t.Errorf("Expected mytopic[3]@42, got %s", msg.String())
	}

	// A message without a topic is represented with an empty topic name.
	msg = &DeserializedMessage[string, string]{
		TopicPartition: TopicPartition{Partition: 1, Offset: OffsetBeginning},
	}
	if msg.String() != "[1]@beginning" {
		t.Errorf("Expected [1]@beginning, got %s", msg.String())
	}
}

func TestDeserializedMessageSerializedSizes(t *testing.T) {
	msg := &DeserializedMessage[string, string]{}
	if msg.SerializedKeySize() != -1 {
		t.Errorf("Expected -1 for a message without a key, got %d", msg.SerializedKeySize())
	}
	if msg.SerializedValueSize() != -1 {
		t.Errorf("Expected -1 for a message without a value, got %d", msg.SerializedValueSize())
	}

	msg.keyBytes = []byte("key")
	msg.valueBytes = []byte("value!")
	if msg.SerializedKeySize() != 3 {
		t.Errorf("Expected 3, got %d", msg.SerializedKeySize())
	}
	if msg.SerializedValueSize() != 6 {
		t.Errorf("Expected 6, got %d", msg.SerializedValueSize())
	}

	// An empty, but present, key or value is distinct from a missing one.
	msg.keyBytes = []byte{}
	msg.valueBytes = []byte{}
	if msg.SerializedKeySize() != 0 {
		t.Errorf("Expected 0, got %d", msg.SerializedKeySize())
	}
	if msg.SerializedValueSize() != 0 {
		t.Errorf("Expected 0, got %d", msg.SerializedValueSize())
	}
}

func TestNewDeserializedMessage(t *testing.T) {
	topic := "mytopic"
	timestamp := time.Now().Truncate(time.Millisecond)
	headers := []Header{{Key: "hkey", Value: []byte("hvalue")}}
	msg := &Message{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 2, Offset: 7},
		Key:            []byte("serialized-key"),
		Value:          []byte("serialized-value"),
		Timestamp:      timestamp,
		TimestampType:  TimestampLogAppendTime,
		Opaque:         "opaque",
		Headers:        headers,
	}

	deserializedMessage := newDeserializedMessage(msg, "key", 17)

	if !reflect.DeepEqual(deserializedMessage.TopicPartition, msg.TopicPartition) {
		t.Errorf("Expected %v, got %v", msg.TopicPartition, deserializedMessage.TopicPartition)
	}
	if deserializedMessage.Key != "key" {
		t.Errorf("Expected key, got %v", deserializedMessage.Key)
	}
	if deserializedMessage.Value != 17 {
		t.Errorf("Expected 17, got %v", deserializedMessage.Value)
	}
	if deserializedMessage.SerializedKeySize() != len(msg.Key) {
		t.Errorf("Expected %d, got %d", len(msg.Key), deserializedMessage.SerializedKeySize())
	}
	if deserializedMessage.SerializedValueSize() != len(msg.Value) {
		t.Errorf("Expected %d, got %d", len(msg.Value), deserializedMessage.SerializedValueSize())
	}
	if !deserializedMessage.Timestamp.Equal(timestamp) {
		t.Errorf("Expected %v, got %v", timestamp, deserializedMessage.Timestamp)
	}
	if deserializedMessage.TimestampType != TimestampLogAppendTime {
		t.Errorf("Expected %v, got %v", TimestampLogAppendTime, deserializedMessage.TimestampType)
	}
	if deserializedMessage.Opaque != "opaque" {
		t.Errorf("Expected opaque, got %v", deserializedMessage.Opaque)
	}
	if !reflect.DeepEqual(deserializedMessage.Headers, headers) {
		t.Errorf("Expected %v, got %v", headers, deserializedMessage.Headers)
	}
	// The original message is kept so that it can be committed or stored.
	if deserializedMessage.message != msg {
		t.Errorf("Expected the underlying message to be kept")
	}
}
