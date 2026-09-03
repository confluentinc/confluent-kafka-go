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

func TestSerializableMessageString(t *testing.T) {
	topic := "mytopic"
	msg := &SerializableMessage[string, string]{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 3, Offset: 42},
	}
	if msg.String() != "mytopic[3]@42" {
		t.Errorf("Expected mytopic[3]@42, got %s", msg.String())
	}

	// A message without a topic is represented with an empty topic name.
	msg = &SerializableMessage[string, string]{
		TopicPartition: TopicPartition{Partition: 1, Offset: OffsetEnd},
	}
	if msg.String() != "[1]@end" {
		t.Errorf("Expected [1]@end, got %s", msg.String())
	}
}

func TestSerializableMessageSerializedSizes(t *testing.T) {
	msg := &SerializableMessage[string, string]{}
	if msg.SerializedKeySize() != -1 {
		t.Errorf("Expected -1 for an unserialized key, got %d", msg.SerializedKeySize())
	}
	if msg.SerializedValueSize() != -1 {
		t.Errorf("Expected -1 for an unserialized value, got %d", msg.SerializedValueSize())
	}

	msg.keyBytes = []byte("key")
	msg.valueBytes = []byte("value!")
	if msg.SerializedKeySize() != 3 {
		t.Errorf("Expected 3, got %d", msg.SerializedKeySize())
	}
	if msg.SerializedValueSize() != 6 {
		t.Errorf("Expected 6, got %d", msg.SerializedValueSize())
	}

	// An empty, but serialized, key or value is distinct from an unserialized one.
	msg.keyBytes = []byte{}
	msg.valueBytes = []byte{}
	if msg.SerializedKeySize() != 0 {
		t.Errorf("Expected 0, got %d", msg.SerializedKeySize())
	}
	if msg.SerializedValueSize() != 0 {
		t.Errorf("Expected 0, got %d", msg.SerializedValueSize())
	}
}

func TestSerializableMessageToMessage(t *testing.T) {
	topic := "mytopic"
	timestamp := time.Now().Truncate(time.Millisecond)
	headers := []Header{{Key: "hkey", Value: []byte("hvalue")}}
	serializableMessage := &SerializableMessage[string, int]{
		TopicPartition: TopicPartition{Topic: &topic, Partition: 2, Offset: 7},
		Key:            "key",
		Value:          17,
		keyBytes:       []byte("serialized-key"),
		valueBytes:     []byte("serialized-value"),
		Timestamp:      timestamp,
		TimestampType:  TimestampCreateTime,
		Opaque:         "ignored",
		Headers:        headers,
	}

	msg := serializableMessage.toMessage()

	if !reflect.DeepEqual(msg.TopicPartition, serializableMessage.TopicPartition) {
		t.Errorf("Expected %v, got %v", serializableMessage.TopicPartition, msg.TopicPartition)
	}
	if string(msg.Key) != "serialized-key" {
		t.Errorf("Expected the serialized key, got %s", string(msg.Key))
	}
	if string(msg.Value) != "serialized-value" {
		t.Errorf("Expected the serialized value, got %s", string(msg.Value))
	}
	if !msg.Timestamp.Equal(timestamp) {
		t.Errorf("Expected %v, got %v", timestamp, msg.Timestamp)
	}
	if msg.TimestampType != TimestampCreateTime {
		t.Errorf("Expected %v, got %v", TimestampCreateTime, msg.TimestampType)
	}
	if !reflect.DeepEqual(msg.Headers, headers) {
		t.Errorf("Expected %v, got %v", headers, msg.Headers)
	}
	// The opaque of the produced message carries the SerializableMessage itself,
	// so that the delivery report can be mapped back to it.
	if msg.Opaque != any(serializableMessage) {
		t.Errorf("Expected the opaque to be the SerializableMessage, got %v", msg.Opaque)
	}
}
