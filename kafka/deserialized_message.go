package kafka

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
	"fmt"
	"time"
)

// DeserializedMessage represents a Kafka message with deserialized key and value types. It is used with the DeserializingConsumer to consume messages with specific key and value types.
type DeserializedMessage[K any, V any] struct {
	TopicPartition TopicPartition
	Key            K
	Value          V
	keyBytes       []byte
	valueBytes     []byte
	Timestamp      time.Time
	TimestampType  TimestampType
	Opaque         interface{}
	Headers        []Header
	message		   *Message
}

// String returns a human readable representation of a DeserializedMessage.
// Key and Value are not represented.
func (m *DeserializedMessage[K, V]) String() string {
	var topic string
	if m.TopicPartition.Topic != nil {
		topic = *m.TopicPartition.Topic
	} else {
		topic = ""
	}
	return fmt.Sprintf("%s[%d]@%s", topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
}

func (m *DeserializedMessage[K, V]) SerializedKeySize() int {
	if m.keyBytes == nil {
		return -1
	}
	return len(m.keyBytes)
}

func (m *DeserializedMessage[K, V]) SerializedValueSize() int {
	if m.valueBytes == nil {
		return -1
	}
	return len(m.valueBytes)
}

func newDeserializedMessage[K, V any](msg *Message, key K, value V) *DeserializedMessage[K, V] {
	return &DeserializedMessage[K, V]{
		TopicPartition: msg.TopicPartition,
		Key:            key,
		Value:          value,
		keyBytes:       msg.Key,
		valueBytes:		msg.Value,
		Timestamp:      msg.Timestamp,
		TimestampType:  msg.TimestampType,
		Opaque:         msg.Opaque,
		Headers:        msg.Headers,
		message:		msg,
	}
}
