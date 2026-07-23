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
	"fmt"
)

type DeserializingConsumer[K, V any] struct {
	consumer          *Consumer
	keyDeserializer   Deserializer
	valueDeserializer Deserializer
}

type Deserializer interface {
	DeserializeWithHeaders(topic string, headers []Header, payload []byte) (interface{}, error)
	Close() error
}

type KeyDeserializationError struct {
	TopicPartition TopicPartition
	err            error
}

type ValueDeserializationError struct {
	TopicPartition TopicPartition
	err            error
}

func (e *KeyDeserializationError) Error() string {
	return fmt.Sprintf("Error deserializing key for partition %s-%s at offset %d. If needed, please seek past the record to continue consumption: %v",
		e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset, e.err)
}

func (e *ValueDeserializationError) Error() string {
	return fmt.Sprintf("Error deserializing value for partition %s-%d at offset %d. If needed, please seek past the record to continue consumption: %v",
		*e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset, e.err)
}

func (e *KeyDeserializationError) String() string {
	return e.Error()
}

func (e *ValueDeserializationError) String() string {
	return e.Error()
}

func NewKeyDeserializationError(topicPartition TopicPartition, err error) *KeyDeserializationError {
	return &KeyDeserializationError{
		TopicPartition: topicPartition,
		err:            err,
	}
}

func NewValueDeserializationError(topicPartition TopicPartition, err error) *ValueDeserializationError {
	return &ValueDeserializationError{
		TopicPartition: topicPartition,
		err:            err,
	}
}

type DeserializerBuilder func(*ConfigMap, bool) (Deserializer, *ConfigMap, error)

func NewDeserializingConsumer[K, V any](conf *ConfigMap,
	keyDeserializerBuilder DeserializerBuilder,
	valueDeserializerBuilder DeserializerBuilder) (*DeserializingConsumer[K, V], error) {

	var keyDeserializer, valueDeserializer Deserializer
	var filteredConf *ConfigMap = conf
	var err error
	if keyDeserializerBuilder != nil {
		keyDeserializer, filteredConf, err = keyDeserializerBuilder(conf, true)
		if err != nil {
			return nil, err
		}
	}

	if valueDeserializerBuilder != nil {
		valueDeserializer, filteredConf, err = valueDeserializerBuilder(conf, false)
		if err != nil {
			return nil, err
		}
	}

	c, err := NewConsumer(filteredConf)
	if err != nil {
		return nil, err
	}
	dc := &DeserializingConsumer[K, V]{consumer: c, keyDeserializer: keyDeserializer, valueDeserializer: valueDeserializer}
	return dc, nil
}

// IsClosed is the same as [Consumer.IsClosed].
func (dc *DeserializingConsumer[K, V]) IsClosed() bool {
	return dc.consumer.IsClosed()
}

// String is the same as [Consumer.String].
func (dc *DeserializingConsumer[K, V]) String() string {
	return dc.consumer.String()
}

// Subscribe is the same as [Consumer.Subscribe].
func (dc *DeserializingConsumer[K, V]) Subscribe(topic string, rebalanceCb RebalanceCb) error {
	return dc.consumer.Subscribe(topic, rebalanceCb)
}

// SubscribeTopics is the same as [Consumer.SubscribeTopics].
func (dc *DeserializingConsumer[K, V]) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) (err error) {
	return dc.consumer.SubscribeTopics(topics, rebalanceCb)
}

// Unsubscribe is the same as [Consumer.Unsubscribe].
func (dc *DeserializingConsumer[K, V]) Unsubscribe() (err error) {
	return dc.consumer.Unsubscribe()
}

// Assign is the same as [Consumer.Assign].
func (dc *DeserializingConsumer[K, V]) Assign(partitions []TopicPartition) (err error) {
	return dc.consumer.Assign(partitions)
}

// Unassign is the same as [Consumer.Unassign].
func (dc *DeserializingConsumer[K, V]) Unassign() (err error) {
	return dc.consumer.Unassign()
}

// IncrementalAssign is the same as [Consumer.IncrementalAssign].
func (dc *DeserializingConsumer[K, V]) IncrementalAssign(partitions []TopicPartition) (err error) {
	return dc.consumer.IncrementalAssign(partitions)
}

// IncrementalUnassign is the same as [Consumer.IncrementalUnassign].
func (dc *DeserializingConsumer[K, V]) IncrementalUnassign(partitions []TopicPartition) (err error) {
	return dc.consumer.IncrementalUnassign(partitions)
}

// GetRebalanceProtocol is the same as [Consumer.GetRebalanceProtocol].
func (dc *DeserializingConsumer[K, V]) GetRebalanceProtocol() string {
	return dc.consumer.GetRebalanceProtocol()
}

// AssignmentLost is the same as [Consumer.AssignmentLost].
func (dc *DeserializingConsumer[K, V]) AssignmentLost() bool {
	return dc.consumer.AssignmentLost()
}

// Commit is the same as [Consumer.Commit].
func (dc *DeserializingConsumer[K, V]) Commit() ([]TopicPartition, error) {
	return dc.consumer.Commit()
}

// CommitMessage is the same as [Consumer.CommitMessage].
func (dc *DeserializingConsumer[K, V]) CommitMessage(m *DeserializedMessage[K, V]) ([]TopicPartition, error) {
	return dc.consumer.CommitMessage(m.message)
}

// CommitOffsets is the same as [Consumer.CommitOffsets].
func (dc *DeserializingConsumer[K, V]) CommitOffsets(offsets []TopicPartition) ([]TopicPartition, error) {
	return dc.consumer.CommitOffsets(offsets)
}

// StoreOffsets is the same as [Consumer.StoreOffsets].
func (dc *DeserializingConsumer[K, V]) StoreOffsets(offsets []TopicPartition) (storedOffsets []TopicPartition, err error) {
	return dc.consumer.StoreOffsets(offsets)
}

// StoreMessage is the same as [Consumer.StoreMessage].
func (dc *DeserializingConsumer[K, V]) StoreMessage(m *DeserializedMessage[K, V]) (storedOffsets []TopicPartition, err error) {
	return dc.consumer.StoreMessage(m.message)
}

// SeekPartitions is the same as [Consumer.SeekPartitions].
func (dc *DeserializingConsumer[K, V]) SeekPartitions(partitions []TopicPartition) ([]TopicPartition, error) {
	return dc.consumer.SeekPartitions(partitions)
}

// Poll is the same as [Consumer.Poll].
func (dc *DeserializingConsumer[K, V]) Poll(timeoutMs int) (event Event) {
	ev := dc.consumer.Poll(timeoutMs)
	if ev == nil {
		return nil
	}

	switch e := ev.(type) {
	case *Message:
		var deserializedKey K
		var deserializedValue V
		var ok bool

		msg := e
		if msg.TopicPartition.Topic == nil {
			return msg
		}

		if msg.Key != nil && dc.keyDeserializer != nil {
			deserializedKeyInterface, err := dc.keyDeserializer.DeserializeWithHeaders(*msg.TopicPartition.Topic, msg.Headers, msg.Key)
			if err != nil {
				return NewKeyDeserializationError(msg.TopicPartition, err)
			}

			deserializedKey, ok = deserializedKeyInterface.(K)
			if !ok {
				return NewKeyDeserializationError(msg.TopicPartition,
					fmt.Errorf("Wrong deserialized key type: %T", deserializedKeyInterface))
			}
		}
		if msg.Value != nil && dc.valueDeserializer != nil {
			deserializedValueInterface, err := dc.valueDeserializer.DeserializeWithHeaders(*msg.TopicPartition.Topic, msg.Headers, msg.Value)
			if err != nil {
				return NewValueDeserializationError(msg.TopicPartition, err)
			}

			deserializedValue, ok = deserializedValueInterface.(V)
			if !ok {
				return NewValueDeserializationError(msg.TopicPartition,
					fmt.Errorf("Wrong deserialized value type: %T", deserializedValueInterface))
			}
		}

		return newDeserializedMessage(msg, deserializedKey, deserializedValue)
	default:
		return e
	}
}

// Logs is the same as [Consumer.Logs].
func (dc *DeserializingConsumer[K, V]) Logs() chan LogEvent {
	return dc.consumer.Logs()
}

// Close is the same as [Consumer.Close].
func (dc *DeserializingConsumer[K, V]) Close() (err error) {
	return dc.consumer.Close()
}

// GetMetadata is the same as [Consumer.GetMetadata].
func (dc *DeserializingConsumer[K, V]) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*Metadata, error) {
	return dc.consumer.GetMetadata(topic, allTopics, timeoutMs)
}

// QueryWatermarkOffsets is the same as [Consumer.QueryWatermarkOffsets].
func (dc *DeserializingConsumer[K, V]) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	return dc.consumer.QueryWatermarkOffsets(topic, partition, timeoutMs)
}

// GetWatermarkOffsets is the same as [Consumer.GetWatermarkOffsets].
func (dc *DeserializingConsumer[K, V]) GetWatermarkOffsets(topic string, partition int32) (low, high int64, err error) {
	return dc.consumer.GetWatermarkOffsets(topic, partition)
}

// OffsetsForTimes is the same as [Consumer.OffsetsForTimes].
func (dc *DeserializingConsumer[K, V]) OffsetsForTimes(times []TopicPartition, timeoutMs int) (offsets []TopicPartition, err error) {
	return dc.consumer.OffsetsForTimes(times, timeoutMs)
}

// Subscription is the same as [Consumer.Subscription].
func (dc *DeserializingConsumer[K, V]) Subscription() (topics []string, err error) {
	return dc.consumer.Subscription()
}

// Assignment is the same as [Consumer.Assignment].
func (dc *DeserializingConsumer[K, V]) Assignment() (partitions []TopicPartition, err error) {
	return dc.consumer.Assignment()
}

// Committed is the same as [Consumer.Committed].
func (dc *DeserializingConsumer[K, V]) Committed(partitions []TopicPartition, timeoutMs int) (offsets []TopicPartition, err error) {
	return dc.consumer.Committed(partitions, timeoutMs)
}

// Position is the same as [Consumer.Position].
func (dc *DeserializingConsumer[K, V]) Position(partitions []TopicPartition) (offsets []TopicPartition, err error) {
	return dc.consumer.Position(partitions)
}

// Pause is the same as [Consumer.Pause].
func (dc *DeserializingConsumer[K, V]) Pause(partitions []TopicPartition) (err error) {
	return dc.consumer.Pause(partitions)
}

// Resume is the same as [Consumer.Resume].
func (dc *DeserializingConsumer[K, V]) Resume(partitions []TopicPartition) (err error) {
	return dc.consumer.Resume(partitions)
}

// SetOAuthBearerToken is the same as [Consumer.SetOAuthBearerToken].
func (dc *DeserializingConsumer[K, V]) SetOAuthBearerToken(oauthBearerToken OAuthBearerToken) error {
	return dc.consumer.SetOAuthBearerToken(oauthBearerToken)
}

// SetOAuthBearerTokenFailure is the same as [Consumer.SetOAuthBearerTokenFailure].
func (dc *DeserializingConsumer[K, V]) SetOAuthBearerTokenFailure(errstr string) error {
	return dc.consumer.SetOAuthBearerTokenFailure(errstr)
}

// GetConsumerGroupMetadata is the same as [Consumer.GetConsumerGroupMetadata].
func (dc *DeserializingConsumer[K, V]) GetConsumerGroupMetadata() (*ConsumerGroupMetadata, error) {
	return dc.consumer.GetConsumerGroupMetadata()
}

// SetSaslCredentials is the same as [Consumer.SetSaslCredentials].
func (dc *DeserializingConsumer[K, V]) SetSaslCredentials(username, password string) error {
	return dc.consumer.SetSaslCredentials(username, password)
}
