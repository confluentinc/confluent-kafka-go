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
)

// SerializingProducer wraps a [Producer] and exposes all of its public
// methods. See [Producer] for detailed documentation of the underlying
// behavior.
type SerializingProducer[K, V any] struct {
	producer        *Producer
	keySerializer   Serializer
	valueSerializer Serializer
}

type Serializer interface {
	Serialize(topic string, msg interface{}) ([]byte, error)
	SerializeWithHeaders(topic string, msg interface{}) ([]Header, []byte, error)
	SetClusterID(clusterID string)
	Close() error
}

type SerializerBuilder interface {
	Build(conf *ConfigMap, isKey bool) (Serializer, *ConfigMap, error)
}

// NewSerializingProducer is the same as [NewProducer], returning a
// [SerializingProducer] wrapping the created [Producer].
func NewSerializingProducer[K, V any](conf *ConfigMap,
	keySerializerBuilder SerializerBuilder,
	valueSerializerBuilder SerializerBuilder) (*SerializingProducer[K, V], error) {

	var keySerializer, valueSerializer Serializer
	var filteredConf *ConfigMap = conf
	var err error
	if keySerializerBuilder != nil {
		keySerializer, filteredConf, err = keySerializerBuilder.Build(conf, true)
		if err != nil {
			return nil, err
		}
	}

	if valueSerializerBuilder != nil {
		valueSerializer, filteredConf, err = valueSerializerBuilder.Build(conf, false)
		if err != nil {
			return nil, err
		}
	}

	p, err := NewProducer(filteredConf)
	if err != nil {
		return nil, err
	}

	clusterID, err := p.getClusterID(5000)
	if err != nil {
		return nil, err
	}
	if keySerializer != nil {
		keySerializer.SetClusterID(clusterID)
	}
	if valueSerializer != nil {
		valueSerializer.SetClusterID(clusterID)
	}
	sp := &SerializingProducer[K, V]{producer: p, keySerializer: keySerializer, valueSerializer: valueSerializer}
	p.setSendMessageToChannelFunction(sp.sendToChannel)
	return sp, nil
}

func (sp *SerializingProducer[K, V]) sendToChannel(msg *Message, deliveryChan *chan Event, termChan chan bool) bool {
	serializableMessage, ok := msg.Opaque.(*SerializableMessage[K, V])
	if !ok {
		// If the message is not a SerializableMessage, ignore it and return false to indicate that processing should continue.
		return false
	}

	serializableMessage.Timestamp = msg.Timestamp
	serializableMessage.TimestampType = msg.TimestampType
	serializableMessage.TopicPartition = msg.TopicPartition
	return sp.producer.handle.sendToChannel(serializableMessage, deliveryChan, termChan)
}

// IsClosed is the same as [Producer.IsClosed].
func (sp *SerializingProducer[K, V]) IsClosed() bool {
	return sp.producer.IsClosed()
}

// String is the same as [Producer.String].
func (sp *SerializingProducer[K, V]) String() string {
	return sp.producer.String()
}

// Produce is the same as [Producer.Produce].
func (sp *SerializingProducer[K, V]) Produce(serializableMessage *SerializableMessage[K, V], deliveryChan chan Event) error {
	var err error
	if sp.keySerializer != nil {
		serializableMessage.keyBytes, err = sp.keySerializer.Serialize(*serializableMessage.TopicPartition.Topic, serializableMessage.Key)
		if err != nil {
			return err
		}
	} else {
		switch any(serializableMessage.Key).(type) {
		case nil:
		case string:
			if any(serializableMessage.Key).(string) == "" {
				break
			}
			return NewError(ErrInvalidArg, "Key serializer is not set, but Key is not empty", false)
		default:
			return NewError(ErrInvalidArg, "Key serializer is not set, but Key is not empty", false)
		}
	}

	if sp.valueSerializer != nil {
		serializableMessage.valueBytes, err = sp.valueSerializer.Serialize(*serializableMessage.TopicPartition.Topic, serializableMessage.Value)
		if err != nil {
			return err
		}
	} else {
		switch any(serializableMessage.Value).(type) {
		case nil:
		case string:
			if any(serializableMessage.Value).(string) == "" {
				break
			}
			return NewError(ErrInvalidArg, "Value serializer is not set, but Value is not empty", false)
		default:
			return NewError(ErrInvalidArg, "Value serializer is not set, but Value is not empty", false)
		}
	}

	msg := serializableMessage.toMessage()
	return sp.producer.Produce(msg, deliveryChan)
}

// Events is the same as [Producer.Events].
func (sp *SerializingProducer[K, V]) Events() chan Event {
	return sp.producer.Events()
}

// Logs is the same as [Producer.Logs].
func (sp *SerializingProducer[K, V]) Logs() chan LogEvent {
	return sp.producer.Logs()
}

// Len is the same as [Producer.Len].
func (sp *SerializingProducer[K, V]) Len() int {
	return sp.producer.Len()
}

// Flush is the same as [Producer.Flush].
func (sp *SerializingProducer[K, V]) Flush(timeoutMs int) int {
	return sp.producer.Flush(timeoutMs)
}

// Close is the same as [Producer.Close].
func (sp *SerializingProducer[K, V]) Close() {
	sp.producer.Close()
}

// Purge is the same as [Producer.Purge].
func (sp *SerializingProducer[K, V]) Purge(flags int) error {
	return sp.producer.Purge(flags)
}

// GetMetadata is the same as [Producer.GetMetadata].
func (sp *SerializingProducer[K, V]) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*Metadata, error) {
	return sp.producer.GetMetadata(topic, allTopics, timeoutMs)
}

// QueryWatermarkOffsets is the same as [Producer.QueryWatermarkOffsets].
func (sp *SerializingProducer[K, V]) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	return sp.producer.QueryWatermarkOffsets(topic, partition, timeoutMs)
}

// OffsetsForTimes is the same as [Producer.OffsetsForTimes].
func (sp *SerializingProducer[K, V]) OffsetsForTimes(times []TopicPartition, timeoutMs int) (offsets []TopicPartition, err error) {
	return sp.producer.OffsetsForTimes(times, timeoutMs)
}

// GetFatalError is the same as [Producer.GetFatalError].
func (sp *SerializingProducer[K, V]) GetFatalError() error {
	return sp.producer.GetFatalError()
}

// TestFatalError is the same as [Producer.TestFatalError].
func (sp *SerializingProducer[K, V]) TestFatalError(code ErrorCode, str string) ErrorCode {
	return sp.producer.TestFatalError(code, str)
}

// SetOAuthBearerToken is the same as [Producer.SetOAuthBearerToken].
func (sp *SerializingProducer[K, V]) SetOAuthBearerToken(oauthBearerToken OAuthBearerToken) error {
	return sp.producer.SetOAuthBearerToken(oauthBearerToken)
}

// SetOAuthBearerTokenFailure is the same as [Producer.SetOAuthBearerTokenFailure].
func (sp *SerializingProducer[K, V]) SetOAuthBearerTokenFailure(errstr string) error {
	return sp.producer.SetOAuthBearerTokenFailure(errstr)
}

// InitTransactions is the same as [Producer.InitTransactions].
func (sp *SerializingProducer[K, V]) InitTransactions(ctx context.Context) error {
	return sp.producer.InitTransactions(ctx)
}

// BeginTransaction is the same as [Producer.BeginTransaction].
func (sp *SerializingProducer[K, V]) BeginTransaction() error {
	return sp.producer.BeginTransaction()
}

// SendOffsetsToTransaction is the same as [Producer.SendOffsetsToTransaction].
func (sp *SerializingProducer[K, V]) SendOffsetsToTransaction(ctx context.Context, offsets []TopicPartition, consumerMetadata *ConsumerGroupMetadata) error {
	return sp.producer.SendOffsetsToTransaction(ctx, offsets, consumerMetadata)
}

// CommitTransaction is the same as [Producer.CommitTransaction].
func (sp *SerializingProducer[K, V]) CommitTransaction(ctx context.Context) error {
	return sp.producer.CommitTransaction(ctx)
}

// AbortTransaction is the same as [Producer.AbortTransaction].
func (sp *SerializingProducer[K, V]) AbortTransaction(ctx context.Context) error {
	return sp.producer.AbortTransaction(ctx)
}

// SetSaslCredentials is the same as [Producer.SetSaslCredentials].
func (sp *SerializingProducer[K, V]) SetSaslCredentials(username, password string) error {
	return sp.producer.SetSaslCredentials(username, password)
}
