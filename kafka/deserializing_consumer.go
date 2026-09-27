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
	"fmt"
	"math"
	"time"
)

// DeserializingConsumer wraps a [Consumer] and exposes all of its public
// methods. See [Consumer] for detailed documentation of the underlying
// behavior.
type DeserializingConsumer[K, V any] struct {
	consumer          *Consumer
	keyDeserializer   Deserializer
	valueDeserializer Deserializer
}

// Deserializer turns the bytes consumed from Kafka into a typed key or value.
type Deserializer interface {
	DeserializeWithHeaders(topic string, headers []Header, payload []byte) (interface{}, error)

	// SetClusterIDResolver hands the deserializer a resolver for the ID of the
	// Kafka cluster the [DeserializingConsumer] is connected to, for the part
	// of its configuration that depends on it.
	//
	// The resolver is invoked whenever the deserializer actually needs the ID,
	// never while the consumer is being built, so construction never waits on
	// a broker - which it could not reach anyway when, for instance, the
	// OAUTHBEARER token refresh callback is only served from Poll. By the time
	// a message has been fetched the metadata is already cached, so the
	// resolver returns at once. It may block for up to a minute while the
	// consumer reaches a broker, and reports an error if it cannot.
	//
	// The resolver is bound to the consumer that supplied it and fails once
	// that consumer is closed, so a deserializer must not outlive the consumer
	// it was given to, unless the cluster ID it needs was configured
	// explicitly. Implementations that do not use the cluster ID, or that had
	// one configured explicitly, ignore the resolver.
	SetClusterIDResolver(resolve func() (string, error))

	// Close releases the resources the deserializer created itself. Resources
	// the application supplied are left alone.
	Close() error
}

// KeyDeserializationError is the [Event] returned by
// [DeserializingConsumer.Poll] when the key of a message could not be
// deserialized. It carries the partition and offset of that message, so that
// consumption can be resumed past it.
type KeyDeserializationError struct {
	TopicPartition TopicPartition
	err            error
}

// ValueDeserializationError is the [Event] returned by
// [DeserializingConsumer.Poll] when the value of a message could not be
// deserialized. It carries the partition and offset of that message, so that
// consumption can be resumed past it.
type ValueDeserializationError struct {
	TopicPartition TopicPartition
	err            error
}

// deserializationErrorMessage reports which part of the message at
// topicPartition could not be deserialized, and why.
//
// A missing topic is rendered the way [TopicPartition.String] renders it,
// since a message without one is itself a case these errors report.
func deserializationErrorMessage(part string, topicPartition TopicPartition, err error) string {
	topic := "<null>"
	if topicPartition.Topic != nil {
		topic = *topicPartition.Topic
	}
	return fmt.Sprintf("Error deserializing %s for partition %s-%d at offset %d. If needed, please seek past the record to continue consumption: %v",
		part, topic, topicPartition.Partition, topicPartition.Offset, err)
}

// Error implements the error interface, reporting the partition and offset of
// the message whose key could not be deserialized.
func (e KeyDeserializationError) Error() string {
	return deserializationErrorMessage("key", e.TopicPartition, e.err)
}

// Error implements the error interface, reporting the partition and offset of
// the message whose value could not be deserialized.
func (e ValueDeserializationError) Error() string {
	return deserializationErrorMessage("value", e.TopicPartition, e.err)
}

// String returns the same text as Error.
func (e KeyDeserializationError) String() string {
	return e.Error()
}

// String returns the same text as Error.
func (e ValueDeserializationError) String() string {
	return e.Error()
}

// NewKeyDeserializationError creates a [KeyDeserializationError] for the
// message at topicPartition, wrapping the error the key deserializer returned.
func NewKeyDeserializationError(topicPartition TopicPartition, err error) KeyDeserializationError {
	return KeyDeserializationError{
		TopicPartition: topicPartition,
		err:            err,
	}
}

// NewValueDeserializationError creates a [ValueDeserializationError] for the
// message at topicPartition, wrapping the error the value deserializer
// returned.
func NewValueDeserializationError(topicPartition TopicPartition, err error) ValueDeserializationError {
	return ValueDeserializationError{
		TopicPartition: topicPartition,
		err:            err,
	}
}

// DeserializerBuilder creates the [Deserializer] a [DeserializingConsumer] uses
// for its keys or its values.
//
// Build is given the consumer's [ConfigMap] and whether it is building the key
// deserializer, and returns the deserializer together with the ConfigMap to
// carry on with: any property the deserializer consumed itself is filtered out,
// so that what reaches [NewConsumer] holds Kafka properties only.
type DeserializerBuilder interface {
	Build(conf *ConfigMap, isKey bool) (Deserializer, *ConfigMap, error)
}

// NewDeserializingConsumer is the same as [NewConsumer], returning a
// [DeserializingConsumer] wrapping the created [Consumer].
//
// A deserializer a builder created is owned by the returned consumer and
// closed along with it. Should construction fail at any point, whatever was
// built up to then - the deserializers, and the [Consumer] itself - is
// released before the error is returned, since the caller has no handle to
// close.
func NewDeserializingConsumer[K, V any](conf *ConfigMap,
	keyDeserializerBuilder DeserializerBuilder,
	valueDeserializerBuilder DeserializerBuilder) (*DeserializingConsumer[K, V], error) {

	var keyDeserializer, valueDeserializer Deserializer
	var c *Consumer
	var filteredKeyConf *ConfigMap
	var filteredValueConf *ConfigMap
	var filteredConf = conf
	var err error

	succeeded := false
	defer func() {
		if succeeded {
			return
		}
		if keyDeserializer != nil {
			_ = keyDeserializer.Close()
		}
		if valueDeserializer != nil {
			_ = valueDeserializer.Close()
		}
		if c != nil {
			_ = c.Close()
		}
	}()

	// The deserializers are built before the consumer, so that a builder that
	// fails leaves no Kafka client behind.
	if keyDeserializerBuilder != nil {
		keyDeserializer, filteredKeyConf, err = keyDeserializerBuilder.Build(conf, true)
		if err != nil {
			return nil, err
		}
		filteredConf = filteredKeyConf
	}

	if valueDeserializerBuilder != nil {
		valueDeserializer, filteredValueConf, err = valueDeserializerBuilder.Build(conf, false)
		if err != nil {
			return nil, err
		}
		if filteredKeyConf != nil {
			for k := range *filteredValueConf {
				if _, found := (*filteredKeyConf)[k]; !found {
					delete(*filteredValueConf, k)
				}
			}
		}
		filteredConf = filteredValueConf
	}

	c, err = NewConsumer(filteredConf)
	if err != nil {
		return nil, err
	}

	propagateClusterIDResolverToDeserializers(c, keyDeserializer, valueDeserializer)

	dc := &DeserializingConsumer[K, V]{consumer: c, keyDeserializer: keyDeserializer, valueDeserializer: valueDeserializer}
	succeeded = true
	return dc, nil
}

// propagateClusterIDResolverToDeserializers hands each deserializer a resolver
// for the ID of the Kafka cluster the consumer is connected to.
//
// The ID is resolved lazily, when a deserializer needs it, rather than here:
// by then a message has been fetched, so the metadata is already cached and
// the resolver returns at once, whereas resolving during construction would
// wait on a broker that an OAUTHBEARER consumer, whose token refresh callback
// is only served from Poll, cannot yet reach.
//
// Concurrent resolutions, from either deserializer and any number of
// goroutines, share a single lookup, as [handle.resolveClusterID] describes.
func propagateClusterIDResolverToDeserializers(c *Consumer, deserializers ...Deserializer) {
	resolve := func() (string, error) {
		if err := c.verifyClient(); err != nil {
			return "", err
		}
		return c.handle.resolveClusterID(clusterIDTimeoutMs)
	}

	for _, deserializer := range deserializers {
		if deserializer != nil {
			deserializer.SetClusterIDResolver(resolve)
		}
	}
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
	return dc.consumer.CommitMessage(&Message{TopicPartition: m.TopicPartition})
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
	return dc.consumer.StoreMessage(&Message{TopicPartition: m.TopicPartition})
}

// SeekPartitions is the same as [Consumer.SeekPartitions].
func (dc *DeserializingConsumer[K, V]) SeekPartitions(partitions []TopicPartition) ([]TopicPartition, error) {
	return dc.consumer.SeekPartitions(partitions)
}

// Poll is the same as [Consumer.Poll].
//
// A [*Message] is returned as a [*DeserializedMessage] with its key and value
// deserialized. If deserialization fails, a [KeyDeserializationError] or a
// [ValueDeserializationError] is returned instead. Any other event is returned
// as-is.
func (dc *DeserializingConsumer[K, V]) Poll(timeoutMs int) (event Event) {
	ev := dc.consumer.Poll(timeoutMs)
	if ev == nil {
		return nil
	}

	switch e := ev.(type) {
	case *Message:
		return dc.deserializeMessage(e)
	default:
		return e
	}
}

// deserializeMessage deserializes the key and the value of msg and returns the
// resulting [*DeserializedMessage], or a deserialization error event.
func (dc *DeserializingConsumer[K, V]) deserializeMessage(msg *Message) Event {
	var deserializedKey K
	var deserializedValue V
	var deserializedKeyInterface interface{}
	var deserializedValueInterface interface{}
	var ok bool
	var err error
	emptyTopic := msg.TopicPartition.Topic == nil || len(*msg.TopicPartition.Topic) == 0

	if msg.Key != nil && dc.keyDeserializer != nil {
		if emptyTopic {
			return NewKeyDeserializationError(msg.TopicPartition, fmt.Errorf("Key deserialization needs a non-empty topic name"))
		}
		deserializedKeyInterface, err = dc.keyDeserializer.DeserializeWithHeaders(*msg.TopicPartition.Topic, msg.Headers, msg.Key)
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
		if emptyTopic {
			return NewValueDeserializationError(msg.TopicPartition, fmt.Errorf("Value deserialization needs a non-empty topic name"))
		}
		deserializedValueInterface, err = dc.valueDeserializer.DeserializeWithHeaders(*msg.TopicPartition.Topic, msg.Headers, msg.Value)
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
}

// Logs is the same as [Consumer.Logs].
func (dc *DeserializingConsumer[K, V]) Logs() chan LogEvent {
	return dc.consumer.Logs()
}

// ReadMessage is the same as [Consumer.ReadMessage], returning the message as
// a [*DeserializedMessage] with its key and value deserialized.
//
// This is a convenience API that wraps [DeserializingConsumer.Poll] and only
// returns messages or errors. All other event types are discarded.
//
// A [KeyDeserializationError] or a [ValueDeserializationError] is returned as
// (nil, err), so that a message that cannot be deserialized is reported rather
// than skipped; both carry the partition and offset to seek past to continue.
func (dc *DeserializingConsumer[K, V]) ReadMessage(timeout time.Duration) (*DeserializedMessage[K, V], error) {
	err := dc.consumer.verifyClient()
	if err != nil {
		return nil, err
	}

	var absTimeout time.Time
	var timeoutMs int

	if timeout > 0 {
		absTimeout = time.Now().Add(timeout)
		timeoutMs = (int)(timeout.Seconds() * 1000.0)
	} else {
		timeoutMs = (int)(timeout)
	}

	for {
		ev := dc.Poll(timeoutMs)

		switch e := ev.(type) {
		case *DeserializedMessage[K, V]:
			if e.TopicPartition.Error != nil {
				return e, e.TopicPartition.Error
			}
			return e, nil
		case KeyDeserializationError:
			return nil, e
		case ValueDeserializationError:
			return nil, e
		case Error:
			return nil, e
		default:
			// Ignore other event types
		}

		if timeout > 0 {
			// Calculate remaining time
			timeoutMs = int(math.Max(0.0, absTimeout.Sub(time.Now()).Seconds()*1000.0))
		}

		if timeoutMs == 0 && ev == nil {
			return nil, newErrorFromString(ErrTimedOut, "")
		}
	}
}

// Close is the same as [Consumer.Close], and also closes the key and the value
// deserializers. The errors of all three are joined.
func (dc *DeserializingConsumer[K, V]) Close() (err error) {
	err = dc.consumer.Close()
	if dc.keyDeserializer != nil {
		err = errors.Join(err, dc.keyDeserializer.Close())
	}
	if dc.valueDeserializer != nil {
		err = errors.Join(err, dc.valueDeserializer.Close())
	}
	return err
}

// GetMetadata is the same as [Consumer.GetMetadata].
func (dc *DeserializingConsumer[K, V]) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*Metadata, error) {
	return dc.consumer.GetMetadata(topic, allTopics, timeoutMs)
}

// GetClusterID is the same as [Consumer.GetClusterID].
func (dc *DeserializingConsumer[K, V]) GetClusterID(timeoutMs int) (string, error) {
	return dc.consumer.GetClusterID(timeoutMs)
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
