package kafka

/**
 * Copyright 2016-2020 Confluent Inc.
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
	"math"
	"time"
	"unsafe"
)

/*
#include <stdlib.h>
#include "select_rdkafka.h"


static rd_kafka_topic_partition_t *_c_rdkafka_topic_partition_list_entry(rd_kafka_topic_partition_list_t *rktparlist, int idx) {
   return idx < rktparlist->cnt ? &rktparlist->elems[idx] : NULL;
}
*/
import "C"

// RebalanceCb provides a per-Subscribe*() rebalance event callback.
// The passed Event will be either AssignedPartitions or RevokedPartitions
type RebalanceCb func(*Consumer, Event) error

// Consumer implements a High-level Apache Kafka Consumer instance
type Consumer struct {
	events             chan Event
	handle             handle
	eventsChanEnable   bool
	readerTermChan     chan bool
	rebalanceCb        RebalanceCb
	appReassigned      bool
	appRebalanceEnable bool // Config setting
}

// Strings returns a human readable name for a Consumer instance
func (c *Consumer) String() string {
	return c.handle.String()
}

// getHandle implements the Handle interface
func (c *Consumer) gethandle() *handle {
	return &c.handle
}

// Subscribe to a single topic
// This replaces the current subscription
func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error {
	return c.SubscribeTopics([]string{topic}, rebalanceCb)
}

// SubscribeTopics subscribes to the provided list of topics.
// This replaces the current subscription.
func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) (err error) {
	ctopics := C.rd_kafka_topic_partition_list_new(C.int(len(topics)))
	defer C.rd_kafka_topic_partition_list_destroy(ctopics)

	for _, topic := range topics {
		ctopic := C.CString(topic)
		defer C.free(unsafe.Pointer(ctopic))
		C.rd_kafka_topic_partition_list_add(ctopics, ctopic, C.RD_KAFKA_PARTITION_UA)
	}

	e := C.rd_kafka_subscribe(c.handle.rk, ctopics)
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(e)
	}

	c.rebalanceCb = rebalanceCb

	return nil
}

// Unsubscribe from the current subscription, if any.
func (c *Consumer) Unsubscribe() (err error) {
	C.rd_kafka_unsubscribe(c.handle.rk)
	return nil
}

// Assign an atomic set of partitions to consume.
//
// The .Offset field of each TopicPartition must either be set to an absolute
// starting offset (>= 0), or one of the logical offsets (`kafka.OffsetEnd` etc),
// but should typically be set to `kafka.OffsetStored` to have the consumer
// use the committed offset as a start position, with a fallback to
// `auto.offset.reset` if there is no committed offset.
//
// This replaces the current assignment.
func (c *Consumer) Assign(partitions []TopicPartition) (err error) {
	c.appReassigned = true

	cparts := newCPartsFromTopicPartitions(partitions)
	defer C.rd_kafka_topic_partition_list_destroy(cparts)

	e := C.rd_kafka_assign(c.handle.rk, cparts)
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(e)
	}

	return nil
}

// Unassign the current set of partitions to consume.
func (c *Consumer) Unassign() (err error) {
	c.appReassigned = true

	e := C.rd_kafka_assign(c.handle.rk, nil)
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(e)
	}

	return nil
}

// IncrementalAssign adds the specified partitions to the current set of
// partitions to consume.
//
// The .Offset field of each TopicPartition must either be set to an absolute
// starting offset (>= 0), or one of the logical offsets (`kafka.OffsetEnd` etc),
// but should typically be set to `kafka.OffsetStored` to have the consumer
// use the committed offset as a start position, with a fallback to
// `auto.offset.reset` if there is no committed offset.
//
// The new partitions must not be part of the current assignment.
func (c *Consumer) IncrementalAssign(partitions []TopicPartition) (err error) {
	c.appReassigned = true

	cparts := newCPartsFromTopicPartitions(partitions)
	defer C.rd_kafka_topic_partition_list_destroy(cparts)

	cError := C.rd_kafka_incremental_assign(c.handle.rk, cparts)
	if cError != nil {
		return newErrorFromCErrorDestroy(cError)
	}

	return nil
}

// IncrementalUnassign removes the specified partitions from the current set of
// partitions to consume.
//
// The .Offset field of the TopicPartition is ignored.
//
// The removed partitions must be part of the current assignment.
func (c *Consumer) IncrementalUnassign(partitions []TopicPartition) (err error) {
	c.appReassigned = true

	cparts := newCPartsFromTopicPartitions(partitions)
	defer C.rd_kafka_topic_partition_list_destroy(cparts)

	cError := C.rd_kafka_incremental_unassign(c.handle.rk, cparts)
	if cError != nil {
		return newErrorFromCErrorDestroy(cError)
	}

	return nil
}

// GetRebalanceProtocol returns the current consumer group rebalance protocol,
// which is either "EAGER" or "COOPERATIVE".
// If the rebalance protocol is not known in the current state an empty string
// is returned.
// Should typically only be called during rebalancing.
func (c *Consumer) GetRebalanceProtocol() string {
	cStr := C.rd_kafka_rebalance_protocol(c.handle.rk)
	if cStr == nil {
		return ""
	}

	return C.GoString(cStr)
}

// AssignmentLost returns true if current partition assignment has been lost.
// This method is only applicable for use with a subscribing consumer when
// handling a rebalance event or callback.
// Partitions that have been lost may already be owned by other members in the
// group and therefore commiting offsets, for example, may fail.
func (c *Consumer) AssignmentLost() bool {
	return cint2bool(C.rd_kafka_assignment_lost(c.handle.rk))
}

// commit offsets for specified offsets.
// If offsets is nil the currently assigned partitions' offsets are committed.
// This is a blocking call, caller will need to wrap in go-routine to
// get async or throw-away behaviour.
func (c *Consumer) commit(offsets []TopicPartition) (committedOffsets []TopicPartition, err error) {
	var rkqu *C.rd_kafka_queue_t

	rkqu = C.rd_kafka_queue_new(c.handle.rk)
	defer C.rd_kafka_queue_destroy(rkqu)

	var coffsets *C.rd_kafka_topic_partition_list_t
	if offsets != nil {
		coffsets = newCPartsFromTopicPartitions(offsets)
		defer C.rd_kafka_topic_partition_list_destroy(coffsets)
	}

	cErr := C.rd_kafka_commit_queue(c.handle.rk, coffsets, rkqu, nil, nil)
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil, newError(cErr)
	}

	rkev := C.rd_kafka_queue_poll(rkqu, C.int(-1))
	if rkev == nil {
		// shouldn't happen
		return nil, newError(C.RD_KAFKA_RESP_ERR__DESTROY)
	}
	defer C.rd_kafka_event_destroy(rkev)

	if C.rd_kafka_event_type(rkev) != C.RD_KAFKA_EVENT_OFFSET_COMMIT {
		panic(fmt.Sprintf("Expected OFFSET_COMMIT, got %s",
			C.GoString(C.rd_kafka_event_name(rkev))))
	}

	cErr = C.rd_kafka_event_error(rkev)
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil, newErrorFromCString(cErr, C.rd_kafka_event_error_string(rkev))
	}

	cRetoffsets := C.rd_kafka_event_topic_partition_list(rkev)
	if cRetoffsets == nil {
		// no offsets, no error
		return nil, nil
	}
	committedOffsets = newTopicPartitionsFromCparts(cRetoffsets)

	return committedOffsets, nil
}

// Commit offsets for currently assigned partitions
// This is a blocking call.
// Returns the committed offsets on success.
func (c *Consumer) Commit() ([]TopicPartition, error) {
	return c.commit(nil)
}

// CommitMessage commits offset based on the provided message.
// This is a blocking call.
// Returns the committed offsets on success.
func (c *Consumer) CommitMessage(m *Message) ([]TopicPartition, error) {
	if m.TopicPartition.Error != nil {
		return nil, newErrorFromString(ErrInvalidArg, "Can't commit errored message")
	}
	offsets := []TopicPartition{m.TopicPartition}
	offsets[0].Offset++
	return c.commit(offsets)
}

// CommitOffsets commits the provided list of offsets
// This is a blocking call.
// Returns the committed offsets on success.
func (c *Consumer) CommitOffsets(offsets []TopicPartition) ([]TopicPartition, error) {
	return c.commit(offsets)
}

// StoreOffsets stores the provided list of offsets that will be committed
// to the offset store according to `auto.commit.interval.ms` or manual
// offset-less Commit().
//
// Returns the stored offsets on success. If at least one offset couldn't be stored,
// an error and a list of offsets is returned. Each offset can be checked for
// specific errors via its `.Error` member.
func (c *Consumer) StoreOffsets(offsets []TopicPartition) (storedOffsets []TopicPartition, err error) {
	coffsets := newCPartsFromTopicPartitions(offsets)
	defer C.rd_kafka_topic_partition_list_destroy(coffsets)

	cErr := C.rd_kafka_offsets_store(c.handle.rk, coffsets)

	// coffsets might be annotated with an error
	storedOffsets = newTopicPartitionsFromCparts(coffsets)

	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return storedOffsets, newError(cErr)
	}

	return storedOffsets, nil
}

// Seek seeks the given topic partitions using the offset from the TopicPartition.
//
// If timeoutMs is not 0 the call will wait this long for the
// seek to be performed. If the timeout is reached the internal state
// will be unknown and this function returns ErrTimedOut.
// If timeoutMs is 0 it will initiate the seek but return
// immediately without any error reporting (e.g., async).
//
// Seek() may only be used for partitions already being consumed
// (through Assign() or implicitly through a self-rebalanced Subscribe()).
// To set the starting offset it is preferred to use Assign() and provide
// a starting offset for each partition.
//
// Returns an error on failure or nil otherwise.
func (c *Consumer) Seek(partition TopicPartition, timeoutMs int) error {
	rkt := c.handle.getRkt(*partition.Topic)
	cErr := C.rd_kafka_seek(rkt,
		C.int32_t(partition.Partition),
		C.int64_t(partition.Offset),
		C.int(timeoutMs))
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(cErr)
	}
	return nil
}

// Poll the consumer for messages or events.
//
// Will block for at most timeoutMs milliseconds
//
// The following callbacks may be triggered:
//   Subscribe()'s rebalanceCb
//
// Returns nil on timeout, else an Event
func (c *Consumer) Poll(timeoutMs int) (event Event) {
	ev, _ := c.handle.eventPoll(nil, timeoutMs, 1, nil)
	return ev
}

// Events returns the Events channel (if enabled)
func (c *Consumer) Events() chan Event {
	return c.events
}

// Logs returns the log channel if enabled, or nil otherwise.
func (c *Consumer) Logs() chan LogEvent {
	return c.handle.logs
}

// ReadMessage polls the consumer for a message.
//
// This is a convenience API that wraps Poll() and only returns
// messages or errors. All other event types are discarded.
//
// The call will block for at most `timeout` waiting for
// a new message or error. `timeout` may be set to -1 for
// indefinite wait.
//
// Timeout is returned as (nil, err) where err is `err.(kafka.Error).Code() == kafka.ErrTimedOut`.
//
// Messages are returned as (msg, nil),
// while general errors are returned as (nil, err),
// and partition-specific errors are returned as (msg, err) where
// msg.TopicPartition provides partition-specific information (such as topic, partition and offset).
//
// All other event types, such as PartitionEOF, AssignedPartitions, etc, are silently discarded.
//
func (c *Consumer) ReadMessage(timeout time.Duration) (*Message, error) {

	var absTimeout time.Time
	var timeoutMs int

	if timeout > 0 {
		absTimeout = time.Now().Add(timeout)
		timeoutMs = (int)(timeout.Seconds() * 1000.0)
	} else {
		timeoutMs = (int)(timeout)
	}

	for {
		ev := c.Poll(timeoutMs)

		switch e := ev.(type) {
		case *Message:
			if e.TopicPartition.Error != nil {
				return e, e.TopicPartition.Error
			}
			return e, nil
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
			return nil, newError(C.RD_KAFKA_RESP_ERR__TIMED_OUT)
		}

	}

}

// Close Consumer instance.
// The object is no longer usable after this call.
func (c *Consumer) Close() (err error) {

	// Wait for consumerReader() or pollLogEvents to terminate (by closing readerTermChan)
	close(c.readerTermChan)
	c.handle.waitGroup.Wait()
	if c.eventsChanEnable {
		close(c.events)
	}

	// librdkafka's rd_kafka_consumer_close() will block
	// and trigger the rebalance_cb() if one is set, if not, which is the
	// case with the Go client since it registers EVENTs rather than callbacks,
	// librdkafka will shortcut the rebalance_cb and do a forced unassign.
	// But we can't have that since the application might need the final RevokePartitions
	// before shutting down. So we trigger an Unsubscribe() first, wait for that to
	// propagate (in the Poll loop below), and then close the consumer.
	c.Unsubscribe()

	// Poll for rebalance events
	for {
		c.Poll(10 * 1000)
		if int(C.rd_kafka_queue_length(c.handle.rkq)) == 0 {
			break
		}
	}

	// Destroy our queue
	C.rd_kafka_queue_destroy(c.handle.rkq)
	c.handle.rkq = nil

	// Close the consumer
	C.rd_kafka_consumer_close(c.handle.rk)

	c.handle.cleanup()

	C.rd_kafka_destroy(c.handle.rk)

	return nil
}

// NewConsumer creates a new high-level Consumer instance.
//
// conf is a *ConfigMap with standard librdkafka configuration properties.
//
// Supported special configuration properties:
//   go.application.rebalance.enable (bool, false) - Forward rebalancing responsibility to application via the Events() channel.
//                                        If set to true the app must handle the AssignedPartitions and
//                                        RevokedPartitions events and call Assign() and Unassign()
//                                        respectively.
//   go.events.channel.enable (bool, false) - [deprecated] Enable the Events() channel. Messages and events will be pushed on the Events() channel and the Poll() interface will be disabled.
//   go.events.channel.size (int, 1000) - Events() channel size
//   go.logs.channel.enable (bool, false) - Forward log to Logs() channel.
//   go.logs.channel (chan kafka.LogEvent, nil) - Forward logs to application-provided channel instead of Logs(). Requires go.logs.channel.enable=true.
//
// WARNING: Due to the buffering nature of channels (and queues in general) the
// use of the events channel risks receiving outdated events and
// messages. Minimizing go.events.channel.size reduces the risk
// and number of outdated events and messages but does not eliminate
// the factor completely. With a channel size of 1 at most one
// event or message may be outdated.
func NewConsumer(conf *ConfigMap) (*Consumer, error) {

	err := versionCheck()
	if err != nil {
		return nil, err
	}

	// before we do anything with the configuration, create a copy such that
	// the original is not mutated.
	confCopy := conf.clone()

	groupid, _ := confCopy.get("group.id", nil)
	if groupid == nil {
		// without a group.id the underlying cgrp subsystem in librdkafka wont get started
		// and without it there is no way to consume assigned partitions.
		// So for now require the group.id, this might change in the future.
		return nil, newErrorFromString(ErrInvalidArg, "Required property group.id not set")
	}

	c := &Consumer{}

	v, err := confCopy.extract("go.application.rebalance.enable", false)
	if err != nil {
		return nil, err
	}
	c.appRebalanceEnable = v.(bool)

	v, err = confCopy.extract("go.events.channel.enable", false)
	if err != nil {
		return nil, err
	}
	c.eventsChanEnable = v.(bool)

	v, err = confCopy.extract("go.events.channel.size", 1000)
	if err != nil {
		return nil, err
	}
	eventsChanSize := v.(int)

	logsChanEnable, logsChan, err := confCopy.extractLogConfig()
	if err != nil {
		return nil, err
	}

	cConf, err := confCopy.convert()
	if err != nil {
		return nil, err
	}
	cErrstr := (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(cErrstr))

	C.rd_kafka_conf_set_events(cConf, C.RD_KAFKA_EVENT_REBALANCE|C.RD_KAFKA_EVENT_OFFSET_COMMIT|C.RD_KAFKA_EVENT_STATS|C.RD_KAFKA_EVENT_ERROR|C.RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH)

	c.handle.rk = C.rd_kafka_new(C.RD_KAFKA_CONSUMER, cConf, cErrstr, 256)
	if c.handle.rk == nil {
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	C.rd_kafka_poll_set_consumer(c.handle.rk)

	c.handle.c = c
	c.handle.setup()
	c.readerTermChan = make(chan bool)
	c.handle.rkq = C.rd_kafka_queue_get_consumer(c.handle.rk)
	if c.handle.rkq == nil {
		// no cgrp (no group.id configured), revert to main queue.
		c.handle.rkq = C.rd_kafka_queue_get_main(c.handle.rk)
	}

	if logsChanEnable {
		c.handle.setupLogQueue(logsChan, c.readerTermChan)
	}

	if c.eventsChanEnable {
		c.events = make(chan Event, eventsChanSize)
		/* Start rdkafka consumer queue reader -> events writer goroutine */
		c.handle.waitGroup.Add(1)
		go func() {
			consumerReader(c, c.readerTermChan)
			c.handle.waitGroup.Done()
		}()
	}

	return c, nil
}

// consumerReader reads messages and events from the librdkafka consumer queue
// and posts them on the consumer channel.
// Runs until termChan closes
func consumerReader(c *Consumer, termChan chan bool) {
	for {
		select {
		case _ = <-termChan:
			return
		default:
			_, term := c.handle.eventPoll(c.events, 100, 1000, termChan)
			if term {
				return
			}

		}
	}
}

// GetMetadata queries broker for cluster and topic metadata.
// If topic is non-nil only information about that topic is returned, else if
// allTopics is false only information about locally used topics is returned,
// else information about all topics is returned.
// GetMetadata is equivalent to listTopics, describeTopics and describeCluster in the Java API.
func (c *Consumer) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*Metadata, error) {
	return getMetadata(c, topic, allTopics, timeoutMs)
}

// QueryWatermarkOffsets queries the broker for the low and high offsets for the given topic and partition.
func (c *Consumer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	return queryWatermarkOffsets(c, topic, partition, timeoutMs)
}

// GetWatermarkOffsets returns the cached low and high offsets for the given topic
// and partition.  The high offset is populated on every fetch response or via calling QueryWatermarkOffsets.
// The low offset is populated every statistics.interval.ms if that value is set.
// OffsetInvalid will be returned if there is no cached offset for either value.
func (c *Consumer) GetWatermarkOffsets(topic string, partition int32) (low, high int64, err error) {
	return getWatermarkOffsets(c, topic, partition)
}

// OffsetsForTimes looks up offsets by timestamp for the given partitions.
//
// The returned offset for each partition is the earliest offset whose
// timestamp is greater than or equal to the given timestamp in the
// corresponding partition. If the provided timestamp exceeds that of the
// last message in the partition, a value of -1 will be returned.
//
// The timestamps to query are represented as `.Offset` in the `times`
// argument and the looked up offsets are represented as `.Offset` in the returned
// `offsets` list.
//
// The function will block for at most timeoutMs milliseconds.
//
// Duplicate Topic+Partitions are not supported.
// Per-partition errors may be returned in the `.Error` field.
func (c *Consumer) OffsetsForTimes(times []TopicPartition, timeoutMs int) (offsets []TopicPartition, err error) {
	return offsetsForTimes(c, times, timeoutMs)
}

// Subscription returns the current subscription as set by Subscribe()
func (c *Consumer) Subscription() (topics []string, err error) {
	var cTopics *C.rd_kafka_topic_partition_list_t

	cErr := C.rd_kafka_subscription(c.handle.rk, &cTopics)
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil, newError(cErr)
	}
	defer C.rd_kafka_topic_partition_list_destroy(cTopics)

	topicCnt := int(cTopics.cnt)
	topics = make([]string, topicCnt)
	for i := 0; i < topicCnt; i++ {
		crktpar := C._c_rdkafka_topic_partition_list_entry(cTopics,
			C.int(i))
		topics[i] = C.GoString(crktpar.topic)
	}

	return topics, nil
}

// Assignment returns the current partition assignments
func (c *Consumer) Assignment() (partitions []TopicPartition, err error) {
	var cParts *C.rd_kafka_topic_partition_list_t

	cErr := C.rd_kafka_assignment(c.handle.rk, &cParts)
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil, newError(cErr)
	}
	defer C.rd_kafka_topic_partition_list_destroy(cParts)

	partitions = newTopicPartitionsFromCparts(cParts)

	return partitions, nil
}

// Committed retrieves committed offsets for the given set of partitions
func (c *Consumer) Committed(partitions []TopicPartition, timeoutMs int) (offsets []TopicPartition, err error) {
	cparts := newCPartsFromTopicPartitions(partitions)
	defer C.rd_kafka_topic_partition_list_destroy(cparts)
	cerr := C.rd_kafka_committed(c.handle.rk, cparts, C.int(timeoutMs))
	if cerr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil, newError(cerr)
	}

	return newTopicPartitionsFromCparts(cparts), nil
}

// Position returns the current consume position for the given partitions.
// Typical use is to call Assignment() to get the partition list
// and then pass it to Position() to get the current consume position for
// each of the assigned partitions.
// The consume position is the next message to read from the partition.
// i.e., the offset of the last message seen by the application + 1.
func (c *Consumer) Position(partitions []TopicPartition) (offsets []TopicPartition, err error) {
	cparts := newCPartsFromTopicPartitions(partitions)
	defer C.rd_kafka_topic_partition_list_destroy(cparts)
	cerr := C.rd_kafka_position(c.handle.rk, cparts)
	if cerr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil, newError(cerr)
	}

	return newTopicPartitionsFromCparts(cparts), nil
}

// Pause consumption for the provided list of partitions
//
// Note that messages already enqueued on the consumer's Event channel
// (if `go.events.channel.enable` has been set) will NOT be purged by
// this call, set `go.events.channel.size` accordingly.
func (c *Consumer) Pause(partitions []TopicPartition) (err error) {
	cparts := newCPartsFromTopicPartitions(partitions)
	defer C.rd_kafka_topic_partition_list_destroy(cparts)
	cerr := C.rd_kafka_pause_partitions(c.handle.rk, cparts)
	if cerr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(cerr)
	}
	return nil
}

// Resume consumption for the provided list of partitions
func (c *Consumer) Resume(partitions []TopicPartition) (err error) {
	cparts := newCPartsFromTopicPartitions(partitions)
	defer C.rd_kafka_topic_partition_list_destroy(cparts)
	cerr := C.rd_kafka_resume_partitions(c.handle.rk, cparts)
	if cerr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(cerr)
	}
	return nil
}

// SetOAuthBearerToken sets the the data to be transmitted
// to a broker during SASL/OAUTHBEARER authentication. It will return nil
// on success, otherwise an error if:
// 1) the token data is invalid (meaning an expiration time in the past
// or either a token value or an extension key or value that does not meet
// the regular expression requirements as per
// https://tools.ietf.org/html/rfc7628#section-3.1);
// 2) SASL/OAUTHBEARER is not supported by the underlying librdkafka build;
// 3) SASL/OAUTHBEARER is supported but is not configured as the client's
// authentication mechanism.
func (c *Consumer) SetOAuthBearerToken(oauthBearerToken OAuthBearerToken) error {
	return c.handle.setOAuthBearerToken(oauthBearerToken)
}

// SetOAuthBearerTokenFailure sets the error message describing why token
// retrieval/setting failed; it also schedules a new token refresh event for 10
// seconds later so the attempt may be retried. It will return nil on
// success, otherwise an error if:
// 1) SASL/OAUTHBEARER is not supported by the underlying librdkafka build;
// 2) SASL/OAUTHBEARER is supported but is not configured as the client's
// authentication mechanism.
func (c *Consumer) SetOAuthBearerTokenFailure(errstr string) error {
	return c.handle.setOAuthBearerTokenFailure(errstr)
}

// ConsumerGroupMetadata reflects the current consumer group member metadata.
type ConsumerGroupMetadata struct {
	serialized []byte
}

// serializeConsumerGroupMetadata converts a C metadata object to its
// binary representation so we don't have to hold on to the C object,
// which would require an explicit .Close().
func serializeConsumerGroupMetadata(cgmd *C.rd_kafka_consumer_group_metadata_t) ([]byte, error) {
	var cBuffer *C.void
	var cSize C.size_t
	cError := C.rd_kafka_consumer_group_metadata_write(cgmd,
		(*unsafe.Pointer)(unsafe.Pointer(&cBuffer)), &cSize)
	if cError != nil {
		return nil, newErrorFromCErrorDestroy(cError)
	}
	defer C.rd_kafka_mem_free(nil, unsafe.Pointer(cBuffer))

	return C.GoBytes(unsafe.Pointer(cBuffer), C.int(cSize)), nil
}

// deserializeConsumerGroupMetadata converts a serialized metadata object
// back to a C object.
func deserializeConsumerGroupMetadata(serialized []byte) (*C.rd_kafka_consumer_group_metadata_t, error) {
	var cgmd *C.rd_kafka_consumer_group_metadata_t

	cSerialized := C.CBytes(serialized)
	defer C.free(cSerialized)

	cError := C.rd_kafka_consumer_group_metadata_read(
		&cgmd, cSerialized, C.size_t(len(serialized)))
	if cError != nil {
		return nil, newErrorFromCErrorDestroy(cError)
	}

	return cgmd, nil
}

// GetConsumerGroupMetadata returns the consumer's current group metadata.
// This object should be passed to the transactional producer's
// SendOffsetsToTransaction() API.
func (c *Consumer) GetConsumerGroupMetadata() (*ConsumerGroupMetadata, error) {
	cgmd := C.rd_kafka_consumer_group_metadata(c.handle.rk)
	if cgmd == nil {
		return nil, NewError(ErrState, "Consumer group metadata not available", false)
	}
	defer C.rd_kafka_consumer_group_metadata_destroy(cgmd)

	serialized, err := serializeConsumerGroupMetadata(cgmd)
	if err != nil {
		return nil, err
	}

	return &ConsumerGroupMetadata{serialized}, nil
}

// NewTestConsumerGroupMetadata creates a new consumer group metadata instance
// mainly for testing use.
// Use GetConsumerGroupMetadata() to retrieve the real metadata.
func NewTestConsumerGroupMetadata(groupID string) (*ConsumerGroupMetadata, error) {
	cGroupID := C.CString(groupID)
	defer C.free(unsafe.Pointer(cGroupID))

	cgmd := C.rd_kafka_consumer_group_metadata_new(cGroupID)
	if cgmd == nil {
		return nil, NewError(ErrInvalidArg, "Failed to create metadata object", false)
	}

	defer C.rd_kafka_consumer_group_metadata_destroy(cgmd)
	serialized, err := serializeConsumerGroupMetadata(cgmd)
	if err != nil {
		return nil, err
	}

	return &ConsumerGroupMetadata{serialized}, nil
}

// cEventToRebalanceEvent returns an Event (AssignedPartitions or RevokedPartitions)
// based on the specified rkev.
func cEventToRebalanceEvent(rkev *C.rd_kafka_event_t) Event {
	if C.rd_kafka_event_error(rkev) == C.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
		var ev AssignedPartitions
		ev.Partitions = newTopicPartitionsFromCparts(C.rd_kafka_event_topic_partition_list(rkev))
		return ev
	} else if C.rd_kafka_event_error(rkev) == C.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS {
		var ev RevokedPartitions
		ev.Partitions = newTopicPartitionsFromCparts(C.rd_kafka_event_topic_partition_list(rkev))
		return ev
	} else {
		panic(fmt.Sprintf("Unable to create rebalance event from C type %s",
			C.GoString(C.rd_kafka_err2name(C.rd_kafka_event_error(rkev)))))
	}

}

// handleRebalanceEvent handles a assign/rebalance rebalance event.
//
// If the app provided a RebalanceCb to Subscribe*() or
// has go.application.rebalance.enable=true we create an event
// and forward it to the application thru the RebalanceCb or the
// Events channel respectively.
// Since librdkafka requires the rebalance event to be "acked" by
// the application (by calling *assign()) to synchronize state we keep track
// of if the application performed *Assign() or *Unassign(), but this only
// works for the non-channel case. For the channel case we assume the
// application calls *Assign() or *Unassign().
// Failure to do so will "hang" the consumer, e.g., it wont start consuming
// and it wont close cleanly, so this error case should be visible
// immediately to the application developer.
//
// In the polling case (not channel based consumer) the rebalance event
// is returned in retval, else nil is returned.

func (c *Consumer) handleRebalanceEvent(channel chan Event, rkev *C.rd_kafka_event_t) (retval Event) {

	var ev Event

	if c.rebalanceCb != nil || c.appRebalanceEnable {
		// Application has a rebalance callback or has enabled
		// rebalances on the events channel, create the appropriate Event.
		ev = cEventToRebalanceEvent(rkev)

	}

	if channel != nil && c.appRebalanceEnable && c.rebalanceCb == nil {
		// Channel-based consumer with rebalancing enabled,
		// return the rebalance event and rely on the application
		// to call *Assign() / *Unassign().
		return ev
	}

	// Call the application's rebalance callback, if any.
	if c.rebalanceCb != nil {
		// Mark .appReassigned as false to keep track of whether the
		// application called *Assign() / *Unassign().
		c.appReassigned = false

		c.rebalanceCb(c, ev)

		if c.appReassigned {
			// Rebalance event handled by application.
			return nil
		}
	}

	// Either there was no rebalance callback, or the application
	// did not call *Assign / *Unassign, so we need to do it.

	isCooperative := c.GetRebalanceProtocol() == "COOPERATIVE"
	var cError *C.rd_kafka_error_t
	var cErr C.rd_kafka_resp_err_t

	if C.rd_kafka_event_error(rkev) == C.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
		// Assign partitions
		if isCooperative {
			cError = C.rd_kafka_incremental_assign(
				c.handle.rk,
				C.rd_kafka_event_topic_partition_list(rkev))
		} else {
			cErr = C.rd_kafka_assign(
				c.handle.rk,
				C.rd_kafka_event_topic_partition_list(rkev))
		}
	} else {
		// Revoke partitions

		if isCooperative {
			cError = C.rd_kafka_incremental_unassign(
				c.handle.rk,
				C.rd_kafka_event_topic_partition_list(rkev))
		} else {
			cErr = C.rd_kafka_assign(c.handle.rk, nil)
		}
	}

	// If the *assign() call returned error, forward it to the
	// the consumer's Events() channel for visibility.
	if cError != nil {
		c.events <- newErrorFromCErrorDestroy(cError)
	} else if cErr != 0 {
		c.events <- newError(cErr)
	}

	return nil
}
