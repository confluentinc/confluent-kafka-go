/**
 * Copyright 2016 Confluent Inc.
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

// kafka client.
package kafka

import (
	"unsafe"
)

/*
#include <stdlib.h>
#include <librdkafka/rdkafka.h>

void _rebalance_cb_trampoline (rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *c_parts, void *opaque) {

}
*/
import "C"

type RebalanceCb func(*Consumer, Event) error

// Consumer: High-level Kafka Consumer instance
type Consumer struct {
	Events                chan Event
	handle                handle
	events_channel_enable bool
	reader_term_chan      chan bool
	rebalance_cb          RebalanceCb
	app_reassigned        bool
	app_rebalance_enable  bool // config setting
}

// Strings returns a human readable name for a Consumer instance
func (c *Consumer) String() string {
	return c.handle.String()
}

// get_handle implements the Handle interface
func (c *Consumer) get_handle() *handle {
	return &c.handle
}

// Subscribe to a single topic
// This replaces the current subscription
func (c *Consumer) Subscribe(topic string, rebalance_cb RebalanceCb) error {
	return c.SubscribeTopics([]string{topic}, rebalance_cb)
}

// Subscribe to list of topics.
// This replaces the current subscription.
func (c *Consumer) SubscribeTopics(topics []string, rebalance_cb RebalanceCb) (err error) {
	c_topics := C.rd_kafka_topic_partition_list_new(C.int(len(topics)))
	defer C.rd_kafka_topic_partition_list_destroy(c_topics)

	for _, topic := range topics {
		c_topic := C.CString(topic)
		defer C.free(unsafe.Pointer(c_topic))
		C.rd_kafka_topic_partition_list_add(c_topics, c_topic, C.RD_KAFKA_PARTITION_UA)
	}

	e := C.rd_kafka_subscribe(c.handle.rk, c_topics)
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return NewKafkaError(e)
	}

	c.rebalance_cb = rebalance_cb
	c.handle.curr_app_rebalance_enable = c.rebalance_cb != nil || c.app_rebalance_enable

	return nil
}

// Unsubscribe from the current subscription, if any.
func (c *Consumer) Unsubscribe() (err error) {
	C.rd_kafka_unsubscribe(c.handle.rk)
	return nil
}

// Assign an atomic set of partitions to consume.
// This replaces the current assignment.
func (c *Consumer) Assign(partitions []TopicPartition) (err error) {
	c.app_reassigned = true

	c_parts := new_c_parts_from_TopicPartitions(partitions)
	defer C.rd_kafka_topic_partition_list_destroy(c_parts)

	e := C.rd_kafka_assign(c.handle.rk, c_parts)
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return NewKafkaError(e)
	}

	return nil
}

// Unassign the current set of partitions to consume.
func (c *Consumer) Unassign() (err error) {
	c.app_reassigned = true

	e := C.rd_kafka_assign(c.handle.rk, nil)
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return NewKafkaError(e)
	}

	return nil
}

// Commit offsets for currently assigned partitions
func (c *Consumer) Commit(async bool) (err error) {
	e := C.rd_kafka_commit(c.handle.rk, nil, bool2cint(async))
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return NewKafkaError(e)
	}
	return nil
}

// Commit offset based on the provided message.
func (c *Consumer) CommitMessage(m *Message, async bool) (err error) {
	c_offsets := new_c_parts_from_TopicPartitions([]TopicPartition{m.TopicPartition})
	defer C.rd_kafka_topic_partition_list_destroy(c_offsets)

	e := C.rd_kafka_commit(c.handle.rk, c_offsets, bool2cint(async))
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return NewKafkaError(e)
	}

	return nil
}

// Commit offset(s) provided in offsets list
func (c *Consumer) CommitOffsets(offsets []TopicPartition, async bool) (err error) {
	c_offsets := new_c_parts_from_TopicPartitions(offsets)
	defer C.rd_kafka_topic_partition_list_destroy(c_offsets)

	e := C.rd_kafka_commit(c.handle.rk, c_offsets, bool2cint(async))
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return NewKafkaError(e)
	}

	return nil
}

// Poll the consumer for messages or events.
//
// Will block for at most timeout_ms milliseconds
//
// The following callbacks may be triggered:
//   Subscribe()'s rebalance_cb
//
// Returns nil on timeout, else an Event
func (c *Consumer) Poll(timeout_ms int) (event Event) {
	return c.handle.event_poll(nil, timeout_ms, 1)
}

// Close Consumer instance.
// The object is no longer usable after this call.
func (c *Consumer) Close() (err error) {

	if c.events_channel_enable {
		// Wait for consumer_reader() to terminate (by closing reader_term_chan)
		close(c.reader_term_chan)
		c.handle.wait_terminated(1)

	}

	C.rd_kafka_queue_destroy(c.handle.rkq)
	c.handle.rkq = nil

	e := C.rd_kafka_consumer_close(c.handle.rk)
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return NewKafkaError(e)
	}

	c.handle.cleanup()

	C.rd_kafka_destroy(c.handle.rk)

	return nil
}

// NewConsumer creates a new high-level Consumer instance.
//
// Supported special configuration properties:
//   go.application.rebalance.enable (bool, false) - Forward rebalancing responsibility to application via the Events channel.
//                                        If set to true the app must handle the AssignedPartitions and
//                                        RevokedPartitions events and call Assign() and Unassign()
//                                        respectively.
//   go.message.timestamp.enable (bool, false) - Enable extraction of message timestamps for received messages.
//                                        (Experimental for increased performance (false)).
//   go.events.channel.enable (bool, false) - Enable the Events channel. Messages and events will be pushed on the Events channel and the Poll() interface will be disabled.
//                                        (Experimental)
func NewConsumer(conf *ConfigMap) (*Consumer, error) {

	groupid, _ := conf.get("group.id", nil)
	if groupid == nil {
		// without a group.id the underlying cgrp subsystem in librdkafka wont get started
		// and without it there is no way to consume assigned partitions.
		// So for now require the group.id, this might change in the future.
		return nil, NewKafkaErrorFromString(ERR__INVALID_ARG, "Required property group.id not set")
	}

	c := &Consumer{}

	v, err := conf.extract("go.application.rebalance.enable", false)
	if err != nil {
		return nil, err
	}
	c.app_rebalance_enable = v.(bool)

	v, err = conf.extract("go.events.channel.enable", false)
	if err != nil {
		return nil, err
	}
	c.events_channel_enable = v.(bool)

	c_conf, err := conf.convert()
	if err != nil {
		return nil, err
	}
	var c_errstr *C.char = (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(c_errstr))

	C.rd_kafka_conf_set_events(c_conf, C.RD_KAFKA_EVENT_REBALANCE)

	c.handle.rk = C.rd_kafka_new(C.RD_KAFKA_CONSUMER, c_conf, c_errstr, 256)
	if c.handle.rk == nil {
		return nil, NewKafkaErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, c_errstr)
	}

	C.rd_kafka_poll_set_consumer(c.handle.rk)

	c.handle.c = c
	c.handle.setup()
	c.handle.cgomap = make(map[int]cgoif)
	c.handle.rkq = C.rd_kafka_queue_get_consumer(c.handle.rk)
	if c.handle.rkq == nil {
		// no cgrp (no group.id configured), revert to main queue.
		c.handle.rkq = C.rd_kafka_queue_get_main(c.handle.rk)
	}

	if c.events_channel_enable {
		c.Events = make(chan Event, 1000)
		c.reader_term_chan = make(chan bool)

		/* Start rdkafka consumer queue reader -> Events writer goroutine */
		go consumer_reader(c, c.reader_term_chan)
	}

	return c, nil
}

// rebalance calls the application's rebalance callback, if any.
// Returns true if the underlying assignment was updated, else false.
func (c *Consumer) rebalance(ev Event) bool {
	c.app_reassigned = false

	if c.rebalance_cb != nil {
		c.rebalance_cb(c, ev)
	}

	return c.app_reassigned
}

// consumer_reader reads messages and events from the librdkafka consumer queue
// and posts them on the consumer channel.
// Runs until term_chan closes
func consumer_reader(c *Consumer, term_chan chan bool) {

	for true {
		select {
		case _ = <-term_chan:
			c.handle.terminated_chan <- "consumer_reader"
			return
		default:
			c.handle.event_poll(c.Events, 100, 1000)
		}
	}
}

// GetMetadata queries broker for cluster and topic metadata.
// If topic is non-nil only information about that topic is returned, else if
// all_topics is false only information about locally used topics is returned,
// else information about all topics is returned.
func (c *Consumer) GetMetadata(topic *string, all_topics bool, timeout_ms int) (*Metadata, error) {
	return get_metadata(c, topic, all_topics, timeout_ms)
}

// QueryWatermarkOffsets returns the broker's low and high offsets for the given topic
// and partition.
func (c *Consumer) QueryWatermarkOffsets(topic string, partition int32, timeout_ms int) (low, high int64, err error) {
	return queryWatermarkOffsets(c, topic, partition, timeout_ms)
}
