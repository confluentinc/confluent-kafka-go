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
// This package implements high-level Apache Kafka producer and consumers
// using bindings on-top of the C librdkafka library.
package kafka

import (
	"fmt"
	"time"
	"unsafe"
)

/*
#include <string.h>
#include <librdkafka/rdkafka.h>
#include "glue_rdkafka.h"

void setup_rkmessage (rd_kafka_message_t *rkmessage,
                      rd_kafka_topic_t *rkt, int32_t partition,
                      const void *payload, size_t len,
                      void *key, size_t key_len, void *opaque) {
     rkmessage->rkt       = rkt;
     rkmessage->partition = partition;
     rkmessage->payload   = (void *)payload;
     rkmessage->len       = len;
     rkmessage->key       = (void *)key;
     rkmessage->key_len   = key_len;
     rkmessage->_private  = opaque;
}


rd_kafka_message_t *event_rkmessage_next(rd_kafka_event_t *rkev,
                    rd_kafka_timestamp_type_t *tstype, int64_t *ts) {
  const rd_kafka_message_t *rkmessage;

  rkmessage = rd_kafka_event_message_next(rkev);
  if (!rkmessage)
     return NULL;

  *ts = rd_kafka_message_timestamp(rkmessage, tstype);

  return (rd_kafka_message_t *)rkmessage;
}
*/
import "C"

// TimestampType is a the Message timestamp type or source
//
type TimestampType int

const (
	// Timestamp not set, or not available due to lacking broker support
	TIMESTAMP_NOT_AVAILABLE = TimestampType(C.RD_KAFKA_TIMESTAMP_NOT_AVAILABLE)
	// Timestamp set by producer (source time)
	TIMESTAMP_CREATE_TIME = TimestampType(C.RD_KAFKA_TIMESTAMP_CREATE_TIME)
	// Timestamp set by broker (store time)
	TIMESTAMP_LOG_APPEND_TIME = TimestampType(C.RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME)
)

func (t TimestampType) String() string {
	switch t {
	case TIMESTAMP_CREATE_TIME:
		return "CreateTime"
	case TIMESTAMP_LOG_APPEND_TIME:
		return "LogAppendTime"
	case TIMESTAMP_NOT_AVAILABLE:
		fallthrough
	default:
		return "NotAvailable"
	}
}

// Message represents a Kafka message
type Message struct {
	TopicPartition TopicPartition
	Value          []byte
	Key            []byte
	Timestamp      time.Time
	TimestampType  TimestampType
	Opaque         interface{}
}

// String returns a human readable representation of a Message.
// Key and payload are not represented.
func (m *Message) String() string {
	var topic string
	if m.TopicPartition.Topic != nil {
		topic = *m.TopicPartition.Topic
	} else {
		topic = ""
	}
	return fmt.Sprintf("%s[%d]@%s", topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
}

func (h *handle) get_rkt_from_message(msg *Message) (c_rkt *C.rd_kafka_topic_t) {
	if msg.TopicPartition.Topic == nil {
		return nil
	}

	return h.get_rkt(*msg.TopicPartition.Topic)
}

// new_message_from_event reads a message from the provided C event
// and creates a new Message object from the extracted information.
// returns nil if no message was available.
func (h *handle) new_message_from_event(rkev *C.rd_kafka_event_t) (msg *Message) {
	var tstype C.rd_kafka_timestamp_type_t
	var c_ts C.int64_t

	c_msg := C.event_rkmessage_next(rkev, &tstype, &c_ts)
	if c_msg == nil {
		return nil
	}

	msg = &Message{}

	if c_ts != -1 {
		ts := int64(c_ts)
		msg.TimestampType = TimestampType(tstype)
		msg.Timestamp = time.Unix(ts/1000, (ts%1000)*1000000)
	}

	h.setup_message_from_c(msg, c_msg)

	return msg
}

func (h *handle) new_message_from_fc_msg(fc_msg *C.fetched_c_msg_t) (msg *Message) {
	msg = &Message{}

	if fc_msg.ts != -1 {
		ts := int64(fc_msg.ts)
		msg.TimestampType = TimestampType(fc_msg.tstype)
		msg.Timestamp = time.Unix(ts/1000, (ts%1000)*1000000)
	}

	h.setup_message_from_c(msg, fc_msg.msg)

	return msg
}

// setup_message_from_c sets up a message object from a C rd_kafka_message_t
func (h *handle) setup_message_from_c(msg *Message, c_msg *C.rd_kafka_message_t) {
	if c_msg.rkt != nil {
		topic := h.get_topic_name_from_rkt(c_msg.rkt)
		msg.TopicPartition.Topic = &topic
	}
	msg.TopicPartition.Partition = int32(c_msg.partition)
	if c_msg.payload != nil {
		msg.Value = C.GoBytes(unsafe.Pointer(c_msg.payload), C.int(c_msg.len))
	}
	if c_msg.key != nil {
		msg.Key = C.GoBytes(unsafe.Pointer(c_msg.key), C.int(c_msg.key_len))
	}
	msg.TopicPartition.Offset = Offset(c_msg.offset)
	if c_msg.err != 0 {
		msg.TopicPartition.Error = NewKafkaError(c_msg.err)
	}
}

// new_message_from_c creates a new message object from a C rd_kafka_message_t
// NOTE: For use with Producer: does not set message timestamp fields.
func (h *handle) new_message_from_c(c_msg *C.rd_kafka_message_t) (msg *Message) {
	msg = &Message{}

	h.setup_message_from_c(msg, c_msg)

	return msg
}

// message_to_C sets up c_msg as a clone of msg
func (h *handle) message_to_c(msg *Message, c_msg *C.rd_kafka_message_t) {
	var valp unsafe.Pointer = nil
	var keyp unsafe.Pointer = nil

	// to circumvent Cgo constraints we need to allocate C heap memory
	// for both Value and Key (one allocation back to back)
	// and copy the bytes from Value and Key to the C memory.
	// We later tell librdkafka (in produce()) to free the
	// C memory pointer when it is done.
	var payload unsafe.Pointer

	value_len := 0
	key_len := 0
	if msg.Value != nil {
		value_len = len(msg.Value)
	}
	if msg.Key != nil {
		key_len = len(msg.Key)
	}

	alloc_len := value_len + key_len
	if alloc_len > 0 {
		payload = C.malloc(C.size_t(alloc_len))
		if value_len > 0 {
			copy((*[1 << 31]byte)(payload)[0:value_len], msg.Value)
			valp = payload
		}
		if key_len > 0 {
			copy((*[1 << 31]byte)(payload)[value_len:key_len], msg.Key)
			keyp = unsafe.Pointer(&((*[1 << 31]byte)(payload)[value_len]))
		}
	}

	c_msg.rkt = h.get_rkt_from_message(msg)
	c_msg.partition = C.int32_t(msg.TopicPartition.Partition)
	c_msg.payload = valp
	c_msg.len = C.size_t(value_len)
	c_msg.key = keyp
	c_msg.key_len = C.size_t(key_len)
	c_msg._private = nil
}

// used for testing message_to_c performance
func (h *handle) message_to_c_dummy(msg *Message) {
	var c_msg C.rd_kafka_message_t
	h.message_to_c(msg, &c_msg)
}
