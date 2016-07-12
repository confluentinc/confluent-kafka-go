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
	Opaque         *interface{}
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

func (h *handle) get_rkt_from_Message(msg *Message) (c_rkt *C.rd_kafka_topic_t) {
	if msg.TopicPartition.Topic == nil {
		return nil
	}

	return h.get_rkt(*msg.TopicPartition.Topic)
}

// new_Message_from_c creates a new Message object from a C rd_kafka_message_t
func (h *handle) new_message_from_c(c_msg *C.rd_kafka_message_t) (msg *Message) {
	msg = &Message{}
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
		msg.TopicPartition.Err = NewKafkaError(c_msg.err)
	}

	// cgo calls are costly so we require extraction of message timestamps
	// to be explicitly enabled.
	if h.msg_timestamps_enable {
		var tstype C.rd_kafka_timestamp_type_t
		timestamp := int64(C.rd_kafka_message_timestamp(c_msg, &tstype))
		if timestamp != -1 {
			msg.TimestampType = TimestampType(tstype)
			msg.Timestamp = time.Unix(timestamp/1000, (timestamp%1000)*1000000)
		}
	}
	return msg
}

// Message_to_C sets up c_msg as a clone of msg
// WARNING: the c_msg payload and key will point to the Go memory of \p msg
//          so make sure \p msg does not go out of scope for the lifetime of
//          \p c_msg
func (h *handle) message_to_c(msg *Message, c_msg *C.rd_kafka_message_t) {
	var valp unsafe.Pointer = nil
	var keyp unsafe.Pointer = nil
	if msg.Value != nil {
		valp = unsafe.Pointer(&msg.Value[0])
	}
	if msg.Key != nil {
		keyp = unsafe.Pointer(&msg.Key[0])
	}

	C.setup_rkmessage(
		c_msg,
		h.get_rkt_from_Message(msg),
		C.int32_t(msg.TopicPartition.Partition),
		valp, C.size_t(len(msg.Value)),
		keyp, C.size_t(len(msg.Key)),
		nil)
}
