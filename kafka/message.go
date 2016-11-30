package kafka

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
                      void *key, size_t keyLen, void *opaque) {
     rkmessage->rkt       = rkt;
     rkmessage->partition = partition;
     rkmessage->payload   = (void *)payload;
     rkmessage->len       = len;
     rkmessage->key       = (void *)key;
     rkmessage->key_len   = keyLen;
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
	// TimestampNotAvailable indicates no timestamp was set, or not available due to lacking broker support
	TimestampNotAvailable = TimestampType(C.RD_KAFKA_TIMESTAMP_NOT_AVAILABLE)
	// TimestampCreateTime indicates timestamp set by producer (source time)
	TimestampCreateTime = TimestampType(C.RD_KAFKA_TIMESTAMP_CREATE_TIME)
	// TimestampLogAppendTime indicates timestamp set set by broker (store time)
	TimestampLogAppendTime = TimestampType(C.RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME)
)

func (t TimestampType) String() string {
	switch t {
	case TimestampCreateTime:
		return "CreateTime"
	case TimestampLogAppendTime:
		return "LogAppendTime"
	case TimestampNotAvailable:
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

func (h *handle) getRktFromMessage(msg *Message) (crkt *C.rd_kafka_topic_t) {
	if msg.TopicPartition.Topic == nil {
		return nil
	}

	return h.getRkt(*msg.TopicPartition.Topic)
}

// newMessageFromEvent reads a message from the provided C event
// and creates a new Message object from the extracted information.
// returns nil if no message was available.
func (h *handle) newMessageFromEvent(rkev *C.rd_kafka_event_t) (msg *Message) {
	var tstype C.rd_kafka_timestamp_type_t
	var cts C.int64_t

	cmsg := C.event_rkmessage_next(rkev, &tstype, &cts)
	if cmsg == nil {
		return nil
	}

	msg = &Message{}

	if cts != -1 {
		ts := int64(cts)
		msg.TimestampType = TimestampType(tstype)
		msg.Timestamp = time.Unix(ts/1000, (ts%1000)*1000000)
	}

	h.setupMessageFromC(msg, cmsg)

	return msg
}

func (h *handle) newMessageFromFcMsg(fcMsg *C.fetched_c_msg_t) (msg *Message) {
	msg = &Message{}

	if fcMsg.ts != -1 {
		ts := int64(fcMsg.ts)
		msg.TimestampType = TimestampType(fcMsg.tstype)
		msg.Timestamp = time.Unix(ts/1000, (ts%1000)*1000000)
	}

	h.setupMessageFromC(msg, fcMsg.msg)

	return msg
}

// setupMessageFromC sets up a message object from a C rd_kafka_message_t
func (h *handle) setupMessageFromC(msg *Message, cmsg *C.rd_kafka_message_t) {
	if cmsg.rkt != nil {
		topic := h.getTopicNameFromRkt(cmsg.rkt)
		msg.TopicPartition.Topic = &topic
	}
	msg.TopicPartition.Partition = int32(cmsg.partition)
	if cmsg.payload != nil {
		msg.Value = C.GoBytes(unsafe.Pointer(cmsg.payload), C.int(cmsg.len))
	}
	if cmsg.key != nil {
		msg.Key = C.GoBytes(unsafe.Pointer(cmsg.key), C.int(cmsg.key_len))
	}
	msg.TopicPartition.Offset = Offset(cmsg.offset)
	if cmsg.err != 0 {
		msg.TopicPartition.Error = newError(cmsg.err)
	}
}

// newMessageFromC creates a new message object from a C rd_kafka_message_t
// NOTE: For use with Producer: does not set message timestamp fields.
func (h *handle) newMessageFromC(cmsg *C.rd_kafka_message_t) (msg *Message) {
	msg = &Message{}

	h.setupMessageFromC(msg, cmsg)

	return msg
}

// messageToC sets up cmsg as a clone of msg
func (h *handle) messageToC(msg *Message, cmsg *C.rd_kafka_message_t) {
	var valp unsafe.Pointer
	var keyp unsafe.Pointer

	// to circumvent Cgo constraints we need to allocate C heap memory
	// for both Value and Key (one allocation back to back)
	// and copy the bytes from Value and Key to the C memory.
	// We later tell librdkafka (in produce()) to free the
	// C memory pointer when it is done.
	var payload unsafe.Pointer

	valueLen := 0
	keyLen := 0
	if msg.Value != nil {
		valueLen = len(msg.Value)
	}
	if msg.Key != nil {
		keyLen = len(msg.Key)
	}

	allocLen := valueLen + keyLen
	if allocLen > 0 {
		payload = C.malloc(C.size_t(allocLen))
		if valueLen > 0 {
			copy((*[1 << 30]byte)(payload)[0:valueLen], msg.Value)
			valp = payload
		}
		if keyLen > 0 {
			copy((*[1 << 30]byte)(payload)[valueLen:allocLen], msg.Key)
			keyp = unsafe.Pointer(&((*[1 << 31]byte)(payload)[valueLen]))
		}
	}

	cmsg.rkt = h.getRktFromMessage(msg)
	cmsg.partition = C.int32_t(msg.TopicPartition.Partition)
	cmsg.payload = valp
	cmsg.len = C.size_t(valueLen)
	cmsg.key = keyp
	cmsg.key_len = C.size_t(keyLen)
	cmsg._private = nil
}

// used for testing messageToC performance
func (h *handle) messageToCDummy(msg *Message) {
	var cmsg C.rd_kafka_message_t
	h.messageToC(msg, &cmsg)
}
