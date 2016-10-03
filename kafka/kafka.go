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
	"strconv"
	"unsafe"
)

/*
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

static int64_t _c_rdkafka_offset_tail(int64_t rel) {
   return RD_KAFKA_OFFSET_TAIL(rel);
}

static rd_kafka_topic_partition_t *_c_rdkafka_topic_partition_list_entry(rd_kafka_topic_partition_list_t *rktparlist, int idx) {
   return idx < rktparlist->cnt ? &rktparlist->elems[idx] : NULL;
}
*/
import "C"

// Any partition (for partitioning), or unspecified value (for all other cases)
const KAFKA_PARTITION_ANY = int32(C.RD_KAFKA_PARTITION_UA)

// Offset type (int64) with support for canonical names
type Offset int64

// Earliest offset (logical)
const KAFKA_OFFSET_BEGINNING = Offset(C.RD_KAFKA_OFFSET_BEGINNING)

// Latest offset (logical)
const KAFKA_OFFSET_END = Offset(C.RD_KAFKA_OFFSET_END)

// Invalid/unspecified offset
const KAFKA_OFFSET_INVALID = Offset(C.RD_KAFKA_OFFSET_INVALID)

// Use stored offset
const KAFKA_OFFSET_STORED = Offset(C.RD_KAFKA_OFFSET_STORED)

func (o Offset) String() string {
	switch o {
	case KAFKA_OFFSET_BEGINNING:
		return "beginning"
	case KAFKA_OFFSET_END:
		return "end"
	case KAFKA_OFFSET_INVALID:
		return "unset"
	case KAFKA_OFFSET_STORED:
		return "stored"
	default:
		return fmt.Sprintf("%d", int64(o))
	}
}

func (o Offset) Set(v string) error {
	n, err := NewOffset(v)

	if err == nil {
		o = n
	}

	return err
}

func NewOffset(offstr string) (Offset, error) {
	switch offstr {
	case "beginning":
		fallthrough
	case "earliest":
		return Offset(KAFKA_OFFSET_BEGINNING), nil

	case "end":
		fallthrough
	case "latest":
		return Offset(KAFKA_OFFSET_END), nil

	case "unset":
		fallthrough
	case "invalid":
		return Offset(KAFKA_OFFSET_INVALID), nil

	case "stored":
		return Offset(KAFKA_OFFSET_STORED), nil

	default:
		off, err := strconv.Atoi(offstr)
		return Offset(off), err
	}
}

// Logical offset relative_offset from current end of partition
func KAFKA_OFFSET_TAIL(relative_offset Offset) Offset {
	return Offset(C._c_rdkafka_offset_tail(C.int64_t(relative_offset)))
}

// TopicPartition is a generic placeholder for a Topic+Partition and optionally Offset.
type TopicPartition struct {
	Topic     *string
	Partition int32
	Offset    Offset
	Error     error
}

func (p TopicPartition) String() string {
	if p.Error != nil {
		return fmt.Sprintf("%s[%d]@%s(%s)",
			*p.Topic, p.Partition, p.Offset, p.Error)
	} else {
		return fmt.Sprintf("%s[%d]@%s",
			*p.Topic, p.Partition, p.Offset)
	}
}

// new_c_parts_from_TopicPartitions creates a new C rd_kafka_topic_partition_list_t
// from a TopicPartition array.
func new_c_parts_from_TopicPartitions(partitions []TopicPartition) (c_parts *C.rd_kafka_topic_partition_list_t) {
	c_parts = C.rd_kafka_topic_partition_list_new(C.int(len(partitions)))
	for _, part := range partitions {
		c_topic := C.CString(*part.Topic)
		defer C.free(unsafe.Pointer(c_topic))
		rktpar := C.rd_kafka_topic_partition_list_add(c_parts, c_topic, C.int32_t(part.Partition))
		rktpar.offset = C.int64_t(part.Offset)
	}

	return c_parts
}

func setup_TopicPartition_from_c_rktpar(partition *TopicPartition, c_rktpar *C.rd_kafka_topic_partition_t) {

	topic := C.GoString(c_rktpar.topic)
	partition.Topic = &topic
	partition.Partition = int32(c_rktpar.partition)
	partition.Offset = Offset(c_rktpar.offset)
	if c_rktpar.err != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		partition.Error = NewKafkaError(c_rktpar.err)
	}
}

func new_TopicPartitions_from_c_parts(c_parts *C.rd_kafka_topic_partition_list_t) (partitions []TopicPartition) {

	partcnt := int(c_parts.cnt)

	partitions = make([]TopicPartition, partcnt)
	for i := 0; i < partcnt; i += 1 {
		c_rktpar := C._c_rdkafka_topic_partition_list_entry(c_parts, C.int(i))
		setup_TopicPartition_from_c_rktpar(&partitions[i], c_rktpar)
	}

	return partitions
}
