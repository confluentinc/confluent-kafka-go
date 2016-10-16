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

// Package kafka provides high-level Apache Kafka producer and consumers
// using bindings on-top of the librdkafka C library.
//
// Hint: If your application registers a signal notification
// (signal.Notify) makes sure the signals channel is buffered to avoid
// possible complications with blocking Poll() calls.
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
const PartitionAny = int32(C.RD_KAFKA_PARTITION_UA)

// Offset type (int64) with support for canonical names
type Offset int64

// Earliest offset (logical)
const OffsetBeginning = Offset(C.RD_KAFKA_OFFSET_BEGINNING)

// Latest offset (logical)
const OffsetEnd = Offset(C.RD_KAFKA_OFFSET_END)

// Invalid/unspecified offset
const OffsetInvalid = Offset(C.RD_KAFKA_OFFSET_INVALID)

// Use stored offset
const OffsetStored = Offset(C.RD_KAFKA_OFFSET_STORED)

func (o Offset) String() string {
	switch o {
	case OffsetBeginning:
		return "beginning"
	case OffsetEnd:
		return "end"
	case OffsetInvalid:
		return "unset"
	case OffsetStored:
		return "stored"
	default:
		return fmt.Sprintf("%d", int64(o))
	}
}

// Set offset value, see NewOffset()
func (o Offset) Set(offset interface{}) error {
	n, err := NewOffset(offset)

	if err == nil {
		o = n
	}

	return err
}

// NewOffset creates a new Offset using the provided logical string, or an
// absolute int64 offset value.
// Logical offsets: "beginning", "earliest", "end", "latest", "unset", "invalid", "stored"
func NewOffset(offset interface{}) (Offset, error) {

	switch v := offset.(type) {
	case string:
		switch v {
		case "beginning":
			fallthrough
		case "earliest":
			return Offset(OffsetBeginning), nil

		case "end":
			fallthrough
		case "latest":
			return Offset(OffsetEnd), nil

		case "unset":
			fallthrough
		case "invalid":
			return Offset(OffsetInvalid), nil

		case "stored":
			return Offset(OffsetStored), nil

		default:
			off, err := strconv.Atoi(v)
			return Offset(off), err
		}

	case int:
		return Offset((int64)(v)), nil
	case int64:
		return Offset(v), nil
	default:
		return OffsetInvalid, newErrorFromString(ErrInvalidArg,
			fmt.Sprintf("Invalid offset type: %t", v))
	}
}

// OffsetTail returns the logical offset relativeOffset from current end of partition
func OffsetTail(relativeOffset Offset) Offset {
	return Offset(C._c_rdkafka_offset_tail(C.int64_t(relativeOffset)))
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
	}
	return fmt.Sprintf("%s[%d]@%s",
		*p.Topic, p.Partition, p.Offset)
}

// new_cparts_from_TopicPartitions creates a new C rd_kafka_topic_partition_list_t
// from a TopicPartition array.
func newCPartsFromTopicPartitions(partitions []TopicPartition) (cparts *C.rd_kafka_topic_partition_list_t) {
	cparts = C.rd_kafka_topic_partition_list_new(C.int(len(partitions)))
	for _, part := range partitions {
		ctopic := C.CString(*part.Topic)
		defer C.free(unsafe.Pointer(ctopic))
		rktpar := C.rd_kafka_topic_partition_list_add(cparts, ctopic, C.int32_t(part.Partition))
		rktpar.offset = C.int64_t(part.Offset)
	}

	return cparts
}

func setupTopicPartitionFromCrktpar(partition *TopicPartition, crktpar *C.rd_kafka_topic_partition_t) {

	topic := C.GoString(crktpar.topic)
	partition.Topic = &topic
	partition.Partition = int32(crktpar.partition)
	partition.Offset = Offset(crktpar.offset)
	if crktpar.err != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		partition.Error = newError(crktpar.err)
	}
}

func newTopicPartitionsFromCparts(cparts *C.rd_kafka_topic_partition_list_t) (partitions []TopicPartition) {

	partcnt := int(cparts.cnt)

	partitions = make([]TopicPartition, partcnt)
	for i := 0; i < partcnt; i++ {
		crktpar := C._c_rdkafka_topic_partition_list_entry(cparts, C.int(i))
		setupTopicPartitionFromCrktpar(&partitions[i], crktpar)
	}

	return partitions
}

// LibraryVersion returns the underlying librdkafka library version as a
// (version_int, version_str) tuple.
func LibraryVersion() (int, string) {
	ver := (int)(C.rd_kafka_version())
	verstr := C.GoString(C.rd_kafka_version_str())
	return ver, verstr
}
