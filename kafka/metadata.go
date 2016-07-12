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

package kafka

import ()

/*
#include <stdlib.h>
#include <librdkafka/rdkafka.h>

struct rd_kafka_metadata_broker *_get_metadata_broker_element(struct rd_kafka_metadata *m, int i) {
  return &m->brokers[i];
}

struct rd_kafka_metadata_topic *_get_metadata_topic_element(struct rd_kafka_metadata *m, int i) {
  return &m->topics[i];
}

struct rd_kafka_metadata_partition *_get_metadata_partition_element(struct rd_kafka_metadata *m, int topic_idx, int partition_idx) {
  return &m->topics[topic_idx].partitions[partition_idx];
}

int32_t _get_int32_element (int32_t *arr, int i) {
  return arr[i];
}

*/
import "C"

type BrokerMetadata struct {
	Id   int32
	Host string
	Port int
}

type PartitionMetadata struct {
	Id       int32
	Error    KafkaError
	Leader   int32
	Replicas []int32
	Isrs     []int32
}

type TopicMetadata struct {
	Topic      string
	Partitions []PartitionMetadata
	Error      KafkaError
}

type Metadata struct {
	Brokers []BrokerMetadata
	Topics  map[string]TopicMetadata

	OriginatingBroker BrokerMetadata
}

// GetMetadata queries broker for cluster and topic metadata.
// If topic is non-nil only information about that topic is returned, else if
// all_topics is false only information about locally used topics is returned,
// else information about all topics is returned.
// FIXME: Not sure where this function should go, or if it should be a method.
func GetMetadata(H Handle, topic *string, all_topics bool, timeout_ms int) (*Metadata, error) {
	h := H.get_handle()

	var rkt *C.rd_kafka_topic_t
	if topic != nil {
		rkt = h.get_rkt(*topic)
	}

	var c_md *C.struct_rd_kafka_metadata
	c_err := C.rd_kafka_metadata(h.rk, bool2cint(all_topics),
		rkt, &c_md, C.int(timeout_ms))
	if c_err != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil, NewKafkaError(c_err)
	}

	m := Metadata{}

	m.Brokers = make([]BrokerMetadata, c_md.broker_cnt)
	for i := 0; i < int(c_md.broker_cnt); i += 1 {
		b := C._get_metadata_broker_element(c_md, C.int(i))
		m.Brokers[i] = BrokerMetadata{int32(b.id), C.GoString(b.host),
			int(b.port)}
	}

	m.Topics = make(map[string]TopicMetadata, int(c_md.topic_cnt))
	for i := 0; i < int(c_md.topic_cnt); i += 1 {
		t := C._get_metadata_topic_element(c_md, C.int(i))

		this_topic := C.GoString(t.topic)
		m.Topics[this_topic] = TopicMetadata{Topic: this_topic,
			Error:      NewKafkaError(t.err),
			Partitions: make([]PartitionMetadata, int(t.partition_cnt))}

		for j := 0; j < int(t.partition_cnt); j += 1 {
			p := C._get_metadata_partition_element(c_md, C.int(i), C.int(j))
			m.Topics[this_topic].Partitions[j] = PartitionMetadata{
				Id:     int32(p.id),
				Error:  NewKafkaError(p.err),
				Leader: int32(p.leader)}
			m.Topics[this_topic].Partitions[j].Replicas = make([]int32, int(p.replica_cnt))
			for ir := 0; ir < int(p.replica_cnt); ir += 1 {
				m.Topics[this_topic].Partitions[j].Replicas[ir] = int32(C._get_int32_element(p.replicas, C.int(ir)))
			}

			m.Topics[this_topic].Partitions[j].Isrs = make([]int32, int(p.isr_cnt))
			for ii := 0; ii < int(p.isr_cnt); ii += 1 {
				m.Topics[this_topic].Partitions[j].Isrs[ii] = int32(C._get_int32_element(p.isrs, C.int(ii)))
			}
		}
	}

	m.OriginatingBroker = BrokerMetadata{int32(c_md.orig_broker_id),
		C.GoString(c_md.orig_broker_name), 0}

	return &m, nil
}
