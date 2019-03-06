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

import (
	"encoding/binary"
	"unsafe"
)

/*
#include <stdlib.h>
#include <librdkafka/rdkafka.h>

struct rd_kafka_metadata_broker *_getMetadata_broker_element(struct rd_kafka_metadata *m, int i) {
  return &m->brokers[i];
}

struct rd_kafka_metadata_topic *_getMetadata_topic_element(struct rd_kafka_metadata *m, int i) {
  return &m->topics[i];
}

struct rd_kafka_group_info *_getGrouplist_element(struct rd_kafka_group_list *g, int i) {
  return &g->groups[i];
}

struct rd_kafka_group_member_info *_getMember_element(struct rd_kafka_group_info *info, int i) {
  return &info->members[i];
}

struct rd_kafka_metadata_partition *_getMetadata_partition_element(struct rd_kafka_metadata *m, int topic_idx, int partition_idx) {
  return &m->topics[topic_idx].partitions[partition_idx];
}

int32_t _get_int32_element (int32_t *arr, int i) {
  return arr[i];
}

*/
import "C"

// BrokerMetadata contains per-broker metadata
type BrokerMetadata struct {
	ID   int32
	Host string
	Port int
}

// PartitionMetadata contains per-partition metadata
type PartitionMetadata struct {
	ID       int32
	Error    Error
	Leader   int32
	Replicas []int32
	Isrs     []int32
}

// TopicMetadata contains per-topic metadata
type TopicMetadata struct {
	Topic      string
	Partitions []PartitionMetadata
	Error      Error
}

// Metadata contains broker and topic metadata for all (matching) topics
type Metadata struct {
	Brokers           []BrokerMetadata
	Topics            map[string]TopicMetadata
	OriginatingBroker BrokerMetadata
}

// ConsumerGroupMetadata contains metadata per-consumer-group and all of its members
type ConsumerGroupMetadata struct {
	Group        string
	State        string
	Protocol     string
	ProtocolType string
	Members      []ConsumerGroupMember
}

// ConsumerGroupMember contains per-consumer-group-member info and metadata
type ConsumerGroupMember struct {
	MemberId       string
	ClientId       string
	ClientHost     string
	MemberMetadata *ConsumerGroupMemberMetadata
}

// ConsumerGroupMemberMetadata contains per-consumer-group-member metadata
type ConsumerGroupMemberMetadata struct {
	Version  int16
	Topics   []string
	UserData []byte
}

// getMetadata queries broker for cluster and topic metadata.
// If topic is non-nil only information about that topic is returned, else if
// allTopics is false only information about locally used topics is returned,
// else information about all topics is returned.
func getMetadata(H Handle, topic *string, allTopics bool, timeoutMs int) (*Metadata, error) {
	h := H.gethandle()

	var rkt *C.rd_kafka_topic_t
	if topic != nil {
		rkt = h.getRkt(*topic)
	}

	var cMd *C.struct_rd_kafka_metadata
	cErr := C.rd_kafka_metadata(h.rk, bool2cint(allTopics),
		rkt, &cMd, C.int(timeoutMs))
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil, newError(cErr)
	}

	m := Metadata{}
	defer C.rd_kafka_metadata_destroy(cMd)

	m.Brokers = make([]BrokerMetadata, cMd.broker_cnt)
	for i := 0; i < int(cMd.broker_cnt); i++ {
		b := C._getMetadata_broker_element(cMd, C.int(i))
		m.Brokers[i] = BrokerMetadata{int32(b.id), C.GoString(b.host),
			int(b.port)}
	}

	m.Topics = make(map[string]TopicMetadata, int(cMd.topic_cnt))
	for i := 0; i < int(cMd.topic_cnt); i++ {
		t := C._getMetadata_topic_element(cMd, C.int(i))

		thisTopic := C.GoString(t.topic)
		m.Topics[thisTopic] = TopicMetadata{Topic: thisTopic,
			Error:      newError(t.err),
			Partitions: make([]PartitionMetadata, int(t.partition_cnt))}

		for j := 0; j < int(t.partition_cnt); j++ {
			p := C._getMetadata_partition_element(cMd, C.int(i), C.int(j))
			m.Topics[thisTopic].Partitions[j] = PartitionMetadata{
				ID:     int32(p.id),
				Error:  newError(p.err),
				Leader: int32(p.leader)}
			m.Topics[thisTopic].Partitions[j].Replicas = make([]int32, int(p.replica_cnt))
			for ir := 0; ir < int(p.replica_cnt); ir++ {
				m.Topics[thisTopic].Partitions[j].Replicas[ir] = int32(C._get_int32_element(p.replicas, C.int(ir)))
			}

			m.Topics[thisTopic].Partitions[j].Isrs = make([]int32, int(p.isr_cnt))
			for ii := 0; ii < int(p.isr_cnt); ii++ {
				m.Topics[thisTopic].Partitions[j].Isrs[ii] = int32(C._get_int32_element(p.isrs, C.int(ii)))
			}
		}
	}

	m.OriginatingBroker = BrokerMetadata{int32(cMd.orig_broker_id),
		C.GoString(cMd.orig_broker_name), 0}

	return &m, nil
}

// queryWatermarkOffsets returns the broker's low and high offsets for the given topic
// and partition.
func queryWatermarkOffsets(H Handle, topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	h := H.gethandle()

	ctopic := C.CString(topic)
	defer C.free(unsafe.Pointer(ctopic))

	var cLow, cHigh C.int64_t

	e := C.rd_kafka_query_watermark_offsets(h.rk, ctopic, C.int32_t(partition),
		&cLow, &cHigh, C.int(timeoutMs))
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return 0, 0, newError(e)
	}

	low = int64(cLow)
	high = int64(cHigh)
	return low, high, nil
}

// getConsumerGroupsMetadata queries broker for consumer groups metadata and all its members
// like which consumer groups and which corresponding members consume which topics
// if consumerGroup is nil, metadata for all consumer groups will be returned
// otherwise only metadata for that specific consumer group will be returned
func getConsumerGroupsMetadata(H Handle, consumerGroup *string, timeoutMs int) ([]ConsumerGroupMetadata, error) {
	h := H.gethandle()
	var cGroupList *C.struct_rd_kafka_group_list
	var cErr C.rd_kafka_resp_err_t

	if consumerGroup == nil {
		cErr = C.rd_kafka_list_groups(h.rk, nil, &cGroupList, C.int(timeoutMs))
	} else {
		cErr = C.rd_kafka_list_groups(h.rk, C.CString(*consumerGroup), &cGroupList, C.int(timeoutMs))
	}
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil, newError(cErr)
	}

	defer C.rd_kafka_group_list_destroy(cGroupList)

	consumerGroupCnt := int(cGroupList.group_cnt)
	consumerGroupsMetadata := make([]ConsumerGroupMetadata, consumerGroupCnt)
	for i := 0; i < consumerGroupCnt; i++ {
		cGroup := C._getGrouplist_element(cGroupList, C.int(i))
		memberCnt := int(cGroup.member_cnt)
		members := make([]ConsumerGroupMember, memberCnt)

		for j := 0; j < memberCnt; j++ {
			cMember := C._getMember_element(cGroup, C.int(j))

			memberMetadataBytes := C.GoBytes(cMember.member_metadata, cMember.member_metadata_size)
			memberMetadata, err := newConsumerGroupMemberMetadataFromBytes(memberMetadataBytes)
			if err != nil {
				return nil, err
			}

			member := ConsumerGroupMember{
				MemberId:       C.GoString(cMember.member_id),
				ClientId:       C.GoString(cMember.client_id),
				ClientHost:     C.GoString(cMember.client_host),
				MemberMetadata: memberMetadata,
			}

			members[j] = member
		}

		consumerGroupsMetadata[i] = ConsumerGroupMetadata{
			Group:        C.GoString(cGroup.group),
			State:        C.GoString(cGroup.state),
			Protocol:     C.GoString(cGroup.protocol),
			ProtocolType: C.GoString(cGroup.protocol_type),
			Members:      members,
		}
	}

	return consumerGroupsMetadata, nil
}

//
func newConsumerGroupMemberMetadataFromBytes(b []byte) (*ConsumerGroupMemberMetadata, error) {
	offset := 0

	if len(b[offset:]) < 2 {
		return nil, newError(C.RD_KAFKA_RESP_ERR__UNDERFLOW)
	}
	version := int16(binary.BigEndian.Uint16(b[offset:]))
	offset += 2

	if len(b[offset:]) < 4 {
		return nil, newError(C.RD_KAFKA_RESP_ERR__UNDERFLOW)
	}
	topicArrayLength := int(binary.BigEndian.Uint32(b[offset:]))
	offset += 4

	topics := make([]string, topicArrayLength)
	for i := 0; i < topicArrayLength; i++ {
		if len(b[offset:]) < 2 {
			return nil, newError(C.RD_KAFKA_RESP_ERR__UNDERFLOW)
		}
		stringLength := int(binary.BigEndian.Uint16(b[offset:]))
		offset += 2

		if len(b[offset:]) < stringLength {
			return nil, newError(C.RD_KAFKA_RESP_ERR__UNDERFLOW)
		}
		topic := string(b[offset : offset+stringLength])
		offset += stringLength

		topics[i] = topic
	}

	metadata := ConsumerGroupMemberMetadata{
		Version:  version,
		Topics:   topics,
		UserData: b[offset:],
	}

	return &metadata, nil
}
