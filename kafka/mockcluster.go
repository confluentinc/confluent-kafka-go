/**
 * Copyright 2023 Confluent Inc.
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
	"time"
	"unsafe"
)

/*
#include <stdlib.h>
#include "select_rdkafka.h"
#include "glue_rdkafka.h"
*/
import "C"

// MockCluster represents a Kafka mock cluster instance which can be used
// for testing.
type MockCluster struct {
	rk       *C.rd_kafka_t
	mcluster *C.rd_kafka_mock_cluster_t
}

// NewMockCluster provides a mock Kafka cluster with a configurable
// number of brokers that support a reasonable subset of Kafka protocol
// operations, error injection, etc.
//
// The broker ids will start at 1 up to and including brokerCount.
//
// Mock clusters provide localhost listeners that can be used as the bootstrap
// servers by multiple Kafka client instances.
//
// Currently supported functionality:
// - Producer
// - Idempotent Producer
// - Transactional Producer
// - Low-level consumer
// - High-level balanced consumer groups with offset commits
// - Topic Metadata and auto creation
//
// Warning THIS IS AN EXPERIMENTAL API, SUBJECT TO CHANGE OR REMOVAL.
func NewMockCluster(brokerCount int) (*MockCluster, error) {

	mc := &MockCluster{}

	cErrstr := (*C.char)(C.malloc(C.size_t(512)))
	defer C.free(unsafe.Pointer(cErrstr))

	cConf := C.rd_kafka_conf_new()

	mc.rk = C.rd_kafka_new(C.RD_KAFKA_PRODUCER, cConf, cErrstr, 256)
	if mc.rk == nil {
		C.rd_kafka_conf_destroy(cConf)
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	mc.mcluster = C.rd_kafka_mock_cluster_new(mc.rk, C.int(brokerCount))
	if mc.mcluster == nil {
		C.rd_kafka_destroy(mc.rk)
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	return mc, nil
}

// BootstrapServers returns the bootstrap.servers property for this MockCluster
func (mc *MockCluster) BootstrapServers() string {
	return C.GoString(C.rd_kafka_mock_cluster_bootstraps(mc.mcluster))
}

// SetRoundtripDuration sets the broker round-trip-time delay for the given broker.
// Use brokerID -1 for all brokers, or >= 0 for a specific broker.
func (mc *MockCluster) SetRoundtripDuration(brokerID int, duration time.Duration) error {
	durationInMillis := C.int(duration.Milliseconds())
	cError := C.rd_kafka_mock_broker_set_rtt(mc.mcluster, C.int(brokerID), durationInMillis)
	if cError != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(cError)
	}
	return nil
}

// SetBrokerDown disconnects the broker and disallows any new connections.
// This does NOT trigger leader change.
// Use brokerID -1 for all brokers, or >= 0 for a specific broker.
func (mc *MockCluster) SetBrokerDown(brokerID int) error {
	cError := C.rd_kafka_mock_broker_set_down(mc.mcluster, C.int(brokerID))
	if cError != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(cError)
	}
	return nil
}

// SetBrokerUp makes the broker accept connections again.
// This does NOT trigger leader change.
// Use brokerID -1 for all brokers, or >= 0 for a specific broker.
func (mc *MockCluster) SetBrokerUp(brokerID int) error {
	cError := C.rd_kafka_mock_broker_set_up(mc.mcluster, C.int(brokerID))
	if cError != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(cError)
	}
	return nil
}

// CreateTopic creates a topic without having to use a producer
func (mc *MockCluster) CreateTopic(topic string, partitions, replicationFactor int) error {
	topicStr := C.CString(topic)
	defer C.free(unsafe.Pointer(topicStr))

	cError := C.rd_kafka_mock_topic_create(mc.mcluster, topicStr, C.int(partitions), C.int(replicationFactor))
	if cError != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(cError)
	}
	return nil
}

// Close and destroy the MockCluster
func (mc *MockCluster) Close() {
	C.rd_kafka_mock_cluster_destroy(mc.mcluster)
	C.rd_kafka_destroy(mc.rk)
}
