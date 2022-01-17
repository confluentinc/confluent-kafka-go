/**
 * Copyright 2022 Confluent Inc.
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

import "C"
import "unsafe"

/*
#include <stdlib.h>
#include "select_rdkafka.h"
#include "glue_rdkafka.h"
*/
import "C"

type MockCluster struct {
	rk       *C.rd_kafka_t
	mcluster *C.struct_rd_kafka_mock_cluster_s
	cConf    *C.rd_kafka_conf_t
}

/*	NewMockCluster provides a mock Kafka cluster with a configurable
 *	number of brokers that support a reasonable subset of Kafka protocol
 *	operations, error injection, etc.
 *
 *	Mock clusters provide localhost listeners that can be used as the bootstrap
 *	servers by multiple rd_kafka_t instances.
 *
 *	Currently supported functionality:
 *	 - Producer
 *	 - Idempotent Producer
 *	 - Transactional Producer
 *	 - Low-level consumer
 *	 - High-level balanced consumer groups with offset commits
 *	 - Topic Metadata and auto creation
 *
 *	@remark This is an experimental public API that is NOT covered by the
 *	        librdkafka API or ABI stability guarantees.
 *
 *
 *	@warning THIS IS AN EXPERIMENTAL API, SUBJECT TO CHANGE OR REMOVAL.
 */
func NewMockCluster(brokerCount int) (*MockCluster, error) {

	mc := &MockCluster{}

	cErrstr := (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(cErrstr))

	mc.cConf = C.rd_kafka_conf_new()

	mc.rk = C.rd_kafka_new(C.RD_KAFKA_PRODUCER, mc.cConf, cErrstr, 256)
	if mc.rk == nil {
		C.rd_kafka_conf_destroy(mc.cConf)
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	mc.mcluster = C.rd_kafka_mock_cluster_new(mc.rk, C.int(brokerCount))
	if mc.mcluster == nil {
		C.rd_kafka_destroy(mc.rk)
		C.rd_kafka_conf_destroy(mc.cConf)
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	return mc, nil
}

// Bootstrapservers returns the bootstrap.servers property for this MockCluster
func (mc *MockCluster) Bootstrapservers() string {
	return C.GoString(C.rd_kafka_mock_cluster_bootstraps(mc.mcluster))
}

// Close and destroy the MockCluster
func (mc *MockCluster) Close() {
	C.rd_kafka_mock_cluster_destroy(mc.mcluster)
	C.rd_kafka_destroy(mc.rk)
	C.rd_kafka_conf_destroy(mc.cConf)
}
