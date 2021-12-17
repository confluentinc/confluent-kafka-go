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
	handle handle
	mcluster *C.struct_rd_kafka_mock_cluster_s
}

func NewMockCluster(count int) (*MockCluster, error) {

	mc := &MockCluster{}

	cErrstr := (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(cErrstr))

	cConf := C.rd_kafka_conf_new();

	//https://github.com/edenhill/librdkafka/issues/2693

	mc.handle.rk = C.rd_kafka_new(C.RD_KAFKA_PRODUCER, cConf, cErrstr, 256)
	if mc.handle.rk == nil {
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	mc.mcluster = C.rd_kafka_mock_cluster_new(mc.handle.rk, C.int(count))
	if mc.mcluster == nil {
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	return mc, nil
}

func (mc *MockCluster) Bootstrapservers() string {
	return C.GoString(C.rd_kafka_mock_cluster_bootstraps(mc.mcluster))
}

func (mc *MockCluster) Close() {
	C.rd_kafka_mock_cluster_destroy(mc.mcluster);
	C.rd_kafka_destroy(mc.handle.rk);
}