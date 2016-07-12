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
	"unsafe"
)

/*
#include <stdlib.h>
#include <librdkafka/rdkafka.h>
*/
import "C"

// bool2int converts a bool to a C.int (1 or 0)
func bool2cint(b bool) C.int {
	if b {
		return 1
	} else {
		return 0
	}
}

// QueryWatermarkOffsets returns the broker's low and high offsets for the given topic
// and partition.
// FIXME: Not sure where this should go.
func QueryWatermarkOffsets(H Handle, topic string, partition int32, timeout_ms int) (low, high int64, err error) {
	h := H.get_handle()

	c_topic := C.CString(topic)
	defer C.free(unsafe.Pointer(c_topic))

	var c_low, c_high C.int64_t

	e := C.rd_kafka_query_watermark_offsets(h.rk, c_topic, C.int32_t(partition),
		&c_low, &c_high, C.int(timeout_ms))
	if e != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return 0, 0, NewKafkaError(e)
	}

	low = int64(c_low)
	high = int64(c_high)
	return low, high, nil
}
