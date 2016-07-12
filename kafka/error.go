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

// Automatically generate error codes from librdkafka
// See README for instructions
//go:generate $GOPATH/bin/go_rdkafka_generr generated_errors.go

/*
#include <librdkafka/rdkafka.h>
*/
import "C"

type KafkaError struct {
	code KafkaErrorCode
	str  string
}

func NewKafkaError(code C.rd_kafka_resp_err_t) (err KafkaError) {
	return KafkaError{KafkaErrorCode(code), ""}
}

func NewKafkaErrorFromCString(cstr *C.char) (err KafkaError) {
	return KafkaError{ERR__FAIL, C.GoString(cstr)}
}

func (e KafkaError) Error() string {
	return e.String()
}

func (e KafkaError) String() string {
	if len(e.str) > 0 {
		return e.str
	} else {
		return e.code.String()
	}
}

func (e KafkaError) Code() KafkaErrorCode {
	return e.code
}
