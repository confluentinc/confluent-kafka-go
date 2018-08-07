/**
 * Copyright 2018 Confluent Inc.
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
	"fmt"
	"time"
	"unsafe"
)

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
*/
import "C"

// AdminOptionOperationTimeout sets the broker's operation timeout, such as the
// timeout for CreateTopics to complete the creation of topics on the controller
// before returning a result to the application.
//
// CreateTopics, DeleteTopics, CreatePartitions:
// a value 0 will return immediately after triggering topic
// creation, while > 0 will wait this long for topic creation to propagate
// in cluster.
//
// Default: 0 (return immediately).
//
// Valid for CreateTopics, DeleteTopics, CreatePartitions.
type AdminOptionOperationTimeout struct {
	isSet bool
	val   time.Duration
}

func (ao AdminOptionOperationTimeout) supportsCreateTopics() {
}
func (ao AdminOptionOperationTimeout) supportsDeleteTopics() {
}
func (ao AdminOptionOperationTimeout) supportsCreatePartitions() {
}

func (ao AdminOptionOperationTimeout) apply(cOptions *C.rd_kafka_AdminOptions_t) error {
	if !ao.isSet {
		return nil
	}

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	cErr := C.rd_kafka_AdminOptions_set_operation_timeout(
		cOptions, C.int(durationToMilliseconds(ao.val)),
		cErrstr, cErrstrSize)
	if cErr != 0 {
		C.rd_kafka_AdminOptions_destroy(cOptions)
		return newCErrorFromString(cErr,
			fmt.Sprintf("Failed to set operation timeout: %s", C.GoString(cErrstr)))

	}

	return nil
}

// SetAdminOperationTimeout sets the broker's operation timeout, such as the
// timeout for CreateTopics to complete the creation of topics on the controller
// before returning a result to the application.
//
// CreateTopics, DeleteTopics, CreatePartitions:
// a value 0 will return immediately after triggering topic
// creation, while > 0 will wait this long for topic creation to propagate
// in cluster.
//
// Default: 0 (return immediately).
//
// Valid for CreateTopics, DeleteTopics, CreatePartitions.
func SetAdminOperationTimeout(t time.Duration) (ao AdminOptionOperationTimeout) {
	ao.isSet = true
	ao.val = t
	return ao
}

// AdminOptionRequestTimeout sets the overall request timeout, including broker
// lookup, request transmission, operation time on broker, and response.
//
// Default: `socket.timeout.ms`.
//
// Valid for all Admin API methods.
type AdminOptionRequestTimeout struct {
	isSet bool
	val   time.Duration
}

func (ao AdminOptionRequestTimeout) supportsCreateTopics() {
}
func (ao AdminOptionRequestTimeout) supportsDeleteTopics() {
}
func (ao AdminOptionRequestTimeout) supportsCreatePartitions() {
}
func (ao AdminOptionRequestTimeout) supportsAlterConfigs() {
}
func (ao AdminOptionRequestTimeout) supportsDescribeConfigs() {
}

func (ao AdminOptionRequestTimeout) apply(cOptions *C.rd_kafka_AdminOptions_t) error {
	if !ao.isSet {
		return nil
	}

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	cErr := C.rd_kafka_AdminOptions_set_request_timeout(
		cOptions, C.int(durationToMilliseconds(ao.val)),
		cErrstr, cErrstrSize)
	if cErr != 0 {
		C.rd_kafka_AdminOptions_destroy(cOptions)
		return newCErrorFromString(cErr,
			fmt.Sprintf("%s", C.GoString(cErrstr)))

	}

	return nil
}

// SetAdminRequestTimeout sets the overall request timeout, including broker
// lookup, request transmission, operation time on broker, and response.
//
// Default: `socket.timeout.ms`.
//
// Valid for all Admin API methods.
func SetAdminRequestTimeout(t time.Duration) (ao AdminOptionRequestTimeout) {
	ao.isSet = true
	ao.val = t
	return ao
}

// AdminOptionValidateOnly tells the broker to only validate the request,
// without performing the requested operation (create topics, etc).
//
// Default: false.
//
// Valid for CreateTopics, CreatePartitions, AlterConfigs
type AdminOptionValidateOnly struct {
	isSet bool
	val   bool
}

func (ao AdminOptionValidateOnly) supportsCreateTopics() {
}
func (ao AdminOptionValidateOnly) supportsCreatePartitions() {
}
func (ao AdminOptionValidateOnly) supportsAlterConfigs() {
}

func (ao AdminOptionValidateOnly) apply(cOptions *C.rd_kafka_AdminOptions_t) error {
	if !ao.isSet {
		return nil
	}

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	cErr := C.rd_kafka_AdminOptions_set_validate_only(
		cOptions, bool2cint(ao.val),
		cErrstr, cErrstrSize)
	if cErr != 0 {
		C.rd_kafka_AdminOptions_destroy(cOptions)
		return newCErrorFromString(cErr,
			fmt.Sprintf("%s", C.GoString(cErrstr)))

	}

	return nil
}

// SetAdminValidateOnly tells the broker to only validate the request,
// without performing the requested operation (create topics, etc).
//
// Default: false.
//
// Valid for CreateTopics, DeleteTopics, CreatePartitions, AlterConfigs
func SetAdminValidateOnly(validateOnly bool) (ao AdminOptionValidateOnly) {
	ao.isSet = true
	ao.val = validateOnly
	return ao
}

// CreateTopicsAdminOption - see setters.
//
// See SetAdminRequestTimeout, SetAdminOperationTimeout, SetAdminValidateOnly.
type CreateTopicsAdminOption interface {
	supportsCreateTopics()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// DeleteTopicsAdminOption - see setters.
//
// See SetAdminRequestTimeout, SetAdminOperationTimeout.
type DeleteTopicsAdminOption interface {
	supportsDeleteTopics()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// CreatePartitionsAdminOption - see setters.
//
// See SetAdminRequestTimeout, SetAdminOperationTimeout, SetAdminValidateOnly.
type CreatePartitionsAdminOption interface {
	supportsCreatePartitions()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// AlterConfigsAdminOption - see setters.
//
// See SetAdminRequestTimeout, SetAdminValidateOnly, SetAdminIncremental.
type AlterConfigsAdminOption interface {
	supportsAlterConfigs()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// DescribeConfigsAdminOption - see setters.
//
// See SetAdminRequestTimeout.
type DescribeConfigsAdminOption interface {
	supportsDescribeConfigs()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// AdminOption is a generic type not to be used directly.
//
// See CreateTopicsAdminOption et.al.
type AdminOption interface {
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

func adminOptionsSetup(h *handle, opType C.rd_kafka_admin_op_t, options []AdminOption) (*C.rd_kafka_AdminOptions_t, error) {

	cOptions := C.rd_kafka_AdminOptions_new(h.rk, opType)
	for _, opt := range options {
		if opt == nil {
			continue
		}
		err := opt.apply(cOptions)
		if err != nil {
			return nil, err
		}
	}

	return cOptions, nil
}
