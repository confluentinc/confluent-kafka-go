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
#include "select_rdkafka.h"
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

func (ao AdminOptionRequestTimeout) supportsCreateACLs() {
}

func (ao AdminOptionRequestTimeout) supportsDescribeACLs() {
}

func (ao AdminOptionRequestTimeout) supportsDeleteACLs() {
}

func (ao AdminOptionRequestTimeout) supportsListConsumerGroups() {
}
func (ao AdminOptionRequestTimeout) supportsDescribeConsumerGroups() {
}
func (ao AdminOptionRequestTimeout) supportsDeleteConsumerGroups() {
}
func (ao AdminOptionRequestTimeout) supportsListConsumerGroupOffsets() {
}
func (ao AdminOptionRequestTimeout) supportsAlterConsumerGroupOffsets() {
}
func (ao AdminOptionRequestTimeout) supportsDescribeUserScramCredentials() {
}
func (ao AdminOptionRequestTimeout) supportsAlterUserScramCredentials() {
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

// AdminOptionRequireStableOffsets decides if the broker should return stable
// offsets (transaction-committed).
//
// Default: false
//
// Valid for ListConsumerGroupOffsets.
type AdminOptionRequireStableOffsets struct {
	isSet bool
	val   bool
}

func (ao AdminOptionRequireStableOffsets) supportsListConsumerGroupOffsets() {
}

func (ao AdminOptionRequireStableOffsets) apply(cOptions *C.rd_kafka_AdminOptions_t) error {
	if !ao.isSet {
		return nil
	}

	cError := C.rd_kafka_AdminOptions_set_require_stable_offsets(
		cOptions, bool2cint(ao.val))
	if cError != nil {
		C.rd_kafka_AdminOptions_destroy(cOptions)
		return newErrorFromCErrorDestroy(cError)
	}

	return nil
}

// SetAdminRequireStableOffsets decides if the broker should return stable
// offsets (transaction-committed).
//
// Default: false
//
// Valid for ListConsumerGroupOffsets.
func SetAdminRequireStableOffsets(val bool) (ao AdminOptionRequireStableOffsets) {
	ao.isSet = true
	ao.val = val
	return ao
}

// AdminOptionMatchConsumerGroupStates decides groups in which state(s) should be
// listed.
//
// Default: nil (lists groups in all states).
//
// Valid for ListConsumerGroups.
type AdminOptionMatchConsumerGroupStates struct {
	isSet bool
	val   []ConsumerGroupState
}

func (ao AdminOptionMatchConsumerGroupStates) supportsListConsumerGroups() {
}

func (ao AdminOptionMatchConsumerGroupStates) apply(cOptions *C.rd_kafka_AdminOptions_t) error {
	if !ao.isSet || ao.val == nil {
		return nil
	}

	// Convert states from Go slice to C pointer.
	cStates := make([]C.rd_kafka_consumer_group_state_t, len(ao.val))
	cStatesCount := C.size_t(len(ao.val))

	for idx, state := range ao.val {
		cStates[idx] = C.rd_kafka_consumer_group_state_t(state)
	}

	cStatesPtr := ((*C.rd_kafka_consumer_group_state_t)(&cStates[0]))
	cError := C.rd_kafka_AdminOptions_set_match_consumer_group_states(
		cOptions, cStatesPtr, cStatesCount)
	if cError != nil {
		C.rd_kafka_AdminOptions_destroy(cOptions)
		return newErrorFromCErrorDestroy(cError)
	}

	return nil
}

// SetAdminMatchConsumerGroupStates decides groups in which state(s) should be
// listed.
//
// Default: nil (lists groups in all states).
//
// Valid for ListConsumerGroups.
func SetAdminMatchConsumerGroupStates(val []ConsumerGroupState) (ao AdminOptionMatchConsumerGroupStates) {
	ao.isSet = true
	ao.val = val
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

// CreateACLsAdminOption - see setter.
//
// See SetAdminRequestTimeout
type CreateACLsAdminOption interface {
	supportsCreateACLs()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// DescribeACLsAdminOption - see setter.
//
// See SetAdminRequestTimeout
type DescribeACLsAdminOption interface {
	supportsDescribeACLs()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// DeleteACLsAdminOption - see setter.
//
// See SetAdminRequestTimeout
type DeleteACLsAdminOption interface {
	supportsDeleteACLs()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// ListConsumerGroupsAdminOption - see setter.
//
// See SetAdminRequestTimeout, SetAdminMatchConsumerGroupStates.
type ListConsumerGroupsAdminOption interface {
	supportsListConsumerGroups()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// DescribeConsumerGroupsAdminOption - see setter.
//
// See SetAdminRequestTimeout.
type DescribeConsumerGroupsAdminOption interface {
	supportsDescribeConsumerGroups()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// DeleteConsumerGroupsAdminOption - see setters.
//
// See SetAdminRequestTimeout.
type DeleteConsumerGroupsAdminOption interface {
	supportsDeleteConsumerGroups()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// ListConsumerGroupOffsetsAdminOption - see setter.
//
// See SetAdminRequestTimeout, SetAdminRequireStableOffsets.
type ListConsumerGroupOffsetsAdminOption interface {
	supportsListConsumerGroupOffsets()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// AlterConsumerGroupOffsetsAdminOption - see setter.
//
// See SetAdminRequestTimeout.
type AlterConsumerGroupOffsetsAdminOption interface {
	supportsAlterConsumerGroupOffsets()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// DescribeUserScramCredentialsAdminOption - see setter.
//
// See SetAdminRequestTimeout.
type DescribeUserScramCredentialsAdminOption interface {
	supportsDescribeUserScramCredentials()
	apply(cOptions *C.rd_kafka_AdminOptions_t) error
}

// AlterUserScramCredentialsAdminOption - see setter.
//
// See SetAdminRequestTimeout.
type AlterUserScramCredentialsAdminOption interface {
	supportsAlterUserScramCredentials()
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
