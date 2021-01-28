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
	"context"
	"fmt"
	"strings"
	"time"
	"unsafe"
)

/*
#include "select_rdkafka.h"
#include <stdlib.h>

static const rd_kafka_topic_result_t *
topic_result_by_idx (const rd_kafka_topic_result_t **topics, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return topics[idx];
}

static const rd_kafka_ConfigResource_t *
ConfigResource_by_idx (const rd_kafka_ConfigResource_t **res, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return res[idx];
}

static const rd_kafka_ConfigEntry_t *
ConfigEntry_by_idx (const rd_kafka_ConfigEntry_t **entries, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return entries[idx];
}
*/
import "C"

// AdminClient is derived from an existing Producer or Consumer
type AdminClient struct {
	handle    *handle
	isDerived bool // Derived from existing client handle
}

func durationToMilliseconds(t time.Duration) int {
	if t > 0 {
		return (int)(t.Seconds() * 1000.0)
	}
	return (int)(t)
}

// TopicResult provides per-topic operation result (error) information.
type TopicResult struct {
	// Topic name
	Topic string
	// Error, if any, of result. Check with `Error.Code() != ErrNoError`.
	Error Error
}

// String returns a human-readable representation of a TopicResult.
func (t TopicResult) String() string {
	if t.Error.code == 0 {
		return t.Topic
	}
	return fmt.Sprintf("%s (%s)", t.Topic, t.Error.str)
}

// TopicSpecification holds parameters for creating a new topic.
// TopicSpecification is analogous to NewTopic in the Java Topic Admin API.
type TopicSpecification struct {
	// Topic name to create.
	Topic string
	// Number of partitions in topic.
	NumPartitions int
	// Default replication factor for the topic's partitions, or zero
	// if an explicit ReplicaAssignment is set.
	ReplicationFactor int
	// (Optional) Explicit replica assignment. The outer array is
	// indexed by the partition number, while the inner per-partition array
	// contains the replica broker ids. The first broker in each
	// broker id list will be the preferred replica.
	ReplicaAssignment [][]int32
	// Topic configuration.
	Config map[string]string
}

// PartitionsSpecification holds parameters for creating additional partitions for a topic.
// PartitionsSpecification is analogous to NewPartitions in the Java Topic Admin API.
type PartitionsSpecification struct {
	// Topic to create more partitions for.
	Topic string
	// New partition count for topic, must be higher than current partition count.
	IncreaseTo int
	// (Optional) Explicit replica assignment. The outer array is
	// indexed by the new partition index (i.e., 0 for the first added
	// partition), while the inner per-partition array
	// contains the replica broker ids. The first broker in each
	// broker id list will be the preferred replica.
	ReplicaAssignment [][]int32
}

// ResourceType represents an Apache Kafka resource type
type ResourceType int

const (
	// ResourceUnknown - Unknown
	ResourceUnknown = ResourceType(C.RD_KAFKA_RESOURCE_UNKNOWN)
	// ResourceAny - match any resource type (DescribeConfigs)
	ResourceAny = ResourceType(C.RD_KAFKA_RESOURCE_ANY)
	// ResourceTopic - Topic
	ResourceTopic = ResourceType(C.RD_KAFKA_RESOURCE_TOPIC)
	// ResourceGroup - Group
	ResourceGroup = ResourceType(C.RD_KAFKA_RESOURCE_GROUP)
	// ResourceBroker - Broker
	ResourceBroker = ResourceType(C.RD_KAFKA_RESOURCE_BROKER)
)

// String returns the human-readable representation of a ResourceType
func (t ResourceType) String() string {
	return C.GoString(C.rd_kafka_ResourceType_name(C.rd_kafka_ResourceType_t(t)))
}

// ResourceTypeFromString translates a resource type name/string to
// a ResourceType value.
func ResourceTypeFromString(typeString string) (ResourceType, error) {
	switch strings.ToUpper(typeString) {
	case "ANY":
		return ResourceAny, nil
	case "TOPIC":
		return ResourceTopic, nil
	case "GROUP":
		return ResourceGroup, nil
	case "BROKER":
		return ResourceBroker, nil
	default:
		return ResourceUnknown, NewError(ErrInvalidArg, "Unknown resource type", false)
	}
}

// ConfigSource represents an Apache Kafka config source
type ConfigSource int

const (
	// ConfigSourceUnknown is the default value
	ConfigSourceUnknown = ConfigSource(C.RD_KAFKA_CONFIG_SOURCE_UNKNOWN_CONFIG)
	// ConfigSourceDynamicTopic is dynamic topic config that is configured for a specific topic
	ConfigSourceDynamicTopic = ConfigSource(C.RD_KAFKA_CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG)
	// ConfigSourceDynamicBroker is dynamic broker config that is configured for a specific broker
	ConfigSourceDynamicBroker = ConfigSource(C.RD_KAFKA_CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG)
	// ConfigSourceDynamicDefaultBroker is dynamic broker config that is configured as default for all brokers in the cluster
	ConfigSourceDynamicDefaultBroker = ConfigSource(C.RD_KAFKA_CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG)
	// ConfigSourceStaticBroker is static broker config provided as broker properties at startup (e.g. from server.properties file)
	ConfigSourceStaticBroker = ConfigSource(C.RD_KAFKA_CONFIG_SOURCE_STATIC_BROKER_CONFIG)
	// ConfigSourceDefault is built-in default configuration for configs that have a default value
	ConfigSourceDefault = ConfigSource(C.RD_KAFKA_CONFIG_SOURCE_DEFAULT_CONFIG)
)

// String returns the human-readable representation of a ConfigSource type
func (t ConfigSource) String() string {
	return C.GoString(C.rd_kafka_ConfigSource_name(C.rd_kafka_ConfigSource_t(t)))
}

// ConfigResource holds parameters for altering an Apache Kafka configuration resource
type ConfigResource struct {
	// Type of resource to set.
	Type ResourceType
	// Name of resource to set.
	Name string
	// Config entries to set.
	// Configuration updates are atomic, any configuration property not provided
	// here will be reverted (by the broker) to its default value.
	// Use DescribeConfigs to retrieve the list of current configuration entry values.
	Config []ConfigEntry
}

// String returns a human-readable representation of a ConfigResource
func (c ConfigResource) String() string {
	return fmt.Sprintf("Resource(%s, %s)", c.Type, c.Name)
}

// AlterOperation specifies the operation to perform on the ConfigEntry.
// Currently only AlterOperationSet.
type AlterOperation int

const (
	// AlterOperationSet sets/overwrites the configuration setting.
	AlterOperationSet = iota
)

// String returns the human-readable representation of an AlterOperation
func (o AlterOperation) String() string {
	switch o {
	case AlterOperationSet:
		return "Set"
	default:
		return fmt.Sprintf("Unknown%d?", int(o))
	}
}

// ConfigEntry holds parameters for altering a resource's configuration.
type ConfigEntry struct {
	// Name of configuration entry, e.g., topic configuration property name.
	Name string
	// Value of configuration entry.
	Value string
	// Operation to perform on the entry.
	Operation AlterOperation
}

// StringMapToConfigEntries creates a new map of ConfigEntry objects from the
// provided string map. The AlterOperation is set on each created entry.
func StringMapToConfigEntries(stringMap map[string]string, operation AlterOperation) []ConfigEntry {
	var ceList []ConfigEntry

	for k, v := range stringMap {
		ceList = append(ceList, ConfigEntry{Name: k, Value: v, Operation: operation})
	}

	return ceList
}

// String returns a human-readable representation of a ConfigEntry.
func (c ConfigEntry) String() string {
	return fmt.Sprintf("%v %s=\"%s\"", c.Operation, c.Name, c.Value)
}

// ConfigEntryResult contains the result of a single configuration entry from a
// DescribeConfigs request.
type ConfigEntryResult struct {
	// Name of configuration entry, e.g., topic configuration property name.
	Name string
	// Value of configuration entry.
	Value string
	// Source indicates the configuration source.
	Source ConfigSource
	// IsReadOnly indicates whether the configuration entry can be altered.
	IsReadOnly bool
	// IsSensitive indicates whether the configuration entry contains sensitive information, in which case the value will be unset.
	IsSensitive bool
	// IsSynonym indicates whether the configuration entry is a synonym for another configuration property.
	IsSynonym bool
	// Synonyms contains a map of configuration entries that are synonyms to this configuration entry.
	Synonyms map[string]ConfigEntryResult
}

// String returns a human-readable representation of a ConfigEntryResult.
func (c ConfigEntryResult) String() string {
	return fmt.Sprintf("%s=\"%s\"", c.Name, c.Value)
}

// setFromC sets up a ConfigEntryResult from a C ConfigEntry
func configEntryResultFromC(cEntry *C.rd_kafka_ConfigEntry_t) (entry ConfigEntryResult) {
	entry.Name = C.GoString(C.rd_kafka_ConfigEntry_name(cEntry))
	cValue := C.rd_kafka_ConfigEntry_value(cEntry)
	if cValue != nil {
		entry.Value = C.GoString(cValue)
	}
	entry.Source = ConfigSource(C.rd_kafka_ConfigEntry_source(cEntry))
	entry.IsReadOnly = cint2bool(C.rd_kafka_ConfigEntry_is_read_only(cEntry))
	entry.IsSensitive = cint2bool(C.rd_kafka_ConfigEntry_is_sensitive(cEntry))
	entry.IsSynonym = cint2bool(C.rd_kafka_ConfigEntry_is_synonym(cEntry))

	var cSynCnt C.size_t
	cSyns := C.rd_kafka_ConfigEntry_synonyms(cEntry, &cSynCnt)
	if cSynCnt > 0 {
		entry.Synonyms = make(map[string]ConfigEntryResult)
	}

	for si := 0; si < int(cSynCnt); si++ {
		cSyn := C.ConfigEntry_by_idx(cSyns, cSynCnt, C.size_t(si))
		Syn := configEntryResultFromC(cSyn)
		entry.Synonyms[Syn.Name] = Syn
	}

	return entry
}

// ConfigResourceResult provides the result for a resource from a AlterConfigs or
// DescribeConfigs request.
type ConfigResourceResult struct {
	// Type of returned result resource.
	Type ResourceType
	// Name of returned result resource.
	Name string
	// Error, if any, of returned result resource.
	Error Error
	// Config entries, if any, of returned result resource.
	Config map[string]ConfigEntryResult
}

// String returns a human-readable representation of a ConfigResourceResult.
func (c ConfigResourceResult) String() string {
	if c.Error.Code() != 0 {
		return fmt.Sprintf("ResourceResult(%s, %s, \"%v\")", c.Type, c.Name, c.Error)

	}
	return fmt.Sprintf("ResourceResult(%s, %s, %d config(s))", c.Type, c.Name, len(c.Config))
}

// waitResult waits for a result event on cQueue or the ctx to be cancelled, whichever happens
// first.
// The returned result event is checked for errors its error is returned if set.
func (a *AdminClient) waitResult(ctx context.Context, cQueue *C.rd_kafka_queue_t, cEventType C.rd_kafka_event_type_t) (rkev *C.rd_kafka_event_t, err error) {

	resultChan := make(chan *C.rd_kafka_event_t)
	closeChan := make(chan bool) // never written to, just closed

	go func() {
		for {
			select {
			case _, ok := <-closeChan:
				if !ok {
					// Context cancelled/timed out
					close(resultChan)
					return
				}

			default:
				// Wait for result event for at most 50ms
				// to avoid blocking for too long if
				// context is cancelled.
				rkev := C.rd_kafka_queue_poll(cQueue, 50)
				if rkev != nil {
					resultChan <- rkev
					close(resultChan)
					return
				}
			}
		}
	}()

	select {
	case rkev = <-resultChan:
		// Result type check
		if cEventType != C.rd_kafka_event_type(rkev) {
			err = newErrorFromString(ErrInvalidType,
				fmt.Sprintf("Expected %d result event, not %d", (int)(cEventType), (int)(C.rd_kafka_event_type(rkev))))
			C.rd_kafka_event_destroy(rkev)
			return nil, err
		}

		// Generic error handling
		cErr := C.rd_kafka_event_error(rkev)
		if cErr != 0 {
			err = newErrorFromCString(cErr, C.rd_kafka_event_error_string(rkev))
			C.rd_kafka_event_destroy(rkev)
			return nil, err
		}
		close(closeChan)
		return rkev, nil
	case <-ctx.Done():
		// signal close to go-routine
		close(closeChan)
		// wait for close from go-routine to make sure it is done
		// using cQueue before we return.
		rkev, ok := <-resultChan
		if ok {
			// throw away result since context was cancelled
			C.rd_kafka_event_destroy(rkev)
		}
		return nil, ctx.Err()
	}
}

// cToTopicResults converts a C topic_result_t array to Go TopicResult list.
func (a *AdminClient) cToTopicResults(cTopicRes **C.rd_kafka_topic_result_t, cCnt C.size_t) (result []TopicResult, err error) {

	result = make([]TopicResult, int(cCnt))

	for i := 0; i < int(cCnt); i++ {
		cTopic := C.topic_result_by_idx(cTopicRes, cCnt, C.size_t(i))
		result[i].Topic = C.GoString(C.rd_kafka_topic_result_name(cTopic))
		result[i].Error = newErrorFromCString(
			C.rd_kafka_topic_result_error(cTopic),
			C.rd_kafka_topic_result_error_string(cTopic))
	}

	return result, nil
}

// cConfigResourceToResult converts a C ConfigResource result array to Go ConfigResourceResult
func (a *AdminClient) cConfigResourceToResult(cRes **C.rd_kafka_ConfigResource_t, cCnt C.size_t) (result []ConfigResourceResult, err error) {

	result = make([]ConfigResourceResult, int(cCnt))

	for i := 0; i < int(cCnt); i++ {
		cRes := C.ConfigResource_by_idx(cRes, cCnt, C.size_t(i))
		result[i].Type = ResourceType(C.rd_kafka_ConfigResource_type(cRes))
		result[i].Name = C.GoString(C.rd_kafka_ConfigResource_name(cRes))
		result[i].Error = newErrorFromCString(
			C.rd_kafka_ConfigResource_error(cRes),
			C.rd_kafka_ConfigResource_error_string(cRes))
		var cConfigCnt C.size_t
		cConfigs := C.rd_kafka_ConfigResource_configs(cRes, &cConfigCnt)
		if cConfigCnt > 0 {
			result[i].Config = make(map[string]ConfigEntryResult)
		}
		for ci := 0; ci < int(cConfigCnt); ci++ {
			cEntry := C.ConfigEntry_by_idx(cConfigs, cConfigCnt, C.size_t(ci))
			entry := configEntryResultFromC(cEntry)
			result[i].Config[entry.Name] = entry
		}
	}

	return result, nil
}

// ClusterID returns the cluster ID as reported in broker metadata.
//
// Note on cancellation: Although the underlying C function respects the
// timeout, it currently cannot be manually cancelled. That means manually
// cancelling the context will block until the C function call returns.
//
// Requires broker version >= 0.10.0.
func (a *AdminClient) ClusterID(ctx context.Context) (clusterID string, err error) {
	responseChan := make(chan *C.char, 1)

	go func() {
		responseChan <- C.rd_kafka_clusterid(a.handle.rk, cTimeoutFromContext(ctx))
	}()

	select {
	case <-ctx.Done():
		if cClusterID := <-responseChan; cClusterID != nil {
			C.rd_kafka_mem_free(a.handle.rk, unsafe.Pointer(cClusterID))
		}
		return "", ctx.Err()

	case cClusterID := <-responseChan:
		if cClusterID == nil { // C timeout
			<-ctx.Done()
			return "", ctx.Err()
		}
		defer C.rd_kafka_mem_free(a.handle.rk, unsafe.Pointer(cClusterID))
		return C.GoString(cClusterID), nil
	}
}

// ControllerID returns the broker ID of the current controller as reported in
// broker metadata.
//
// Note on cancellation: Although the underlying C function respects the
// timeout, it currently cannot be manually cancelled. That means manually
// cancelling the context will block until the C function call returns.
//
// Requires broker version >= 0.10.0.
func (a *AdminClient) ControllerID(ctx context.Context) (controllerID int32, err error) {
	responseChan := make(chan int32, 1)

	go func() {
		responseChan <- int32(C.rd_kafka_controllerid(a.handle.rk, cTimeoutFromContext(ctx)))
	}()

	select {
	case <-ctx.Done():
		<-responseChan
		return 0, ctx.Err()

	case controllerID := <-responseChan:
		if controllerID < 0 { // C timeout
			<-ctx.Done()
			return 0, ctx.Err()
		}
		return controllerID, nil
	}
}

// CreateTopics creates topics in cluster.
//
// The list of TopicSpecification objects define the per-topic partition count, replicas, etc.
//
// Topic creation is non-atomic and may succeed for some topics but fail for others,
// make sure to check the result for topic-specific errors.
//
// Note: TopicSpecification is analogous to NewTopic in the Java Topic Admin API.
func (a *AdminClient) CreateTopics(ctx context.Context, topics []TopicSpecification, options ...CreateTopicsAdminOption) (result []TopicResult, err error) {
	cTopics := make([]*C.rd_kafka_NewTopic_t, len(topics))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go TopicSpecifications to C TopicSpecifications
	for i, topic := range topics {

		var cReplicationFactor C.int
		if topic.ReplicationFactor == 0 {
			cReplicationFactor = -1
		} else {
			cReplicationFactor = C.int(topic.ReplicationFactor)
		}
		if topic.ReplicaAssignment != nil {
			if cReplicationFactor != -1 {
				return nil, newErrorFromString(ErrInvalidArg,
					"TopicSpecification.ReplicationFactor and TopicSpecification.ReplicaAssignment are mutually exclusive")
			}

			if len(topic.ReplicaAssignment) != topic.NumPartitions {
				return nil, newErrorFromString(ErrInvalidArg,
					"TopicSpecification.ReplicaAssignment must contain exactly TopicSpecification.NumPartitions partitions")
			}

		} else if cReplicationFactor == -1 {
			return nil, newErrorFromString(ErrInvalidArg,
				"TopicSpecification.ReplicationFactor or TopicSpecification.ReplicaAssignment must be specified")
		}

		cTopics[i] = C.rd_kafka_NewTopic_new(
			C.CString(topic.Topic),
			C.int(topic.NumPartitions),
			cReplicationFactor,
			cErrstr, cErrstrSize)
		if cTopics[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Topic %s: %s", topic.Topic, C.GoString(cErrstr)))
		}

		defer C.rd_kafka_NewTopic_destroy(cTopics[i])

		for p, replicas := range topic.ReplicaAssignment {
			cReplicas := make([]C.int32_t, len(replicas))
			for ri, replica := range replicas {
				cReplicas[ri] = C.int32_t(replica)
			}
			cErr := C.rd_kafka_NewTopic_set_replica_assignment(
				cTopics[i], C.int32_t(p),
				(*C.int32_t)(&cReplicas[0]), C.size_t(len(cReplicas)),
				cErrstr, cErrstrSize)
			if cErr != 0 {
				return nil, newCErrorFromString(cErr,
					fmt.Sprintf("Failed to set replica assignment for topic %s partition %d: %s", topic.Topic, p, C.GoString(cErrstr)))
			}
		}

		for key, value := range topic.Config {
			cErr := C.rd_kafka_NewTopic_set_config(
				cTopics[i],
				C.CString(key), C.CString(value))
			if cErr != 0 {
				return nil, newCErrorFromString(cErr,
					fmt.Sprintf("Failed to set config %s=%s for topic %s", key, value, topic.Topic))
			}
		}
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_CREATETOPICS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_CreateTopics(
		a.handle.rk,
		(**C.rd_kafka_NewTopic_t)(&cTopics[0]),
		C.size_t(len(cTopics)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_CREATETOPICS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_CreateTopics_result(rkev)

	// Convert result from C to Go
	var cCnt C.size_t
	cTopicRes := C.rd_kafka_CreateTopics_result_topics(cRes, &cCnt)

	return a.cToTopicResults(cTopicRes, cCnt)
}

// DeleteTopics deletes a batch of topics.
//
// This operation is not transactional and may succeed for a subset of topics while
// failing others.
// It may take several seconds after the DeleteTopics result returns success for
// all the brokers to become aware that the topics are gone. During this time,
// topic metadata and configuration may continue to return information about deleted topics.
//
// Requires broker version >= 0.10.1.0
func (a *AdminClient) DeleteTopics(ctx context.Context, topics []string, options ...DeleteTopicsAdminOption) (result []TopicResult, err error) {
	cTopics := make([]*C.rd_kafka_DeleteTopic_t, len(topics))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go DeleteTopics to C DeleteTopics
	for i, topic := range topics {
		cTopics[i] = C.rd_kafka_DeleteTopic_new(C.CString(topic))
		if cTopics[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Invalid arguments for topic %s", topic))
		}

		defer C.rd_kafka_DeleteTopic_destroy(cTopics[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_DELETETOPICS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_DeleteTopics(
		a.handle.rk,
		(**C.rd_kafka_DeleteTopic_t)(&cTopics[0]),
		C.size_t(len(cTopics)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_DELETETOPICS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DeleteTopics_result(rkev)

	// Convert result from C to Go
	var cCnt C.size_t
	cTopicRes := C.rd_kafka_DeleteTopics_result_topics(cRes, &cCnt)

	return a.cToTopicResults(cTopicRes, cCnt)
}

// CreatePartitions creates additional partitions for topics.
func (a *AdminClient) CreatePartitions(ctx context.Context, partitions []PartitionsSpecification, options ...CreatePartitionsAdminOption) (result []TopicResult, err error) {
	cParts := make([]*C.rd_kafka_NewPartitions_t, len(partitions))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go PartitionsSpecification to C NewPartitions
	for i, part := range partitions {
		cParts[i] = C.rd_kafka_NewPartitions_new(C.CString(part.Topic), C.size_t(part.IncreaseTo), cErrstr, cErrstrSize)
		if cParts[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Topic %s: %s", part.Topic, C.GoString(cErrstr)))
		}

		defer C.rd_kafka_NewPartitions_destroy(cParts[i])

		for pidx, replicas := range part.ReplicaAssignment {
			cReplicas := make([]C.int32_t, len(replicas))
			for ri, replica := range replicas {
				cReplicas[ri] = C.int32_t(replica)
			}
			cErr := C.rd_kafka_NewPartitions_set_replica_assignment(
				cParts[i], C.int32_t(pidx),
				(*C.int32_t)(&cReplicas[0]), C.size_t(len(cReplicas)),
				cErrstr, cErrstrSize)
			if cErr != 0 {
				return nil, newCErrorFromString(cErr,
					fmt.Sprintf("Failed to set replica assignment for topic %s new partition index %d: %s", part.Topic, pidx, C.GoString(cErrstr)))
			}
		}

	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_CREATEPARTITIONS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_CreatePartitions(
		a.handle.rk,
		(**C.rd_kafka_NewPartitions_t)(&cParts[0]),
		C.size_t(len(cParts)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_CreatePartitions_result(rkev)

	// Convert result from C to Go
	var cCnt C.size_t
	cTopicRes := C.rd_kafka_CreatePartitions_result_topics(cRes, &cCnt)

	return a.cToTopicResults(cTopicRes, cCnt)
}

// AlterConfigs alters/updates cluster resource configuration.
//
// Updates are not transactional so they may succeed for a subset
// of the provided resources while others fail.
// The configuration for a particular resource is updated atomically,
// replacing values using the provided ConfigEntrys and reverting
// unspecified ConfigEntrys to their default values.
//
// Requires broker version >=0.11.0.0
//
// AlterConfigs will replace all existing configuration for
// the provided resources with the new configuration given,
// reverting all other configuration to their default values.
//
// Multiple resources and resource types may be set, but at most one
// resource of type ResourceBroker is allowed per call since these
// resource requests must be sent to the broker specified in the resource.
func (a *AdminClient) AlterConfigs(ctx context.Context, resources []ConfigResource, options ...AlterConfigsAdminOption) (result []ConfigResourceResult, err error) {
	cRes := make([]*C.rd_kafka_ConfigResource_t, len(resources))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go ConfigResources to C ConfigResources
	for i, res := range resources {
		cRes[i] = C.rd_kafka_ConfigResource_new(
			C.rd_kafka_ResourceType_t(res.Type), C.CString(res.Name))
		if cRes[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Invalid arguments for resource %v", res))
		}

		defer C.rd_kafka_ConfigResource_destroy(cRes[i])

		for _, entry := range res.Config {
			var cErr C.rd_kafka_resp_err_t
			switch entry.Operation {
			case AlterOperationSet:
				cErr = C.rd_kafka_ConfigResource_set_config(
					cRes[i], C.CString(entry.Name), C.CString(entry.Value))
			default:
				panic(fmt.Sprintf("Invalid ConfigEntry.Operation: %v", entry.Operation))
			}

			if cErr != 0 {
				return nil,
					newCErrorFromString(cErr,
						fmt.Sprintf("Failed to add configuration %s: %s",
							entry, C.GoString(C.rd_kafka_err2str(cErr))))
			}
		}
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_ALTERCONFIGS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_AlterConfigs(
		a.handle.rk,
		(**C.rd_kafka_ConfigResource_t)(&cRes[0]),
		C.size_t(len(cRes)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_ALTERCONFIGS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cResult := C.rd_kafka_event_AlterConfigs_result(rkev)

	// Convert results from C to Go
	var cCnt C.size_t
	cResults := C.rd_kafka_AlterConfigs_result_resources(cResult, &cCnt)

	return a.cConfigResourceToResult(cResults, cCnt)
}

// DescribeConfigs retrieves configuration for cluster resources.
//
// The returned configuration includes default values, use
// ConfigEntryResult.IsDefault or ConfigEntryResult.Source to distinguish
// default values from manually configured settings.
//
// The value of config entries where .IsSensitive is true
// will always be nil to avoid disclosing sensitive
// information, such as security settings.
//
// Configuration entries where .IsReadOnly is true can't be modified
// (with AlterConfigs).
//
// Synonym configuration entries are returned if the broker supports
// it (broker version >= 1.1.0). See .Synonyms.
//
// Requires broker version >=0.11.0.0
//
// Multiple resources and resource types may be requested, but at most
// one resource of type ResourceBroker is allowed per call
// since these resource requests must be sent to the broker specified
// in the resource.
func (a *AdminClient) DescribeConfigs(ctx context.Context, resources []ConfigResource, options ...DescribeConfigsAdminOption) (result []ConfigResourceResult, err error) {
	cRes := make([]*C.rd_kafka_ConfigResource_t, len(resources))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go ConfigResources to C ConfigResources
	for i, res := range resources {
		cRes[i] = C.rd_kafka_ConfigResource_new(
			C.rd_kafka_ResourceType_t(res.Type), C.CString(res.Name))
		if cRes[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Invalid arguments for resource %v", res))
		}

		defer C.rd_kafka_ConfigResource_destroy(cRes[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_DescribeConfigs(
		a.handle.rk,
		(**C.rd_kafka_ConfigResource_t)(&cRes[0]),
		C.size_t(len(cRes)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cResult := C.rd_kafka_event_DescribeConfigs_result(rkev)

	// Convert results from C to Go
	var cCnt C.size_t
	cResults := C.rd_kafka_DescribeConfigs_result_resources(cResult, &cCnt)

	return a.cConfigResourceToResult(cResults, cCnt)
}

// GetMetadata queries broker for cluster and topic metadata.
// If topic is non-nil only information about that topic is returned, else if
// allTopics is false only information about locally used topics is returned,
// else information about all topics is returned.
// GetMetadata is equivalent to listTopics, describeTopics and describeCluster in the Java API.
func (a *AdminClient) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*Metadata, error) {
	return getMetadata(a, topic, allTopics, timeoutMs)
}

// String returns a human readable name for an AdminClient instance
func (a *AdminClient) String() string {
	return fmt.Sprintf("admin-%s", a.handle.String())
}

// get_handle implements the Handle interface
func (a *AdminClient) gethandle() *handle {
	return a.handle
}

// SetOAuthBearerToken sets the the data to be transmitted
// to a broker during SASL/OAUTHBEARER authentication. It will return nil
// on success, otherwise an error if:
// 1) the token data is invalid (meaning an expiration time in the past
// or either a token value or an extension key or value that does not meet
// the regular expression requirements as per
// https://tools.ietf.org/html/rfc7628#section-3.1);
// 2) SASL/OAUTHBEARER is not supported by the underlying librdkafka build;
// 3) SASL/OAUTHBEARER is supported but is not configured as the client's
// authentication mechanism.
func (a *AdminClient) SetOAuthBearerToken(oauthBearerToken OAuthBearerToken) error {
	return a.handle.setOAuthBearerToken(oauthBearerToken)
}

// SetOAuthBearerTokenFailure sets the error message describing why token
// retrieval/setting failed; it also schedules a new token refresh event for 10
// seconds later so the attempt may be retried. It will return nil on
// success, otherwise an error if:
// 1) SASL/OAUTHBEARER is not supported by the underlying librdkafka build;
// 2) SASL/OAUTHBEARER is supported but is not configured as the client's
// authentication mechanism.
func (a *AdminClient) SetOAuthBearerTokenFailure(errstr string) error {
	return a.handle.setOAuthBearerTokenFailure(errstr)
}

// Close an AdminClient instance.
func (a *AdminClient) Close() {
	if a.isDerived {
		// Derived AdminClient needs no cleanup.
		a.handle = &handle{}
		return
	}

	a.handle.cleanup()

	C.rd_kafka_destroy(a.handle.rk)
}

// NewAdminClient creats a new AdminClient instance with a new underlying client instance
func NewAdminClient(conf *ConfigMap) (*AdminClient, error) {

	err := versionCheck()
	if err != nil {
		return nil, err
	}

	a := &AdminClient{}
	a.handle = &handle{}

	// Convert ConfigMap to librdkafka conf_t
	cConf, err := conf.convert()
	if err != nil {
		return nil, err
	}

	cErrstr := (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(cErrstr))

	C.rd_kafka_conf_set_events(cConf, C.RD_KAFKA_EVENT_STATS|C.RD_KAFKA_EVENT_ERROR|C.RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH)

	// Create librdkafka producer instance. The Producer is somewhat cheaper than
	// the consumer, but any instance type can be used for Admin APIs.
	a.handle.rk = C.rd_kafka_new(C.RD_KAFKA_PRODUCER, cConf, cErrstr, 256)
	if a.handle.rk == nil {
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	a.isDerived = false
	a.handle.setup()

	return a, nil
}

// NewAdminClientFromProducer derives a new AdminClient from an existing Producer instance.
// The AdminClient will use the same configuration and connections as the parent instance.
func NewAdminClientFromProducer(p *Producer) (a *AdminClient, err error) {
	if p.handle.rk == nil {
		return nil, newErrorFromString(ErrInvalidArg, "Can't derive AdminClient from closed producer")
	}

	a = &AdminClient{}
	a.handle = &p.handle
	a.isDerived = true
	return a, nil
}

// NewAdminClientFromConsumer derives a new AdminClient from an existing Consumer instance.
// The AdminClient will use the same configuration and connections as the parent instance.
func NewAdminClientFromConsumer(c *Consumer) (a *AdminClient, err error) {
	if c.handle.rk == nil {
		return nil, newErrorFromString(ErrInvalidArg, "Can't derive AdminClient from closed consumer")
	}

	a = &AdminClient{}
	a.handle = &c.handle
	a.isDerived = true
	return a, nil
}
