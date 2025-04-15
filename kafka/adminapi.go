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
	"sync/atomic"
	"time"
	"unsafe"
)

/*
#include "select_rdkafka.h"
#include <stdlib.h>

static const rd_kafka_group_result_t *
group_result_by_idx (const rd_kafka_group_result_t **groups, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return groups[idx];
}

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

static const rd_kafka_acl_result_t *
acl_result_by_idx (const rd_kafka_acl_result_t **acl_results, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return acl_results[idx];
}

static const rd_kafka_DeleteAcls_result_response_t *
DeleteAcls_result_response_by_idx (const rd_kafka_DeleteAcls_result_response_t **delete_acls_result_responses, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return delete_acls_result_responses[idx];
}

static const rd_kafka_AclBinding_t *
AclBinding_by_idx (const rd_kafka_AclBinding_t **acl_bindings, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return acl_bindings[idx];
}

static const rd_kafka_ConsumerGroupListing_t *
ConsumerGroupListing_by_idx(const rd_kafka_ConsumerGroupListing_t **result_groups, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return result_groups[idx];
}

static const rd_kafka_ConsumerGroupDescription_t *
ConsumerGroupDescription_by_idx(const rd_kafka_ConsumerGroupDescription_t **result_groups, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return result_groups[idx];
}

static const rd_kafka_TopicDescription_t *
TopicDescription_by_idx(const rd_kafka_TopicDescription_t **result_topics, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return result_topics[idx];
}

static const rd_kafka_TopicPartitionInfo_t *
TopicPartitionInfo_by_idx(const rd_kafka_TopicPartitionInfo_t **partitions, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return partitions[idx];
}

static const rd_kafka_AclOperation_t AclOperation_by_idx(const rd_kafka_AclOperation_t *acl_operations, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return RD_KAFKA_ACL_OPERATION_UNKNOWN;
	return acl_operations[idx];
}

static const rd_kafka_Node_t *Node_by_idx(const rd_kafka_Node_t **nodes, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return nodes[idx];
}

static const rd_kafka_UserScramCredentialsDescription_t *
DescribeUserScramCredentials_result_description_by_idx(const rd_kafka_UserScramCredentialsDescription_t **descriptions, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return descriptions[idx];
}

static const rd_kafka_AlterUserScramCredentials_result_response_t*
AlterUserScramCredentials_result_response_by_idx(const rd_kafka_AlterUserScramCredentials_result_response_t **responses, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return responses[idx];
}

static const rd_kafka_ListOffsetsResultInfo_t *
ListOffsetsResultInfo_by_idx(const rd_kafka_ListOffsetsResultInfo_t **result_infos, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return result_infos[idx];
}

static const rd_kafka_error_t *
error_by_idx(const rd_kafka_error_t **errors, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return errors[idx];
}

static const rd_kafka_topic_partition_result_t *
TopicPartitionResult_by_idx(const rd_kafka_topic_partition_result_t **results, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return results[idx];
}
*/
import "C"

// AdminClient is derived from an existing Producer or Consumer
type AdminClient struct {
	handle    *handle
	isDerived bool   // Derived from existing client handle
	isClosed  uint32 // to check if Admin Client is closed or not.
}

// IsClosed returns boolean representing if client is closed or not
func (a *AdminClient) IsClosed() bool {
	return atomic.LoadUint32(&a.isClosed) == 1
}

func (a *AdminClient) verifyClient() error {
	if a.IsClosed() {
		return getOperationNotAllowedErrorForClosedClient()
	}
	return nil
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

// ConsumerGroupResult provides per-group operation result (error) information.
type ConsumerGroupResult struct {
	// Group name
	Group string
	// Error, if any, of result. Check with `Error.Code() != ErrNoError`.
	Error Error
}

// String returns a human-readable representation of a ConsumerGroupResult.
func (g ConsumerGroupResult) String() string {
	if g.Error.code == ErrNoError {
		return g.Group
	}
	return fmt.Sprintf("%s (%s)", g.Group, g.Error.str)
}

// ConsumerGroupState represents a consumer group state
type ConsumerGroupState int

const (
	// ConsumerGroupStateUnknown - Unknown ConsumerGroupState
	ConsumerGroupStateUnknown ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_UNKNOWN
	// ConsumerGroupStatePreparingRebalance - preparing rebalance
	ConsumerGroupStatePreparingRebalance ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE
	// ConsumerGroupStateCompletingRebalance - completing rebalance
	ConsumerGroupStateCompletingRebalance ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE
	// ConsumerGroupStateStable - stable
	ConsumerGroupStateStable ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_STABLE
	// ConsumerGroupStateDead - dead group
	ConsumerGroupStateDead ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_DEAD
	// ConsumerGroupStateEmpty - empty group
	ConsumerGroupStateEmpty ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY
)

// String returns the human-readable representation of a consumer_group_state
func (t ConsumerGroupState) String() string {
	return C.GoString(C.rd_kafka_consumer_group_state_name(
		C.rd_kafka_consumer_group_state_t(t)))
}

// ConsumerGroupStateFromString translates a consumer group state name/string to
// a ConsumerGroupState value.
func ConsumerGroupStateFromString(stateString string) (ConsumerGroupState, error) {
	cStr := C.CString(stateString)
	defer C.free(unsafe.Pointer(cStr))
	state := ConsumerGroupState(C.rd_kafka_consumer_group_state_code(cStr))
	return state, nil
}

// ConsumerGroupType represents a consumer group type
type ConsumerGroupType int

const (
	// ConsumerGroupTypeUnknown - Unknown ConsumerGroupType
	ConsumerGroupTypeUnknown ConsumerGroupType = C.RD_KAFKA_CONSUMER_GROUP_TYPE_UNKNOWN
	// ConsumerGroupTypeConsumer - Consumer ConsumerGroupType
	ConsumerGroupTypeConsumer ConsumerGroupType = C.RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER
	// ConsumerGroupTypeClassic - Classic ConsumerGroupType
	ConsumerGroupTypeClassic ConsumerGroupType = C.RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC
)

// String returns the human-readable representation of a ConsumerGroupType
func (t ConsumerGroupType) String() string {
	return C.GoString(C.rd_kafka_consumer_group_type_name(
		C.rd_kafka_consumer_group_type_t(t)))
}

// ConsumerGroupTypeFromString translates a consumer group type name/string to
// a ConsumerGroupType value.
func ConsumerGroupTypeFromString(typeString string) ConsumerGroupType {
	cStr := C.CString(typeString)
	defer C.free(unsafe.Pointer(cStr))
	groupType := ConsumerGroupType(C.rd_kafka_consumer_group_type_code(cStr))
	return groupType
}

// ConsumerGroupListing represents the result of ListConsumerGroups for a single
// group.
type ConsumerGroupListing struct {
	// Group id.
	GroupID string
	// Is a simple consumer group.
	IsSimpleConsumerGroup bool
	// Group state.
	State ConsumerGroupState
	// Group type.
	Type ConsumerGroupType
}

// ListConsumerGroupsResult represents ListConsumerGroups results and errors.
type ListConsumerGroupsResult struct {
	// List of valid ConsumerGroupListings.
	Valid []ConsumerGroupListing
	// List of errors.
	Errors []error
}

// DeletedRecords contains information about deleted
// records of a single partition
type DeletedRecords struct {
	// Low-watermark offset after deletion
	LowWatermark Offset
}

// DeleteRecordsResult represents the result of a DeleteRecords call
// for a single partition.
type DeleteRecordsResult struct {
	// One of requested partitions.
	// The Error field is set if any occurred for that partition.
	TopicPartition TopicPartition
	// Deleted records information, or nil if an error occurred.
	DeletedRecords *DeletedRecords
}

// DeleteRecordsResults represents the results of a DeleteRecords call.
type DeleteRecordsResults struct {
	// A slice of DeleteRecordsResult, one for each requested topic partition.
	DeleteRecordsResults []DeleteRecordsResult
}

// MemberAssignment represents the assignment of a consumer group member.
type MemberAssignment struct {
	// Partitions assigned to current member.
	TopicPartitions []TopicPartition
}

// MemberDescription represents the description of a consumer group member.
type MemberDescription struct {
	// Client id.
	ClientID string
	// Group instance id.
	GroupInstanceID string
	// Consumer id.
	ConsumerID string
	// Group member host.
	Host string
	// Member assignment.
	Assignment MemberAssignment
	// Member Target Assignment. Set to `nil` for `Classic` GroupType.
	TargetAssignment *MemberAssignment
}

// ConsumerGroupDescription represents the result of DescribeConsumerGroups for
// a single group.
type ConsumerGroupDescription struct {
	// Group id.
	GroupID string
	// Error, if any, of result. Check with `Error.Code() != ErrNoError`.
	Error Error
	// Is a simple consumer group.
	IsSimpleConsumerGroup bool
	// Partition assignor identifier.
	PartitionAssignor string
	// Consumer group state.
	State ConsumerGroupState
	// Consumer group type.
	Type ConsumerGroupType
	// Consumer group coordinator (has ID == -1 if not known).
	Coordinator Node
	// Members list.
	Members []MemberDescription
	// Operations allowed for the group (nil if not available or not requested)
	AuthorizedOperations []ACLOperation
}

// DescribeConsumerGroupsResult represents the result of a
// DescribeConsumerGroups call.
type DescribeConsumerGroupsResult struct {
	// Slice of ConsumerGroupDescription.
	ConsumerGroupDescriptions []ConsumerGroupDescription
}

// TopicCollection represents a collection of topics.
type TopicCollection struct {
	// Slice of topic names.
	topicNames []string
}

// NewTopicCollectionOfTopicNames creates a new TopicCollection based on a list
// of topic names.
func NewTopicCollectionOfTopicNames(names []string) TopicCollection {
	return TopicCollection{
		topicNames: names,
	}
}

// TopicPartitionInfo represents a specific partition's information inside a
// TopicDescription.
type TopicPartitionInfo struct {
	// Partition id.
	Partition int
	// Leader broker.
	Leader *Node
	// Replicas of the partition.
	Replicas []Node
	// In-Sync-Replicas of the partition.
	Isr []Node
}

// TopicDescription represents the result of DescribeTopics for
// a single topic.
type TopicDescription struct {
	// Topic name.
	Name string
	// Topic Id
	TopicID UUID
	// Error, if any, of the result. Check with `Error.Code() != ErrNoError`.
	Error Error
	// Is the topic internal to Kafka?
	IsInternal bool
	// Partitions' information list.
	Partitions []TopicPartitionInfo
	// Operations allowed for the topic (nil if not available or not requested).
	AuthorizedOperations []ACLOperation
}

// DescribeTopicsResult represents the result of a
// DescribeTopics call.
type DescribeTopicsResult struct {
	// Slice of TopicDescription.
	TopicDescriptions []TopicDescription
}

// DescribeClusterResult represents the result of DescribeCluster.
type DescribeClusterResult struct {
	// Cluster id for the cluster (always available if broker version >= 0.10.1.0, otherwise nil).
	ClusterID *string
	// Current controller broker for the cluster (nil if there is none).
	Controller *Node
	// List of brokers in the cluster.
	Nodes []Node
	// Operations allowed for the cluster (nil if not available or not requested).
	AuthorizedOperations []ACLOperation
}

// DeleteConsumerGroupsResult represents the result of a DeleteConsumerGroups
// call.
type DeleteConsumerGroupsResult struct {
	// Slice of ConsumerGroupResult.
	ConsumerGroupResults []ConsumerGroupResult
}

// ListConsumerGroupOffsetsResult represents the result of a
// ListConsumerGroupOffsets operation.
type ListConsumerGroupOffsetsResult struct {
	// A slice of ConsumerGroupTopicPartitions, each element represents a group's
	// TopicPartitions and Offsets.
	ConsumerGroupsTopicPartitions []ConsumerGroupTopicPartitions
}

// AlterConsumerGroupOffsetsResult represents the result of a
// AlterConsumerGroupOffsets operation.
type AlterConsumerGroupOffsetsResult struct {
	// A slice of ConsumerGroupTopicPartitions, each element represents a group's
	// TopicPartitions and Offsets.
	ConsumerGroupsTopicPartitions []ConsumerGroupTopicPartitions
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
	ResourceUnknown ResourceType = C.RD_KAFKA_RESOURCE_UNKNOWN
	// ResourceAny - match any resource type (DescribeConfigs)
	ResourceAny ResourceType = C.RD_KAFKA_RESOURCE_ANY
	// ResourceTopic - Topic
	ResourceTopic ResourceType = C.RD_KAFKA_RESOURCE_TOPIC
	// ResourceGroup - Group
	ResourceGroup ResourceType = C.RD_KAFKA_RESOURCE_GROUP
	// ResourceBroker - Broker
	ResourceBroker ResourceType = C.RD_KAFKA_RESOURCE_BROKER
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
	ConfigSourceUnknown ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_UNKNOWN_CONFIG
	// ConfigSourceDynamicTopic is dynamic topic config that is configured for a specific topic
	ConfigSourceDynamicTopic ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG
	// ConfigSourceDynamicBroker is dynamic broker config that is configured for a specific broker
	ConfigSourceDynamicBroker ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG
	// ConfigSourceDynamicDefaultBroker is dynamic broker config that is configured as default for all brokers in the cluster
	ConfigSourceDynamicDefaultBroker ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG
	// ConfigSourceStaticBroker is static broker config provided as broker properties at startup (e.g. from server.properties file)
	ConfigSourceStaticBroker ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_STATIC_BROKER_CONFIG
	// ConfigSourceDefault is built-in default configuration for configs that have a default value
	ConfigSourceDefault ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_DEFAULT_CONFIG
	// ConfigSourceGroup is group config that is configured for a specific group
	ConfigSourceGroup ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_GROUP_CONFIG
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

// AlterConfigOpType specifies the operation to perform
// on the ConfigEntry for IncrementalAlterConfig
type AlterConfigOpType int

const (
	// AlterConfigOpTypeSet sets/overwrites the configuration
	// setting.
	AlterConfigOpTypeSet AlterConfigOpType = C.RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET
	// AlterConfigOpTypeDelete sets the configuration setting
	// to default or NULL.
	AlterConfigOpTypeDelete AlterConfigOpType = C.RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE
	// AlterConfigOpTypeAppend appends the value to existing
	// configuration settings.
	AlterConfigOpTypeAppend AlterConfigOpType = C.RD_KAFKA_ALTER_CONFIG_OP_TYPE_APPEND
	// AlterConfigOpTypeSubtract subtracts the value from
	// existing configuration settings.
	AlterConfigOpTypeSubtract AlterConfigOpType = C.RD_KAFKA_ALTER_CONFIG_OP_TYPE_SUBTRACT
)

// String returns the human-readable representation of an AlterOperation
func (o AlterConfigOpType) String() string {
	switch o {
	case AlterConfigOpTypeSet:
		return "Set"
	case AlterConfigOpTypeDelete:
		return "Delete"
	case AlterConfigOpTypeAppend:
		return "Append"
	case AlterConfigOpTypeSubtract:
		return "Subtract"
	default:
		return fmt.Sprintf("Unknown %d", int(o))
	}
}

// ConfigEntry holds parameters for altering a resource's configuration.
type ConfigEntry struct {
	// Name of configuration entry, e.g., topic configuration property name.
	Name string
	// Value of configuration entry.
	Value string
	// Deprecated: Operation to perform on the entry.
	Operation AlterOperation
	// Operation to perform on the entry incrementally.
	IncrementalOperation AlterConfigOpType
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

// StringMapToIncrementalConfigEntries creates a new map of ConfigEntry objects from the
// provided string map an operation map. The AlterConfigOpType is set on each created entry.
func StringMapToIncrementalConfigEntries(stringMap map[string]string,
	operationMap map[string]AlterConfigOpType) []ConfigEntry {
	var ceList []ConfigEntry

	for k, v := range stringMap {
		ceList = append(ceList, ConfigEntry{Name: k, Value: v, IncrementalOperation: operationMap[k]})
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
	// IsDefault indicates whether the value is at its default.
	IsDefault bool
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
	entry.IsDefault = cint2bool(C.rd_kafka_ConfigEntry_is_default(cEntry))
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

// ResourcePatternType enumerates the different types of Kafka resource patterns.
type ResourcePatternType int

const (
	// ResourcePatternTypeUnknown is a resource pattern type not known or not set.
	ResourcePatternTypeUnknown ResourcePatternType = C.RD_KAFKA_RESOURCE_PATTERN_UNKNOWN
	// ResourcePatternTypeAny matches any resource, used for lookups.
	ResourcePatternTypeAny ResourcePatternType = C.RD_KAFKA_RESOURCE_PATTERN_ANY
	// ResourcePatternTypeMatch will perform pattern matching
	ResourcePatternTypeMatch ResourcePatternType = C.RD_KAFKA_RESOURCE_PATTERN_MATCH
	// ResourcePatternTypeLiteral matches a literal resource name
	ResourcePatternTypeLiteral ResourcePatternType = C.RD_KAFKA_RESOURCE_PATTERN_LITERAL
	// ResourcePatternTypePrefixed matches a prefixed resource name
	ResourcePatternTypePrefixed ResourcePatternType = C.RD_KAFKA_RESOURCE_PATTERN_PREFIXED
)

// String returns the human-readable representation of a ResourcePatternType
func (t ResourcePatternType) String() string {
	return C.GoString(C.rd_kafka_ResourcePatternType_name(C.rd_kafka_ResourcePatternType_t(t)))
}

// ResourcePatternTypeFromString translates a resource pattern type name to
// a ResourcePatternType value.
func ResourcePatternTypeFromString(patternTypeString string) (ResourcePatternType, error) {
	switch strings.ToUpper(patternTypeString) {
	case "ANY":
		return ResourcePatternTypeAny, nil
	case "MATCH":
		return ResourcePatternTypeMatch, nil
	case "LITERAL":
		return ResourcePatternTypeLiteral, nil
	case "PREFIXED":
		return ResourcePatternTypePrefixed, nil
	default:
		return ResourcePatternTypeUnknown, NewError(ErrInvalidArg, "Unknown resource pattern type", false)
	}
}

// ACLOperation enumerates the different types of ACL operation.
type ACLOperation int

const (
	// ACLOperationUnknown represents an unknown or unset operation
	ACLOperationUnknown ACLOperation = C.RD_KAFKA_ACL_OPERATION_UNKNOWN
	// ACLOperationAny in a filter, matches any ACLOperation
	ACLOperationAny ACLOperation = C.RD_KAFKA_ACL_OPERATION_ANY
	// ACLOperationAll represents all the operations
	ACLOperationAll ACLOperation = C.RD_KAFKA_ACL_OPERATION_ALL
	// ACLOperationRead a read operation
	ACLOperationRead ACLOperation = C.RD_KAFKA_ACL_OPERATION_READ
	// ACLOperationWrite represents a write operation
	ACLOperationWrite ACLOperation = C.RD_KAFKA_ACL_OPERATION_WRITE
	// ACLOperationCreate represents a create operation
	ACLOperationCreate ACLOperation = C.RD_KAFKA_ACL_OPERATION_CREATE
	// ACLOperationDelete represents a delete operation
	ACLOperationDelete ACLOperation = C.RD_KAFKA_ACL_OPERATION_DELETE
	// ACLOperationAlter represents an alter operation
	ACLOperationAlter ACLOperation = C.RD_KAFKA_ACL_OPERATION_ALTER
	// ACLOperationDescribe represents a describe operation
	ACLOperationDescribe ACLOperation = C.RD_KAFKA_ACL_OPERATION_DESCRIBE
	// ACLOperationClusterAction represents a cluster action operation
	ACLOperationClusterAction ACLOperation = C.RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION
	// ACLOperationDescribeConfigs represents a describe configs operation
	ACLOperationDescribeConfigs ACLOperation = C.RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS
	// ACLOperationAlterConfigs represents an alter configs operation
	ACLOperationAlterConfigs ACLOperation = C.RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS
	// ACLOperationIdempotentWrite represents an idempotent write operation
	ACLOperationIdempotentWrite ACLOperation = C.RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE
)

// String returns the human-readable representation of an ACLOperation
func (o ACLOperation) String() string {
	return C.GoString(C.rd_kafka_AclOperation_name(C.rd_kafka_AclOperation_t(o)))
}

// ACLOperationFromString translates a ACL operation name to
// a ACLOperation value.
func ACLOperationFromString(aclOperationString string) (ACLOperation, error) {
	switch strings.ToUpper(aclOperationString) {
	case "ANY":
		return ACLOperationAny, nil
	case "ALL":
		return ACLOperationAll, nil
	case "READ":
		return ACLOperationRead, nil
	case "WRITE":
		return ACLOperationWrite, nil
	case "CREATE":
		return ACLOperationCreate, nil
	case "DELETE":
		return ACLOperationDelete, nil
	case "ALTER":
		return ACLOperationAlter, nil
	case "DESCRIBE":
		return ACLOperationDescribe, nil
	case "CLUSTER_ACTION":
		return ACLOperationClusterAction, nil
	case "DESCRIBE_CONFIGS":
		return ACLOperationDescribeConfigs, nil
	case "ALTER_CONFIGS":
		return ACLOperationAlterConfigs, nil
	case "IDEMPOTENT_WRITE":
		return ACLOperationIdempotentWrite, nil
	default:
		return ACLOperationUnknown, NewError(ErrInvalidArg, "Unknown ACL operation", false)
	}
}

// ACLPermissionType enumerates the different types of ACL permission types.
type ACLPermissionType int

const (
	// ACLPermissionTypeUnknown represents an unknown ACLPermissionType
	ACLPermissionTypeUnknown ACLPermissionType = C.RD_KAFKA_ACL_PERMISSION_TYPE_UNKNOWN
	// ACLPermissionTypeAny in a filter, matches any ACLPermissionType
	ACLPermissionTypeAny ACLPermissionType = C.RD_KAFKA_ACL_PERMISSION_TYPE_ANY
	// ACLPermissionTypeDeny disallows access
	ACLPermissionTypeDeny ACLPermissionType = C.RD_KAFKA_ACL_PERMISSION_TYPE_DENY
	// ACLPermissionTypeAllow grants access
	ACLPermissionTypeAllow ACLPermissionType = C.RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
)

// String returns the human-readable representation of an ACLPermissionType
func (o ACLPermissionType) String() string {
	return C.GoString(C.rd_kafka_AclPermissionType_name(C.rd_kafka_AclPermissionType_t(o)))
}

// ACLPermissionTypeFromString translates a ACL permission type name to
// a ACLPermissionType value.
func ACLPermissionTypeFromString(aclPermissionTypeString string) (ACLPermissionType, error) {
	switch strings.ToUpper(aclPermissionTypeString) {
	case "ANY":
		return ACLPermissionTypeAny, nil
	case "DENY":
		return ACLPermissionTypeDeny, nil
	case "ALLOW":
		return ACLPermissionTypeAllow, nil
	default:
		return ACLPermissionTypeUnknown, NewError(ErrInvalidArg, "Unknown ACL permission type", false)
	}
}

// ACLBinding specifies the operation and permission type for a specific principal
// over one or more resources of the same type. Used by `AdminClient.CreateACLs`,
// returned by `AdminClient.DescribeACLs` and `AdminClient.DeleteACLs`.
type ACLBinding struct {
	Type ResourceType // The resource type.
	// The resource name, which depends on the resource type.
	// For ResourceBroker the resource name is the broker id.
	Name                string
	ResourcePatternType ResourcePatternType // The resource pattern, relative to the name.
	Principal           string              // The principal this ACLBinding refers to.
	Host                string              // The host that the call is allowed to come from.
	Operation           ACLOperation        // The operation/s specified by this binding.
	PermissionType      ACLPermissionType   // The permission type for the specified operation.
}

// ACLBindingFilter specifies a filter used to return a list of ACL bindings matching some or all of its attributes.
// Used by `AdminClient.DescribeACLs` and `AdminClient.DeleteACLs`.
type ACLBindingFilter = ACLBinding

// ACLBindings is a slice of ACLBinding that also implements
// the sort interface
type ACLBindings []ACLBinding

// ACLBindingFilters is a slice of ACLBindingFilter that also implements
// the sort interface
type ACLBindingFilters []ACLBindingFilter

func (a ACLBindings) Len() int {
	return len(a)
}

func (a ACLBindings) Less(i, j int) bool {
	if a[i].Type != a[j].Type {
		return a[i].Type < a[j].Type
	}
	if a[i].Name != a[j].Name {
		return a[i].Name < a[j].Name
	}
	if a[i].ResourcePatternType != a[j].ResourcePatternType {
		return a[i].ResourcePatternType < a[j].ResourcePatternType
	}
	if a[i].Principal != a[j].Principal {
		return a[i].Principal < a[j].Principal
	}
	if a[i].Host != a[j].Host {
		return a[i].Host < a[j].Host
	}
	if a[i].Operation != a[j].Operation {
		return a[i].Operation < a[j].Operation
	}
	if a[i].PermissionType != a[j].PermissionType {
		return a[i].PermissionType < a[j].PermissionType
	}
	return true
}

func (a ACLBindings) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// CreateACLResult provides create ACL error information.
type CreateACLResult struct {
	// Error, if any, of result. Check with `Error.Code() != ErrNoError`.
	Error Error
}

// DescribeACLsResult provides describe ACLs result or error information.
type DescribeACLsResult struct {
	// Slice of ACL bindings matching the provided filter
	ACLBindings ACLBindings
	// Error, if any, of result. Check with `Error.Code() != ErrNoError`.
	Error Error
}

// DeleteACLsResult provides delete ACLs result or error information.
type DeleteACLsResult = DescribeACLsResult

// ScramMechanism enumerates SASL/SCRAM mechanisms.
// Used by `AdminClient.AlterUserScramCredentials`
// and `AdminClient.DescribeUserScramCredentials`.
type ScramMechanism int

const (
	// ScramMechanismUnknown - Unknown SASL/SCRAM mechanism
	ScramMechanismUnknown ScramMechanism = C.RD_KAFKA_SCRAM_MECHANISM_UNKNOWN
	// ScramMechanismSHA256 - SCRAM-SHA-256 mechanism
	ScramMechanismSHA256 ScramMechanism = C.RD_KAFKA_SCRAM_MECHANISM_SHA_256
	// ScramMechanismSHA512 - SCRAM-SHA-512 mechanism
	ScramMechanismSHA512 ScramMechanism = C.RD_KAFKA_SCRAM_MECHANISM_SHA_512
)

// String returns the human-readable representation of an ScramMechanism
func (o ScramMechanism) String() string {
	switch o {
	case ScramMechanismSHA256:
		return "SCRAM-SHA-256"
	case ScramMechanismSHA512:
		return "SCRAM-SHA-512"
	default:
		return "UNKNOWN"
	}
}

// ScramMechanismFromString translates a Scram Mechanism name to
// a ScramMechanism value.
func ScramMechanismFromString(mechanism string) (ScramMechanism, error) {
	switch strings.ToUpper(mechanism) {
	case "SCRAM-SHA-256":
		return ScramMechanismSHA256, nil
	case "SCRAM-SHA-512":
		return ScramMechanismSHA512, nil
	default:
		return ScramMechanismUnknown,
			NewError(ErrInvalidArg, "Unknown SCRAM mechanism", false)
	}
}

// ScramCredentialInfo contains Mechanism and Iterations for a
// SASL/SCRAM credential associated with a user.
type ScramCredentialInfo struct {
	// Iterations - positive number of iterations used when creating the credential
	Iterations int
	// Mechanism - SASL/SCRAM mechanism
	Mechanism ScramMechanism
}

// UserScramCredentialsDescription represent all SASL/SCRAM credentials
// associated with a user that can be retrieved, or an error indicating
// why credentials could not be retrieved.
type UserScramCredentialsDescription struct {
	// User - the user name.
	User string
	// ScramCredentialInfos - SASL/SCRAM credential representations for the user.
	ScramCredentialInfos []ScramCredentialInfo
	// Error - error corresponding to this user description.
	Error Error
}

// UserScramCredentialDeletion is a request to delete
// a SASL/SCRAM credential for a user.
type UserScramCredentialDeletion struct {
	// User - user name
	User string
	// Mechanism - SASL/SCRAM mechanism.
	Mechanism ScramMechanism
}

// UserScramCredentialUpsertion is a request to update/insert
// a SASL/SCRAM credential for a user.
type UserScramCredentialUpsertion struct {
	// User - user name
	User string
	// ScramCredentialInfo - the mechanism and iterations.
	ScramCredentialInfo ScramCredentialInfo
	// Password - password to HMAC before storage.
	Password []byte
	// Salt - salt to use. Will be generated randomly if nil. (optional)
	Salt []byte
}

// DescribeUserScramCredentialsResult represents the result of a
// DescribeUserScramCredentials call.
type DescribeUserScramCredentialsResult struct {
	// Descriptions - Map from user name
	// to UserScramCredentialsDescription
	Descriptions map[string]UserScramCredentialsDescription
}

// AlterUserScramCredentialsResult represents the result of a
// AlterUserScramCredentials call.
type AlterUserScramCredentialsResult struct {
	// Errors - Map from user name
	// to an Error, with ErrNoError code on success.
	Errors map[string]Error
}

// OffsetSpec specifies desired offsets while using ListOffsets.
type OffsetSpec int64

const (
	// MaxTimestampOffsetSpec is used to describe the offset with the Max Timestamp which may be different then LatestOffsetSpec as Timestamp can be set client side.
	MaxTimestampOffsetSpec OffsetSpec = C.RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP
	// EarliestOffsetSpec is used to describe the earliest offset for the TopicPartition.
	EarliestOffsetSpec OffsetSpec = C.RD_KAFKA_OFFSET_SPEC_EARLIEST
	// LatestOffsetSpec is used to describe the latest offset for the TopicPartition.
	LatestOffsetSpec OffsetSpec = C.RD_KAFKA_OFFSET_SPEC_LATEST
)

// NewOffsetSpecForTimestamp creates an OffsetSpec corresponding to the timestamp.
func NewOffsetSpecForTimestamp(timestamp int64) OffsetSpec {
	return OffsetSpec(timestamp)
}

// ListOffsetsResultInfo describes the result of ListOffsets request for a Topic Partition.
type ListOffsetsResultInfo struct {
	Offset      Offset
	Timestamp   int64
	LeaderEpoch *int32
	Error       Error
}

// ListOffsetsResult holds the map of TopicPartition to ListOffsetsResultInfo for a request.
type ListOffsetsResult struct {
	ResultInfos map[TopicPartition]ListOffsetsResultInfo
}

// ElectionType represents the type of election to be performed
type ElectionType int

const (
	// ElectionTypePreferred - Preferred election type
	ElectionTypePreferred ElectionType = C.RD_KAFKA_ELECTION_TYPE_PREFERRED
	// ElectionTypeUnclean - Unclean election type
	ElectionTypeUnclean ElectionType = C.RD_KAFKA_ELECTION_TYPE_UNCLEAN
)

// ElectionTypeFromString translates an election type name to
// an ElectionType value.
func ElectionTypeFromString(electionTypeString string) (ElectionType, error) {
	switch strings.ToUpper(electionTypeString) {
	case "PREFERRED":
		return ElectionTypePreferred, nil
	case "UNCLEAN":
		return ElectionTypeUnclean, nil
	default:
		return ElectionTypePreferred, NewError(ErrInvalidArg, "Unknown election type", false)
	}
}

// ElectLeadersRequest holds parameters for the type of election to be performed and
// the topic partitions for which election has to be performed
type ElectLeadersRequest struct {
	// Election type to be performed
	electionType ElectionType
	// TopicPartitions for which election has to be performed
	partitions []TopicPartition
}

// NewElectLeadersRequest creates a new ElectLeadersRequest with the given election type
// and topic partitions
func NewElectLeadersRequest(electionType ElectionType, partitions []TopicPartition) ElectLeadersRequest {
	return ElectLeadersRequest{
		electionType: electionType,
		partitions:   partitions,
	}
}

// ElectLeadersResult holds the result of the election performed
type ElectLeadersResult struct {
	// TopicPartitions for which election has been performed and the per-partition error, if any
	// that occurred while running the election for the specific TopicPartition.
	TopicPartitions []TopicPartition
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

// cToConsumerGroupResults converts a C group_result_t array to Go ConsumerGroupResult list.
func (a *AdminClient) cToConsumerGroupResults(
	cGroupRes **C.rd_kafka_group_result_t, cCnt C.size_t) (result []ConsumerGroupResult, err error) {
	result = make([]ConsumerGroupResult, int(cCnt))

	for idx := 0; idx < int(cCnt); idx++ {
		cGroup := C.group_result_by_idx(cGroupRes, cCnt, C.size_t(idx))
		result[idx].Group = C.GoString(C.rd_kafka_group_result_name(cGroup))
		result[idx].Error = newErrorFromCError(C.rd_kafka_group_result_error(cGroup))
	}

	return result, nil
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

// cToAuthorizedOperations converts a C AclOperation_t array to a Go
// ACLOperation list.
func (a *AdminClient) cToAuthorizedOperations(
	cAuthorizedOperations *C.rd_kafka_AclOperation_t,
	cAuthorizedOperationCnt C.size_t) []ACLOperation {
	if cAuthorizedOperations == nil {
		return nil
	}

	authorizedOperations := make([]ACLOperation, int(cAuthorizedOperationCnt))
	for i := 0; i < int(cAuthorizedOperationCnt); i++ {
		cAuthorizedOperation := C.AclOperation_by_idx(
			cAuthorizedOperations, cAuthorizedOperationCnt, C.size_t(i))
		authorizedOperations[i] = ACLOperation(cAuthorizedOperation)
	}

	return authorizedOperations
}

// cToUUID converts a C rd_kafka_Uuid_t to a Go UUID.
func (a *AdminClient) cToUUID(cUUID *C.rd_kafka_Uuid_t) UUID {
	uuid := UUID{
		mostSignificantBits:  int64(C.rd_kafka_Uuid_most_significant_bits(cUUID)),
		leastSignificantBits: int64(C.rd_kafka_Uuid_least_significant_bits(cUUID)),
		base64str:            C.GoString(C.rd_kafka_Uuid_base64str(cUUID)),
	}
	return uuid
}

// cToNode converts a C Node_t* to a Go Node.
// If cNode is nil returns a Node with ID: -1.
func (a *AdminClient) cToNode(cNode *C.rd_kafka_Node_t) Node {
	if cNode == nil {
		return Node{
			ID: -1,
		}
	}

	node := Node{
		ID:   int(C.rd_kafka_Node_id(cNode)),
		Host: C.GoString(C.rd_kafka_Node_host(cNode)),
		Port: int(C.rd_kafka_Node_port(cNode)),
	}

	cRack := C.rd_kafka_Node_rack(cNode)
	if cRack != nil {
		rackID := C.GoString(cRack)
		node.Rack = &rackID
	}

	return node
}

// cToNodePtr converts a C Node_t* to a Go *Node.
func (a *AdminClient) cToNodePtr(cNode *C.rd_kafka_Node_t) *Node {
	if cNode == nil {
		return nil
	}

	node := a.cToNode(cNode)
	return &node
}

// cToNode converts a C Node_t array to a Go Node list.
func (a *AdminClient) cToNodes(
	cNodes **C.rd_kafka_Node_t, cNodeCnt C.size_t) []Node {
	nodes := make([]Node, int(cNodeCnt))
	for i := 0; i < int(cNodeCnt); i++ {
		cNode := C.Node_by_idx(cNodes, cNodeCnt, C.size_t(i))
		nodes[i] = a.cToNode(cNode)
	}
	return nodes
}

// cToConsumerGroupDescriptions converts a C rd_kafka_ConsumerGroupDescription_t
// array to a Go ConsumerGroupDescription slice.
func (a *AdminClient) cToConsumerGroupDescriptions(
	cGroups **C.rd_kafka_ConsumerGroupDescription_t,
	cGroupCount C.size_t) (result []ConsumerGroupDescription) {
	result = make([]ConsumerGroupDescription, cGroupCount)
	for idx := 0; idx < int(cGroupCount); idx++ {
		cGroup := C.ConsumerGroupDescription_by_idx(
			cGroups, cGroupCount, C.size_t(idx))

		groupID := C.GoString(
			C.rd_kafka_ConsumerGroupDescription_group_id(cGroup))
		err := newErrorFromCError(
			C.rd_kafka_ConsumerGroupDescription_error(cGroup))
		isSimple := cint2bool(
			C.rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(cGroup))
		paritionAssignor := C.GoString(
			C.rd_kafka_ConsumerGroupDescription_partition_assignor(cGroup))
		state := ConsumerGroupState(
			C.rd_kafka_ConsumerGroupDescription_state(cGroup))
		groupType := ConsumerGroupType(
			C.rd_kafka_ConsumerGroupDescription_type(cGroup))

		cNode := C.rd_kafka_ConsumerGroupDescription_coordinator(cGroup)
		coordinator := a.cToNode(cNode)

		membersCount := int(
			C.rd_kafka_ConsumerGroupDescription_member_count(cGroup))
		members := make([]MemberDescription, membersCount)

		for midx := 0; midx < membersCount; midx++ {
			cMember :=
				C.rd_kafka_ConsumerGroupDescription_member(cGroup, C.size_t(midx))
			cMemberAssignment :=
				C.rd_kafka_MemberDescription_assignment(cMember)
			cToppars :=
				C.rd_kafka_MemberAssignment_partitions(cMemberAssignment)
			memberAssignment := MemberAssignment{}
			if cToppars != nil {
				memberAssignment.TopicPartitions = newTopicPartitionsFromCparts(cToppars)
			}
			cMemberTargetAssignment :=
				C.rd_kafka_MemberDescription_target_assignment(cMember)
			memberTargetAssignment := &MemberAssignment{}
			if cMemberTargetAssignment != nil {
				cTargetToppars := C.rd_kafka_MemberAssignment_partitions(cMemberTargetAssignment)
				if cTargetToppars != nil {
					memberTargetAssignment.TopicPartitions = newTopicPartitionsFromCparts(cTargetToppars)
				}
			} else {
				memberTargetAssignment = nil
			}

			members[midx] = MemberDescription{
				ClientID: C.GoString(
					C.rd_kafka_MemberDescription_client_id(cMember)),
				GroupInstanceID: C.GoString(
					C.rd_kafka_MemberDescription_group_instance_id(cMember)),
				ConsumerID: C.GoString(
					C.rd_kafka_MemberDescription_consumer_id(cMember)),
				Host: C.GoString(
					C.rd_kafka_MemberDescription_host(cMember)),
				Assignment:       memberAssignment,
				TargetAssignment: memberTargetAssignment,
			}
		}

		cAuthorizedOperationsCnt := C.size_t(0)
		cAuthorizedOperations := C.rd_kafka_ConsumerGroupDescription_authorized_operations(
			cGroup, &cAuthorizedOperationsCnt)
		authorizedOperations := a.cToAuthorizedOperations(cAuthorizedOperations,
			cAuthorizedOperationsCnt)

		result[idx] = ConsumerGroupDescription{
			GroupID:               groupID,
			Error:                 err,
			IsSimpleConsumerGroup: isSimple,
			PartitionAssignor:     paritionAssignor,
			Type:                  groupType,
			State:                 state,
			Coordinator:           coordinator,
			Members:               members,
			AuthorizedOperations:  authorizedOperations,
		}
	}
	return result
}

// cToTopicPartitionInfo converts a C TopicPartitionInfo_t into a Go
// TopicPartitionInfo.
func (a *AdminClient) cToTopicPartitionInfo(
	partitionInfo *C.rd_kafka_TopicPartitionInfo_t) TopicPartitionInfo {
	cPartitionID := C.rd_kafka_TopicPartitionInfo_partition(partitionInfo)
	info := TopicPartitionInfo{
		Partition: int(cPartitionID),
	}

	cLeader := C.rd_kafka_TopicPartitionInfo_leader(partitionInfo)
	info.Leader = a.cToNodePtr(cLeader)

	cReplicaCnt := C.size_t(0)
	cReplicas := C.rd_kafka_TopicPartitionInfo_replicas(
		partitionInfo, &cReplicaCnt)
	info.Replicas = a.cToNodes(cReplicas, cReplicaCnt)

	cIsrCnt := C.size_t(0)
	cIsr := C.rd_kafka_TopicPartitionInfo_isr(partitionInfo, &cIsrCnt)
	info.Isr = a.cToNodes(cIsr, cIsrCnt)

	return info
}

// cToTopicDescriptions converts a C TopicDescription_t
// array to a Go TopicDescription list.
func (a *AdminClient) cToTopicDescriptions(
	cTopicDescriptions **C.rd_kafka_TopicDescription_t,
	cTopicDescriptionCount C.size_t) (result []TopicDescription) {
	result = make([]TopicDescription, cTopicDescriptionCount)
	for idx := 0; idx < int(cTopicDescriptionCount); idx++ {
		cTopic := C.TopicDescription_by_idx(
			cTopicDescriptions, cTopicDescriptionCount, C.size_t(idx))

		topicName := C.GoString(
			C.rd_kafka_TopicDescription_name(cTopic))
		TopicID := a.cToUUID(C.rd_kafka_TopicDescription_topic_id(cTopic))
		err := newErrorFromCError(
			C.rd_kafka_TopicDescription_error(cTopic))

		if err.Code() != ErrNoError {
			result[idx] = TopicDescription{
				Name:  topicName,
				Error: err,
			}
			continue
		}

		cPartitionInfoCnt := C.size_t(0)
		cPartitionInfos := C.rd_kafka_TopicDescription_partitions(cTopic, &cPartitionInfoCnt)

		partitions := make([]TopicPartitionInfo, int(cPartitionInfoCnt))

		for pidx := 0; pidx < int(cPartitionInfoCnt); pidx++ {
			cPartitionInfo := C.TopicPartitionInfo_by_idx(cPartitionInfos, cPartitionInfoCnt, C.size_t(pidx))
			partitions[pidx] = a.cToTopicPartitionInfo(cPartitionInfo)
		}

		cAuthorizedOperationsCnt := C.size_t(0)
		cAuthorizedOperations := C.rd_kafka_TopicDescription_authorized_operations(
			cTopic, &cAuthorizedOperationsCnt)
		authorizedOperations := a.cToAuthorizedOperations(cAuthorizedOperations, cAuthorizedOperationsCnt)

		result[idx] = TopicDescription{
			Name:                 topicName,
			TopicID:              TopicID,
			Error:                err,
			Partitions:           partitions,
			AuthorizedOperations: authorizedOperations,
		}
	}
	return result
}

// cToDescribeClusterResult converts a C DescribeTopics_result_t to a Go
// DescribeClusterResult.
func (a *AdminClient) cToDescribeClusterResult(
	cResult *C.rd_kafka_DescribeTopics_result_t) (result DescribeClusterResult) {
	var clusterIDPtr *string = nil
	cClusterID := C.rd_kafka_DescribeCluster_result_cluster_id(cResult)
	if cClusterID != nil {
		clusterID := C.GoString(cClusterID)
		clusterIDPtr = &clusterID
	}

	var controller *Node = nil
	cController := C.rd_kafka_DescribeCluster_result_controller(cResult)
	controller = a.cToNodePtr(cController)

	cNodeCnt := C.size_t(0)
	cNodes := C.rd_kafka_DescribeCluster_result_nodes(cResult, &cNodeCnt)
	nodes := a.cToNodes(cNodes, cNodeCnt)

	cAuthorizedOperationsCnt := C.size_t(0)
	cAuthorizedOperations :=
		C.rd_kafka_DescribeCluster_result_authorized_operations(
			cResult, &cAuthorizedOperationsCnt)
	authorizedOperations := a.cToAuthorizedOperations(
		cAuthorizedOperations, cAuthorizedOperationsCnt)

	return DescribeClusterResult{
		ClusterID:            clusterIDPtr,
		Controller:           controller,
		Nodes:                nodes,
		AuthorizedOperations: authorizedOperations,
	}
}

// cToDescribeUserScramCredentialsResult converts a C
// rd_kafka_DescribeUserScramCredentials_result_t to a Go map of users to
// UserScramCredentialsDescription.
func cToDescribeUserScramCredentialsResult(
	cRes *C.rd_kafka_DescribeUserScramCredentials_result_t) map[string]UserScramCredentialsDescription {
	result := make(map[string]UserScramCredentialsDescription)
	var cDescriptionCount C.size_t
	cDescriptions :=
		C.rd_kafka_DescribeUserScramCredentials_result_descriptions(cRes,
			&cDescriptionCount)

	for i := 0; i < int(cDescriptionCount); i++ {
		cDescription :=
			C.DescribeUserScramCredentials_result_description_by_idx(
				cDescriptions, cDescriptionCount, C.size_t(i))
		user := C.GoString(C.rd_kafka_UserScramCredentialsDescription_user(cDescription))
		userDescription := UserScramCredentialsDescription{User: user}

		// Populate the error if required.
		cError := C.rd_kafka_UserScramCredentialsDescription_error(cDescription)
		if C.rd_kafka_error_code(cError) != C.RD_KAFKA_RESP_ERR_NO_ERROR {
			userDescription.Error = newError(C.rd_kafka_error_code(cError))
			result[user] = userDescription
			continue
		}

		cCredentialCount := C.rd_kafka_UserScramCredentialsDescription_scramcredentialinfo_count(cDescription)
		scramCredentialInfos := make([]ScramCredentialInfo, int(cCredentialCount))
		for j := 0; j < int(cCredentialCount); j++ {
			cScramCredentialInfo :=
				C.rd_kafka_UserScramCredentialsDescription_scramcredentialinfo(
					cDescription, C.size_t(j))
			cMechanism := C.rd_kafka_ScramCredentialInfo_mechanism(cScramCredentialInfo)
			cIterations := C.rd_kafka_ScramCredentialInfo_iterations(cScramCredentialInfo)
			scramCredentialInfos[j] = ScramCredentialInfo{
				Mechanism:  ScramMechanism(cMechanism),
				Iterations: int(cIterations),
			}
		}
		userDescription.ScramCredentialInfos = scramCredentialInfos
		result[user] = userDescription
	}
	return result
}

// cToListOffsetsResult converts a C
// rd_kafka_ListOffsets_result_t to a Go ListOffsetsResult
func cToListOffsetsResult(cRes *C.rd_kafka_ListOffsets_result_t) (result ListOffsetsResult) {
	result = ListOffsetsResult{ResultInfos: make(map[TopicPartition]ListOffsetsResultInfo)}
	var cPartitionCount C.size_t
	cResultInfos := C.rd_kafka_ListOffsets_result_infos(cRes, &cPartitionCount)
	for itr := 0; itr < int(cPartitionCount); itr++ {
		cResultInfo := C.ListOffsetsResultInfo_by_idx(cResultInfos, cPartitionCount, C.size_t(itr))
		resultInfo := ListOffsetsResultInfo{}
		cPartition := C.rd_kafka_ListOffsetsResultInfo_topic_partition(cResultInfo)
		Topic := C.GoString(cPartition.topic)
		Partition := TopicPartition{Topic: &Topic, Partition: int32(cPartition.partition)}
		resultInfo.Offset = Offset(cPartition.offset)
		resultInfo.Timestamp = int64(C.rd_kafka_ListOffsetsResultInfo_timestamp(cResultInfo))
		cLeaderEpoch := int32(C.rd_kafka_topic_partition_get_leader_epoch(cPartition))
		if cLeaderEpoch >= 0 {
			resultInfo.LeaderEpoch = &cLeaderEpoch
		}
		resultInfo.Error = newError(cPartition.err)
		result.ResultInfos[Partition] = resultInfo
	}
	return result
}

// ConsumerGroupDescription converts a C rd_kafka_ConsumerGroupListing_t array
// to a Go ConsumerGroupListing slice.
func (a *AdminClient) cToConsumerGroupListings(
	cGroups **C.rd_kafka_ConsumerGroupListing_t,
	cGroupCount C.size_t) (result []ConsumerGroupListing) {
	result = make([]ConsumerGroupListing, cGroupCount)

	for idx := 0; idx < int(cGroupCount); idx++ {
		cGroup :=
			C.ConsumerGroupListing_by_idx(cGroups, cGroupCount, C.size_t(idx))
		state := ConsumerGroupState(
			C.rd_kafka_ConsumerGroupListing_state(cGroup))
		groupType := ConsumerGroupType(C.rd_kafka_ConsumerGroupListing_type(cGroup))
		result[idx] = ConsumerGroupListing{
			GroupID: C.GoString(
				C.rd_kafka_ConsumerGroupListing_group_id(cGroup)),
			IsSimpleConsumerGroup: cint2bool(
				C.rd_kafka_ConsumerGroupListing_is_simple_consumer_group(cGroup)),
			State: state,
			Type:  groupType,
		}
	}
	return result
}

// cToErrorList converts a C rd_kafka_error_t array to a Go errors slice.
func (a *AdminClient) cToErrorList(
	cErrs **C.rd_kafka_error_t, cErrCount C.size_t) (errs []error) {
	errs = make([]error, cErrCount)

	for idx := 0; idx < int(cErrCount); idx++ {
		cErr := C.error_by_idx(cErrs, cErrCount, C.size_t(idx))
		errs[idx] = newErrorFromCError(cErr)
	}

	return errs
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

// setupTopicPartitionFromCtopicPartitionResult sets up a Go TopicPartition from a C rd_kafka_topic_partition_t & C.rd_kafka_error_t.
func setupTopicPartitionFromCtopicPartitionResult(partition *TopicPartition, ctopicPartRes *C.rd_kafka_topic_partition_result_t) {

	setupTopicPartitionFromCrktpar(partition, C.rd_kafka_topic_partition_result_partition(ctopicPartRes))
	partition.Error = newErrorFromCError(C.rd_kafka_topic_partition_result_error(ctopicPartRes))
}

// Convert a C rd_kafka_topic_partition_result_t array to a Go TopicPartition list.
func newTopicPartitionsFromCTopicPartitionResult(cResponse **C.rd_kafka_topic_partition_result_t, size C.size_t) (partitions []TopicPartition) {

	partCnt := int(size)

	partitions = make([]TopicPartition, partCnt)

	for i := 0; i < partCnt; i++ {
		setupTopicPartitionFromCtopicPartitionResult(&partitions[i], C.TopicPartitionResult_by_idx(cResponse, C.size_t(partCnt), C.size_t(i)))
	}

	return partitions
}

// cToDeletedRecordResult converts a C topic partitions list to a Go DeleteRecordsResult slice.
func cToDeletedRecordResult(
	cparts *C.rd_kafka_topic_partition_list_t) (results []DeleteRecordsResult) {
	partitions := newTopicPartitionsFromCparts(cparts)
	partitionsLen := len(partitions)
	results = make([]DeleteRecordsResult, partitionsLen)

	for i := 0; i < partitionsLen; i++ {
		results[i].TopicPartition = partitions[i]
		if results[i].TopicPartition.Error == nil {
			results[i].DeletedRecords = &DeletedRecords{
				LowWatermark: results[i].TopicPartition.Offset}
		}
	}

	return results
}

// ClusterID returns the cluster ID as reported in broker metadata.
//
// Note on cancellation: Although the underlying C function respects the
// timeout, it currently cannot be manually cancelled. That means manually
// cancelling the context will block until the C function call returns.
//
// Requires broker version >= 0.10.0.
func (a *AdminClient) ClusterID(ctx context.Context) (clusterID string, err error) {
	err = a.verifyClient()
	if err != nil {
		return "", err
	}

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
	err = a.verifyClient()
	if err != nil {
		return -1, err
	}

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
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

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
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

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
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

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
// Deprecated: AlterConfigs is deprecated in favour of IncrementalAlterConfigs
func (a *AdminClient) AlterConfigs(ctx context.Context, resources []ConfigResource, options ...AlterConfigsAdminOption) (result []ConfigResourceResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

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

// IncrementalAlterConfigs alters/updates cluster resource configuration.
//
// Updates are not transactional so they may succeed for some resources
// while fail for others. The configs for a particular resource are
// updated atomically, executing the corresponding incremental
// operations on the provided configurations.
//
// Requires broker version >=2.3.0
//
// IncrementalAlterConfigs will only change configurations for provided
// resources with the new configuration given.
//
// Multiple resources and resource types may be set, but at most one
// resource of type ResourceBroker is allowed per call since these
// resource requests must be sent to the broker specified in the resource.
func (a *AdminClient) IncrementalAlterConfigs(ctx context.Context, resources []ConfigResource, options ...AlterConfigsAdminOption) (result []ConfigResourceResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	cRes := make([]*C.rd_kafka_ConfigResource_t, len(resources))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go ConfigResources to C ConfigResources
	for i, res := range resources {
		cName := C.CString(res.Name)
		defer C.free(unsafe.Pointer(cName))
		cRes[i] = C.rd_kafka_ConfigResource_new(
			C.rd_kafka_ResourceType_t(res.Type), cName)
		if cRes[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Invalid arguments for resource %v", res))
		}

		defer C.rd_kafka_ConfigResource_destroy(cRes[i])

		for _, entry := range res.Config {
			cName := C.CString(entry.Name)
			defer C.free(unsafe.Pointer(cName))
			cValue := C.CString(entry.Value)
			defer C.free(unsafe.Pointer(cValue))
			cError := C.rd_kafka_ConfigResource_add_incremental_config(
				cRes[i], cName,
				C.rd_kafka_AlterConfigOpType_t(entry.IncrementalOperation),
				cValue)

			if cError != nil {
				err := newErrorFromCErrorDestroy(cError)
				return nil, err
			}
		}
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_INCREMENTALALTERCONFIGS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_IncrementalAlterConfigs(
		a.handle.rk,
		(**C.rd_kafka_ConfigResource_t)(&cRes[0]),
		C.size_t(len(cRes)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_INCREMENTALALTERCONFIGS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cResult := C.rd_kafka_event_IncrementalAlterConfigs_result(rkev)

	// Convert results from C to Go
	var cCnt C.size_t
	cResults := C.rd_kafka_IncrementalAlterConfigs_result_resources(cResult, &cCnt)

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
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

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
	err := a.verifyClient()
	if err != nil {
		return nil, err
	}
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
	err := a.verifyClient()
	if err != nil {
		return err
	}
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
	err := a.verifyClient()
	if err != nil {
		return err
	}
	return a.handle.setOAuthBearerTokenFailure(errstr)
}

// aclBindingToC converts a Go ACLBinding struct to a C rd_kafka_AclBinding_t
func (a *AdminClient) aclBindingToC(aclBinding *ACLBinding, cErrstr *C.char, cErrstrSize C.size_t) (result *C.rd_kafka_AclBinding_t, err error) {
	var cName, cPrincipal, cHost *C.char
	cName, cPrincipal, cHost = nil, nil, nil
	if len(aclBinding.Name) > 0 {
		cName = C.CString(aclBinding.Name)
		defer C.free(unsafe.Pointer(cName))
	}
	if len(aclBinding.Principal) > 0 {
		cPrincipal = C.CString(aclBinding.Principal)
		defer C.free(unsafe.Pointer(cPrincipal))
	}
	if len(aclBinding.Host) > 0 {
		cHost = C.CString(aclBinding.Host)
		defer C.free(unsafe.Pointer(cHost))
	}

	result = C.rd_kafka_AclBinding_new(
		C.rd_kafka_ResourceType_t(aclBinding.Type),
		cName,
		C.rd_kafka_ResourcePatternType_t(aclBinding.ResourcePatternType),
		cPrincipal,
		cHost,
		C.rd_kafka_AclOperation_t(aclBinding.Operation),
		C.rd_kafka_AclPermissionType_t(aclBinding.PermissionType),
		cErrstr,
		cErrstrSize,
	)
	if result == nil {
		err = newErrorFromString(ErrInvalidArg,
			fmt.Sprintf("Invalid arguments for ACL binding %v: %v", aclBinding, C.GoString(cErrstr)))
	}
	return
}

// aclBindingFilterToC converts a Go ACLBindingFilter struct to a C rd_kafka_AclBindingFilter_t
func (a *AdminClient) aclBindingFilterToC(aclBindingFilter *ACLBindingFilter, cErrstr *C.char, cErrstrSize C.size_t) (result *C.rd_kafka_AclBindingFilter_t, err error) {
	var cName, cPrincipal, cHost *C.char
	cName, cPrincipal, cHost = nil, nil, nil
	if len(aclBindingFilter.Name) > 0 {
		cName = C.CString(aclBindingFilter.Name)
		defer C.free(unsafe.Pointer(cName))
	}
	if len(aclBindingFilter.Principal) > 0 {
		cPrincipal = C.CString(aclBindingFilter.Principal)
		defer C.free(unsafe.Pointer(cPrincipal))
	}
	if len(aclBindingFilter.Host) > 0 {
		cHost = C.CString(aclBindingFilter.Host)
		defer C.free(unsafe.Pointer(cHost))
	}

	result = C.rd_kafka_AclBindingFilter_new(
		C.rd_kafka_ResourceType_t(aclBindingFilter.Type),
		cName,
		C.rd_kafka_ResourcePatternType_t(aclBindingFilter.ResourcePatternType),
		cPrincipal,
		cHost,
		C.rd_kafka_AclOperation_t(aclBindingFilter.Operation),
		C.rd_kafka_AclPermissionType_t(aclBindingFilter.PermissionType),
		cErrstr,
		cErrstrSize,
	)
	if result == nil {
		err = newErrorFromString(ErrInvalidArg,
			fmt.Sprintf("Invalid arguments for ACL binding filter %v: %v", aclBindingFilter, C.GoString(cErrstr)))
	}
	return
}

// cToACLBinding converts a C rd_kafka_AclBinding_t to Go ACLBinding
func (a *AdminClient) cToACLBinding(cACLBinding *C.rd_kafka_AclBinding_t) ACLBinding {
	return ACLBinding{
		ResourceType(C.rd_kafka_AclBinding_restype(cACLBinding)),
		C.GoString(C.rd_kafka_AclBinding_name(cACLBinding)),
		ResourcePatternType(C.rd_kafka_AclBinding_resource_pattern_type(cACLBinding)),
		C.GoString(C.rd_kafka_AclBinding_principal(cACLBinding)),
		C.GoString(C.rd_kafka_AclBinding_host(cACLBinding)),
		ACLOperation(C.rd_kafka_AclBinding_operation(cACLBinding)),
		ACLPermissionType(C.rd_kafka_AclBinding_permission_type(cACLBinding)),
	}
}

// cToACLBindings converts a C rd_kafka_AclBinding_t list to Go ACLBindings
func (a *AdminClient) cToACLBindings(cACLBindings **C.rd_kafka_AclBinding_t, aclCnt C.size_t) (result ACLBindings) {
	result = make(ACLBindings, aclCnt)
	for i := uint(0); i < uint(aclCnt); i++ {
		cACLBinding := C.AclBinding_by_idx(cACLBindings, aclCnt, C.size_t(i))
		if cACLBinding == nil {
			panic("AclBinding_by_idx must not return nil")
		}
		result[i] = a.cToACLBinding(cACLBinding)
	}
	return
}

// cToCreateACLResults converts a C acl_result_t array to Go CreateACLResult list.
func (a *AdminClient) cToCreateACLResults(cCreateAclsRes **C.rd_kafka_acl_result_t, aclCnt C.size_t) (result []CreateACLResult, err error) {
	result = make([]CreateACLResult, uint(aclCnt))

	for i := uint(0); i < uint(aclCnt); i++ {
		cCreateACLRes := C.acl_result_by_idx(cCreateAclsRes, aclCnt, C.size_t(i))
		if cCreateACLRes != nil {
			cCreateACLError := C.rd_kafka_acl_result_error(cCreateACLRes)
			result[i].Error = newErrorFromCError(cCreateACLError)
		}
	}

	return result, nil
}

// cToDescribeACLsResult converts a C rd_kafka_event_t to a Go DescribeAclsResult struct.
func (a *AdminClient) cToDescribeACLsResult(rkev *C.rd_kafka_event_t) (result *DescribeACLsResult) {
	result = &DescribeACLsResult{}
	err := C.rd_kafka_event_error(rkev)
	errCode := ErrorCode(err)
	errStr := C.rd_kafka_event_error_string(rkev)

	var cResultACLsCount C.size_t
	cResult := C.rd_kafka_event_DescribeAcls_result(rkev)
	cResultACLs := C.rd_kafka_DescribeAcls_result_acls(cResult, &cResultACLsCount)
	if errCode != ErrNoError {
		result.Error = newErrorFromCString(err, errStr)
	}
	result.ACLBindings = a.cToACLBindings(cResultACLs, cResultACLsCount)
	return
}

// cToDeleteACLsResults converts a C rd_kafka_DeleteAcls_result_response_t array to Go DeleteAclsResult slice.
func (a *AdminClient) cToDeleteACLsResults(cDeleteACLsResResponse **C.rd_kafka_DeleteAcls_result_response_t, resResponseCnt C.size_t) (result []DeleteACLsResult) {
	result = make([]DeleteACLsResult, uint(resResponseCnt))

	for i := uint(0); i < uint(resResponseCnt); i++ {
		cDeleteACLsResResponse := C.DeleteAcls_result_response_by_idx(cDeleteACLsResResponse, resResponseCnt, C.size_t(i))
		if cDeleteACLsResResponse == nil {
			panic("DeleteAcls_result_response_by_idx must not return nil")
		}

		cDeleteACLsError := C.rd_kafka_DeleteAcls_result_response_error(cDeleteACLsResResponse)
		result[i].Error = newErrorFromCError(cDeleteACLsError)

		var cMatchingACLsCount C.size_t
		cMatchingACLs := C.rd_kafka_DeleteAcls_result_response_matching_acls(
			cDeleteACLsResResponse, &cMatchingACLsCount)

		result[i].ACLBindings = a.cToACLBindings(cMatchingACLs, cMatchingACLsCount)
	}
	return
}

// CreateACLs creates one or more ACL bindings.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for indefinite.
//   - `aclBindings` - A slice of ACL binding specifications to create.
//   - `options` - Create ACLs options
//
// Returns a slice of CreateACLResult with a ErrNoError ErrorCode when the operation was successful
// plus an error that is not nil for client level errors
func (a *AdminClient) CreateACLs(ctx context.Context, aclBindings ACLBindings, options ...CreateACLsAdminOption) (result []CreateACLResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	if aclBindings == nil {
		return nil, newErrorFromString(ErrInvalidArg,
			"Expected non-nil slice of ACLBinding structs")
	}
	if len(aclBindings) == 0 {
		return nil, newErrorFromString(ErrInvalidArg,
			"Expected non-empty slice of ACLBinding structs")
	}

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	cACLBindings := make([]*C.rd_kafka_AclBinding_t, len(aclBindings))

	for i, aclBinding := range aclBindings {
		cACLBindings[i], err = a.aclBindingToC(&aclBinding, cErrstr, cErrstrSize)
		if err != nil {
			return
		}
		defer C.rd_kafka_AclBinding_destroy(cACLBindings[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_CREATEACLS, genericOptions)
	if err != nil {
		return nil, err
	}

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_CreateAcls(
		a.handle.rk,
		(**C.rd_kafka_AclBinding_t)(&cACLBindings[0]),
		C.size_t(len(cACLBindings)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_CREATEACLS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	var cResultCnt C.size_t
	cResult := C.rd_kafka_event_CreateAcls_result(rkev)
	aclResults := C.rd_kafka_CreateAcls_result_acls(cResult, &cResultCnt)
	result, err = a.cToCreateACLResults(aclResults, cResultCnt)
	return
}

// DescribeACLs matches ACL bindings by filter.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for indefinite.
//   - `aclBindingFilter` - A filter with attributes that must match.
//     string attributes match exact values or any string if set to empty string.
//     Enum attributes match exact values or any value if ending with `Any`.
//     If `ResourcePatternType` is set to `ResourcePatternTypeMatch` returns ACL bindings with:
//   - `ResourcePatternTypeLiteral` pattern type with resource name equal to the given resource name
//   - `ResourcePatternTypeLiteral` pattern type with wildcard resource name that matches the given resource name
//   - `ResourcePatternTypePrefixed` pattern type with resource name that is a prefix of the given resource name
//   - `options` - Describe ACLs options
//
// Returns a slice of ACLBindings when the operation was successful
// plus an error that is not `nil` for client level errors
func (a *AdminClient) DescribeACLs(ctx context.Context, aclBindingFilter ACLBindingFilter, options ...DescribeACLsAdminOption) (result *DescribeACLsResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	cACLBindingFilter, err := a.aclBindingFilterToC(&aclBindingFilter, cErrstr, cErrstrSize)
	if err != nil {
		return
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_DESCRIBEACLS, genericOptions)
	if err != nil {
		return nil, err
	}
	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_DescribeAcls(
		a.handle.rk,
		cACLBindingFilter,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBEACLS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)
	result = a.cToDescribeACLsResult(rkev)
	return
}

// DeleteACLs deletes ACL bindings matching one or more ACL binding filters.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for indefinite.
//   - `aclBindingFilters` - a slice of ACL binding filters to match ACLs to delete.
//     string attributes match exact values or any string if set to empty string.
//     Enum attributes match exact values or any value if ending with `Any`.
//     If `ResourcePatternType` is set to `ResourcePatternTypeMatch` deletes ACL bindings with:
//   - `ResourcePatternTypeLiteral` pattern type with resource name equal to the given resource name
//   - `ResourcePatternTypeLiteral` pattern type with wildcard resource name that matches the given resource name
//   - `ResourcePatternTypePrefixed` pattern type with resource name that is a prefix of the given resource name
//   - `options` - Delete ACLs options
//
// Returns a slice of ACLBinding for each filter when the operation was successful
// plus an error that is not `nil` for client level errors
func (a *AdminClient) DeleteACLs(ctx context.Context, aclBindingFilters ACLBindingFilters, options ...DeleteACLsAdminOption) (result []DeleteACLsResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	if aclBindingFilters == nil {
		return nil, newErrorFromString(ErrInvalidArg,
			"Expected non-nil slice of ACLBindingFilter structs")
	}
	if len(aclBindingFilters) == 0 {
		return nil, newErrorFromString(ErrInvalidArg,
			"Expected non-empty slice of ACLBindingFilter structs")
	}

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	cACLBindingFilters := make([]*C.rd_kafka_AclBindingFilter_t, len(aclBindingFilters))

	for i, aclBindingFilter := range aclBindingFilters {
		cACLBindingFilters[i], err = a.aclBindingFilterToC(&aclBindingFilter, cErrstr, cErrstrSize)
		if err != nil {
			return
		}
		defer C.rd_kafka_AclBinding_destroy(cACLBindingFilters[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_DELETEACLS, genericOptions)
	if err != nil {
		return nil, err
	}
	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_DeleteAcls(
		a.handle.rk,
		(**C.rd_kafka_AclBindingFilter_t)(&cACLBindingFilters[0]),
		C.size_t(len(cACLBindingFilters)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_DELETEACLS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	var cResultResponsesCount C.size_t
	cResult := C.rd_kafka_event_DeleteAcls_result(rkev)
	cResultResponses := C.rd_kafka_DeleteAcls_result_responses(cResult, &cResultResponsesCount)
	result = a.cToDeleteACLsResults(cResultResponses, cResultResponsesCount)
	return
}

// SetSaslCredentials sets the SASL credentials used for this admin client.
// The new credentials will overwrite the old ones (which were set when creating
// the admin client or by a previous call to SetSaslCredentials). The new
// credentials will be used the next time the admin client needs to authenticate
// to a broker. This method will not disconnect existing broker connections that
// were established with the old credentials.
// This method applies only to the SASL PLAIN and SCRAM mechanisms.
func (a *AdminClient) SetSaslCredentials(username, password string) error {
	err := a.verifyClient()
	if err != nil {
		return err
	}

	return setSaslCredentials(a.handle.rk, username, password)
}

// Close an AdminClient instance.
func (a *AdminClient) Close() {
	if !atomic.CompareAndSwapUint32(&a.isClosed, 0, 1) {
		return
	}
	if a.isDerived {
		// Derived AdminClient needs no cleanup.
		a.handle = &handle{}
		return
	}

	a.handle.cleanup()

	C.rd_kafka_destroy(a.handle.rk)
}

// ListConsumerGroups lists the consumer groups available in the cluster.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `options` - ListConsumerGroupsAdminOption options.
//
// Returns a ListConsumerGroupsResult, which contains a slice corresponding to
// each group in the cluster and a slice of errors encountered while listing.
// Additionally, an error that is not nil for client-level errors is returned.
// Both the returned error, and the errors slice should be checked.
func (a *AdminClient) ListConsumerGroups(
	ctx context.Context,
	options ...ListConsumerGroupsAdminOption) (result ListConsumerGroupsResult, err error) {

	result = ListConsumerGroupsResult{}
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle,
		C.RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_ListConsumerGroups (asynchronous).
	C.rd_kafka_ListConsumerGroups(
		a.handle.rk,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_ListConsumerGroups_result(rkev)

	// Convert result and broker errors from C to Go.
	var cGroupCount C.size_t
	cGroups := C.rd_kafka_ListConsumerGroups_result_valid(cRes, &cGroupCount)
	result.Valid = a.cToConsumerGroupListings(cGroups, cGroupCount)

	var cErrsCount C.size_t
	cErrs := C.rd_kafka_ListConsumerGroups_result_errors(cRes, &cErrsCount)
	if cErrsCount == 0 {
		return result, nil
	}

	result.Errors = a.cToErrorList(cErrs, cErrsCount)
	return result, nil
}

// DescribeConsumerGroups describes groups from cluster as specified by the
// groups list.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `groups` - Slice of groups to describe. This should not be nil/empty.
//   - `options` - DescribeConsumerGroupsAdminOption options.
//
// Returns DescribeConsumerGroupsResult, which contains a slice of
// ConsumerGroupDescriptions corresponding to the input groups, plus an error
// that is not `nil` for client level errors. Individual
// ConsumerGroupDescriptions inside the slice should also be checked for
// errors.
func (a *AdminClient) DescribeConsumerGroups(
	ctx context.Context, groups []string,
	options ...DescribeConsumerGroupsAdminOption) (result DescribeConsumerGroupsResult, err error) {

	describeResult := DescribeConsumerGroupsResult{}
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	// Convert group names into char** required by the implementation.
	cGroupNameList := make([]*C.char, len(groups))
	cGroupNameCount := C.size_t(len(groups))

	for idx, group := range groups {
		cGroupNameList[idx] = C.CString(group)
		defer C.free(unsafe.Pointer(cGroupNameList[idx]))
	}

	var cGroupNameListPtr **C.char
	if cGroupNameCount > 0 {
		cGroupNameListPtr = ((**C.char)(&cGroupNameList[0]))
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS, genericOptions)
	if err != nil {
		return describeResult, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_DescribeConsumerGroups (asynchronous).
	C.rd_kafka_DescribeConsumerGroups(
		a.handle.rk,
		cGroupNameListPtr,
		cGroupNameCount,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT)
	if err != nil {
		return describeResult, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DescribeConsumerGroups_result(rkev)

	// Convert result from C to Go.
	var cGroupCount C.size_t
	cGroups := C.rd_kafka_DescribeConsumerGroups_result_groups(cRes, &cGroupCount)
	describeResult.ConsumerGroupDescriptions = a.cToConsumerGroupDescriptions(cGroups, cGroupCount)

	return describeResult, nil
}

// DescribeTopics describes topics from cluster as specified by the
// topics list.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `topics` - Collection of topics to describe. This should not have nil
//     topic names.
//   - `options` - DescribeTopicsAdminOption options.
//
// Returns DescribeTopicsResult, which contains a slice of
// TopicDescriptions corresponding to the input topics, plus an error
// that is not `nil` for client level errors. Individual
// TopicDescriptions inside the slice should also be checked for
// errors. Individual TopicDescriptions also have a
// slice of allowed ACLOperations.
func (a *AdminClient) DescribeTopics(
	ctx context.Context, topics TopicCollection,
	options ...DescribeTopicsAdminOption) (result DescribeTopicsResult, err error) {

	describeResult := DescribeTopicsResult{}
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	// Convert topic names into char**.
	cTopicNameList := make([]*C.char, len(topics.topicNames))
	cTopicNameCount := C.size_t(len(topics.topicNames))

	if topics.topicNames == nil {
		return describeResult, newErrorFromString(ErrInvalidArg,
			"TopicCollection of topic names cannot be nil")
	}

	for idx, topic := range topics.topicNames {
		cTopicNameList[idx] = C.CString(topic)
		defer C.free(unsafe.Pointer(cTopicNameList[idx]))
	}

	var cTopicNameListPtr **C.char
	if cTopicNameCount > 0 {
		cTopicNameListPtr = ((**C.char)(&cTopicNameList[0]))
	}

	// Convert char** of topic names into rd_kafka_TopicCollection_t*
	cTopicCollection := C.rd_kafka_TopicCollection_of_topic_names(
		cTopicNameListPtr, cTopicNameCount)
	defer C.rd_kafka_TopicCollection_destroy(cTopicCollection)

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_DESCRIBETOPICS, genericOptions)
	if err != nil {
		return describeResult, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_DescribeTopics (asynchronous).
	C.rd_kafka_DescribeTopics(
		a.handle.rk,
		cTopicCollection,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT)
	if err != nil {
		return describeResult, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DescribeTopics_result(rkev)

	// Convert result from C to Go.
	var cTopicDescriptionCount C.size_t
	cTopicDescriptions :=
		C.rd_kafka_DescribeTopics_result_topics(cRes, &cTopicDescriptionCount)
	describeResult.TopicDescriptions =
		a.cToTopicDescriptions(cTopicDescriptions, cTopicDescriptionCount)

	return describeResult, nil
}

// DescribeCluster describes the cluster
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `options` - DescribeClusterAdminOption options.
//
// Returns ClusterDescription, which contains current cluster ID and controller
// along with a slice of Nodes. It also has a slice of allowed ACLOperations.
func (a *AdminClient) DescribeCluster(
	ctx context.Context,
	options ...DescribeClusterAdminOption) (result DescribeClusterResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return result, err
	}
	clusterDesc := DescribeClusterResult{}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER, genericOptions)
	if err != nil {
		return clusterDesc, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_DescribeCluster (asynchronous).
	C.rd_kafka_DescribeCluster(
		a.handle.rk,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBECLUSTER_RESULT)
	if err != nil {
		return clusterDesc, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DescribeCluster_result(rkev)

	// Convert result from C to Go.
	clusterDesc = a.cToDescribeClusterResult(cRes)

	return clusterDesc, nil
}

// DeleteConsumerGroups deletes a batch of consumer groups.
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `groups` - A slice of groupIDs to delete.
//   - `options` - DeleteConsumerGroupsAdminOption options.
//
// Returns a DeleteConsumerGroupsResult containing a slice of ConsumerGroupResult, with
// group-level errors, (if any) contained inside; and an error that is not nil
// for client level errors.
func (a *AdminClient) DeleteConsumerGroups(
	ctx context.Context,
	groups []string, options ...DeleteConsumerGroupsAdminOption) (result DeleteConsumerGroupsResult, err error) {
	cGroups := make([]*C.rd_kafka_DeleteGroup_t, len(groups))
	deleteResult := DeleteConsumerGroupsResult{}
	err = a.verifyClient()
	if err != nil {
		return deleteResult, err
	}

	// Convert Go DeleteGroups to C DeleteGroups
	for i, group := range groups {
		cGroupID := C.CString(group)
		defer C.free(unsafe.Pointer(cGroupID))

		cGroups[i] = C.rd_kafka_DeleteGroup_new(cGroupID)
		if cGroups[i] == nil {
			return deleteResult, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Invalid arguments for group %s", group))
		}

		defer C.rd_kafka_DeleteGroup_destroy(cGroups[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_DELETEGROUPS, genericOptions)
	if err != nil {
		return deleteResult, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_DeleteGroups(
		a.handle.rk,
		(**C.rd_kafka_DeleteGroup_t)(&cGroups[0]),
		C.size_t(len(cGroups)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_DELETEGROUPS_RESULT)
	if err != nil {
		return deleteResult, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DeleteGroups_result(rkev)

	// Convert result from C to Go
	var cCnt C.size_t
	cGroupRes := C.rd_kafka_DeleteGroups_result_groups(cRes, &cCnt)

	deleteResult.ConsumerGroupResults, err = a.cToConsumerGroupResults(cGroupRes, cCnt)
	return deleteResult, err
}

// ListConsumerGroupOffsets fetches the offsets for topic partition(s) for
// consumer group(s).
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for indefinite.
//   - `groupsPartitions` - a slice of ConsumerGroupTopicPartitions, each element of which
//     has the id of a consumer group, and a slice of the TopicPartitions we
//     need to fetch the offsets for. The slice of TopicPartitions can be nil, to fetch
//     all topic partitions for that group.
//     Currently, the size of `groupsPartitions` has to be exactly one.
//   - `options` - ListConsumerGroupOffsetsAdminOption options.
//
// Returns a ListConsumerGroupOffsetsResult, containing a slice of
// ConsumerGroupTopicPartitions corresponding to the input slice, plus an error that is
// not `nil` for client level errors. Individual TopicPartitions inside each of
// the ConsumerGroupTopicPartitions should also be checked for errors.
func (a *AdminClient) ListConsumerGroupOffsets(
	ctx context.Context, groupsPartitions []ConsumerGroupTopicPartitions,
	options ...ListConsumerGroupOffsetsAdminOption) (lcgor ListConsumerGroupOffsetsResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return lcgor, err
	}

	lcgor.ConsumerGroupsTopicPartitions = nil

	// For now, we only support one group at a time given as a single element of
	// groupsPartitions.
	// Code has been written so that only this if-guard needs to be removed when
	// we add support for multiple ConsumerGroupTopicPartitions.
	if len(groupsPartitions) != 1 {
		return lcgor, fmt.Errorf(
			"expected length of groupsPartitions is 1, got %d", len(groupsPartitions))
	}

	cGroupsPartitions := make([]*C.rd_kafka_ListConsumerGroupOffsets_t,
		len(groupsPartitions))

	// Convert Go ConsumerGroupTopicPartitions to C ListConsumerGroupOffsets.
	for i, groupPartitions := range groupsPartitions {
		// We need to destroy this list because rd_kafka_ListConsumerGroupOffsets_new
		// creates a copy of it.
		var cPartitions *C.rd_kafka_topic_partition_list_t = nil

		if groupPartitions.Partitions != nil {
			cPartitions = newCPartsFromTopicPartitions(groupPartitions.Partitions)
			defer C.rd_kafka_topic_partition_list_destroy(cPartitions)
		}

		cGroupID := C.CString(groupPartitions.Group)
		defer C.free(unsafe.Pointer(cGroupID))

		cGroupsPartitions[i] =
			C.rd_kafka_ListConsumerGroupOffsets_new(cGroupID, cPartitions)
		defer C.rd_kafka_ListConsumerGroupOffsets_destroy(cGroupsPartitions[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS, genericOptions)
	if err != nil {
		return lcgor, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_ListConsumerGroupOffsets (asynchronous).
	C.rd_kafka_ListConsumerGroupOffsets(
		a.handle.rk,
		(**C.rd_kafka_ListConsumerGroupOffsets_t)(&cGroupsPartitions[0]),
		C.size_t(len(cGroupsPartitions)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT)
	if err != nil {
		return lcgor, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_ListConsumerGroupOffsets_result(rkev)

	// Convert result from C to Go.
	var cGroupCount C.size_t
	cGroups := C.rd_kafka_ListConsumerGroupOffsets_result_groups(cRes, &cGroupCount)
	lcgor.ConsumerGroupsTopicPartitions = a.cToConsumerGroupTopicPartitions(cGroups, cGroupCount)

	return lcgor, nil
}

// AlterConsumerGroupOffsets alters the offsets for topic partition(s) for
// consumer group(s).
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `groupsPartitions` - a slice of ConsumerGroupTopicPartitions, each element of
//     which has the id of a consumer group, and a slice of the TopicPartitions
//     we need to alter the offsets for. Currently, the size of
//     `groupsPartitions` has to be exactly one.
//   - `options` - AlterConsumerGroupOffsetsAdminOption options.
//
// Returns a AlterConsumerGroupOffsetsResult, containing a slice of
// ConsumerGroupTopicPartitions corresponding to the input slice, plus an error
// that is not `nil` for client level errors. Individual TopicPartitions inside
// each of the ConsumerGroupTopicPartitions should also be checked for errors.
// This will succeed at the partition level only if the group is not actively
// subscribed to the corresponding topic(s).
func (a *AdminClient) AlterConsumerGroupOffsets(
	ctx context.Context, groupsPartitions []ConsumerGroupTopicPartitions,
	options ...AlterConsumerGroupOffsetsAdminOption) (acgor AlterConsumerGroupOffsetsResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return acgor, err
	}

	acgor.ConsumerGroupsTopicPartitions = nil

	// For now, we only support one group at a time given as a single element of groupsPartitions.
	// Code has been written so that only this if-guard needs to be removed when we add support for
	// multiple ConsumerGroupTopicPartitions.
	if len(groupsPartitions) != 1 {
		return acgor, fmt.Errorf(
			"expected length of groupsPartitions is 1, got %d",
			len(groupsPartitions))
	}

	cGroupsPartitions := make(
		[]*C.rd_kafka_AlterConsumerGroupOffsets_t, len(groupsPartitions))

	// Convert Go ConsumerGroupTopicPartitions to C AlterConsumerGroupOffsets.
	for idx, groupPartitions := range groupsPartitions {
		// We need to destroy this list because rd_kafka_AlterConsumerGroupOffsets_new
		// creates a copy of it.
		cPartitions := newCPartsFromTopicPartitions(groupPartitions.Partitions)

		cGroupID := C.CString(groupPartitions.Group)
		defer C.free(unsafe.Pointer(cGroupID))

		cGroupsPartitions[idx] =
			C.rd_kafka_AlterConsumerGroupOffsets_new(cGroupID, cPartitions)
		defer C.rd_kafka_AlterConsumerGroupOffsets_destroy(cGroupsPartitions[idx])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS, genericOptions)
	if err != nil {
		return acgor, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_AlterConsumerGroupOffsets (asynchronous).
	C.rd_kafka_AlterConsumerGroupOffsets(
		a.handle.rk,
		(**C.rd_kafka_AlterConsumerGroupOffsets_t)(&cGroupsPartitions[0]),
		C.size_t(len(cGroupsPartitions)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_ALTERCONSUMERGROUPOFFSETS_RESULT)
	if err != nil {
		return acgor, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_AlterConsumerGroupOffsets_result(rkev)

	// Convert result from C to Go.
	var cGroupCount C.size_t
	cGroups := C.rd_kafka_AlterConsumerGroupOffsets_result_groups(cRes, &cGroupCount)
	acgor.ConsumerGroupsTopicPartitions = a.cToConsumerGroupTopicPartitions(cGroups, cGroupCount)

	return acgor, nil
}

// DescribeUserScramCredentials describe SASL/SCRAM credentials for the
// specified user names.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `users` - a slice of string, each one correspond to a user name, no
//     duplicates are allowed
//   - `options` - DescribeUserScramCredentialsAdminOption options.
//
// Returns a map from user name to user SCRAM credentials description.
// Each description can have an individual error.
func (a *AdminClient) DescribeUserScramCredentials(
	ctx context.Context, users []string,
	options ...DescribeUserScramCredentialsAdminOption) (result DescribeUserScramCredentialsResult, err error) {
	result = DescribeUserScramCredentialsResult{
		Descriptions: make(map[string]UserScramCredentialsDescription),
	}
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	// Convert user names into char** required by the implementation.
	cUserList := make([]*C.char, len(users))
	cUserCount := C.size_t(len(users))

	for idx, user := range users {
		cUserList[idx] = C.CString(user)
		defer C.free(unsafe.Pointer(cUserList[idx]))
	}

	var cUserListPtr **C.char
	if cUserCount > 0 {
		cUserListPtr = ((**C.char)(&cUserList[0]))
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle,
		C.RD_KAFKA_ADMIN_OP_DESCRIBEUSERSCRAMCREDENTIALS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_DescribeConsumerGroups (asynchronous).
	C.rd_kafka_DescribeUserScramCredentials(
		a.handle.rk,
		cUserListPtr,
		cUserCount,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBEUSERSCRAMCREDENTIALS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DescribeUserScramCredentials_result(rkev)

	// Convert result from C to Go.
	result.Descriptions = cToDescribeUserScramCredentialsResult(cRes)
	return result, nil
}

// ListOffsets describe offsets for the
// specified TopicPartiton based on an OffsetSpec.
//
// Parameters:
//
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `topicPartitionOffsets` - a map from TopicPartition to OffsetSpec, it
//     holds either the OffsetSpec enum value or timestamp. Must not be nil.
//   - `options` - ListOffsetsAdminOption options.
//
// Returns a ListOffsetsResult.
// Each TopicPartition's ListOffset can have an individual error.
func (a *AdminClient) ListOffsets(
	ctx context.Context, topicPartitionOffsets map[TopicPartition]OffsetSpec,
	options ...ListOffsetsAdminOption) (result ListOffsetsResult, err error) {
	if topicPartitionOffsets == nil {
		return result, newErrorFromString(ErrInvalidArg, "expected topicPartitionOffsets parameter.")
	}

	topicPartitions := C.rd_kafka_topic_partition_list_new(C.int(len(topicPartitionOffsets)))
	defer C.rd_kafka_topic_partition_list_destroy(topicPartitions)

	for tp, offsetValue := range topicPartitionOffsets {
		cStr := C.CString(*tp.Topic)
		defer C.free(unsafe.Pointer(cStr))
		topicPartition := C.rd_kafka_topic_partition_list_add(topicPartitions, cStr, C.int32_t(tp.Partition))
		topicPartition.offset = C.int64_t(offsetValue)
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_LISTOFFSETS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_ListOffsets (asynchronous).
	C.rd_kafka_ListOffsets(
		a.handle.rk,
		topicPartitions,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_LISTOFFSETS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_ListOffsets_result(rkev)

	// Convert result from C to Go.
	result = cToListOffsetsResult(cRes)

	return result, nil
}

// AlterUserScramCredentials alters SASL/SCRAM credentials.
// The pair (user, mechanism) must be unique among upsertions and deletions.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `upsertions` - a slice of user credential upsertions
//   - `deletions` - a slice of user credential deletions
//   - `options` - AlterUserScramCredentialsAdminOption options.
//
// Returns a map from user name to the corresponding Error, with error code
// ErrNoError when the request succeeded.
func (a *AdminClient) AlterUserScramCredentials(
	ctx context.Context, upsertions []UserScramCredentialUpsertion, deletions []UserScramCredentialDeletion,
	options ...AlterUserScramCredentialsAdminOption) (result AlterUserScramCredentialsResult, err error) {
	result = AlterUserScramCredentialsResult{
		Errors: make(map[string]Error),
	}
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	// Convert user names into char** required by the implementation.
	cAlterationList := make([]*C.rd_kafka_UserScramCredentialAlteration_t, len(upsertions)+len(deletions))
	cAlterationCount := C.size_t(len(upsertions) + len(deletions))
	idx := 0

	for _, upsertion := range upsertions {
		user := C.CString(upsertion.User)
		defer C.free(unsafe.Pointer(user))

		var salt *C.uchar = nil
		var saltSize C.size_t = 0
		if upsertion.Salt != nil {
			salt = (*C.uchar)(&upsertion.Salt[0])
			saltSize = C.size_t(len(upsertion.Salt))
		}

		cAlterationList[idx] = C.rd_kafka_UserScramCredentialUpsertion_new(user,
			C.rd_kafka_ScramMechanism_t(upsertion.ScramCredentialInfo.Mechanism),
			C.int(upsertion.ScramCredentialInfo.Iterations),
			(*C.uchar)(&upsertion.Password[0]), C.size_t(len(upsertion.Password)),
			salt, saltSize)
		defer C.rd_kafka_UserScramCredentialAlteration_destroy(cAlterationList[idx])
		idx = idx + 1
	}

	for _, deletion := range deletions {
		user := C.CString(deletion.User)
		defer C.free(unsafe.Pointer(user))
		cAlterationList[idx] = C.rd_kafka_UserScramCredentialDeletion_new(
			user, C.rd_kafka_ScramMechanism_t(deletion.Mechanism))
		defer C.rd_kafka_UserScramCredentialAlteration_destroy(cAlterationList[idx])
		idx = idx + 1
	}

	var cAlterationListPtr **C.rd_kafka_UserScramCredentialAlteration_t
	if cAlterationCount > 0 {
		cAlterationListPtr = ((**C.rd_kafka_UserScramCredentialAlteration_t)(&cAlterationList[0]))
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_ALTERUSERSCRAMCREDENTIALS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_AlterUserScramCredentials (asynchronous).
	C.rd_kafka_AlterUserScramCredentials(
		a.handle.rk,
		cAlterationListPtr,
		cAlterationCount,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_ALTERUSERSCRAMCREDENTIALS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_AlterUserScramCredentials_result(rkev)

	// Convert result from C to Go.
	var cResponseSize C.size_t
	cResponses := C.rd_kafka_AlterUserScramCredentials_result_responses(cRes, &cResponseSize)
	for i := 0; i < int(cResponseSize); i++ {
		cResponse := C.AlterUserScramCredentials_result_response_by_idx(
			cResponses, cResponseSize, C.size_t(i))
		user := C.GoString(C.rd_kafka_AlterUserScramCredentials_result_response_user(cResponse))
		err := newErrorFromCError(C.rd_kafka_AlterUserScramCredentials_result_response_error(cResponse))
		result.Errors[user] = err
	}

	return result, nil
}

// DeleteRecords deletes records (messages) in topic partitions older than the offsets provided.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `recordsToDelete` - A slice of TopicPartitions with the offset field set.
//     For each partition, delete all messages up to but not including the specified offset.
//     The offset could be set to kafka.OffsetEnd to delete all the messages in the partition.
//   - `options` - DeleteRecordsAdminOptions options.
//
// Returns a DeleteRecordsResults, which contains a slice of
// DeleteRecordsResult, each representing the result for one topic partition.
// Individual TopicPartitions inside the DeleteRecordsResult should be checked for errors.
// If successful, the DeletedRecords within the DeleteRecordsResult will be non-nil,
// and contain the low-watermark offset (smallest available offset of all live replicas).
func (a *AdminClient) DeleteRecords(ctx context.Context,
	recordsToDelete []TopicPartition,
	options ...DeleteRecordsAdminOption) (result DeleteRecordsResults, err error) {
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	if len(recordsToDelete) == 0 {
		return result, newErrorFromString(ErrInvalidArg, "No records to delete")
	}

	// convert recordsToDelete to rd_kafka_DeleteRecords_t** required by implementation
	cRecordsToDelete := newCPartsFromTopicPartitions(recordsToDelete)
	defer C.rd_kafka_topic_partition_list_destroy(cRecordsToDelete)

	cDelRecords := make([]*C.rd_kafka_DeleteRecords_t, 1)
	defer C.rd_kafka_DeleteRecords_destroy_array(&cDelRecords[0], C.size_t(1))

	cDelRecords[0] = C.rd_kafka_DeleteRecords_new(cRecordsToDelete)

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_DELETERECORDS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_DeleteRecords (asynchronous).
	C.rd_kafka_DeleteRecords(
		a.handle.rk,
		&cDelRecords[0],
		C.size_t(1),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_DELETERECORDS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DeleteRecords_result(rkev)
	cDeleteRecordsResultList := C.rd_kafka_DeleteRecords_result_offsets(cRes)

	// Convert result from C to Go.
	result.DeleteRecordsResults =
		cToDeletedRecordResult(cDeleteRecordsResultList)

	return result, nil
}

// ElectLeaders performs Preferred or Unclean Elections for the specified topic Partitions or for all of them.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `electLeaderRequest` - ElectLeadersRequest containing the election type
//     and the partitions to elect leaders for or nil for election in all the
//     partitions.
//   - `options` - ElectLeadersAdminOption options.
//
// Returns ElectLeadersResult, which contains a slice of TopicPartitions containing the partitions for which the leader election was performed.
// If we are passing partitions as nil, the broker will perform leader elections for all partitions,
// but the results will only contain partitions for which there was an election or resulted in an error.
// Individual TopicPartitions inside the ElectLeadersResult should be checked for errors.
// Additionally, an error that is not nil for client-level errors is returned.
func (a *AdminClient) ElectLeaders(ctx context.Context, electLeaderRequest ElectLeadersRequest, options ...ElectLeadersAdminOption) (result ElectLeadersResult, err error) {

	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	var cTopicPartitions *C.rd_kafka_topic_partition_list_t
	if electLeaderRequest.partitions != nil {
		cTopicPartitions = newCPartsFromTopicPartitions(electLeaderRequest.partitions)
		defer C.rd_kafka_topic_partition_list_destroy(cTopicPartitions)
	}

	cElectLeadersRequest := C.rd_kafka_ElectLeaders_new(C.rd_kafka_ElectionType_t(electLeaderRequest.electionType), cTopicPartitions)
	defer C.rd_kafka_ElectLeaders_destroy(cElectLeadersRequest)

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_ELECTLEADERS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_ElectLeader (asynchronous).
	C.rd_kafka_ElectLeaders(
		a.handle.rk,
		cElectLeadersRequest,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_ELECTLEADERS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_ElectLeaders_result(rkev)
	var cResponseSize C.size_t

	cResultPartitions := C.rd_kafka_ElectLeaders_result_partitions(cRes, &cResponseSize)
	result.TopicPartitions = newTopicPartitionsFromCTopicPartitionResult(cResultPartitions, cResponseSize)

	return result, nil
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

	a.isClosed = 0

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
	a.isClosed = 0
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
	a.isClosed = 0
	return a, nil
}
