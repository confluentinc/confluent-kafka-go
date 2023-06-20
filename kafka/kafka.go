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

// Package kafka provides high-level Apache Kafka producer and consumers
// using bindings on-top of the librdkafka C library.
//
// # High-level Consumer
//
// * Decide if you want to read messages and events by calling `.Poll()` or
// the deprecated option of using the `.Events()` channel. (If you want to use
// `.Events()` channel then set `"go.events.channel.enable": true`).
//
// * Create a Consumer with `kafka.NewConsumer()` providing at
// least the `bootstrap.servers` and `group.id` configuration properties.
//
// * Call `.Subscribe()` or (`.SubscribeTopics()` to subscribe to multiple topics)
// to join the group with the specified subscription set.
// Subscriptions are atomic, calling `.Subscribe*()` again will leave
// the group and rejoin with the new set of topics.
//
// * Start reading events and messages from either the `.Events` channel
// or by calling `.Poll()`.
//
// * When the group has rebalanced each client member is assigned a
// (sub-)set of topic+partitions.
// By default the consumer will start fetching messages for its assigned
// partitions at this point, but your application may enable rebalance
// events to get an insight into what the assigned partitions where
// as well as set the initial offsets. To do this you need to pass
// `"go.application.rebalance.enable": true` to the `NewConsumer()` call
// mentioned above. You will (eventually) see a `kafka.AssignedPartitions` event
// with the assigned partition set. You can optionally modify the initial
// offsets (they'll default to stored offsets and if there are no previously stored
// offsets it will fall back to `"auto.offset.reset"`
// which defaults to the `latest` message) and then call `.Assign(partitions)`
// to start consuming. If you don't need to modify the initial offsets you will
// not need to call `.Assign()`, the client will do so automatically for you if
// you dont, unless you are using the channel-based consumer in which case
// you MUST call `.Assign()` when receiving the `AssignedPartitions` and
// `RevokedPartitions` events.
//
// * As messages are fetched they will be made available on either the
// `.Events` channel or by calling `.Poll()`, look for event type `*kafka.Message`.
//
// * Handle messages, events and errors to your liking.
//
// * When you are done consuming call `.Close()` to commit final offsets
// and leave the consumer group.
//
// # Producer
//
// * Create a Producer with `kafka.NewProducer()` providing at least
// the `bootstrap.servers` configuration properties.
//
// * Messages may now be produced either by sending a `*kafka.Message`
// on the `.ProduceChannel` or by calling `.Produce()`.
//
// * Producing is an asynchronous operation so the client notifies the application
// of per-message produce success or failure through something called delivery reports.
// Delivery reports are by default emitted on the `.Events()` channel as `*kafka.Message`
// and you should check `msg.TopicPartition.Error` for `nil` to find out if the message
// was succesfully delivered or not.
// It is also possible to direct delivery reports to alternate channels
// by providing a non-nil `chan Event` channel to `.Produce()`.
// If no delivery reports are wanted they can be completely disabled by
// setting configuration property `"go.delivery.reports": false`.
//
// * When you are done producing messages you will need to make sure all messages
// are indeed delivered to the broker (or failed), remember that this is
// an asynchronous client so some of your messages may be lingering in internal
// channels or tranmission queues.
// To do this you can either keep track of the messages you've produced
// and wait for their corresponding delivery reports, or call the convenience
// function `.Flush()` that will block until all message deliveries are done
// or the provided timeout elapses.
//
// * Finally call `.Close()` to decommission the producer.
//
// # Transactional producer API
//
// The transactional producer operates on top of the idempotent producer,
// and provides full exactly-once semantics (EOS) for Apache Kafka when used
// with the transaction aware consumer (`isolation.level=read_committed`).
//
// A producer instance is configured for transactions by setting the
// `transactional.id` to an identifier unique for the application. This
// id will be used to fence stale transactions from previous instances of
// the application, typically following an outage or crash.
//
// After creating the transactional producer instance using `NewProducer()`
// the transactional state must be initialized by calling
// `InitTransactions()`. This is a blocking call that will
// acquire a runtime producer id from the transaction coordinator broker
// as well as abort any stale transactions and fence any still running producer
// instances with the same `transactional.id`.
//
// Once transactions are initialized the application may begin a new
// transaction by calling `BeginTransaction()`.
// A producer instance may only have one single on-going transaction.
//
// Any messages produced after the transaction has been started will
// belong to the ongoing transaction and will be committed or aborted
// atomically.
// It is not permitted to produce messages outside a transaction
// boundary, e.g., before `BeginTransaction()` or after `CommitTransaction()`,
// `AbortTransaction()` or if the current transaction has failed.
//
// If consumed messages are used as input to the transaction, the consumer
// instance must be configured with `enable.auto.commit` set to `false`.
// To commit the consumed offsets along with the transaction pass the
// list of consumed partitions and the last offset processed + 1 to
// `SendOffsetsToTransaction()` prior to committing the transaction.
// This allows an aborted transaction to be restarted using the previously
// committed offsets.
//
// To commit the produced messages, and any consumed offsets, to the
// current transaction, call `CommitTransaction()`.
// This call will block until the transaction has been fully committed or
// failed (typically due to fencing by a newer producer instance).
//
// Alternatively, if processing fails, or an abortable transaction error is
// raised, the transaction needs to be aborted by calling
// `AbortTransaction()` which marks any produced messages and
// offset commits as aborted.
//
// After the current transaction has been committed or aborted a new
// transaction may be started by calling `BeginTransaction()` again.
//
// Retriable errors:
// Some error cases allow the attempted operation to be retried, this is
// indicated by the error object having the retriable flag set which can
// be detected by calling `err.(kafka.Error).IsRetriable()`.
// When this flag is set the application may retry the operation immediately
// or preferably after a shorter grace period (to avoid busy-looping).
// Retriable errors include timeouts, broker transport failures, etc.
//
// Abortable errors:
// An ongoing transaction may fail permanently due to various errors,
// such as transaction coordinator becoming unavailable, write failures to the
// Apache Kafka log, under-replicated partitions, etc.
// At this point the producer application must abort the current transaction
// using `AbortTransaction()` and optionally start a new transaction
// by calling `BeginTransaction()`.
// Whether an error is abortable or not is detected by calling
// `err.(kafka.Error).TxnRequiresAbort()` on the returned error object.
//
// Fatal errors:
// While the underlying idempotent producer will typically only raise
// fatal errors for unrecoverable cluster errors where the idempotency
// guarantees can't be maintained, most of these are treated as abortable by
// the transactional producer since transactions may be aborted and retried
// in their entirety;
// The transactional producer on the other hand introduces a set of additional
// fatal errors which the application needs to handle by shutting down the
// producer and terminate. There is no way for a producer instance to recover
// from fatal errors.
// Whether an error is fatal or not is detected by calling
// `err.(kafka.Error).IsFatal()` on the returned error object or by checking
// the global `GetFatalError()`.
//
// Handling of other errors:
// For errors that have neither retriable, abortable or the fatal flag set
// it is not always obvious how to handle them. While some of these errors
// may be indicative of bugs in the application code, such as when
// an invalid parameter is passed to a method, other errors might originate
// from the broker and be passed thru as-is to the application.
// The general recommendation is to treat these errors, that have
// neither the retriable or abortable flags set, as fatal.
//
// Error handling example:
//
//	retry:
//
//	   err := producer.CommitTransaction(...)
//	   if err == nil {
//	       return nil
//	   } else if err.(kafka.Error).TxnRequiresAbort() {
//	       do_abort_transaction_and_reset_inputs()
//	   } else if err.(kafka.Error).IsRetriable() {
//	       goto retry
//	   } else { // treat all other errors as fatal errors
//	       panic(err)
//	   }
//
// # Events
//
// Apart from emitting messages and delivery reports the client also communicates
// with the application through a number of different event types.
// An application may choose to handle or ignore these events.
//
// # Consumer events
//
// * `*kafka.Message` - a fetched message.
//
// * `AssignedPartitions` - The assigned partition set for this client following a rebalance.
// Requires `go.application.rebalance.enable`
//
// * `RevokedPartitions` - The counter part to `AssignedPartitions` following a rebalance.
// `AssignedPartitions` and `RevokedPartitions` are symmetrical.
// Requires `go.application.rebalance.enable`
//
// * `PartitionEOF` - Consumer has reached the end of a partition.
// NOTE: The consumer will keep trying to fetch new messages for the partition.
//
// * `OffsetsCommitted` - Offset commit results (when `enable.auto.commit` is enabled).
//
// # Producer events
//
// * `*kafka.Message` - delivery report for produced message.
// Check `.TopicPartition.Error` for delivery result.
//
// # Generic events for both Consumer and Producer
//
// * `KafkaError` - client (error codes are prefixed with _) or broker error.
// These errors are normally just informational since the
// client will try its best to automatically recover (eventually).
//
// * `OAuthBearerTokenRefresh` - retrieval of a new SASL/OAUTHBEARER token is required.
// This event only occurs with sasl.mechanism=OAUTHBEARER.
// Be sure to invoke SetOAuthBearerToken() on the Producer/Consumer/AdminClient
// instance when a successful token retrieval is completed, otherwise be sure to
// invoke SetOAuthBearerTokenFailure() to indicate that retrieval failed (or
// if setting the token failed, which could happen if an extension doesn't meet
// the required regular expression); invoking SetOAuthBearerTokenFailure() will
// schedule a new event for 10 seconds later so another retrieval can be attempted.
//
// Hint: If your application registers a signal notification
// (signal.Notify) makes sure the signals channel is buffered to avoid
// possible complications with blocking Poll() calls.
//
// Note: The Confluent Kafka Go client is safe for concurrent use.
package kafka

import (
	"fmt"
	"unsafe"

	// Make sure librdkafka_vendor/ sub-directory is included in vendor pulls.
	_ "github.com/confluentinc/confluent-kafka-go/v2/kafka/librdkafka_vendor"
)

/*
#include <stdlib.h>
#include <string.h>
#include "select_rdkafka.h"

static rd_kafka_topic_partition_t *_c_rdkafka_topic_partition_list_entry(rd_kafka_topic_partition_list_t *rktparlist, int idx) {
   return idx < rktparlist->cnt ? &rktparlist->elems[idx] : NULL;
}

static const rd_kafka_group_result_t *
group_result_by_idx (const rd_kafka_group_result_t **groups, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return groups[idx];
}
*/
import "C"

// PartitionAny represents any partition (for partitioning),
// or unspecified value (for all other cases)
const PartitionAny = int32(C.RD_KAFKA_PARTITION_UA)

// TopicPartition is a generic placeholder for a Topic+Partition and optionally Offset.
type TopicPartition struct {
	Topic       *string
	Partition   int32
	Offset      Offset
	Metadata    *string
	Error       error
	LeaderEpoch *int32 // LeaderEpoch or nil if not available
}

func (p TopicPartition) String() string {
	topic := "<null>"
	if p.Topic != nil {
		topic = *p.Topic
	}
	if p.Error != nil {
		return fmt.Sprintf("%s[%d]@%s(%s)",
			topic, p.Partition, p.Offset, p.Error)
	}
	return fmt.Sprintf("%s[%d]@%s",
		topic, p.Partition, p.Offset)
}

// TopicPartitions is a slice of TopicPartitions that also implements
// the sort interface
type TopicPartitions []TopicPartition

func (tps TopicPartitions) Len() int {
	return len(tps)
}

func (tps TopicPartitions) Less(i, j int) bool {
	if *tps[i].Topic < *tps[j].Topic {
		return true
	} else if *tps[i].Topic > *tps[j].Topic {
		return false
	}
	return tps[i].Partition < tps[j].Partition
}

func (tps TopicPartitions) Swap(i, j int) {
	tps[i], tps[j] = tps[j], tps[i]
}

// Node represents a Kafka broker.
type Node struct {
	// Node id.
	ID int
	// Node host.
	Host string
	// Node port.
	Port int
}

func (n Node) String() string {
	return fmt.Sprintf("[%s:%d]/%d", n.Host, n.Port, n.ID)
}

// ConsumerGroupTopicPartitions represents a consumer group's TopicPartitions.
type ConsumerGroupTopicPartitions struct {
	// Group name
	Group string
	// Partitions list
	Partitions []TopicPartition
}

func (gtp ConsumerGroupTopicPartitions) String() string {
	res := gtp.Group
	res += "[ "
	for _, tp := range gtp.Partitions {
		res += tp.String() + " "
	}
	res += "]"
	return res
}

// new_cparts_from_TopicPartitions creates a new C rd_kafka_topic_partition_list_t
// from a TopicPartition array.
func newCPartsFromTopicPartitions(partitions []TopicPartition) (cparts *C.rd_kafka_topic_partition_list_t) {
	cparts = C.rd_kafka_topic_partition_list_new(C.int(len(partitions)))
	for _, part := range partitions {
		ctopic := C.CString(*part.Topic)
		defer C.free(unsafe.Pointer(ctopic))
		rktpar := C.rd_kafka_topic_partition_list_add(cparts, ctopic, C.int32_t(part.Partition))
		rktpar.offset = C.int64_t(part.Offset)

		if part.Metadata != nil {
			cmetadata := C.CString(*part.Metadata)
			rktpar.metadata = unsafe.Pointer(cmetadata)
			rktpar.metadata_size = C.size_t(len(*part.Metadata))
		}

		if part.LeaderEpoch != nil {
			cLeaderEpoch := C.int32_t(*part.LeaderEpoch)
			C.rd_kafka_topic_partition_set_leader_epoch(rktpar, cLeaderEpoch)
		}
	}

	return cparts
}

func setupTopicPartitionFromCrktpar(partition *TopicPartition, crktpar *C.rd_kafka_topic_partition_t) {

	topic := C.GoString(crktpar.topic)
	partition.Topic = &topic
	partition.Partition = int32(crktpar.partition)
	partition.Offset = Offset(crktpar.offset)
	if crktpar.metadata_size > 0 {
		size := C.int(crktpar.metadata_size)
		cstr := (*C.char)(unsafe.Pointer(crktpar.metadata))
		metadata := C.GoStringN(cstr, size)
		partition.Metadata = &metadata
	}
	if crktpar.err != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		partition.Error = newError(crktpar.err)
	}

	cLeaderEpoch := int32(C.rd_kafka_topic_partition_get_leader_epoch(crktpar))
	if cLeaderEpoch >= 0 {
		partition.LeaderEpoch = &cLeaderEpoch
	}
}

func newTopicPartitionsFromCparts(cparts *C.rd_kafka_topic_partition_list_t) (partitions []TopicPartition) {

	partcnt := int(cparts.cnt)

	partitions = make([]TopicPartition, partcnt)
	for i := 0; i < partcnt; i++ {
		crktpar := C._c_rdkafka_topic_partition_list_entry(cparts, C.int(i))
		setupTopicPartitionFromCrktpar(&partitions[i], crktpar)
	}

	return partitions
}

// cToConsumerGroupTopicPartitions converts a C rd_kafka_group_result_t array to a
// ConsumerGroupTopicPartitions slice.
func (a *AdminClient) cToConsumerGroupTopicPartitions(
	cGroupResults **C.rd_kafka_group_result_t,
	cGroupCount C.size_t) (result []ConsumerGroupTopicPartitions) {
	result = make([]ConsumerGroupTopicPartitions, uint(cGroupCount))

	for i := uint(0); i < uint(cGroupCount); i++ {
		cGroupResult := C.group_result_by_idx(cGroupResults, cGroupCount, C.size_t(i))
		cGroupPartitions := C.rd_kafka_group_result_partitions(cGroupResult)
		result[i] = ConsumerGroupTopicPartitions{
			Group:      C.GoString(C.rd_kafka_group_result_name(cGroupResult)),
			Partitions: newTopicPartitionsFromCparts(cGroupPartitions),
		}
	}
	return
}

// LibraryVersion returns the underlying librdkafka library version as a
// (version_int, version_str) tuple.
func LibraryVersion() (int, string) {
	ver := (int)(C.rd_kafka_version())
	verstr := C.GoString(C.rd_kafka_version_str())
	return ver, verstr
}

// setSaslCredentials sets the SASL credentials used for the specified Kafka client.
// The new credentials will overwrite the old ones (which were set when creating the
// client or by a previous call to setSaslCredentials). The new credentials will be
// used the next time the client needs to establish a connection to the broker. This
// function will *not* break existing broker connections that were established with the
// old credentials. This method applies only to the SASL PLAIN and SCRAM mechanisms.
func setSaslCredentials(rk *C.rd_kafka_t, username, password string) error {
	cUsername := C.CString(username)
	defer C.free(unsafe.Pointer(cUsername))
	cPassword := C.CString(password)
	defer C.free(unsafe.Pointer(cPassword))

	if err := C.rd_kafka_sasl_set_credentials(rk, cUsername, cPassword); err != nil {
		return newErrorFromCErrorDestroy(err)
	}

	return nil
}
