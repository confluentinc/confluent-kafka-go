/**
 * Copyright 2024 Confluent Inc.
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

// ShareConsumer implements a Kafka share consumer (Queues for Kafka, KIP-932),
// exposing librdkafka's rd_kafka_share_* API. Share groups give point-to-point
// queue semantics: multiple members of the same share group cooperatively read
// from the same partitions with per-message acknowledgement, instead of the
// exclusive partition assignment of a classic consumer group.
//
// PREVIEW: the underlying librdkafka share-consumer API is a preview feature and
// may change before General Availability. It requires a broker with share groups
// enabled (Apache Kafka 4.2.0+). The ShareConsumer handle is NOT safe for
// concurrent use: a single instance must not be used from multiple goroutines
// simultaneously.

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

/*
#include <stdlib.h>
#include "select_rdkafka.h"
#include "glue_rdkafka.h"

static rd_kafka_topic_partition_t *_share_topic_partition_list_entry(rd_kafka_topic_partition_list_t *rktparlist, int idx) {
   return idx < rktparlist->cnt ? &rktparlist->elems[idx] : NULL;
}
*/
import "C"

// ShareAcknowledgeType is the acknowledgement type applied to a message
// consumed by a ShareConsumer in explicit acknowledgement mode.
type ShareAcknowledgeType int

const (
	// ShareAcknowledgeTypeAccept marks the message as processed successfully.
	ShareAcknowledgeTypeAccept ShareAcknowledgeType = C.RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT
	// ShareAcknowledgeTypeRelease marks the message as not processed; it is made
	// available for redelivery (to this or another member of the share group).
	ShareAcknowledgeTypeRelease ShareAcknowledgeType = C.RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE
	// ShareAcknowledgeTypeReject marks the message as rejected; it will not be
	// delivered again.
	ShareAcknowledgeTypeReject ShareAcknowledgeType = C.RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT
)

// String returns a human readable name for the acknowledgement type.
func (t ShareAcknowledgeType) String() string {
	switch t {
	case ShareAcknowledgeTypeAccept:
		return "accept"
	case ShareAcknowledgeTypeRelease:
		return "release"
	case ShareAcknowledgeTypeReject:
		return "reject"
	default:
		return fmt.Sprintf("ShareAcknowledgeType(%d)", int(t))
	}
}

// ShareConsumer implements a high-level Apache Kafka share consumer (KIP-932).
type ShareConsumer struct {
	rkshare *C.rd_kafka_share_t

	name      string
	msgFields *messageFields

	// rkt -> topic name cache. The share handle owns no rd_kafka_t, so topic
	// names are resolved directly from the message's rkt instead of via the
	// handle rkt cache used by the classic Consumer.
	rktNameLock  sync.Mutex
	rktNameCache map[*C.rd_kafka_topic_t]string

	// The batch returned by the most recent Poll(). It is destroyed on the next
	// Poll() or on Close(); this keeps the underlying C message pointers valid
	// for acknowledgement between polls, as librdkafka requires.
	curSet *ShareMessageSet

	isClosed  uint32
	isClosing uint32
}

// ShareMessageSet is a batch of messages returned by ShareConsumer.Poll().
//
// The Go Message values returned by Messages() are independent copies and remain
// valid after the set is released. Acknowledge*() however operate on the
// underlying librdkafka messages, which are only valid until the next Poll() or
// Close(); acknowledging a released set returns ErrState.
type ShareMessageSet struct {
	consumer *ShareConsumer
	cmsgs    *C.rd_kafka_messages_t
	messages []*Message
	cptrs    []*C.rd_kafka_message_t
	released uint32
}

// IsClosed returns true if the share consumer has been closed.
func (c *ShareConsumer) IsClosed() bool {
	return atomic.LoadUint32(&c.isClosed) == 1
}

func (c *ShareConsumer) verifyClient() error {
	if c.IsClosed() {
		return getOperationNotAllowedErrorForClosedClient()
	}
	return nil
}

// String returns a human readable name for the share consumer instance.
func (c *ShareConsumer) String() string {
	return c.name
}

// NewShareConsumer creates a new high-level ShareConsumer instance.
//
// conf is a *ConfigMap with standard librdkafka configuration properties.
// group.id is required. share.acknowledgement.mode is optional and selects the
// acknowledgement mode: "implicit" (default) auto-accepts the previous poll's
// records on the next Poll()/CommitSync()/CommitAsync(); "explicit" requires the
// application to acknowledge every record of a batch before the next Poll().
//
// Supported special (go.*) configuration properties:
//
//	go.message.fields (string, "all") - The fields to enable for consumed
//	    messages, comma-separated list of "key", "value", "headers", or "all"/"none".
func NewShareConsumer(conf *ConfigMap) (*ShareConsumer, error) {
	err := versionCheck()
	if err != nil {
		return nil, err
	}

	// Copy so the caller's ConfigMap is not mutated.
	confCopy := conf.clone()

	groupid, _ := confCopy.get("group.id", nil)
	if groupid == nil {
		return nil, newErrorFromString(ErrInvalidArg,
			"Required property group.id not set")
	}

	c := &ShareConsumer{
		rktNameCache: make(map[*C.rd_kafka_topic_t]string),
	}

	v, err := confCopy.extract("go.message.fields", "all")
	if err != nil {
		return nil, err
	}
	c.msgFields, err = newMessageFieldsFrom(v)
	if err != nil {
		return nil, err
	}

	cConf, err := confCopy.convert()
	if err != nil {
		return nil, err
	}

	cErrstr := (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(cErrstr))

	// rd_kafka_share_consumer_new frees cConf on success.
	c.rkshare = C.rd_kafka_share_consumer_new(cConf, cErrstr, 256)
	if c.rkshare == nil {
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	c.name = fmt.Sprintf("rdkafka#share-consumer-%v", groupid)

	return c, nil
}

// Subscribe subscribes the share consumer to a single topic, replacing the
// current subscription.
func (c *ShareConsumer) Subscribe(topic string) error {
	return c.SubscribeTopics([]string{topic})
}

// SubscribeTopics subscribes the share consumer to the provided list of topics,
// replacing the current subscription. Wildcard (regex) topics are not supported.
//
// The call is asynchronous: partition assignment is entirely broker-driven via
// the share group heartbeat; there is no client-side rebalance callback.
func (c *ShareConsumer) SubscribeTopics(topics []string) error {
	err := c.verifyClient()
	if err != nil {
		return err
	}

	ctopics := C.rd_kafka_topic_partition_list_new(C.int(len(topics)))
	defer C.rd_kafka_topic_partition_list_destroy(ctopics)

	for _, topic := range topics {
		ctopic := C.CString(topic)
		C.rd_kafka_topic_partition_list_add(ctopics, ctopic, C.RD_KAFKA_PARTITION_UA)
		C.free(unsafe.Pointer(ctopic))
	}

	cErr := C.rd_kafka_share_subscribe(c.rkshare, ctopics)
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(cErr)
	}

	return nil
}

// Unsubscribe clears the current subscription; the broker removes this member
// from the share group.
func (c *ShareConsumer) Unsubscribe() error {
	err := c.verifyClient()
	if err != nil {
		return err
	}

	cErr := C.rd_kafka_share_unsubscribe(c.rkshare)
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(cErr)
	}

	return nil
}

// Subscription returns the current topic subscription as set by Subscribe() /
// SubscribeTopics().
func (c *ShareConsumer) Subscription() (topics []string, err error) {
	err = c.verifyClient()
	if err != nil {
		return nil, err
	}

	var cTopics *C.rd_kafka_topic_partition_list_t
	cErr := C.rd_kafka_share_subscription(c.rkshare, &cTopics)
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil, newError(cErr)
	}
	defer C.rd_kafka_topic_partition_list_destroy(cTopics)

	topicCnt := int(cTopics.cnt)
	topics = make([]string, topicCnt)
	for i := 0; i < topicCnt; i++ {
		elem := C._share_topic_partition_list_entry(cTopics, C.int(i))
		topics[i] = C.GoString(elem.topic)
	}

	return topics, nil
}

// Poll polls the share consumer for a batch of messages, blocking for at most
// timeoutMs milliseconds.
//
// Returns (nil, nil) on timeout with no messages. Otherwise returns a
// *ShareMessageSet whose Messages() holds the batch. The set from a previous
// Poll() is released automatically by this call.
func (c *ShareConsumer) Poll(timeoutMs int) (*ShareMessageSet, error) {
	err := c.verifyClient()
	if err != nil {
		return nil, err
	}

	// Release the previous batch. In implicit ack mode librdkafka accepts the
	// prior poll's records on this call; the C container is now safe to free.
	c.releaseCurrentSet()

	var cmsgs *C.rd_kafka_messages_t
	cError := C.rd_kafka_share_poll(c.rkshare, C.int(timeoutMs), &cmsgs)
	if cError != nil {
		return nil, newErrorFromCErrorDestroy(cError)
	}

	if cmsgs == nil {
		// Timeout with no messages.
		return nil, nil
	}

	count := int(C.rd_kafka_messages_count(cmsgs))
	set := &ShareMessageSet{
		consumer: c,
		cmsgs:    cmsgs,
		messages: make([]*Message, count),
		cptrs:    make([]*C.rd_kafka_message_t, count),
	}
	for i := 0; i < count; i++ {
		cmsg := C.rd_kafka_messages_get(cmsgs, C.size_t(i))
		set.cptrs[i] = cmsg
		set.messages[i] = c.messageFromCShare(cmsg)
	}

	c.curSet = set
	return set, nil
}

// releaseCurrentSet destroys the C batch held from the previous Poll(), if any.
func (c *ShareConsumer) releaseCurrentSet() {
	if c.curSet == nil {
		return
	}
	if atomic.CompareAndSwapUint32(&c.curSet.released, 0, 1) {
		C.rd_kafka_messages_destroy(c.curSet.cmsgs)
		c.curSet.cmsgs = nil
	}
	c.curSet = nil
}

// messageFromCShare converts a librdkafka rd_kafka_message_t from a share poll
// batch into a Go Message. It resolves the topic name directly from the
// message's rkt (the share handle owns no rd_kafka_t).
func (c *ShareConsumer) messageFromCShare(cmsg *C.rd_kafka_message_t) *Message {
	msg := &Message{}

	if cmsg.rkt != nil {
		msg.TopicPartition.Topic = c.topicNameFromRkt(cmsg.rkt)
	}
	msg.TopicPartition.Partition = int32(cmsg.partition)
	msg.TopicPartition.Offset = Offset(cmsg.offset)

	if cmsg.payload != nil && c.msgFields.Value {
		msg.Value = C.GoBytes(unsafe.Pointer(cmsg.payload), C.int(cmsg.len))
	}
	if cmsg.key != nil && c.msgFields.Key {
		msg.Key = C.GoBytes(unsafe.Pointer(cmsg.key), C.int(cmsg.key_len))
	}

	var gMsg C.glue_msg_t
	gMsg.msg = cmsg
	gMsg.ts = C.rd_kafka_message_timestamp(cmsg, &gMsg.tstype)
	if gMsg.ts != -1 {
		ts := int64(gMsg.ts)
		msg.TimestampType = TimestampType(gMsg.tstype)
		msg.Timestamp = time.Unix(ts/1000, (ts%1000)*1000000)
	}

	if c.msgFields.Headers {
		gMsg.want_hdrs = C.int8_t(1)
		chdrsToTmphdrs(&gMsg)
		if gMsg.tmphdrsCnt > 0 {
			setupHeadersFromGlueMsg(msg, &gMsg)
		}
	}

	if cmsg.err != 0 {
		msg.TopicPartition.Error = newError(cmsg.err)
	}

	leaderEpoch := int32(C.rd_kafka_message_leader_epoch(cmsg))
	if leaderEpoch >= 0 {
		msg.LeaderEpoch = &leaderEpoch
		msg.TopicPartition.LeaderEpoch = &leaderEpoch
	}

	return msg
}

// topicNameFromRkt returns the topic name for a C topic handle, using a local
// cache to avoid repeated cgo calls.
func (c *ShareConsumer) topicNameFromRkt(crkt *C.rd_kafka_topic_t) *string {
	c.rktNameLock.Lock()
	defer c.rktNameLock.Unlock()

	if topic, ok := c.rktNameCache[crkt]; ok {
		return &topic
	}
	topic := C.GoString(C.rd_kafka_topic_name(crkt))
	c.rktNameCache[crkt] = topic
	return &topic
}

// CommitSync synchronously commits all pending acknowledgements to the broker,
// blocking until all replies are received or timeoutMs elapses.
//
// In implicit ack mode all records acquired by the previous poll are accepted
// first. The returned partition list carries the per-partition result (inspect
// each TopicPartition's Error); it is nil if no acknowledgements were pending.
func (c *ShareConsumer) CommitSync(timeoutMs int) ([]TopicPartition, error) {
	err := c.verifyClient()
	if err != nil {
		return nil, err
	}

	var cParts *C.rd_kafka_topic_partition_list_t
	cError := C.rd_kafka_share_commit_sync(c.rkshare, C.int(timeoutMs), &cParts)
	if cError != nil {
		return nil, newErrorFromCErrorDestroy(cError)
	}

	if cParts == nil {
		return nil, nil
	}
	defer C.rd_kafka_topic_partition_list_destroy(cParts)

	return newTopicPartitionsFromCparts(cParts), nil
}

// CommitAsync sends all pending acknowledgements to the broker without fetching
// new records and returns immediately. Per-partition outcomes are not reported
// by this call (they are delivered to librdkafka's acknowledgement-commit
// callback, which this binding does not yet surface); failed acknowledgements
// are not retried.
func (c *ShareConsumer) CommitAsync() error {
	err := c.verifyClient()
	if err != nil {
		return err
	}

	cError := C.rd_kafka_share_commit_async(c.rkshare)
	if cError != nil {
		return newErrorFromCErrorDestroy(cError)
	}
	return nil
}

// Close closes the share consumer. It commits pending acknowledgements, leaves
// the share group, and destroys the underlying handle. The instance is no longer
// usable after this call.
func (c *ShareConsumer) Close() error {
	if err := c.verifyClient(); err != nil {
		return err
	}
	if !atomic.CompareAndSwapUint32(&c.isClosing, 0, 1) {
		return newErrorFromString(ErrState, "ShareConsumer is already closing")
	}

	c.releaseCurrentSet()

	var closeErr error
	if cError := C.rd_kafka_share_consumer_close(c.rkshare); cError != nil {
		closeErr = newErrorFromCErrorDestroy(cError)
	}

	if cError := C.rd_kafka_share_destroy(c.rkshare); cError != nil && closeErr == nil {
		closeErr = newErrorFromCErrorDestroy(cError)
	} else if cError != nil {
		C.rd_kafka_error_destroy(cError)
	}
	c.rkshare = nil

	atomic.StoreUint32(&c.isClosed, 1)

	return closeErr
}

// Messages returns the batch of consumed messages. The returned Message values
// are independent Go copies and remain valid after the set is released.
func (s *ShareMessageSet) Messages() []*Message {
	return s.messages
}

// Len returns the number of messages in the set.
func (s *ShareMessageSet) Len() int {
	return len(s.messages)
}

func (s *ShareMessageSet) ackByIndex(i int, t ShareAcknowledgeType) error {
	if atomic.LoadUint32(&s.released) == 1 {
		return newErrorFromString(ErrState,
			"cannot acknowledge a message set released by a subsequent Poll or Close")
	}
	if i < 0 || i >= len(s.cptrs) {
		return newErrorFromString(ErrInvalidArg,
			fmt.Sprintf("message index %d out of range [0,%d)", i, len(s.cptrs)))
	}
	cErr := C.rd_kafka_share_acknowledge_type(s.consumer.rkshare, s.cptrs[i],
		C.rd_kafka_share_AcknowledgeType_t(t))
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(cErr)
	}
	return nil
}

// AcknowledgeIndex acknowledges the message at index i in the set (as returned
// by Messages()) with the given acknowledgement type. Only valid in explicit
// acknowledgement mode and only before the next Poll() or Close().
func (s *ShareMessageSet) AcknowledgeIndex(i int, t ShareAcknowledgeType) error {
	return s.ackByIndex(i, t)
}

// Acknowledge acknowledges the given message (which must be one of the messages
// returned by this set's Messages()) with the given acknowledgement type.
func (s *ShareMessageSet) Acknowledge(m *Message, t ShareAcknowledgeType) error {
	for i, candidate := range s.messages {
		if candidate == m {
			return s.ackByIndex(i, t)
		}
	}
	return newErrorFromString(ErrInvalidArg,
		"message does not belong to this ShareMessageSet")
}

// AcknowledgeAll acknowledges every message in the set with the given
// acknowledgement type.
func (s *ShareMessageSet) AcknowledgeAll(t ShareAcknowledgeType) error {
	for i := range s.cptrs {
		if err := s.ackByIndex(i, t); err != nil {
			return err
		}
	}
	return nil
}
