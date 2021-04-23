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
	"context"
	"fmt"
	"math"
	"time"
	"unsafe"
)

/*
#include <stdlib.h>
#include "select_rdkafka.h"
#include "glue_rdkafka.h"


#ifdef RD_KAFKA_V_HEADERS
// Convert tmphdrs to chdrs (created by this function).
// If tmphdr.size == -1: value is considered Null
//    tmphdr.size == 0:  value is considered empty (ignored)
//    tmphdr.size > 0:   value is considered non-empty
//
// WARNING: The header keys and values will be freed by this function.
void tmphdrs_to_chdrs (tmphdr_t *tmphdrs, size_t tmphdrsCnt,
                       rd_kafka_headers_t **chdrs) {
   size_t i;

   *chdrs = rd_kafka_headers_new(tmphdrsCnt);

   for (i = 0 ; i < tmphdrsCnt ; i++) {
      rd_kafka_header_add(*chdrs,
                          tmphdrs[i].key, -1,
                          tmphdrs[i].size == -1 ? NULL :
                          (tmphdrs[i].size == 0 ? "" : tmphdrs[i].val),
                          tmphdrs[i].size == -1 ? 0 : tmphdrs[i].size);
      if (tmphdrs[i].size > 0)
         free((void *)tmphdrs[i].val);
      free((void *)tmphdrs[i].key);
   }
}

#else
void free_tmphdrs (tmphdr_t *tmphdrs, size_t tmphdrsCnt) {
   size_t i;
   for (i = 0 ; i < tmphdrsCnt ; i++) {
      if (tmphdrs[i].size > 0)
         free((void *)tmphdrs[i].val);
      free((void *)tmphdrs[i].key);
   }
}
#endif


rd_kafka_resp_err_t do_produce (rd_kafka_t *rk,
          rd_kafka_topic_t *rkt, int32_t partition,
          int msgflags,
          int valIsNull, void *val, size_t val_len,
          int keyIsNull, void *key, size_t key_len,
          int64_t timestamp,
          tmphdr_t *tmphdrs, size_t tmphdrsCnt,
          uintptr_t cgoid) {
  void *valp = valIsNull ? NULL : val;
  void *keyp = keyIsNull ? NULL : key;
#ifdef RD_KAFKA_V_TIMESTAMP
rd_kafka_resp_err_t err;
#ifdef RD_KAFKA_V_HEADERS
  rd_kafka_headers_t *hdrs = NULL;
#endif
#endif


  if (tmphdrsCnt > 0) {
#ifdef RD_KAFKA_V_HEADERS
     tmphdrs_to_chdrs(tmphdrs, tmphdrsCnt, &hdrs);
#else
     free_tmphdrs(tmphdrs, tmphdrsCnt);
     return RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
#endif
  }


#ifdef RD_KAFKA_V_TIMESTAMP
  err = rd_kafka_producev(rk,
        RD_KAFKA_V_RKT(rkt),
        RD_KAFKA_V_PARTITION(partition),
        RD_KAFKA_V_MSGFLAGS(msgflags),
        RD_KAFKA_V_VALUE(valp, val_len),
        RD_KAFKA_V_KEY(keyp, key_len),
        RD_KAFKA_V_TIMESTAMP(timestamp),
#ifdef RD_KAFKA_V_HEADERS
        RD_KAFKA_V_HEADERS(hdrs),
#endif
        RD_KAFKA_V_OPAQUE((void *)cgoid),
        RD_KAFKA_V_END);
#ifdef RD_KAFKA_V_HEADERS
  if (err && hdrs)
    rd_kafka_headers_destroy(hdrs);
#endif
  return err;
#else
  if (timestamp)
      return RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
  if (rd_kafka_produce(rkt, partition, msgflags,
                       valp, val_len,
                       keyp, key_len,
                       (void *)cgoid) == -1)
      return rd_kafka_last_error();
  else
      return RD_KAFKA_RESP_ERR_NO_ERROR;
#endif
}
*/
import "C"

// Producer implements a High-level Apache Kafka Producer instance
type Producer struct {
	events         chan Event
	produceChannel chan *Message
	handle         handle

	// Terminates the poller() goroutine
	pollerTermChan chan bool
}

// String returns a human readable name for a Producer instance
func (p *Producer) String() string {
	return p.handle.String()
}

// get_handle implements the Handle interface
func (p *Producer) gethandle() *handle {
	return &p.handle
}

func (p *Producer) produce(msg *Message, msgFlags int, deliveryChan chan Event) error {
	if msg == nil || msg.TopicPartition.Topic == nil || len(*msg.TopicPartition.Topic) == 0 {
		return newErrorFromString(ErrInvalidArg, "")
	}

	crkt := p.handle.getRkt(*msg.TopicPartition.Topic)

	// Three problems:
	//  1) There's a difference between an empty Value or Key (length 0, proper pointer) and
	//     a null Value or Key (length 0, null pointer).
	//  2) we need to be able to send a null Value or Key, but the unsafe.Pointer(&slice[0])
	//     dereference can't be performed on a nil slice.
	//  3) cgo's pointer checking requires the unsafe.Pointer(slice..) call to be made
	//     in the call to the C function.
	//
	// Solution:
	//  Keep track of whether the Value or Key were nil (1), but let the valp and keyp pointers
	//  point to a 1-byte slice (but the length to send is still 0) so that the dereference (2)
	//  works.
	//  Then perform the unsafe.Pointer() on the valp and keyp pointers (which now either point
	//  to the original msg.Value and msg.Key or to the 1-byte slices) in the call to C (3).
	//
	var valp []byte
	var keyp []byte
	oneByte := []byte{0}
	var valIsNull C.int
	var keyIsNull C.int
	var valLen int
	var keyLen int

	if msg.Value == nil {
		valIsNull = 1
		valLen = 0
		valp = oneByte
	} else {
		valLen = len(msg.Value)
		if valLen > 0 {
			valp = msg.Value
		} else {
			valp = oneByte
		}
	}

	if msg.Key == nil {
		keyIsNull = 1
		keyLen = 0
		keyp = oneByte
	} else {
		keyLen = len(msg.Key)
		if keyLen > 0 {
			keyp = msg.Key
		} else {
			keyp = oneByte
		}
	}

	var cgoid int

	// Per-message state that needs to be retained through the C code:
	//   delivery channel (if specified)
	//   message opaque   (if specified)
	// Since these cant be passed as opaque pointers to the C code,
	// due to cgo constraints, we add them to a per-producer map for lookup
	// when the C code triggers the callbacks or events.
	if deliveryChan != nil || msg.Opaque != nil {
		cgoid = p.handle.cgoPut(cgoDr{deliveryChan: deliveryChan, opaque: msg.Opaque})
	}

	var timestamp int64
	if !msg.Timestamp.IsZero() {
		timestamp = msg.Timestamp.UnixNano() / 1000000
	}

	// Convert headers to C-friendly tmphdrs
	var tmphdrs []C.tmphdr_t
	tmphdrsCnt := len(msg.Headers)

	if tmphdrsCnt > 0 {
		tmphdrs = make([]C.tmphdr_t, tmphdrsCnt)

		for n, hdr := range msg.Headers {
			// Make a copy of the key
			// to avoid runtime panic with
			// foreign Go pointers in cgo.
			tmphdrs[n].key = C.CString(hdr.Key)
			if hdr.Value != nil {
				tmphdrs[n].size = C.ssize_t(len(hdr.Value))
				if tmphdrs[n].size > 0 {
					// Make a copy of the value
					// to avoid runtime panic with
					// foreign Go pointers in cgo.
					tmphdrs[n].val = C.CBytes(hdr.Value)
				}
			} else {
				// null value
				tmphdrs[n].size = C.ssize_t(-1)
			}
		}
	} else {
		// no headers, need a dummy tmphdrs of size 1 to avoid index
		// out of bounds panic in do_produce() call below.
		// tmphdrsCnt will be 0.
		tmphdrs = []C.tmphdr_t{{nil, nil, 0}}
	}

	cErr := C.do_produce(p.handle.rk, crkt,
		C.int32_t(msg.TopicPartition.Partition),
		C.int(msgFlags)|C.RD_KAFKA_MSG_F_COPY,
		valIsNull, unsafe.Pointer(&valp[0]), C.size_t(valLen),
		keyIsNull, unsafe.Pointer(&keyp[0]), C.size_t(keyLen),
		C.int64_t(timestamp),
		(*C.tmphdr_t)(unsafe.Pointer(&tmphdrs[0])), C.size_t(tmphdrsCnt),
		(C.uintptr_t)(cgoid))
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		if cgoid != 0 {
			p.handle.cgoGet(cgoid)
		}
		return newError(cErr)
	}

	return nil
}

// Produce single message.
// This is an asynchronous call that enqueues the message on the internal
// transmit queue, thus returning immediately.
// The delivery report will be sent on the provided deliveryChan if specified,
// or on the Producer object's Events() channel if not.
// msg.Timestamp requires librdkafka >= 0.9.4 (else returns ErrNotImplemented),
// api.version.request=true, and broker >= 0.10.0.0.
// msg.Headers requires librdkafka >= 0.11.4 (else returns ErrNotImplemented),
// api.version.request=true, and broker >= 0.11.0.0.
// Returns an error if message could not be enqueued.
func (p *Producer) Produce(msg *Message, deliveryChan chan Event) error {
	return p.produce(msg, 0, deliveryChan)
}

// Produce a batch of messages.
// These batches do not relate to the message batches sent to the broker, the latter
// are collected on the fly internally in librdkafka.
// WARNING: This is an experimental API.
// NOTE: timestamps and headers are not supported with this API.
func (p *Producer) produceBatch(topic string, msgs []*Message, msgFlags int) error {
	crkt := p.handle.getRkt(topic)

	cmsgs := make([]C.rd_kafka_message_t, len(msgs))
	for i, m := range msgs {
		p.handle.messageToC(m, &cmsgs[i])
	}
	r := C.rd_kafka_produce_batch(crkt, C.RD_KAFKA_PARTITION_UA, C.int(msgFlags)|C.RD_KAFKA_MSG_F_FREE,
		(*C.rd_kafka_message_t)(&cmsgs[0]), C.int(len(msgs)))
	if r == -1 {
		return newError(C.rd_kafka_last_error())
	}

	return nil
}

// Events returns the Events channel (read)
func (p *Producer) Events() chan Event {
	return p.events
}

// Logs returns the Log channel (if enabled), else nil
func (p *Producer) Logs() chan LogEvent {
	return p.handle.logs
}

// ProduceChannel returns the produce *Message channel (write)
func (p *Producer) ProduceChannel() chan *Message {
	return p.produceChannel
}

// Len returns the number of messages and requests waiting to be transmitted to the broker
// as well as delivery reports queued for the application.
// Includes messages on ProduceChannel.
func (p *Producer) Len() int {
	return len(p.produceChannel) + len(p.events) + int(C.rd_kafka_outq_len(p.handle.rk))
}

// Flush and wait for outstanding messages and requests to complete delivery.
// Includes messages on ProduceChannel.
// Runs until value reaches zero or on timeoutMs.
// Returns the number of outstanding events still un-flushed.
func (p *Producer) Flush(timeoutMs int) int {
	termChan := make(chan bool) // unused stand-in termChan

	d, _ := time.ParseDuration(fmt.Sprintf("%dms", timeoutMs))
	tEnd := time.Now().Add(d)
	for p.Len() > 0 {
		remain := tEnd.Sub(time.Now()).Seconds()
		if remain <= 0.0 {
			return p.Len()
		}

		p.handle.eventPoll(p.events,
			int(math.Min(100, remain*1000)), 1000, termChan)
	}

	return 0
}

// Close a Producer instance.
// The Producer object or its channels are no longer usable after this call.
func (p *Producer) Close() {
	// Wait for poller() (signaled by closing pollerTermChan)
	// and channel_producer() (signaled by closing ProduceChannel)
	close(p.pollerTermChan)
	close(p.produceChannel)
	p.handle.waitGroup.Wait()

	close(p.events)

	p.handle.cleanup()

	C.rd_kafka_destroy(p.handle.rk)
}

const (
	// PurgeInFlight purges messages in-flight to or from the broker.
	// Purging these messages will void any future acknowledgements from the
	// broker, making it impossible for the application to know if these
	// messages were successfully delivered or not.
	// Retrying these messages may lead to duplicates.
	PurgeInFlight = int(C.RD_KAFKA_PURGE_F_INFLIGHT)

	// PurgeQueue Purge messages in internal queues.
	PurgeQueue = int(C.RD_KAFKA_PURGE_F_QUEUE)

	// PurgeNonBlocking Don't wait for background thread queue purging to finish.
	PurgeNonBlocking = int(C.RD_KAFKA_PURGE_F_NON_BLOCKING)
)

// Purge messages currently handled by this producer instance.
//
// flags is a combination of PurgeQueue, PurgeInFlight and PurgeNonBlocking.
//
// The application will need to call Poll(), Flush() or read the Events() channel
// after this call to serve delivery reports for the purged messages.
//
// Messages purged from internal queues fail with the delivery report
// error code set to ErrPurgeQueue, while purged messages that
// are in-flight to or from the broker will fail with the error code set to
// ErrPurgeInflight.
//
// Warning: Purging messages that are in-flight to or from the broker
// will ignore any sub-sequent acknowledgement for these messages
// received from the broker, effectively making it impossible
// for the application to know if the messages were successfully
// produced or not. This may result in duplicate messages if the
// application retries these messages at a later time.
//
// Note: This call may block for a short time while background thread
// queues are purged.
//
// Returns nil on success, ErrInvalidArg if the purge flags are invalid or unknown.
func (p *Producer) Purge(flags int) error {
	cErr := C.rd_kafka_purge(p.handle.rk, C.int(flags))
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return newError(cErr)
	}

	return nil
}

// NewProducer creates a new high-level Producer instance.
//
// conf is a *ConfigMap with standard librdkafka configuration properties.
//
// Supported special configuration properties (type, default):
//   go.batch.producer (bool, false) - EXPERIMENTAL: Enable batch producer (for increased performance).
//                                     These batches do not relate to Kafka message batches in any way.
//                                     Note: timestamps and headers are not supported with this interface.
//   go.delivery.reports (bool, true) - Forward per-message delivery reports to the
//                                      Events() channel.
//   go.delivery.report.fields (string, "key,value") - Comma separated list of fields to enable for delivery reports.
//                                       Allowed values: all, none (or empty string), key, value, headers
//                                       Warning: There is a performance penalty to include headers in the delivery report.
//   go.events.channel.size (int, 1000000) - Events().
//   go.produce.channel.size (int, 1000000) - ProduceChannel() buffer size (in number of messages)
//   go.logs.channel.enable (bool, false) - Forward log to Logs() channel.
//   go.logs.channel (chan kafka.LogEvent, nil) - Forward logs to application-provided channel instead of Logs(). Requires go.logs.channel.enable=true.
//
func NewProducer(conf *ConfigMap) (*Producer, error) {

	err := versionCheck()
	if err != nil {
		return nil, err
	}

	p := &Producer{}

	// before we do anything with the configuration, create a copy such that
	// the original is not mutated.
	confCopy := conf.clone()

	v, err := confCopy.extract("delivery.report.only.error", false)
	if v == true {
		// FIXME: The filtering of successful DRs must be done in
		//        the Go client to avoid cgoDr memory leaks.
		return nil, newErrorFromString(ErrUnsupportedFeature,
			"delivery.report.only.error=true is not currently supported by the Go client")
	}

	v, err = confCopy.extract("go.batch.producer", false)
	if err != nil {
		return nil, err
	}
	batchProducer := v.(bool)

	v, err = confCopy.extract("go.delivery.reports", true)
	if err != nil {
		return nil, err
	}
	p.handle.fwdDr = v.(bool)

	v, err = confCopy.extract("go.delivery.report.fields", "key,value")
	if err != nil {
		return nil, err
	}

	p.handle.msgFields, err = newMessageFieldsFrom(v)
	if err != nil {
		return nil, err
	}

	v, err = confCopy.extract("go.events.channel.size", 1000000)
	if err != nil {
		return nil, err
	}
	eventsChanSize := v.(int)

	v, err = confCopy.extract("go.produce.channel.size", 1000000)
	if err != nil {
		return nil, err
	}
	produceChannelSize := v.(int)

	logsChanEnable, logsChan, err := confCopy.extractLogConfig()
	if err != nil {
		return nil, err
	}

	if int(C.rd_kafka_version()) < 0x01000000 {
		// produce.offset.report is no longer used in librdkafka >= v1.0.0
		v, _ = confCopy.extract("{topic}.produce.offset.report", nil)
		if v == nil {
			// Enable offset reporting by default, unless overriden.
			confCopy.SetKey("{topic}.produce.offset.report", true)
		}
	}

	// Convert ConfigMap to librdkafka conf_t
	cConf, err := confCopy.convert()
	if err != nil {
		return nil, err
	}

	cErrstr := (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(cErrstr))

	C.rd_kafka_conf_set_events(cConf, C.RD_KAFKA_EVENT_DR|C.RD_KAFKA_EVENT_STATS|C.RD_KAFKA_EVENT_ERROR|C.RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH)

	// Create librdkafka producer instance
	p.handle.rk = C.rd_kafka_new(C.RD_KAFKA_PRODUCER, cConf, cErrstr, 256)
	if p.handle.rk == nil {
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	p.handle.p = p
	p.handle.setup()
	p.handle.rkq = C.rd_kafka_queue_get_main(p.handle.rk)
	p.events = make(chan Event, eventsChanSize)
	p.produceChannel = make(chan *Message, produceChannelSize)
	p.pollerTermChan = make(chan bool)

	if logsChanEnable {
		p.handle.setupLogQueue(logsChan, p.pollerTermChan)
	}

	p.handle.waitGroup.Add(1)
	go func() {
		poller(p, p.pollerTermChan)
		p.handle.waitGroup.Done()
	}()

	// non-batch or batch producer, only one must be used
	var producer func(*Producer)
	if batchProducer {
		producer = channelBatchProducer
	} else {
		producer = channelProducer
	}

	p.handle.waitGroup.Add(1)
	go func() {
		producer(p)
		p.handle.waitGroup.Done()
	}()

	return p, nil
}

// channel_producer serves the ProduceChannel channel
func channelProducer(p *Producer) {
	for m := range p.produceChannel {
		err := p.produce(m, C.RD_KAFKA_MSG_F_BLOCK, nil)
		if err != nil {
			m.TopicPartition.Error = err
			p.events <- m
		}
	}
}

// channelBatchProducer serves the ProduceChannel channel and attempts to
// improve cgo performance by using the produceBatch() interface.
func channelBatchProducer(p *Producer) {
	var buffered = make(map[string][]*Message)
	bufferedCnt := 0
	const batchSize int = 1000000
	totMsgCnt := 0
	totBatchCnt := 0

	for m := range p.produceChannel {
		buffered[*m.TopicPartition.Topic] = append(buffered[*m.TopicPartition.Topic], m)
		bufferedCnt++

	loop2:
		for true {
			select {
			case m, ok := <-p.produceChannel:
				if !ok {
					break loop2
				}
				if m == nil {
					panic("nil message received on ProduceChannel")
				}
				if m.TopicPartition.Topic == nil {
					panic(fmt.Sprintf("message without Topic received on ProduceChannel: %v", m))
				}
				buffered[*m.TopicPartition.Topic] = append(buffered[*m.TopicPartition.Topic], m)
				bufferedCnt++
				if bufferedCnt >= batchSize {
					break loop2
				}
			default:
				break loop2
			}
		}

		totBatchCnt++
		totMsgCnt += len(buffered)

		for topic, buffered2 := range buffered {
			err := p.produceBatch(topic, buffered2, C.RD_KAFKA_MSG_F_BLOCK)
			if err != nil {
				for _, m = range buffered2 {
					m.TopicPartition.Error = err
					p.events <- m
				}
			}
		}

		buffered = make(map[string][]*Message)
		bufferedCnt = 0
	}
}

// poller polls the rd_kafka_t handle for events until signalled for termination
func poller(p *Producer, termChan chan bool) {
	for {
		select {
		case _ = <-termChan:
			return

		default:
			_, term := p.handle.eventPoll(p.events, 100, 1000, termChan)
			if term {
				return
			}
			break
		}
	}
}

// GetMetadata queries broker for cluster and topic metadata.
// If topic is non-nil only information about that topic is returned, else if
// allTopics is false only information about locally used topics is returned,
// else information about all topics is returned.
// GetMetadata is equivalent to listTopics, describeTopics and describeCluster in the Java API.
func (p *Producer) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*Metadata, error) {
	return getMetadata(p, topic, allTopics, timeoutMs)
}

// QueryWatermarkOffsets returns the broker's low and high offsets for the given topic
// and partition.
func (p *Producer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	return queryWatermarkOffsets(p, topic, partition, timeoutMs)
}

// OffsetsForTimes looks up offsets by timestamp for the given partitions.
//
// The returned offset for each partition is the earliest offset whose
// timestamp is greater than or equal to the given timestamp in the
// corresponding partition. If the provided timestamp exceeds that of the
// last message in the partition, a value of -1 will be returned.
//
// The timestamps to query are represented as `.Offset` in the `times`
// argument and the looked up offsets are represented as `.Offset` in the returned
// `offsets` list.
//
// The function will block for at most timeoutMs milliseconds.
//
// Duplicate Topic+Partitions are not supported.
// Per-partition errors may be returned in the `.Error` field.
func (p *Producer) OffsetsForTimes(times []TopicPartition, timeoutMs int) (offsets []TopicPartition, err error) {
	return offsetsForTimes(p, times, timeoutMs)
}

// GetFatalError returns an Error object if the client instance has raised a fatal error, else nil.
func (p *Producer) GetFatalError() error {
	return getFatalError(p)
}

// TestFatalError triggers a fatal error in the underlying client.
// This is to be used strictly for testing purposes.
func (p *Producer) TestFatalError(code ErrorCode, str string) ErrorCode {
	return testFatalError(p, code, str)
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
func (p *Producer) SetOAuthBearerToken(oauthBearerToken OAuthBearerToken) error {
	return p.handle.setOAuthBearerToken(oauthBearerToken)
}

// SetOAuthBearerTokenFailure sets the error message describing why token
// retrieval/setting failed; it also schedules a new token refresh event for 10
// seconds later so the attempt may be retried. It will return nil on
// success, otherwise an error if:
// 1) SASL/OAUTHBEARER is not supported by the underlying librdkafka build;
// 2) SASL/OAUTHBEARER is supported but is not configured as the client's
// authentication mechanism.
func (p *Producer) SetOAuthBearerTokenFailure(errstr string) error {
	return p.handle.setOAuthBearerTokenFailure(errstr)
}

// Transactional API

// InitTransactions Initializes transactions for the producer instance.
//
// This function ensures any transactions initiated by previous instances
// of the producer with the same `transactional.id` are completed.
// If the previous instance failed with a transaction in progress the
// previous transaction will be aborted.
// This function needs to be called before any other transactional or
// produce functions are called when the `transactional.id` is configured.
//
// If the last transaction had begun completion (following transaction commit)
// but not yet finished, this function will await the previous transaction's
// completion.
//
// When any previous transactions have been fenced this function
// will acquire the internal producer id and epoch, used in all future
// transactional messages issued by this producer instance.
//
// Upon successful return from this function the application has to perform at
// least one of the following operations within `transaction.timeout.ms` to
// avoid timing out the transaction on the broker:
//  * `Produce()` (et.al)
//  * `SendOffsetsToTransaction()`
//  * `CommitTransaction()`
//  * `AbortTransaction()`
//
// Parameters:
//  * `ctx` - The maximum time to block, or nil for indefinite.
//            On timeout the operation may continue in the background,
//            depending on state, and it is okay to call `InitTransactions()`
//            again.
//
// Returns nil on success or an error on failure.
// Check whether the returned error object permits retrying
// by calling `err.(kafka.Error).IsRetriable()`, or whether a fatal
// error has been raised by calling `err.(kafka.Error).IsFatal()`.
func (p *Producer) InitTransactions(ctx context.Context) error {
	cError := C.rd_kafka_init_transactions(p.handle.rk,
		cTimeoutFromContext(ctx))
	if cError != nil {
		return newErrorFromCErrorDestroy(cError)
	}

	return nil
}

// BeginTransaction starts a new transaction.
//
// `InitTransactions()` must have been called successfully (once)
// before this function is called.
//
// Any messages produced, offsets sent (`SendOffsetsToTransaction()`),
// etc, after the successful return of this function will be part of
// the transaction and committed or aborted atomatically.
//
// Finish the transaction by calling `CommitTransaction()` or
// abort the transaction by calling `AbortTransaction()`.
//
// Returns nil on success or an error object on failure.
// Check whether a fatal error has been raised by
// calling `err.(kafka.Error).IsFatal()`.
//
// Note: With the transactional producer, `Produce()`, et.al, are only
// allowed during an on-going transaction, as started with this function.
// Any produce call outside an on-going transaction, or for a failed
// transaction, will fail.
func (p *Producer) BeginTransaction() error {
	cError := C.rd_kafka_begin_transaction(p.handle.rk)
	if cError != nil {
		return newErrorFromCErrorDestroy(cError)
	}

	return nil
}

// SendOffsetsToTransaction sends a list of topic partition offsets to the
// consumer group coordinator for `consumerMetadata`, and marks the offsets
// as part part of the current transaction.
// These offsets will be considered committed only if the transaction is
// committed successfully.
//
// The offsets should be the next message your application will consume,
// i.e., the last processed message's offset + 1 for each partition.
// Either track the offsets manually during processing or use
// `consumer.Position()` (on the consumer) to get the current offsets for
// the partitions assigned to the consumer.
//
// Use this method at the end of a consume-transform-produce loop prior
// to committing the transaction with `CommitTransaction()`.
//
// Parameters:
//  * `ctx` - The maximum amount of time to block, or nil for indefinite.
//  * `offsets` - List of offsets to commit to the consumer group upon
//                successful commit of the transaction. Offsets should be
//                the next message to consume, e.g., last processed message + 1.
//  * `consumerMetadata` - The current consumer group metadata as returned by
//                `consumer.GetConsumerGroupMetadata()` on the consumer
//                instance the provided offsets were consumed from.
//
// Note: The consumer must disable auto commits (set `enable.auto.commit` to false on the consumer).
//
// Note: Logical and invalid offsets (e.g., OffsetInvalid) in
// `offsets` will be ignored. If there are no valid offsets in
// `offsets` the function will return nil and no action will be taken.
//
// Returns nil on success or an error object on failure.
// Check whether the returned error object permits retrying
// by calling `err.(kafka.Error).IsRetriable()`, or whether an abortable
// or fatal error has been raised by calling
// `err.(kafka.Error).TxnRequiresAbort()` or `err.(kafka.Error).IsFatal()`
// respectively.
func (p *Producer) SendOffsetsToTransaction(ctx context.Context, offsets []TopicPartition, consumerMetadata *ConsumerGroupMetadata) error {
	var cOffsets *C.rd_kafka_topic_partition_list_t
	if offsets != nil {
		cOffsets = newCPartsFromTopicPartitions(offsets)
		defer C.rd_kafka_topic_partition_list_destroy(cOffsets)
	}

	cgmd, err := deserializeConsumerGroupMetadata(consumerMetadata.serialized)
	if err != nil {
		return err
	}
	defer C.rd_kafka_consumer_group_metadata_destroy(cgmd)

	cError := C.rd_kafka_send_offsets_to_transaction(
		p.handle.rk,
		cOffsets,
		cgmd,
		cTimeoutFromContext(ctx))
	if cError != nil {
		return newErrorFromCErrorDestroy(cError)
	}

	return nil
}

// CommitTransaction commits the current transaction.
//
// Any outstanding messages will be flushed (delivered) before actually
// committing the transaction.
//
// If any of the outstanding messages fail permanently the current
// transaction will enter the abortable error state and this
// function will return an abortable error, in this case the application
// must call `AbortTransaction()` before attempting a new
// transaction with `BeginTransaction()`.
//
// Parameters:
//  * `ctx` - The maximum amount of time to block, or nil for indefinite.
//
// Note: This function will block until all outstanding messages are
// delivered and the transaction commit request has been successfully
// handled by the transaction coordinator, or until the `ctx` expires,
// which ever comes first. On timeout the application may
// call the function again.
//
// Note: Will automatically call `Flush()` to ensure all queued
// messages are delivered before attempting to commit the transaction.
// The application MUST serve the `producer.Events()` channel for delivery
// reports in a separate go-routine during this time.
//
// Returns nil on success or an error object on failure.
// Check whether the returned error object permits retrying
// by calling `err.(kafka.Error).IsRetriable()`, or whether an abortable
// or fatal error has been raised by calling
// `err.(kafka.Error).TxnRequiresAbort()` or `err.(kafka.Error).IsFatal()`
// respectively.
func (p *Producer) CommitTransaction(ctx context.Context) error {
	cError := C.rd_kafka_commit_transaction(p.handle.rk,
		cTimeoutFromContext(ctx))
	if cError != nil {
		return newErrorFromCErrorDestroy(cError)
	}

	return nil
}

// AbortTransaction aborts the ongoing transaction.
//
// This function should also be used to recover from non-fatal abortable
// transaction errors.
//
// Any outstanding messages will be purged and fail with
// `ErrPurgeInflight` or `ErrPurgeQueue`.
//
// Parameters:
//  * `ctx` - The maximum amount of time to block, or nil for indefinite.
//
// Note: This function will block until all outstanding messages are purged
// and the transaction abort request has been successfully
// handled by the transaction coordinator, or until the `ctx` expires,
// which ever comes first. On timeout the application may
// call the function again.
//
// Note: Will automatically call `Purge()` and `Flush()` to ensure all queued
// and in-flight messages are purged before attempting to abort the transaction.
// The application MUST serve the `producer.Events()` channel for delivery
// reports in a separate go-routine during this time.
//
// Returns nil on success or an error object on failure.
// Check whether the returned error object permits retrying
// by calling `err.(kafka.Error).IsRetriable()`, or whether a fatal error
// has been raised by calling `err.(kafka.Error).IsFatal()`.
func (p *Producer) AbortTransaction(ctx context.Context) error {
	cError := C.rd_kafka_abort_transaction(p.handle.rk,
		cTimeoutFromContext(ctx))
	if cError != nil {
		return newErrorFromCErrorDestroy(cError)
	}

	return nil
}
