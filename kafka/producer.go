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
	"fmt"
	"math"
	"time"
	"unsafe"
)

/*
#include <stdlib.h>
#include <librdkafka/rdkafka.h>

rd_kafka_resp_err_t do_produce (rd_kafka_t *rk,
          rd_kafka_topic_t *rkt, int32_t partition,
          int msgflags,
          void *val, size_t val_len, void *key, size_t key_len,
          int64_t timestamp,
          uintptr_t cgoid) {

#ifdef RD_KAFKA_V_TIMESTAMP
  return rd_kafka_producev(rk,
        RD_KAFKA_V_RKT(rkt),
        RD_KAFKA_V_PARTITION(partition),
        RD_KAFKA_V_MSGFLAGS(msgflags),
        RD_KAFKA_V_VALUE(val, val_len),
        RD_KAFKA_V_KEY(key, key_len),
        RD_KAFKA_V_TIMESTAMP(timestamp),
        RD_KAFKA_V_OPAQUE((void *)cgoid),
        RD_KAFKA_V_END);
#else
  if (timestamp)
      return RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
  if (rd_kafka_produce(rkt, partition, msgflags, val, val_len, key, key_len,
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

	var valp *byte
	var keyp *byte
	var empty byte
	valLen := 0
	keyLen := 0

	if msg.Value != nil {
		valLen = len(msg.Value)
		// allow sending 0-length messages (as opposed to null messages)
		if valLen > 0 {
			valp = &msg.Value[0]
		} else {
			valp = &empty
		}
	}
	if msg.Key != nil {
		keyLen = len(msg.Key)
		if keyLen > 0 {
			keyp = &msg.Key[0]
		} else {
			keyp = &empty
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

	cErr := C.do_produce(p.handle.rk, crkt,
		C.int32_t(msg.TopicPartition.Partition),
		C.int(msgFlags)|C.RD_KAFKA_MSG_F_COPY,
		unsafe.Pointer(valp), C.size_t(valLen),
		unsafe.Pointer(keyp), C.size_t(keyLen),
		C.int64_t(timestamp),
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
// Returns an error if message could not be enqueued.
func (p *Producer) Produce(msg *Message, deliveryChan chan Event) error {
	return p.produce(msg, 0, deliveryChan)
}

// Produce a batch of messages.
// These batches do not relate to the message batches sent to the broker, the latter
// are collected on the fly internally in librdkafka.
// WARNING: This is an experimental API.
// NOTE: timestamps are not supported with this API.
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
	p.handle.waitTerminated(2)

	close(p.events)

	p.handle.cleanup()

	C.rd_kafka_destroy(p.handle.rk)
}

// NewProducer creates a new high-level Producer instance.
//
// conf is a *ConfigMap with standard librdkafka configuration properties, see here:
//
//
//
//
//
// Supported special configuration properties:
//   go.batch.producer (bool, false) - Enable batch producer (experimental for increased performance).
//                                     These batches do not relate to Kafka message batches in any way.
//   go.delivery.reports (bool, true) - Forward per-message delivery reports to the
//                                      Events() channel.
//   go.produce.channel.size (int, 1000000) - ProduceChannel() buffer size (in number of messages)
//
func NewProducer(conf *ConfigMap) (*Producer, error) {
	p := &Producer{}

	v, err := conf.extract("go.batch.producer", false)
	if err != nil {
		return nil, err
	}
	batchProducer := v.(bool)

	v, err = conf.extract("go.delivery.reports", true)
	if err != nil {
		return nil, err
	}
	p.handle.fwdDr = v.(bool)

	v, err = conf.extract("go.produce.channel.size", 1000000)
	if err != nil {
		return nil, err
	}
	produceChannelSize := v.(int)

	// Convert ConfigMap to librdkafka conf_t
	cConf, err := conf.convert()
	if err != nil {
		return nil, err
	}

	cErrstr := (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(cErrstr))

	C.rd_kafka_conf_set_events(cConf, C.RD_KAFKA_EVENT_DR|C.RD_KAFKA_EVENT_STATS)

	// Create librdkafka producer instance
	p.handle.rk = C.rd_kafka_new(C.RD_KAFKA_PRODUCER, cConf, cErrstr, 256)
	if p.handle.rk == nil {
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	p.handle.p = p
	p.handle.setup()
	p.handle.rkq = C.rd_kafka_queue_get_main(p.handle.rk)
	p.events = make(chan Event, 1000000)
	p.produceChannel = make(chan *Message, produceChannelSize)
	p.pollerTermChan = make(chan bool)

	go poller(p, p.pollerTermChan)

	// non-batch or batch producer, only one must be used
	if batchProducer {
		go channelBatchProducer(p)
	} else {
		go channelProducer(p)
	}

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

	p.handle.terminatedChan <- "channelProducer"
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
	p.handle.terminatedChan <- "channelBatchProducer"
}

// poller polls the rd_kafka_t handle for events until signalled for termination
func poller(p *Producer, termChan chan bool) {
out:
	for true {
		select {
		case _ = <-termChan:
			break out

		default:
			_, term := p.handle.eventPoll(p.events, 100, 1000, termChan)
			if term {
				break out
			}
			break
		}
	}

	p.handle.terminatedChan <- "poller"

}

// GetMetadata queries broker for cluster and topic metadata.
// If topic is non-nil only information about that topic is returned, else if
// allTopics is false only information about locally used topics is returned,
// else information about all topics is returned.
func (p *Producer) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*Metadata, error) {
	return getMetadata(p, topic, allTopics, timeoutMs)
}

// QueryWatermarkOffsets returns the broker's low and high offsets for the given topic
// and partition.
func (p *Producer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	return queryWatermarkOffsets(p, topic, partition, timeoutMs)
}
