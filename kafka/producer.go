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
*/
import "C"

// Producer: High-level Apache Kafka Producer instance
type Producer struct {
	Events         chan Event
	ProduceChannel chan *Message
	handle         handle

	// Terminates the poller() goroutine
	poller_term_chan chan bool
}

// String returns a human readable name for a Producer instance
func (p *Producer) String() string {
	return p.handle.String()
}

// get_handle implements the Handle interface
func (p *Producer) get_handle() *handle {
	return &p.handle
}

// Produce single message.
// This is an asynchronous call that enqueues the message on the internal
// transmit queue, thus returning immediately.
// The delivery report will be sent on the provided delivery_chan if specified,
// or on the Producer object's Events channel if not.
func (p *Producer) produce(msg *Message, msg_flags int, delivery_chan chan Event, opaque *interface{}) error {
	c_rkt := p.handle.get_rkt(*msg.TopicPartition.Topic)

	var valp *byte = nil
	var keyp *byte = nil
	var empty byte
	val_len := 0
	key_len := 0

	if msg.Value != nil {
		val_len = len(msg.Value)
		// allow sending 0-length messages (as opposed to null messages)
		if val_len > 0 {
			valp = &msg.Value[0]
		} else {
			valp = &empty
		}
	}
	if msg.Key != nil {
		key_len = len(msg.Key)
		if key_len > 0 {
			keyp = &msg.Key[0]
		} else {
			keyp = &empty
		}
	}

	var cgoid uintptr

	// Per-message state that needs to be retained through the C code:
	//   delivery channel (if specified)
	//   message opaque   (if specified)
	// Since these cant be passed as opaque pointers to the C code,
	// due to cgo constraints, we add them to a per-producer map for lookup
	// when the C code triggers the callbacks or events.
	if delivery_chan != nil || opaque != nil {
		cgoid = p.handle.cgo_put(cgo_dr{delivery_chan: delivery_chan, opaque: opaque})
	}

	r := int(C.rd_kafka_produce(c_rkt, C.int32_t(msg.TopicPartition.Partition),
		C.int(msg_flags)|C.RD_KAFKA_MSG_F_COPY,
		unsafe.Pointer(valp), C.size_t(val_len),
		unsafe.Pointer(keyp), C.size_t(key_len), unsafe.Pointer(cgoid)))
	if r == -1 {
		if cgoid != 0 {
			p.handle.cgo_get(cgoid)
		}
		return NewKafkaError(C.rd_kafka_last_error())
	}

	return nil
}

func (p *Producer) Produce(msg *Message, delivery_chan chan Event, opaque *interface{}) error {
	return p.produce(msg, 0, delivery_chan, opaque)
}

// Produce a batch of messages.
// These batches do not relate to the message batches sent to the broker, the latter
// are collected on the fly internally in librdkafka.
// This is an experimental API.
func (p *Producer) produce_batch(topic string, msgs []*Message, msg_flags int) error {
	c_rkt := p.handle.get_rkt(topic)

	c_msgs := make([]C.rd_kafka_message_t, len(msgs))
	for i, m := range msgs {
		p.handle.message_to_c(m, &c_msgs[i])
	}
	r := C.rd_kafka_produce_batch(c_rkt, C.RD_KAFKA_PARTITION_UA, C.int(msg_flags)|C.RD_KAFKA_MSG_F_FREE,
		(*C.rd_kafka_message_t)(&c_msgs[0]), C.int(len(msgs)))
	if r == -1 {
		return NewKafkaError(C.rd_kafka_last_error())
	}

	return nil
}

// Len returns the number of messages and requests waiting to be transmitted to the broker
// as well as delivery reports queued for the application.
// Includes messages on ProduceChannel.
func (p *Producer) Len() int {
	return len(p.ProduceChannel) + len(p.Events) + int(C.rd_kafka_outq_len(p.handle.rk))
}

// Flush and wait for outstanding messages and requests to complete delivery.
// Includes messages on ProduceChannel.
// Runs until value reaches zero or on timeout_ms.
// Returns the number of outstanding events still un-flushed.
func (p *Producer) Flush(timeout_ms int) int {
	term_chan := make(chan bool) // unused stand-in term_chan

	d, _ := time.ParseDuration(fmt.Sprintf("%dms", timeout_ms))
	t_end := time.Now().Add(d)
	for p.Len() > 0 {
		remain := t_end.Sub(time.Now()).Seconds()
		if remain <= 0.0 {
			return p.Len()
		}

		p.handle.event_poll(p.Events,
			int(math.Min(100, remain*1000)), 1000, term_chan)
	}

	return 0
}

// Close a Producer instance.
// The Producer object or its channels are no longer usable after this call.
func (p *Producer) Close() {
	// Wait for poller() (signaled by closing poller_term_chan)
	// and channel_producer() (signaled by closing ProduceChannel)
	close(p.poller_term_chan)
	close(p.ProduceChannel)
	p.handle.wait_terminated(2)

	close(p.Events)

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
//                                      Events channel.
//   go.produce.channel.size (int, 1000000) - ProduceChannel buffer size (in number of messages)
//
func NewProducer(conf *ConfigMap) (*Producer, error) {
	p := &Producer{}

	v, err := conf.extract("go.batch.producer", false)
	if err != nil {
		return nil, err
	}
	batch_producer := v.(bool)

	v, err = conf.extract("go.delivery.reports", true)
	if err != nil {
		return nil, err
	}
	p.handle.fwd_dr = v.(bool)

	v, err = conf.extract("go.produce.channel.size", 1000000)
	if err != nil {
		return nil, err
	}
	produce_channel_size := v.(int)

	// Convert ConfigMap to librdkafka conf_t
	c_conf, err := conf.convert()
	if err != nil {
		return nil, err
	}

	var c_errstr *C.char = (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(c_errstr))

	C.rd_kafka_conf_set_events(c_conf, C.RD_KAFKA_EVENT_DR)

	// Create librdkafka producer instance
	p.handle.rk = C.rd_kafka_new(C.RD_KAFKA_PRODUCER, c_conf, c_errstr, 256)
	if p.handle.rk == nil {
		return nil, NewKafkaErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, c_errstr)
	}

	p.handle.p = p
	p.handle.setup()
	p.handle.rkq = C.rd_kafka_queue_get_main(p.handle.rk)
	p.handle.cgomap = make(map[uintptr]cgoif)
	p.Events = make(chan Event, 1000000)
	p.ProduceChannel = make(chan *Message, produce_channel_size)
	p.poller_term_chan = make(chan bool)

	go poller(p, p.poller_term_chan)

	// non-batch or batch producer, only one must be used
	if batch_producer {
		go channel_batch_producer(p)
	} else {
		go channel_producer(p)
	}

	return p, nil
}

// channel_producer serves the ProduceChannel channel
func channel_producer(p *Producer) {

	for m := range p.ProduceChannel {
		err := p.produce(m, C.RD_KAFKA_MSG_F_BLOCK, nil, nil)
		if err != nil {
			m.TopicPartition.Error = err
			p.Events <- m
		}
	}

	p.handle.terminated_chan <- "channel_producer"
}

// channel_batch_producer serves the ProduceChannel channel and attempts to
// improve cgo performance by using the produce_batch() interface.
func channel_batch_producer(p *Producer) {
	var buffered = make(map[string][]*Message)
	buffered_cnt := 0
	const batch_size int = 1000000
	tot_msg_cnt := 0
	tot_batch_cnt := 0

	for m := range p.ProduceChannel {
		buffered[*m.TopicPartition.Topic] = append(buffered[*m.TopicPartition.Topic], m)
		buffered_cnt += 1

	loop2:
		for true {
			select {
			case m, ok := <-p.ProduceChannel:
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
				buffered_cnt += 1
				if buffered_cnt >= batch_size {
					break loop2
				}
			default:
				break loop2
			}
		}

		tot_batch_cnt += 1
		tot_msg_cnt += len(buffered)

		for topic, buffered2 := range buffered {
			err := p.produce_batch(topic, buffered2, C.RD_KAFKA_MSG_F_BLOCK)
			if err != nil {
				for _, m = range buffered2 {
					m.TopicPartition.Error = err
					p.Events <- m
				}
			}
		}

		buffered = make(map[string][]*Message)
		buffered_cnt = 0
	}
	p.handle.terminated_chan <- "channel_batch_producer"
}

// poller polls the rd_kafka_t handle for events until signalled for termination
func poller(p *Producer, term_chan chan bool) {
out:
	for true {
		select {
		case _ = <-term_chan:
			break out

		default:
			_, term := p.handle.event_poll(p.Events, 100, 1000, term_chan)
			if term {
				break out
			}
			break
		}
	}

	p.handle.terminated_chan <- "poller"

}

// GetMetadata queries broker for cluster and topic metadata.
// If topic is non-nil only information about that topic is returned, else if
// all_topics is false only information about locally used topics is returned,
// else information about all topics is returned.
func (p *Producer) GetMetadata(topic *string, all_topics bool, timeout_ms int) (*Metadata, error) {
	return get_metadata(p, topic, all_topics, timeout_ms)
}

// QueryWatermarkOffsets returns the broker's low and high offsets for the given topic
// and partition.
func (p *Producer) QueryWatermarkOffsets(topic string, partition int32, timeout_ms int) (low, high int64, err error) {
	return queryWatermarkOffsets(p, topic, partition, timeout_ms)
}
