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

import (
	"fmt"
	"sync"
	"unsafe"
)

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
*/
import "C"

type Handle interface {
	get_handle() *handle
}

// Common instance handle for both Producer and Consumer
type handle struct {
	rk  *C.rd_kafka_t
	rkq *C.rd_kafka_queue_t

	// Termination of background go-routines
	terminated_chan chan string // string is go-routine name

	// Topic <-> rkt caches
	rkt_cache_lock sync.Mutex
	// topic name -> rkt cache
	rkt_cache map[string]*C.rd_kafka_topic_t
	// rkt -> topic name cache
	rkt_name_cache map[*C.rd_kafka_topic_t]string

	//
	// cgo map
	// Maps C callbacks based on cgoid back to its Go object
	cgo_lock   sync.Mutex
	cgoid_next uintptr
	cgomap     map[int]cgoif

	//
	// producer
	//
	p *Producer

	// Forward delivery reports on Producer.Events channel
	fwd_dr bool

	//
	// consumer
	//
	c *Consumer

	// Forward rebalancing ack responsibility to application (current setting)
	curr_app_rebalance_enable bool
}

func (h *handle) String() string {
	return C.GoString(C.rd_kafka_name(h.rk))
}

func (h *handle) setup() {
	h.rkt_cache = make(map[string]*C.rd_kafka_topic_t)
	h.rkt_name_cache = make(map[*C.rd_kafka_topic_t]string)

	h.terminated_chan = make(chan string, 10)
}

func (h *handle) cleanup() {
	for _, c_rkt := range h.rkt_cache {
		C.rd_kafka_topic_destroy(c_rkt)
	}

	if h.rkq != nil {
		C.rd_kafka_queue_destroy(h.rkq)
	}
}

// wait_terminated waits termination of background go-routines.
// term_cnt is the number of goroutines expected to signal termination completion
// on h.terminated_chan
func (h *handle) wait_terminated(term_cnt int) {
	// Wait for term_cnt termination-done events from goroutines
	for ; term_cnt > 0; term_cnt -= 1 {
		_ = <-h.terminated_chan
	}
}

// get_rkt0 finds or creates and returns a C topic_t object from the local cache.
func (h *handle) get_rkt0(topic string, c_topic *C.char, do_lock bool) (c_rkt *C.rd_kafka_topic_t) {
	if do_lock {
		h.rkt_cache_lock.Lock()
		defer h.rkt_cache_lock.Unlock()
	}
	c_rkt, ok := h.rkt_cache[topic]
	if ok {
		return c_rkt
	}

	if c_topic == nil {
		c_topic = C.CString(topic)
		defer C.free(unsafe.Pointer(c_topic))
	}

	c_rkt = C.rd_kafka_topic_new(h.rk, c_topic, nil)
	if c_rkt == nil {
		panic(fmt.Sprintf("Unable to create now C topic \"%s\": %s",
			topic, C.GoString(C.rd_kafka_err2str(C.rd_kafka_last_error()))))
	}

	h.rkt_cache[topic] = c_rkt
	h.rkt_name_cache[c_rkt] = topic

	return c_rkt
}

// get_rkt finds or creates and returns a C topic_t object from the local cache.
func (h *handle) get_rkt(topic string) (c_rkt *C.rd_kafka_topic_t) {
	return h.get_rkt0(topic, nil, true)
}

// get_topic_name_from_rkt returns the topic name for a C topic_t object, preferably
// using the local cache to avoid a cgo call.
func (h *handle) get_topic_name_from_rkt(c_rkt *C.rd_kafka_topic_t) (topic string) {
	h.rkt_cache_lock.Lock()
	defer h.rkt_cache_lock.Unlock()

	topic, ok := h.rkt_name_cache[c_rkt]
	if ok {
		return topic
	}

	// we need our own copy/refcount of the c_rkt
	c_topic := C.rd_kafka_topic_name(c_rkt)
	topic = C.GoString(c_topic)

	c_rkt = h.get_rkt0(topic, c_topic, false /* dont lock */)

	return topic
}

// cgoif is a generic interface for holding Go state passed as opaque
// value to the C code.
// Since pointers to complex Go types cannot be passed to C we instead create
// a cgoif object, generate a unique id that is added to the cgomap,
// and then pass that id to the C code. When the C code callback is called we
// use the id to look up the cgoif object in the cgomap.
type cgoif interface{}

// delivery report cgoif container
type cgo_dr struct {
	delivery_chan chan Event
	opaque        interface{}
}

// cgo_put adds object cg to the handle's cgo map and returns a
// unique id for the added entry.
// Thread-safe.
// FIXME: the uniquity of the id is questionable over time.
func (h *handle) cgo_put(cg cgoif) (cgoid int) {
	h.cgo_lock.Lock()
	defer h.cgo_lock.Unlock()

	h.cgoid_next += 1
	if h.cgoid_next == 0 {
		h.cgoid_next += 1
	}
	cgoid = (int)(h.cgoid_next)
	h.cgomap[cgoid] = cg
	return cgoid
}

// cgo_get looks up cgoid in the cgo map, deletes the reference from the map
// and returns the object, if found. Else returns nil, false.
// Thread-safe.
func (h *handle) cgo_get(cgoid int) (cg cgoif, found bool) {
	if cgoid == 0 {
		return nil, false
	}

	h.cgo_lock.Lock()
	defer h.cgo_lock.Unlock()
	cg, found = h.cgomap[cgoid]
	if found {
		delete(h.cgomap, cgoid)
	}

	return cg, found
}
