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
	"strings"
	"sync"
	"time"
	"unsafe"
)

/*
#include "select_rdkafka.h"
#include <stdlib.h>
*/
import "C"

// OAuthBearerToken represents the data to be transmitted
// to a broker during SASL/OAUTHBEARER authentication.
type OAuthBearerToken struct {
	// Token value, often (but not necessarily) a JWS compact serialization
	// as per https://tools.ietf.org/html/rfc7515#section-3.1; it must meet
	// the regular expression for a SASL/OAUTHBEARER value defined at
	// https://tools.ietf.org/html/rfc7628#section-3.1
	TokenValue string
	// Metadata about the token indicating when it expires (local time);
	// it must represent a time in the future
	Expiration time.Time
	// Metadata about the token indicating the Kafka principal name
	// to which it applies (for example, "admin")
	Principal string
	// SASL extensions, if any, to be communicated to the broker during
	// authentication (all keys and values of which must meet the regular
	// expressions defined at https://tools.ietf.org/html/rfc7628#section-3.1,
	// and it must not contain the reserved "auth" key)
	Extensions map[string]string
}

// Handle represents a generic client handle containing common parts for
// both Producer and Consumer.
type Handle interface {
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
	SetOAuthBearerToken(oauthBearerToken OAuthBearerToken) error

	// SetOAuthBearerTokenFailure sets the error message describing why token
	// retrieval/setting failed; it also schedules a new token refresh event for 10
	// seconds later so the attempt may be retried. It will return nil on
	// success, otherwise an error if:
	// 1) SASL/OAUTHBEARER is not supported by the underlying librdkafka build;
	// 2) SASL/OAUTHBEARER is supported but is not configured as the client's
	// authentication mechanism.
	SetOAuthBearerTokenFailure(errstr string) error

	// gethandle() returns the internal handle struct pointer
	gethandle() *handle

	// verifyClient() returns the validity of client
	verifyClient() error

	// IsClosed() returns the bool to check if the client is closed
	IsClosed() bool
}

// Common instance handle for both Producer and Consumer
type handle struct {
	rk  *C.rd_kafka_t
	rkq *C.rd_kafka_queue_t

	// Forward logs from librdkafka log queue to logs channel.
	logs          chan LogEvent
	logq          *C.rd_kafka_queue_t
	closeLogsChan bool

	// Topic <-> rkt caches
	rktCacheLock sync.Mutex
	// topic name -> rkt cache
	rktCache map[string]*C.rd_kafka_topic_t
	// rkt -> topic name cache
	rktNameCache map[*C.rd_kafka_topic_t]string

	// Cached instance name to avoid CGo call in String()
	name string

	//
	// cgo map
	// Maps C callbacks based on cgoid back to its Go object
	cgoLock   sync.Mutex
	cgoidNext uintptr
	cgomap    map[int]cgoif

	//
	// producer
	//
	p *Producer

	// Forward delivery reports on Producer.Events channel
	fwdDr bool

	// Enabled message fields for delivery reports and consumed messages.
	msgFields *messageFields

	//
	// consumer
	//
	c *Consumer

	// WaitGroup to wait for spawned go-routines to finish.
	waitGroup sync.WaitGroup
}

func (h *handle) String() string {
	return h.name
}

func (h *handle) setup() {
	h.rktCache = make(map[string]*C.rd_kafka_topic_t)
	h.rktNameCache = make(map[*C.rd_kafka_topic_t]string)
	h.cgomap = make(map[int]cgoif)
	h.name = C.GoString(C.rd_kafka_name(h.rk))
	if h.msgFields == nil {
		h.msgFields = newMessageFields()
	}
}

func (h *handle) cleanup() {
	if h.logs != nil {
		C.rd_kafka_queue_destroy(h.logq)
		if h.closeLogsChan {
			close(h.logs)
		}
	}

	for _, crkt := range h.rktCache {
		C.rd_kafka_topic_destroy(crkt)
	}

	if h.rkq != nil {
		C.rd_kafka_queue_destroy(h.rkq)
	}
}

func (h *handle) setupLogQueue(logsChan chan LogEvent, termChan chan bool) {
	if logsChan == nil {
		logsChan = make(chan LogEvent, 10000)
		h.closeLogsChan = true
	}

	h.logs = logsChan

	// Let librdkafka forward logs to our log queue instead of the main queue
	h.logq = C.rd_kafka_queue_new(h.rk)
	C.rd_kafka_set_log_queue(h.rk, h.logq)

	// Start a polling goroutine to consume the log queue
	h.waitGroup.Add(1)
	go func() {
		h.pollLogEvents(h.logs, 100, termChan)
		h.waitGroup.Done()
	}()

}

// getRkt0 finds or creates and returns a C topic_t object from the local cache.
func (h *handle) getRkt0(topic string, ctopic *C.char, doLock bool) (crkt *C.rd_kafka_topic_t) {
	if doLock {
		h.rktCacheLock.Lock()
		defer h.rktCacheLock.Unlock()
	}
	crkt, ok := h.rktCache[topic]
	if ok {
		return crkt
	}

	if ctopic == nil {
		ctopic = C.CString(topic)
		defer C.free(unsafe.Pointer(ctopic))
	}

	crkt = C.rd_kafka_topic_new(h.rk, ctopic, nil)
	if crkt == nil {
		panic(fmt.Sprintf("Unable to create new C topic \"%s\": %s",
			topic, C.GoString(C.rd_kafka_err2str(C.rd_kafka_last_error()))))
	}

	h.rktCache[topic] = crkt
	h.rktNameCache[crkt] = topic

	return crkt
}

// getRkt finds or creates and returns a C topic_t object from the local cache.
func (h *handle) getRkt(topic string) (crkt *C.rd_kafka_topic_t) {
	return h.getRkt0(topic, nil, true)
}

// getTopicNameFromRkt returns the topic name for a C topic_t object, preferably
// using the local cache to avoid a cgo call.
func (h *handle) getTopicNameFromRkt(crkt *C.rd_kafka_topic_t) (topic string) {
	h.rktCacheLock.Lock()
	defer h.rktCacheLock.Unlock()

	topic, ok := h.rktNameCache[crkt]
	if ok {
		return topic
	}

	// we need our own copy/refcount of the crkt
	ctopic := C.rd_kafka_topic_name(crkt)
	topic = C.GoString(ctopic)

	crkt = h.getRkt0(topic, ctopic, false /* dont lock */)

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
type cgoDr struct {
	deliveryChan chan Event
	opaque       interface{}
}

// cgoPut adds object cg to the handle's cgo map and returns a
// unique id for the added entry.
// Thread-safe.
// FIXME: the uniquity of the id is questionable over time.
func (h *handle) cgoPut(cg cgoif) (cgoid int) {
	h.cgoLock.Lock()
	defer h.cgoLock.Unlock()

	h.cgoidNext++
	if h.cgoidNext == 0 {
		h.cgoidNext++
	}
	cgoid = (int)(h.cgoidNext)
	h.cgomap[cgoid] = cg
	return cgoid
}

// cgoGet looks up cgoid in the cgo map, deletes the reference from the map
// and returns the object, if found. Else returns nil, false.
// Thread-safe.
func (h *handle) cgoGet(cgoid int) (cg cgoif, found bool) {
	if cgoid == 0 {
		return nil, false
	}

	h.cgoLock.Lock()
	defer h.cgoLock.Unlock()
	cg, found = h.cgomap[cgoid]
	if found {
		delete(h.cgomap, cgoid)
	}

	return cg, found
}

// setOauthBearerToken - see rd_kafka_oauthbearer_set_token()
func (h *handle) setOAuthBearerToken(oauthBearerToken OAuthBearerToken) error {
	cTokenValue := C.CString(oauthBearerToken.TokenValue)
	defer C.free(unsafe.Pointer(cTokenValue))

	cPrincipal := C.CString(oauthBearerToken.Principal)
	defer C.free(unsafe.Pointer(cPrincipal))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	cExtensions := make([]*C.char, 2*len(oauthBearerToken.Extensions))
	extensionSize := 0
	for key, value := range oauthBearerToken.Extensions {
		cExtensions[extensionSize] = C.CString(key)
		defer C.free(unsafe.Pointer(cExtensions[extensionSize]))
		extensionSize++
		cExtensions[extensionSize] = C.CString(value)
		defer C.free(unsafe.Pointer(cExtensions[extensionSize]))
		extensionSize++
	}

	var cExtensionsToUse **C.char
	if extensionSize > 0 {
		cExtensionsToUse = (**C.char)(unsafe.Pointer(&cExtensions[0]))
	}

	cErr := C.rd_kafka_oauthbearer_set_token(h.rk, cTokenValue,
		C.int64_t(oauthBearerToken.Expiration.UnixNano()/(1000*1000)), cPrincipal,
		cExtensionsToUse, C.size_t(extensionSize), cErrstr, cErrstrSize)
	if cErr == C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil
	}
	return newErrorFromCString(cErr, cErrstr)
}

// setOauthBearerTokenFailure - see rd_kafka_oauthbearer_set_token_failure()
func (h *handle) setOAuthBearerTokenFailure(errstr string) error {
	cerrstr := C.CString(errstr)
	defer C.free(unsafe.Pointer(cerrstr))
	cErr := C.rd_kafka_oauthbearer_set_token_failure(h.rk, cerrstr)
	if cErr == C.RD_KAFKA_RESP_ERR_NO_ERROR {
		return nil
	}
	return newError(cErr)
}

// messageFields controls which fields are made available for producer delivery reports & consumed messages.
// true values indicate that the field should be included
type messageFields struct {
	Key     bool
	Value   bool
	Headers bool
}

// disableAll disable all fields
func (mf *messageFields) disableAll() {
	mf.Key = false
	mf.Value = false
	mf.Headers = false
}

// newMessageFields returns a new messageFields with all fields enabled
func newMessageFields() *messageFields {
	return &messageFields{
		Key:     true,
		Value:   true,
		Headers: true,
	}
}

// newMessageFieldsFrom constructs a new messageFields from the given configuration value
func newMessageFieldsFrom(v ConfigValue) (*messageFields, error) {
	msgFields := newMessageFields()
	switch v {
	case "all":
		// nothing to do
	case "", "none":
		msgFields.disableAll()
	default:
		msgFields.disableAll()
		for _, value := range strings.Split(v.(string), ",") {
			switch value {
			case "key":
				msgFields.Key = true
			case "value":
				msgFields.Value = true
			case "headers":
				msgFields.Headers = true
			default:
				return nil, fmt.Errorf("unknown message field: %s", value)
			}
		}
	}
	return msgFields, nil
}
