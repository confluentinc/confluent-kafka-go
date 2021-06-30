package kafka

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

import (
	"context"
	"fmt"
	"unsafe"
)

/*
#include <stdlib.h>
#include "select_rdkafka.h"
#include "glue_rdkafka.h"
*/
import "C"

// Event generic interface
type Event interface {
	// String returns a human-readable representation of the event
	String() string
}

// Specific event types

// Stats statistics event
type Stats struct {
	statsJSON string
}

func (e Stats) String() string {
	return e.statsJSON
}

// AssignedPartitions consumer group rebalance event: assigned partition set
type AssignedPartitions struct {
	Partitions []TopicPartition
}

func (e AssignedPartitions) String() string {
	return fmt.Sprintf("AssignedPartitions: %v", e.Partitions)
}

// RevokedPartitions consumer group rebalance event: revoked partition set
type RevokedPartitions struct {
	Partitions []TopicPartition
}

func (e RevokedPartitions) String() string {
	return fmt.Sprintf("RevokedPartitions: %v", e.Partitions)
}

// PartitionEOF consumer reached end of partition
// Needs to be explicitly enabled by setting the `enable.partition.eof`
// configuration property to true.
type PartitionEOF TopicPartition

func (p PartitionEOF) String() string {
	return fmt.Sprintf("EOF at %s", TopicPartition(p))
}

// OffsetsCommitted reports committed offsets
type OffsetsCommitted struct {
	Error   error
	Offsets []TopicPartition
}

func (o OffsetsCommitted) String() string {
	return fmt.Sprintf("OffsetsCommitted (%v, %v)", o.Error, o.Offsets)
}

// OAuthBearerTokenRefresh indicates token refresh is required
type OAuthBearerTokenRefresh struct {
	// Config is the value of the sasl.oauthbearer.config property
	Config string
}

func (o OAuthBearerTokenRefresh) String() string {
	return "OAuthBearerTokenRefresh"
}

// eventPoll waits for a single "returnable" event on the underlying
// rd_kafka_queue_t queue and returns it. It polls indefinitely until
// either an event is found or the provided context is canceled.
//
// An event is considered "returnable" if it is _not_ internally processed by
// this method.
//
// Once an event is retrieved from librdkafka, it is processed or returned as
// required, _regardless of whether ctx is subsequently canceled_. This ensures
// that we do not end up in an inconsistent state where we have thrown away an
// event.
func (h *handle) eventPoll(ctx context.Context, rkq *C.rd_kafka_queue_t) (Event, error) {
	polledEv, err := h.pollSingleCEventWithYield(ctx, rkq)
	if err != nil {
		// poll timed out: no events available
		return nil, err
	}

	if polledEv.rkev != nil {
		defer C.rd_kafka_event_destroy(polledEv.rkev)
	}

	switch polledEv.evType {
	case C.RD_KAFKA_EVENT_FETCH:
		// Consumer fetch event, new message.
		// Extracted into temporary gMsg for optimization
		return h.newMessageFromGlueMsg(&polledEv.gMsg), nil

	case C.RD_KAFKA_EVENT_REBALANCE:
		// Consumer rebalance event
		return h.c.handleRebalanceEvent(polledEv.rkev), nil

	case C.RD_KAFKA_EVENT_ERROR:
		// Error event
		cErr := C.rd_kafka_event_error(polledEv.rkev)
		if cErr == C.RD_KAFKA_RESP_ERR__PARTITION_EOF {
			crktpar := C.rd_kafka_event_topic_partition(polledEv.rkev)
			if crktpar == nil {
				return nil, nil
			}

			defer C.rd_kafka_topic_partition_destroy(crktpar)
			var peof PartitionEOF
			setupTopicPartitionFromCrktpar((*TopicPartition)(&peof), crktpar)

			return peof, nil

		} else if int(C.rd_kafka_event_error_is_fatal(polledEv.rkev)) != 0 {
			// A fatal error has been raised.
			// Extract the actual error from the client
			// instance and return a new Error with
			// fatal set to true.
			cFatalErrstrSize := C.size_t(512)
			cFatalErrstr := (*C.char)(C.malloc(cFatalErrstrSize))
			defer C.free(unsafe.Pointer(cFatalErrstr))
			cFatalErr := C.rd_kafka_fatal_error(h.rk, cFatalErrstr, cFatalErrstrSize)
			fatalErr := newErrorFromCString(cFatalErr, cFatalErrstr)
			fatalErr.fatal = true
			return fatalErr, nil

		} else {
			return newErrorFromCString(cErr, C.rd_kafka_event_error_string(polledEv.rkev)), nil
		}

	case C.RD_KAFKA_EVENT_STATS:
		return &Stats{C.GoString(C.rd_kafka_event_stats(polledEv.rkev))}, nil

	case C.RD_KAFKA_EVENT_DR:
		// Producer Delivery Report event
		// Each such event contains delivery reports for all
		// messages in the produced batch.
		// Forward delivery reports to per-message's response channel
		// or to the global Producer.Events channel, or none.
		rkmessages := make([]*C.rd_kafka_message_t, int(C.rd_kafka_event_message_count(polledEv.rkev)))

		cnt := int(C.rd_kafka_event_message_array(polledEv.rkev, (**C.rd_kafka_message_t)(unsafe.Pointer(&rkmessages[0])), C.size_t(len(rkmessages))))

		for _, rkmessage := range rkmessages[:cnt] {
			msg := h.newMessageFromC(rkmessage)
			var ch *chan Event

			if rkmessage._private != nil {
				// Find cgoif by id
				cg, found := h.cgoGet(rkmessage._private)
				if found {
					cdr := cg.(cgoDr)

					if cdr.deliveryChan != nil {
						ch = &cdr.deliveryChan
					}
					msg.Opaque = cdr.opaque
				}
			}

			var outEvent Event
			if h.fwdDrErrEvents {
				mwe := &MessageWithError{Message: *msg}
				if rkmessage.err != C.RD_KAFKA_RESP_ERR_NO_ERROR {
					ep := newError(rkmessage.err)
					mwe.Error = &ep
				}
				outEvent = mwe
			} else {
				outEvent = msg
			}

			if ch != nil {
				// Note that if nothing is consuming *ch (which could be either a per-message DR channel, or the
				// global deliveryChan) then Poll() and Close() can block indefinitely because of this line.
				// Therefore, it's crucial that users consume deliveryChan/per-message DR channels from another
				// goroutine than the one that calls Poll()/Close().
				*ch <- outEvent
			}
		}
		return nil, ctx.Err()

	case C.RD_KAFKA_EVENT_OFFSET_COMMIT:
		// Offsets committed
		cErr := C.rd_kafka_event_error(polledEv.rkev)
		coffsets := C.rd_kafka_event_topic_partition_list(polledEv.rkev)
		var offsets []TopicPartition
		if coffsets != nil {
			offsets = newTopicPartitionsFromCparts(coffsets)
		}

		if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
			return OffsetsCommitted{newErrorFromCString(cErr, C.rd_kafka_event_error_string(polledEv.rkev)), offsets}, nil
		}
		return OffsetsCommitted{nil, offsets}, nil

	case C.RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH:
		ev := OAuthBearerTokenRefresh{C.GoString(C.rd_kafka_event_config_string(polledEv.rkev))}
		return ev, nil

	case C.RD_KAFKA_EVENT_NONE:
		// poll timed out: no events available
		return nil, ctx.Err()

	default:
		return nil, nil
	}
}
