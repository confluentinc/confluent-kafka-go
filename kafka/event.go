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
	"os"
	"unsafe"
)

/*
#include <stdlib.h>
#include "select_rdkafka.h"
#include "glue_rdkafka.h"


void chdrs_to_tmphdrs (glue_msg_t *gMsg) {
    size_t i = 0;
    const char *name;
    const void *val;
    size_t size;
    rd_kafka_headers_t *chdrs;

    if (rd_kafka_message_headers(gMsg->msg, &chdrs)) {
        gMsg->tmphdrs = NULL;
        gMsg->tmphdrsCnt = 0;
        return;
    }

    gMsg->tmphdrsCnt = rd_kafka_header_cnt(chdrs);
    gMsg->tmphdrs = malloc(sizeof(*gMsg->tmphdrs) * gMsg->tmphdrsCnt);

    while (!rd_kafka_header_get_all(chdrs, i,
                                    &gMsg->tmphdrs[i].key,
                                    &gMsg->tmphdrs[i].val,
                                    (size_t *)&gMsg->tmphdrs[i].size))
        i++;
}

rd_kafka_event_t *_rk_queue_poll (rd_kafka_queue_t *rkq, int timeoutMs,
                                  rd_kafka_event_type_t *evtype,
                                  glue_msg_t *gMsg) {
    rd_kafka_event_t *rkev;

    rkev = rd_kafka_queue_poll(rkq, timeoutMs);
    *evtype = rd_kafka_event_type(rkev);

    if (*evtype == RD_KAFKA_EVENT_FETCH) {
        gMsg->msg = (rd_kafka_message_t *)rd_kafka_event_message_next(rkev);
        gMsg->ts = rd_kafka_message_timestamp(gMsg->msg, &gMsg->tstype);

        if (gMsg->want_hdrs)
            chdrs_to_tmphdrs(gMsg);
    }

    return rkev;
}
*/
import "C"

func chdrsToTmphdrs(gMsg *C.glue_msg_t) {
	C.chdrs_to_tmphdrs(gMsg)
}

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

func (h *handle) eventPollQueueContext(ctx context.Context, trigger *handleIOTrigger) (Event, bool) {
	for {
		// Check if there's a message already
		for {
			var evtype C.rd_kafka_event_type_t
			var gMsg C.glue_msg_t
			rkev := C._rk_queue_poll(trigger.rkq, 0, &evtype, &gMsg)

			if evtype == C.RD_KAFKA_EVENT_NONE {
				// We drained the queue and found nothing, now we must wait
				break
			}
			ev := h.processRkevToGoEvent(ctx, rkev, evtype, &gMsg)
			C.rd_kafka_event_destroy(rkev)
			if ev != nil {
				// We found something
				return ev, false
			}
		}

		select {
		case <-ctx.Done():
			return nil, true
		case _, ok := <-trigger.notifyChan:
			if !ok {
				return nil, false
			}
			// Triggered, go back into check loop.
		}
	}
}

// eventPollContext waits for a single "returnable" event on the underlying
// rd_kafka_queue_t queue and returns it. It uses the ioPollTrigger channel
// on h to be notified of any new event by librdkafka.
//
// An event is considered "returnable" if it is _not_ internally processed by
// processRkevToGoEvent.
//
// returns (event Event, terminate Bool) tuple, where Terminate indicates
// if the context was canceled
func (h *handle) eventPollContext(ctx context.Context) (Event, bool) {
	return h.eventPollQueueContext(ctx, h.ioPollTrigger)
}

// processRkevToGoEvent handles events coming out of librdkafka from the rd_kafka_poll call in
// eventPollContext. It processes some kinds of events itself (for example, it handles delivery reports
// by finding the appropriate channel to dispatch the report to, or handles assignment events by
// assigning partitions if configured to do so).
// All other events are handled by wrapping them in an appropriate Go type and returning them to the caller.
func (h *handle) processRkevToGoEvent(
	ctx context.Context, rkev *C.rd_kafka_event_t, evtype C.rd_kafka_event_type_t, gMsg *C.glue_msg_t,
) Event {

	switch evtype {
	case C.RD_KAFKA_EVENT_FETCH:
		// Consumer fetch event, new message.
		// Extracted into temporary fcMsg for optimization
		return h.newMessageFromGlueMsg(gMsg)

	case C.RD_KAFKA_EVENT_REBALANCE:
		return h.c.handleRebalanceEvent(rkev)

	case C.RD_KAFKA_EVENT_ERROR:
		// Error event
		cErr := C.rd_kafka_event_error(rkev)
		if cErr == C.RD_KAFKA_RESP_ERR__PARTITION_EOF {
			crktpar := C.rd_kafka_event_topic_partition(rkev)
			if crktpar == nil {
				return nil
			}

			defer C.rd_kafka_topic_partition_destroy(crktpar)
			var peof PartitionEOF
			setupTopicPartitionFromCrktpar((*TopicPartition)(&peof), crktpar)

			return peof

		} else if int(C.rd_kafka_event_error_is_fatal(rkev)) != 0 {
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
			return fatalErr

		} else {
			return newErrorFromCString(cErr, C.rd_kafka_event_error_string(rkev))
		}

	case C.RD_KAFKA_EVENT_STATS:
		return &Stats{C.GoString(C.rd_kafka_event_stats(rkev))}

	case C.RD_KAFKA_EVENT_DR:
		// Producer Delivery Report event
		// Each such event contains delivery reports for all
		// messages in the produced batch.
		// Forward delivery reports to per-message's response channel
		// or to the global Producer.Events channel, or none.
		rkmessages := make([]*C.rd_kafka_message_t, int(C.rd_kafka_event_message_count(rkev)))

		cnt := int(C.rd_kafka_event_message_array(rkev, (**C.rd_kafka_message_t)(unsafe.Pointer(&rkmessages[0])), C.size_t(len(rkmessages))))

		for _, rkmessage := range rkmessages[:cnt] {
			msg := h.newMessageFromC(rkmessage)
			var ch *chan Event

			if rkmessage._private != nil {
				// Find cgoif by id
				cg, found := h.cgoGet((int)((uintptr)(rkmessage._private)))
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
				select {
				case *ch <- outEvent:
				case <-ctx.Done():
					// This is a _very_ dangerous codepath. If the context passed in is canceled, and causes us
					// to return in this branch, it means we have only half-processed the batch of message delivery
					// reports in this event, and some of the DR channels will _never_ have the DR posted to them.
					// This is OK when we're shutting down a producer, but not OK otherwise.
					//
					// By the same token, if we _don't_ return here, it's possible to deadlock forever when shutting
					// down a producer because some client is not listening to their DR channel. So we kind of have
					// to do it. (We could change the exceptions of library users around what they have to do with
					// their DR channels, but that would be a pretty significant behaviour change.
					//
					// The producer is careful to pass in a context to this function that only expires when we
					// genuinely are shutting down, so it _should_ be OK.
					return nil
				}

			}
		}
		return nil

	case C.RD_KAFKA_EVENT_OFFSET_COMMIT:
		// Offsets committed
		cErr := C.rd_kafka_event_error(rkev)
		coffsets := C.rd_kafka_event_topic_partition_list(rkev)
		var offsets []TopicPartition
		if coffsets != nil {
			offsets = newTopicPartitionsFromCparts(coffsets)
		}

		if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
			return OffsetsCommitted{newErrorFromCString(cErr, C.rd_kafka_event_error_string(rkev)), offsets}
		}
		return OffsetsCommitted{nil, offsets}

	case C.RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH:
		return OAuthBearerTokenRefresh{C.GoString(C.rd_kafka_event_config_string(rkev))}

	case C.RD_KAFKA_EVENT_NONE:
		// poll timed out: no events available
		return nil

	default:
		if rkev != nil {
			fmt.Fprintf(os.Stderr, "Ignored event %s\n",
				C.GoString(C.rd_kafka_event_name(rkev)))
		}
		return nil

	}
}
