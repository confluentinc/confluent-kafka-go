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
	"fmt"
	"os"
	"unsafe"
)

/*
#include <stdlib.h>
#include "select_rdkafka.h"
#include "glue_rdkafka.h"


#ifdef RD_KAFKA_V_HEADERS
void chdrs_to_tmphdrs (rd_kafka_headers_t *chdrs, tmphdr_t *tmphdrs) {
   size_t i = 0;
   const char *name;
   const void *val;
   size_t size;

   while (!rd_kafka_header_get_all(chdrs, i,
                                   &tmphdrs[i].key,
                                   &tmphdrs[i].val,
                                   (size_t *)&tmphdrs[i].size))
     i++;
}
#endif

rd_kafka_event_t *_rk_queue_poll (rd_kafka_queue_t *rkq, int timeoutMs,
                                  rd_kafka_event_type_t *evtype,
                                  fetched_c_msg_t *fcMsg,
                                  rd_kafka_event_t *prev_rkev) {
    rd_kafka_event_t *rkev;

    if (prev_rkev)
      rd_kafka_event_destroy(prev_rkev);

    rkev = rd_kafka_queue_poll(rkq, timeoutMs);
    *evtype = rd_kafka_event_type(rkev);

    if (*evtype == RD_KAFKA_EVENT_FETCH) {
#ifdef RD_KAFKA_V_HEADERS
        rd_kafka_headers_t *hdrs;
#endif

        fcMsg->msg = (rd_kafka_message_t *)rd_kafka_event_message_next(rkev);
        fcMsg->ts = rd_kafka_message_timestamp(fcMsg->msg, &fcMsg->tstype);

#ifdef RD_KAFKA_V_HEADERS
        if (!rd_kafka_message_headers(fcMsg->msg, &hdrs)) {
           fcMsg->tmphdrsCnt = rd_kafka_header_cnt(hdrs);
           fcMsg->tmphdrs = malloc(sizeof(*fcMsg->tmphdrs) * fcMsg->tmphdrsCnt);
           chdrs_to_tmphdrs(hdrs, fcMsg->tmphdrs);
        } else {
#else
        if (1) {
#endif
           fcMsg->tmphdrs = NULL;
           fcMsg->tmphdrsCnt = 0;
        }
    }
    return rkev;
}
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

// eventPoll polls an event from the handler's C rd_kafka_queue_t,
// translates it into an Event type and then sends on `channel` if non-nil, else returns the Event.
// term_chan is an optional channel to monitor along with producing to channel
// to indicate that `channel` is being terminated.
// returns (event Event, terminate Bool) tuple, where Terminate indicates
// if termChan received a termination event.
func (h *handle) eventPoll(channel chan Event, timeoutMs int, maxEvents int, termChan chan bool) (Event, bool) {

	var prevRkev *C.rd_kafka_event_t
	term := false

	var retval Event

	if channel == nil {
		maxEvents = 1
	}
out:
	for evcnt := 0; evcnt < maxEvents; evcnt++ {
		var evtype C.rd_kafka_event_type_t
		var fcMsg C.fetched_c_msg_t
		rkev := C._rk_queue_poll(h.rkq, C.int(timeoutMs), &evtype, &fcMsg, prevRkev)
		prevRkev = rkev
		timeoutMs = 0

		retval = nil

		switch evtype {
		case C.RD_KAFKA_EVENT_FETCH:
			// Consumer fetch event, new message.
			// Extracted into temporary fcMsg for optimization
			retval = h.newMessageFromFcMsg(&fcMsg)

		case C.RD_KAFKA_EVENT_REBALANCE:
			// Consumer rebalance event
			retval = h.c.handleRebalanceEvent(channel, rkev)

		case C.RD_KAFKA_EVENT_ERROR:
			// Error event
			cErr := C.rd_kafka_event_error(rkev)
			if cErr == C.RD_KAFKA_RESP_ERR__PARTITION_EOF {
				crktpar := C.rd_kafka_event_topic_partition(rkev)
				if crktpar == nil {
					break
				}

				defer C.rd_kafka_topic_partition_destroy(crktpar)
				var peof PartitionEOF
				setupTopicPartitionFromCrktpar((*TopicPartition)(&peof), crktpar)

				retval = peof

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
				retval = fatalErr

			} else {
				retval = newErrorFromCString(cErr, C.rd_kafka_event_error_string(rkev))
			}

		case C.RD_KAFKA_EVENT_STATS:
			retval = &Stats{C.GoString(C.rd_kafka_event_stats(rkev))}

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

				if ch == nil && h.fwdDr {
					ch = &channel
				}

				if ch != nil {
					select {
					case *ch <- msg:
					case <-termChan:
						break out
					}

				} else {
					retval = msg
					break out
				}
			}

		case C.RD_KAFKA_EVENT_OFFSET_COMMIT:
			// Offsets committed
			cErr := C.rd_kafka_event_error(rkev)
			coffsets := C.rd_kafka_event_topic_partition_list(rkev)
			var offsets []TopicPartition
			if coffsets != nil {
				offsets = newTopicPartitionsFromCparts(coffsets)
			}

			if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
				retval = OffsetsCommitted{newErrorFromCString(cErr, C.rd_kafka_event_error_string(rkev)), offsets}
			} else {
				retval = OffsetsCommitted{nil, offsets}
			}

		case C.RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH:
			ev := OAuthBearerTokenRefresh{C.GoString(C.rd_kafka_event_config_string(rkev))}
			retval = ev

		case C.RD_KAFKA_EVENT_NONE:
			// poll timed out: no events available
			break out

		default:
			if rkev != nil {
				fmt.Fprintf(os.Stderr, "Ignored event %s\n",
					C.GoString(C.rd_kafka_event_name(rkev)))
			}

		}

		if retval != nil {
			if channel != nil {
				select {
				case channel <- retval:
				case <-termChan:
					retval = nil
					term = true
					break out
				}
			} else {
				break out
			}
		}
	}

	if prevRkev != nil {
		C.rd_kafka_event_destroy(prevRkev)
	}

	return retval, term
}
