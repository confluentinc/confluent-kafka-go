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
	"os"
	"unsafe"
)

/*
#include <librdkafka/rdkafka.h>

rd_kafka_event_t *_rk_queue_poll (rd_kafka_queue_t *rkq, int timeout_ms,
                                  rd_kafka_event_type_t *evtype) {
    rd_kafka_event_t *rkev;

    rkev = rd_kafka_queue_poll(rkq, timeout_ms);
    *evtype = rd_kafka_event_type(rkev);
    return rkev;
}
*/
import "C"

// Event generic interface
type Event interface {
	String() string
}

// Specific event types
type AssignedPartitions struct {
	Partitions []TopicPartition
}

func (e AssignedPartitions) String() string {
	return fmt.Sprintf("AssignedPartitions: %v", e.Partitions)
}

type RevokedPartitions struct {
	Partitions []TopicPartition
}

func (e RevokedPartitions) String() string {
	return fmt.Sprintf("RevokedPartitions: %v", e.Partitions)
}

type PartitionEof TopicPartition

func (p PartitionEof) String() string {
	return fmt.Sprintf("EOF at %s", TopicPartition(p))
}

// event_poll polls an event from the handler's C rd_kafka_queue_t,
// translates it into an Event type and then sends on `channel` if non-nil, else returns the Event.
func (h *handle) event_poll(channel chan Event, timeout_ms int) Event {

	var evtype C.rd_kafka_event_type_t
	rkev := C._rk_queue_poll(h.rkq, C.int(timeout_ms), &evtype)
	defer C.rd_kafka_event_destroy(rkev)

	switch evtype {
	case C.RD_KAFKA_EVENT_REBALANCE:
		// Consumer rebalance event
		// If the app provided a RebalanceCb to Subscribe*() or
		// has go.application.rebalance.enable=true we create an event
		// and forward it to the application thru the RebalanceCb or the
		// Events channel respectively.
		// Since librdkafka requires the rebalance event to be "acked" by
		// the application to synchronize state we keep track of if the
		// application performed Assign() or Unassign(), but this only works for
		// the non-channel case. For the channel case we assume the application
		// calls Assign() / Unassign().
		// Failure to do so will "hang" the consumer, e.g., it wont start consuming
		// and it wont close cleanly, so this error case should be visible
		// immediately to the application developer.
		app_reassigned := false
		if C.rd_kafka_event_error(rkev) == C.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
			if h.curr_app_rebalance_enable {
				// Application must perform Assign() call
				var ev AssignedPartitions
				ev.Partitions = new_TopicPartitions_from_c_parts(C.rd_kafka_event_topic_partition_list(rkev))
				if channel != nil {
					channel <- ev
					app_reassigned = true
				} else {
					app_reassigned = h.c.rebalance(ev)
				}
			}

			if !app_reassigned {
				C.rd_kafka_assign(h.rk, C.rd_kafka_event_topic_partition_list(rkev))
			}
		} else {
			if h.curr_app_rebalance_enable {
				// Application must perform Unassign() call
				var ev RevokedPartitions
				ev.Partitions = new_TopicPartitions_from_c_parts(C.rd_kafka_event_topic_partition_list(rkev))
				if channel != nil {
					channel <- ev
					app_reassigned = true
				} else {
					app_reassigned = h.c.rebalance(ev)
				}
			}

			if !app_reassigned {
				C.rd_kafka_assign(h.rk, nil)
			}
		}

	case C.RD_KAFKA_EVENT_FETCH:
		// Consumer fetch event, new message.
		m := h.new_message_from_c(C.rd_kafka_event_message_next(rkev))
		if channel != nil {
			channel <- m
		} else {
			return m
		}

	case C.RD_KAFKA_EVENT_ERROR:
		// Error event
		c_err := C.rd_kafka_event_error(rkev)
		switch c_err {
		case C.RD_KAFKA_RESP_ERR__PARTITION_EOF:
			c_rktpar := C.rd_kafka_event_topic_partition(rkev)
			if c_rktpar == nil {
				break
			}

			defer C.rd_kafka_topic_partition_destroy(c_rktpar)
			var peof PartitionEof
			setup_TopicPartition_from_c_rktpar((*TopicPartition)(&peof), c_rktpar)

			if channel != nil {
				channel <- peof
			} else {
				return peof
			}
		}

	case C.RD_KAFKA_EVENT_DR:
		// Producer Delivery Report event
		// Each such event contains delivery reports for all
		// messages in the produced batch.
		// Forward delivery reports to per-message's response channel
		// or to the global Producer.Events channel, or none.
		rkmessages := make([]*C.rd_kafka_message_t, int(C.rd_kafka_event_message_count(rkev)))

		cnt := int(C.rd_kafka_event_message_array(rkev, (**C.rd_kafka_message_t)(unsafe.Pointer(&rkmessages[0])), C.size_t(len(rkmessages))))

		for _, rkmessage := range rkmessages[:cnt] {
			msg := h.new_message_from_c(rkmessage)
			var ch *chan Event = nil

			if rkmessage._private != nil {
				// Find cgoif by id
				cgoid := *(*int)(rkmessage._private)

				cg, found := h.cgo_get(cgoid)
				if found {
					cdr := cg.(cgo_dr)

					if cdr.delivery_chan != nil {
						ch = &cdr.delivery_chan
					}
					msg.Opaque = cdr.opaque
				}
			}

			if ch == nil && h.fwd_dr {
				ch = &channel
			}

			if ch != nil {
				*ch <- msg
			} else {
				return msg
			}
		}

	default:
		if rkev != nil {
			fmt.Fprintf(os.Stderr, "Ignored event %s\n",
				C.GoString(C.rd_kafka_event_name(rkev)))
		}

	}

	return nil
}
