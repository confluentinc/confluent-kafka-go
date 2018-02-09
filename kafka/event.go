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
	"encoding/json"
	"fmt"
	"os"
	"unsafe"
)

/*
#include <stdlib.h>
#include <librdkafka/rdkafka.h>
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

type windowStats struct {
	Min int `json:"min"`
	Max int `json:"max"`
	Avg int `json:"avg"`
	Sum int `json:"sum"`
	Cnt int `json:"cnt"`
}

// Parsed Stats statistics event
type ParsedStats struct {
	Name             string `json:"name"`
	Type             string `json:"type"`
	Ts               uint64 `json:"ts"`
	Time             int    `json:"time"`
	ReplQ            int    `json:"replyq"`
	MsgCnt           int    `json:"msg_cnt"`
	MsgSize          int64  `json:"msg_size"`
	MsgMax           int    `json:"msg_max"`
	MsgSizeMax       int64  `json:"msg_size_max"`
	SimpleCnt        int    `json:"simple_cnt"`
	MetadataCacheCnt int    `json:"metadata_cache_cnt"`
	Brokers          map[string]struct {
		Name           string      `json:"name"`
		NodeId         int         `json:"nodeid"`
		State          string      `json:"state"`
		Stateage       int64       `json:"stateage"`
		OutbufCnt      int         `json:"outbuf_cnt"`
		OutbufMsgCnt   int         `json:"outbuf_msg_cnt"`
		WaitrespCnt    int         `json:"waitresp_cnt"`
		WaitrespMsgCnt int         `json:"waitresp_msg_cnt"`
		Tx             int64       `json:"tx"`
		TxBytes        int64       `json:"txbytes"`
		TxErrs         int64       `json:"txerrs"`
		TxRetries      int64       `json:"txretries"`
		ReqTimeouts    int64       `json:"req_timeouts"`
		Rx             int64       `json:"rx"`
		RxBytes        int64       `json:"rxbytes"`
		RxErrs         int64       `json:"rxerrs"`
		RxCorriderrs   int64       `json:"rxcorriderrs"`
		RxPartial      int64       `json:"rxpartial"`
		ZbufGrow       int64       `json:"zbuf_grow"`
		BufGrow        int64       `json:"buf_grow"`
		Wakeups        int         `json:"wakeups"`
		IntLatency     windowStats `json:"int_latency"`
		Rtt            windowStats `json:"rtt"`
		Throttle       windowStats `json:"throttle"`
		Toppars        map[string]struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
		} `json:"toppars"`
	} `json:"brokers"`
	Topics map[string]struct {
		Topic       string `json:"topic"`
		MetadataAge int64  `json:"metadata_age"`
		Partitions  map[string]struct {
			Partition       int    `json:"partition"`
			Leader          int    `json:"leader"`
			Desired         bool   `json:"desired"`
			Unknown         bool   `json:"unknown"`
			MsgQCnt         int    `json:"msgq_cnt"`
			MsgQBytes       int64  `json:"msgq_bytes"`
			XmitMsgQCnt     int    `json:"xmit_msgq_cnt"`
			XmitMsgQBytes   int64  `json:"xmit_msgq_bytes"`
			FetchQCnt       int    `json:"fetchq_cnt"`
			FetchQSize      int64  `json:"fetchq_size"`
			FetchState      string `json:"fetch_state"`
			QueryOffset     int64  `json:"query_offset"`
			NextOffset      int64  `json:"next_offset"`
			AppOffset       int64  `json:"app_offset"`
			StoredOffset    int64  `json:"stored_offset"`
			CommittedOffset int64  `json:"committed_offset"`
			EofOffset       int64  `json:"eof_offset"`
			LoOffset        int64  `json:"lo_offset"`
			HiOffset        int64  `json:"hi_offset"`
			ConsumerLag     int64  `json:"consumer_lag"`
			TxMsgs          int64  `json:"txmsgs"`
			TxBytes         int64  `json:"txbytes"`
			Msgs            int64  `json:"msgs"`
			RxVerDrops      int64  `json:"rx_ver_drops"`
		} `json:"partitions"`
	} `json:"topics"`
	Cgrp struct {
		RebalanceAge   int64 `json:"rebalance_age"`
		RebalanceCnt   int   `json:"rebalance_cnt"`
		AssignmentSize int   `json:"assignment_size"`
	} `json:"cgrp"`
}

// Stats statistics event
type Stats struct {
	statsJSON string
}

func (e Stats) String() string {
	return e.statsJSON
}

// Use Parse to retrive stats in a parsed go struct
func (s *Stats) Parse() (ParsedStats, error) {
	var stats ParsedStats
	err := json.Unmarshal([]byte(s.statsJSON), &stats)
	return stats, err
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
			appReassigned := false
			if C.rd_kafka_event_error(rkev) == C.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
				if h.currAppRebalanceEnable {
					// Application must perform Assign() call
					var ev AssignedPartitions
					ev.Partitions = newTopicPartitionsFromCparts(C.rd_kafka_event_topic_partition_list(rkev))
					if channel != nil || h.c.rebalanceCb == nil {
						retval = ev
						appReassigned = true
					} else {
						appReassigned = h.c.rebalance(ev)
					}
				}

				if !appReassigned {
					C.rd_kafka_assign(h.rk, C.rd_kafka_event_topic_partition_list(rkev))
				}
			} else {
				if h.currAppRebalanceEnable {
					// Application must perform Unassign() call
					var ev RevokedPartitions
					ev.Partitions = newTopicPartitionsFromCparts(C.rd_kafka_event_topic_partition_list(rkev))
					if channel != nil || h.c.rebalanceCb == nil {
						retval = ev
						appReassigned = true
					} else {
						appReassigned = h.c.rebalance(ev)
					}
				}

				if !appReassigned {
					C.rd_kafka_assign(h.rk, nil)
				}
			}

		case C.RD_KAFKA_EVENT_ERROR:
			// Error event
			cErr := C.rd_kafka_event_error(rkev)
			switch cErr {
			case C.RD_KAFKA_RESP_ERR__PARTITION_EOF:
				crktpar := C.rd_kafka_event_topic_partition(rkev)
				if crktpar == nil {
					break
				}

				defer C.rd_kafka_topic_partition_destroy(crktpar)
				var peof PartitionEOF
				setupTopicPartitionFromCrktpar((*TopicPartition)(&peof), crktpar)

				retval = peof
			default:
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
