package kafka

import (
	"context"
	"fmt"
	"time"
)

/*
#include "select_rdkafka.h"
*/
import "C"

// LogEvent represent the log from librdkafka internal log queue
type LogEvent struct {
	Name      string    // Name of client instance
	Tag       string    // Log tag that provides context to the log Message (e.g., "METADATA" or "GRPCOORD")
	Message   string    // Log message
	Level     int       // Log syslog level, lower is more critical.
	Timestamp time.Time // Log timestamp
}

// newLogEvent creates a new LogEvent from the given rd_kafka_event_t.
//
// This function does not take ownership of the cEvent pointer. You need to
// free its resources using C.rd_kafka_event_destroy afterwards.
//
// The cEvent object needs to be of type C.RD_KAFKA_EVENT_LOG. Calling this
// function with an object of another type has undefined behaviour.
func (h *handle) newLogEvent(cEvent *C.rd_kafka_event_t) LogEvent {
	var tag, message *C.char
	var level C.int

	C.rd_kafka_event_log(cEvent, &(tag), &(message), &(level))

	return LogEvent{
		Name:      h.name,
		Tag:       C.GoString(tag),
		Message:   C.GoString(message),
		Level:     int(level),
		Timestamp: time.Now(),
	}
}

// pollLogEvents polls log events from librdkafka and pushes them to toChannel,
// until doneChan is closed.
//
// Each call to librdkafka times out after timeoutMs. If a call to librdkafka
// is ongoing when doneChan is closed, the function will wait until the call
// returns or times out, whatever happens first.
func (h *handle) pollLogEvents(toChannel chan LogEvent, termChan chan bool) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-termChan
		cancel()
	}()

	for {
		polledEv, err := h.pollSingleCEventWithYield(ctx, h.logq)
		if err != nil {
			// We're shutting down and context is canceled.
			return
		}

		if polledEv.evType != C.RD_KAFKA_EVENT_LOG {
			C.rd_kafka_event_destroy(polledEv.rkev)
			continue
		}

		logEvent := h.newLogEvent(polledEv.rkev)
		C.rd_kafka_event_destroy(polledEv.rkev)

		select {
		case <-termChan:
			return

		case toChannel <- logEvent:
			continue
		}
	}
}

func (logEvent LogEvent) String() string {
	return fmt.Sprintf(
		"[%v][%s][%s][%d]%s",
		logEvent.Timestamp.Format(time.RFC3339),
		logEvent.Name,
		logEvent.Tag,
		logEvent.Level,
		logEvent.Message)
}
