package kafka

import (
	"fmt"
	"time"
)

/*
#include <librdkafka/rdkafka.h>
*/
import "C"

// LogEvent represent the log from librdkafka internal log queue
type LogEvent struct {
	Tag       string
	Message   string
	Level     int
	Timestamp time.Time
}

// newLogEvent creates a new LogEvent from the given rd_kafka_event_t.
//
// This function does not take ownership of the cEvent pointer. You need to
// free its resources using C.rd_kafka_event_destroy afterwards.
//
// The cEvent object needs to be of type C.RD_KAFKA_EVENT_LOG. Calling this
// function with an object of another type has undefined behaviour.
func newLogEvent(cEvent *C.rd_kafka_event_t) LogEvent {
	var tag, message *C.char
	var level C.int

	C.rd_kafka_event_log(cEvent, &(tag), &(message), &(level))

	return LogEvent{
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
func (h *handle) pollLogEvents(toChannel chan LogEvent, timeoutMs int, doneChan chan bool) {
	pollChan := make(chan *C.rd_kafka_event_t, 1)

	for {
		go func() {
			pollChan <- C.rd_kafka_queue_poll(h.logq, C.int(timeoutMs))
		}()

		select {
		case <-doneChan:
			if cEvent := <-pollChan; cEvent != nil {
				C.rd_kafka_event_destroy(cEvent)
			}
			return

		case cEvent := <-pollChan:
			if cEvent == nil { // C timeout
				continue
			}

			if C.rd_kafka_event_type(cEvent) != C.RD_KAFKA_EVENT_LOG {
				continue
			}

			logEvent := newLogEvent(cEvent)
			C.rd_kafka_event_destroy(cEvent)

			select {
			case <-doneChan:
				return

			case toChannel <- logEvent:
				continue
			}
		}
	}
}

func (logEvent LogEvent) String() string {
	return fmt.Sprintf(
		"[%v][%s][%d]%s",
		logEvent.Timestamp.Format(time.RFC3339),
		logEvent.Tag,
		logEvent.Level,
		logEvent.Message)
}
