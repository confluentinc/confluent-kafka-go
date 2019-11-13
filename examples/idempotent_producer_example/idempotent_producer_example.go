// Idempotent Producer example.
//
// The idempotent producer provides strict ordering and
// exactly-once producing guarantees.
//
// From the application developer's perspective, the only difference
// from a standard producer is the enabling of the feature by setting
// the `enable.idempotence` configuration property to true, and
// handling fatal errors (Error.IsFatal()) which are raised when the
// idempotent guarantees can't be satisfied.

package main

/**
 * Copyright 2019 Confluent Inc.
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
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"time"
)

var run = true

func main() {

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	topic := os.Args[2]

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		// Enable the Idempotent Producer
		"enable.idempotence": true})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	// For signalling termination from main to go-routine
	termChan := make(chan bool, 1)
	// For signalling that termination is done from go-routine to main
	doneChan := make(chan bool)

	// Go routine for serving the events channel for delivery reports and error events.
	go func() {
		doTerm := false
		for !doTerm {
			select {
			case e := <-p.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					// Message delivery report
					m := ev
					if m.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
					} else {
						fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}

				case kafka.Error:
					// Generic client instance-level errors, such as
					// broker connection failures, authentication issues, etc.
					//
					// These errors should generally be considered informational
					// as the underlying client will automatically try to
					// recover from any errors encountered, the application
					// does not need to take action on them.
					//
					// But with idempotence enabled, truly fatal errors can
					// be raised when the idempotence guarantees can't be
					// satisfied, these errors are identified by
					// `e.IsFatal()`.

					e := ev
					if e.IsFatal() {
						// Fatal error handling.
						//
						// When a fatal error is detected by the producer
						// instance, it will emit kafka.Error event (with
						// IsFatal()) set on the Events channel.
						//
						// Note:
						//   After a fatal error has been raised, any
						//   subsequent Produce*() calls will fail with
						//   the original error code.
						fmt.Printf("FATAL ERROR: %v: terminating\n", e)
						run = false
					} else {
						fmt.Printf("Error: %v\n", e)
					}

				default:
					fmt.Printf("Ignored event: %s\n", ev)
				}

			case <-termChan:
				doTerm = true
			}
		}

		close(doneChan)
	}()

	msgcnt := 0
	for run == true {
		value := fmt.Sprintf("Go Idempotent Producer example, message #%d", msgcnt)

		// Produce message.
		// This is an asynchronous call, on success it will only
		// enqueue the message on the internal producer queue.
		// The actual delivery attempts to the broker are handled
		// by background threads.
		// Per-message delivery reports are emitted on the Events() channel,
		// see the go-routine above.
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
		}, nil)

		if err != nil {
			fmt.Printf("Failed to produce message: %v\n", err)
		}

		msgcnt++

		// Since fatal errors can't be triggered in practice,
		// use the test API to trigger a fabricated error after some time.
		if msgcnt == 13 {
			p.TestFatalError(kafka.ErrOutOfOrderSequenceNumber, "Testing fatal errors")
		}

		time.Sleep(500 * time.Millisecond)

	}

	// Clean termination to get delivery results
	// for all outstanding/in-transit/queued messages.
	fmt.Printf("Flushing outstanding messages\n")
	p.Flush(15 * 1000)

	// signal termination to go-routine
	termChan <- true
	// wait for go-routine to terminate
	<-doneChan

	fatalErr := p.GetFatalError()

	p.Close()

	// Exit application with an error (1) if there was a fatal error.
	if fatalErr != nil {
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}
