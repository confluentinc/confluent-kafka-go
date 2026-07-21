// Example function-based high-level Apache Kafka consumer
package main

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

// consumer_at_least_once_example implements a consumer using the non-channel Poll() API
// to retrieve messages and events disabling the auto commit configuration.
// The default Kafka’s consumer auto commit configuration can lead to potential data loss.
// It has no idea what you do with the message, and it’s much more free about committing offsets.
// As far as the consumer is concerned, as soon as a message is pulled in, it’s “processed.”
// There aren’t any easy fixes here. Fundamentally, this is a problem of weak consistency guarantees.
// This example ilustrates how to “roll your own” at-least-once strategy that would manually
// commit offsets after the end of the processing pipeline.

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3:]

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest",

		// The best way to achieve at least once semantics is to
		// disable `enable.auto.offset.store`, which marks a message as eligible
		// for commit as soon as it's delivered to the application
		// and use `StoreOffsets` to manually indicate this instead.
		// Leaving `enable.auto.commit` as true to avoid blocking the poll loop.
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe topics: %s\n", err)
		os.Exit(1)
	}

	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}

				// We can do any long running async operation here, because the next
				// offset will not be available for commit until we call `c.StoreOffsets(...)`
				_ = e
				// In this example we store the next offset that will be committed
				// to the offset store according to `auto.commit.interval.ms` or manual
				// offset-less Commit().
				if _, err = c.StoreMessage(e); err != nil {
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", err)
					run = false
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
