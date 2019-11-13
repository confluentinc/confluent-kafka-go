// Example of a consumer handling statistics events.
// The Stats handling code is the same for Consumers and Producers.
//
// The definition of the emitted statistics JSON object can be found here:
// https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
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
	"encoding/json"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3:]
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           group,
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
		// Statistics output from the client may be enabled
		// by setting statistics.interval.ms and
		// handling kafka.Stats events (see below).
		"statistics.interval.ms": 5000})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(topics, nil)

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
			case kafka.Error:
				// The client will automatically try to
				// recover from all types of errors.
				// There is typically no need for an
				// application to handle errors other
				// than to log them.
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			case *kafka.Stats:
				// Stats events are emitted as JSON (as string).
				// Either directly forward the JSON to your
				// statistics collector, or convert it to a
				// map to extract fields of interest.
				// The definition of the statistics JSON
				// object can be found here:
				// https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
				var stats map[string]interface{}
				json.Unmarshal([]byte(e.String()), &stats)
				fmt.Printf("Stats: %v messages (%v bytes) messages consumed\n",
					stats["rxmsgs"], stats["rxmsg_bytes"])
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
