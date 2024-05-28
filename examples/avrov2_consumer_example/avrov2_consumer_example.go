/**
 * Copyright 2022 Confluent Inc.
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

// Example function-based high-level Apache Kafka consumer
package main

// consumer_example implements a consumer using the non-channel Poll() API
// to retrieve messages and events.

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
)

func main() {

	if len(os.Args) < 5 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <schema-registry> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	url := os.Args[2]
	group := os.Args[3]
	topics := os.Args[4:]
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           group,
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))

	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		os.Exit(1)
	}

	deser, err := avrov2.NewDeserializer(client, serde.ValueSerde, avrov2.NewDeserializerConfig())

	if err != nil {
		fmt.Printf("Failed to create deserializer: %s\n", err)
		os.Exit(1)
	}

	err = c.SubscribeTopics(topics, nil)

	run := true

	for run {
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
				value := User{}
				err := deser.DeserializeInto(*e.TopicPartition.Topic, e.Value, &value)
				if err != nil {
					fmt.Printf("Failed to deserialize payload: %s\n", err)
				} else {
					fmt.Printf("%% Message on %s:\n%+v\n", e.TopicPartition, value)
				}
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
