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

// consumer_offset_metadata implements a consumer that commit offset with metadata that represents the state of the partition consumer at that point in time. The
// metadata string can be used by another consumer to restore that state, so it can resume consumption.

import (
	"fmt"
	"os"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	if len(os.Args) < 6 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topic> <partition> <offset> \"<metadata>\"\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topic := os.Args[3]
	partition, err := strconv.Atoi(os.Args[4])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid partition: %s\n", err)
		os.Exit(1)
	}
	offset, err := strconv.Atoi(os.Args[5])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid offset: %s\n", err)
		os.Exit(1)
	}

	metadata := os.Args[6]

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
		"enable.auto.commit":    "false",
		"auto.offset.reset":     "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	_, err = c.CommitOffsets([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: 0,
		Metadata:  &metadata,
		Offset:    kafka.Offset(offset),
	}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to commit offset: %s\n", err)
		os.Exit(1)
	}

	committedOffsets, err := c.Committed([]kafka.TopicPartition{{Topic: &topic, Partition: int32(partition)}}, 100)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to fetch offset: %s\n", err)
		os.Exit(1)
	}

	committedOffset := committedOffsets[0]

	fmt.Printf("Committed partition %d offset: %d", committedOffset.Partition, committedOffset.Offset)

	if committedOffset.Metadata != nil {
		fmt.Printf(" metadata: %s", *committedOffset.Metadata)
	} else {
		fmt.Println("\n Looks like we fetch empty metadata. Ensure that librdkafka version > v1.1.0")
	}

	c.Close()
}
