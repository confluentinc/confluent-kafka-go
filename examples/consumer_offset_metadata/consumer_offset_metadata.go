// Example Apache Kafka consumer that commit offset with metadata
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

// consumer_offset_metadata implements a consumer that commit offset with metadata that represents the state
// of the partition consumer at that point in time. The metadata string can be used by another consumer
// to restore that state, so it can resume consumption.

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"strconv"
)

func main() {

	if len(os.Args) != 7 && len(os.Args) != 5 {
		fmt.Fprintf(os.Stderr, `Usage:
- commit offset with metadata: %s <broker> <group> <topic> <partition> <offset> "<metadata>"
- show partition offset: %s <broker> <group> <topic> <partition>`,
			os.Args[0], os.Args[0])
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

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          group,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	if len(os.Args) == 7 {
		offset, err := strconv.Atoi(os.Args[5])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid offset: %s\n", err)
			os.Exit(1)
		}

		metadata := os.Args[6]

		commitOffset(c, topic, partition, offset, metadata)
	} else {
		showPartitionOffset(c, topic, partition)
	}

	c.Close()
}

func commitOffset(c *kafka.Consumer, topic string, partition int, offset int, metadata string) {
	res, err := c.CommitOffsets([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: int32(partition),
		Metadata:  &metadata,
		Offset:    kafka.Offset(offset),
	}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to commit offset: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Partition %d offset committed successfully", res[0].Partition)
}

func showPartitionOffset(c *kafka.Consumer, topic string, partition int) {
	committedOffsets, err := c.Committed([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: int32(partition),
	}}, 5000)
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
}
