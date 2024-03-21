/**
 * Copyright 2023 Confluent Inc.
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

// List Offsets example
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr,
			"Usage: %s <bootstrap-servers> <topicname> <partition> <EARLIEST/LATEST/MAX_TIMESTAMP/TIMESTAMP t1> ..\n", os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]

	argsCnt := len(os.Args)
	i := 2
	index := 0
	topicPartitionOffsets := make(map[kafka.TopicPartition]kafka.OffsetSpec)
	for i < argsCnt {
		if i+3 > argsCnt {
			fmt.Printf("Expected %d arguments for partition %d, got %d\n", 3, index, argsCnt-i)
			os.Exit(1)
		}

		topicName := os.Args[i]
		partition, err := strconv.Atoi(os.Args[i+1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid partition: %s\n", err)
			os.Exit(1)
		}

		tp := kafka.TopicPartition{Topic: &topicName, Partition: int32(partition)}

		if os.Args[i+2] == "EARLIEST" {
			topicPartitionOffsets[tp] = kafka.EarliestOffsetSpec
		} else if os.Args[i+2] == "LATEST" {
			topicPartitionOffsets[tp] = kafka.LatestOffsetSpec
		} else if os.Args[i+2] == "MAX_TIMESTAMP" {
			topicPartitionOffsets[tp] = kafka.MaxTimestampOffsetSpec
		} else if os.Args[i+2] == "TIMESTAMP" {
			if i+4 > argsCnt {
				fmt.Printf("Expected %d arguments for partition %d, got %d\n", 4, index, argsCnt-i)
				os.Exit(1)
			}

			timestamp, timestampErr := strconv.Atoi(os.Args[i+3])
			if timestampErr != nil {
				fmt.Fprintf(os.Stderr, "Invalid timestamp: %s\n", timestampErr)
				os.Exit(1)
			}
			topicPartitionOffsets[tp] = kafka.NewOffsetSpecForTimestamp(int64(timestamp))
			i = i + 1
		} else {
			fmt.Fprintf(os.Stderr, "Invalid OffsetSpec.\n")
			os.Exit(1)
		}
		i = i + 3
		index++
	}

	// Create a new AdminClient.
	// AdminClient can also be instantiated using an existing
	// Producer or Consumer instance, see NewAdminClientFromProducer and
	// NewAdminClientFromConsumer.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer a.Close()

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results, err := a.ListOffsets(ctx, topicPartitionOffsets,
		kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadCommitted))
	if err != nil {
		fmt.Printf("Failed to List offsets: %v\n", err)
		os.Exit(1)
	}
	// map[TopicPartition]ListOffsetsResultInfo
	// Print results
	for tp, info := range results.ResultInfos {
		fmt.Printf("Topic: %s Partition: %d\n", *tp.Topic, tp.Partition)
		if info.Error.Code() != kafka.ErrNoError {
			fmt.Printf("	ErrorCode: %d ErrorMessage: %s\n\n", info.Error.Code(), info.Error.String())
		} else {
			fmt.Printf("	Offset: %d Timestamp: %d\n\n", info.Offset, info.Timestamp)
		}
	}
}
