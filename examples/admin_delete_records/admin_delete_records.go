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

// Delete Records before a particular offset in specified Topic Partition.

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	args := os.Args

    if len(args) < 5 {
		fmt.Fprintf(os.Stderr,
			"Usage: %s <bootstrap_servers> "+
				"<topic1> <partition1> <offset1> ...\n",
			args[0])
		os.Exit(1)
	}

	// Create new AdminClient.
	ac, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": args[1],
	})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer ac.Close()

	var topicPartitionOffsets []kafka.TopicPartition

	argsCnt := len(os.Args)
	i := 2
	index := 0

	for i < argsCnt {
		if i+3 > argsCnt {
			fmt.Printf("Expected %d arguments for partition %d, got %d\n", 3, index, argsCnt-i)
			os.Exit(1)
		}
		topicName := os.Args[i]
		partition, err := strconv.ParseInt(args[i+1], 10, 32)
		if err != nil {
			panic(err)
		}
		offset, err := strconv.ParseUint(args[i+2], 10, 64)
		if err != nil {
			panic(err)
		}

		topicPartitionOffsets = append(topicPartitionOffsets, kafka.TopicPartition{
			Topic:     &topicName,
			Partition: int32(partition),
			Offset:    kafka.Offset(offset),
		})
		i += 3
		index += 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	res, err := ac.DeleteRecords(ctx, topicPartitionOffsets)
	if err != nil {
		fmt.Printf("Failed to delete records: %s\n", err)
		os.Exit(1)
	}
    
	fmt.Printf("Delete Records result: %+v\n", res)
}