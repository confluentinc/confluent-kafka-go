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

// List consumer group offsets
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	args := os.Args

	if len(args) < 6 {
		fmt.Fprintf(os.Stderr,
			"Usage: %s <bootstrap_servers> <group_id> <require_stable> "+
				"<topic1> <partition1> [<topic2> <partition2> .... ]\n", args[0])
		os.Exit(1)
	}

	requireStable, err := strconv.ParseBool(args[3])
	if err != nil {
		fmt.Printf(
			"Failed to parse value of require_stable %s: %s\n", args[3], err)
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

	var partitions []kafka.TopicPartition
	for i := 4; i+1 < len(args); i += 2 {
		partition, err := strconv.ParseInt(args[i+1], 10, 32)
		if err != nil {
			fmt.Printf("Failed to parse partition %s: %s\n", args[i+1], err)
			os.Exit(1)
		}

		partitions = append(partitions, kafka.TopicPartition{
			Topic:     &args[i],
			Partition: int32(partition),
		})
	}

	gps := []kafka.GroupTopicPartitions{
		{
			Group:      args[2],
			Partitions: partitions,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res, err := ac.ListConsumerGroupOffsets(
		ctx, gps, kafka.SetAdminRequireStableOffsets(requireStable))
	if err != nil {
		fmt.Printf("Failed to list consumer group offsets %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("ListConsumerGroupOffset result: %+v\n", res)
}
