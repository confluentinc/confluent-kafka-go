/**
 * Copyright 2024 Confluent Inc.
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

// Perform Preffered or Unclean Elections for the specified Topic Partitions.

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
				"<election_type (Preferred/Unclean)> <topic1> <partition1> ...\n",
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

	electionType, err := kafka.ElectionTypeFromString(args[2])
	if err != nil {
		fmt.Printf("Invalid election type: %s\n", err)
		os.Exit(1)
	}

	var topicPartitionList []kafka.TopicPartition

	argsCnt := len(os.Args)
	i := 3
	index := 0

	for i < argsCnt {
		if i+2 > argsCnt {
			fmt.Printf("Expected %d arguments for partition %d, got %d\n", 2, index, argsCnt-i)
			os.Exit(1)
		}
		topicName := os.Args[i]
		partition, err := strconv.ParseInt(args[i+1], 10, 32)
		if err != nil {
			panic(err)
		}

		topicPartitionList = append(topicPartitionList, kafka.TopicPartition{
			Topic:     &topicName,
			Partition: int32(partition),
		})
		i += 2
		index++
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	res, err := ac.ElectLeaders(ctx, kafka.NewElectLeaderRequest(electionType, topicPartitionList))
	if err != nil {
		kafkaErr, ok := err.(kafka.Error) // Type assertion
		if ok && kafkaErr.Code() != kafka.ErrNoError {
			fmt.Printf("Failed to elect Leaders: %s\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("ElectLeaders result: %+v\n", res)
}