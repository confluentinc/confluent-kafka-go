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

// Describe topics
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
	if len(os.Args) < 4 {
		fmt.Fprintf(
			os.Stderr,
			"Usage: %s <bootstrap-servers> <include_authorized_operations>"+
				" <topic1> [<topic2> ...]\n",
			os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	include_authorized_operations, err_operations := strconv.ParseBool(os.Args[2])
	if err_operations != nil {
		fmt.Printf(
			"Failed to parse value of include_authorized_operations %s: %s\n",
			os.Args[2], err_operations)
		os.Exit(1)
	}
	topics := os.Args[3:]

	// Create a new AdminClient.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer a.Close()

	// Call DescribeTopics.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	describeTopicsResult, err := a.DescribeTopics(
		ctx, kafka.NewTopicCollectionOfTopicNames(topics),
		kafka.SetAdminOptionIncludeAuthorizedOperations(
			include_authorized_operations))
	if err != nil {
		fmt.Printf("Failed to describe topics: %s\n", err)
		os.Exit(1)
	}

	// Print results
	fmt.Printf("A total of %d topic(s) described:\n\n",
		len(describeTopicsResult.TopicDescriptions))
	for _, t := range describeTopicsResult.TopicDescriptions {
		if t.Error.Code() != 0 {
			fmt.Printf("Topic: %s has error: %s\n",
				t.Name, t.Error)
			continue
		}
		fmt.Printf("Topic: %s has succeeded\n", t.Name)
		fmt.Printf("Topic Id: %s\n", t.TopicId)
		if include_authorized_operations {
			fmt.Printf("Allowed operations: %s\n", t.AuthorizedOperations)
		}
		for i := 0; i < len(t.Partitions); i++ {
			fmt.Printf("\tPartition id: %d with leader: %s\n",
				t.Partitions[i].Partition, t.Partitions[i].Leader)
			fmt.Printf("\t\tThe in-sync replica count is: %d, they are: \n\t\t%s\n",
				len(t.Partitions[i].Isr), t.Partitions[i].Isr)
			fmt.Printf("\t\tThe replica count is: %d, they are: \n\t\t%s\n",
				len(t.Partitions[i].Replicas), t.Partitions[i].Replicas)
		}
		fmt.Printf("\n")
	}
}
