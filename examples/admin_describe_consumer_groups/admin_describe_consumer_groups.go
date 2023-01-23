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

// Describe consumer groups
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(
			os.Stderr,
			"Usage: %s <bootstrap-servers> <group1> [<group2> ...]\n",
			os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	groups := os.Args[2:]

	// Create a new AdminClient.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer a.Close()

	// Call DescribeConsumerGroups.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	describeGroupsResult, err := a.DescribeConsumerGroups(ctx, groups)
	if err != nil {
		fmt.Printf("Failed to describe groups: %s\n", err)
		os.Exit(1)
	}

	// Print results
	fmt.Printf("A total of %d consumer group(s) described:\n\n",
		len(describeGroupsResult.ConsumerGroupDescriptions))
	for _, g := range describeGroupsResult.ConsumerGroupDescriptions {
		fmt.Printf("GroupId: %s\n"+
			"Error: %s\n"+
			"IsSimpleConsumerGroup: %v\n"+
			"PartitionAssignor: %s\n"+
			"State: %s\n"+
			"Coordinator: %+v\n"+
			"Members: %+v\n\n",
			g.GroupID, g.Error, g.IsSimpleConsumerGroup, g.PartitionAssignor,
			g.State, g.Coordinator, g.Members)
	}
}
