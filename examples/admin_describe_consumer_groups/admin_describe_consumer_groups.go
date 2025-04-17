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
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(
			os.Stderr,
			"Usage: %s <bootstrap-servers> <includeAuthorizedOperations>"+
				" <group1> [<group2> ...]\n",
			os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	includeAuthorizedOperations, errOperations := strconv.ParseBool(os.Args[2])
	if errOperations != nil {
		fmt.Printf(
			"Failed to parse value of includeAuthorizedOperations %s: %s\n", os.Args[2], errOperations)
		os.Exit(1)
	}

	groups := os.Args[3:]

	// Create a new AdminClient.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer a.Close()

	// Call DescribeConsumerGroups.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	describeGroupsResult, err := a.DescribeConsumerGroups(ctx, groups,
		kafka.SetAdminOptionIncludeAuthorizedOperations(includeAuthorizedOperations))
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
			"Type: %s\n"+
			"Coordinator: %+v\n"+
			"Members: %+v\n",
			g.GroupID, g.Error, g.IsSimpleConsumerGroup, g.PartitionAssignor,
			g.State, g.Type, g.Coordinator, g.Members)
		if includeAuthorizedOperations {
			fmt.Printf("Allowed operations: %s\n", g.AuthorizedOperations)
		}
		fmt.Printf("\n")
	}
}
