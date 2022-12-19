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

// List consumer groups
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr,
			"Usage: %s <bootstrap-servers> [<state1> <state2> ...]\n", os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	var states []kafka.ConsumerGroupState
	if len(os.Args) > 2 {
		statesStr := os.Args[2:]
		for _, stateStr := range statesStr {
			state, err := kafka.ConsumerGroupStateFromString(stateStr)
			if err != nil {
				fmt.Fprintf(os.Stderr,
					"Given state %s is not a valid state\n", stateStr)
				os.Exit(1)
			}
			states = append(states, state)
		}
	}

	// Create a new AdminClient.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer a.Close()

	// Call ListConsumerGroups.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	listGroupRes, err := a.ListConsumerGroups(
		ctx, kafka.SetAdminMatchConsumerGroupStates(states))

	if err != nil {
		fmt.Printf("Failed to list groups with client-level error %s\n", err)
		os.Exit(1)
	}

	// Print results
	groups := listGroupRes.Valid
	fmt.Printf("A total of %d consumer group(s) listed:\n", len(groups))
	for _, group := range groups {
		fmt.Printf("GroupId: %s\n", group.GroupID)
		fmt.Printf("State: %s\n", group.State)
		fmt.Printf("IsSimpleConsumerGroup: %v\n", group.IsSimpleConsumerGroup)
		fmt.Println()
	}

	errs := listGroupRes.Errors
	if len(errs) == 0 {
		return
	}

	fmt.Printf("A total of %d error(s) while listing:\n", len(errs))
	for _, err := range errs {
		fmt.Println(err)
	}
}
