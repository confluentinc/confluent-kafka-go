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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func usage(message string) {
	if message != "" {
		fmt.Fprintf(os.Stderr,
			message)
	}
	fmt.Fprintf(os.Stderr,
		"Usage: %s <bootstrap-servers> [-states <state1> <state2> ...] [-types <type1> <type2> ...] \n", os.Args[0])
	os.Exit(1)
}

func parseListConsumerGroupsArgs() (states []kafka.ConsumerGroupState, types []kafka.ConsumerGroupType) {
	if len(os.Args) > 2 {
		args := os.Args[2:]
		stateArray := false
		typeArray := false
		lastArray := 0
		for _, arg := range args {
			if arg == "-states" {
				if stateArray {
					usage("Cannot pass the states flag (-states) more than once.\n")
				}
				lastArray = 1
				stateArray = true
			} else if arg == "-types" {
				if typeArray {
					usage("Cannot pass the types flag (-types) more than once.\n")
				}
				lastArray = 2
				typeArray = true
			} else {
				if lastArray == 1 {
					state, _ := kafka.ConsumerGroupStateFromString(arg)
					if state == kafka.ConsumerGroupStateUnknown {
						usage(fmt.Sprintf("Given state %s is not a valid state\n", arg))
					}
					states = append(states, state)
				} else if lastArray == 2 {
					groupType := kafka.ConsumerGroupTypeFromString(arg)
					if groupType == kafka.ConsumerGroupTypeUnknown {
						usage(fmt.Sprintf("Given type %s is not a valid type\n", arg))
					}
					types = append(types, groupType)
				} else {
					usage(fmt.Sprintf("Unknown argument: %s\n", arg))
				}
			}
		}
	}
	return
}

func main() {

	if len(os.Args) < 2 {
		usage("")
	}
	bootstrapServers := os.Args[1]
	states, types := parseListConsumerGroupsArgs()

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
	var options []kafka.ListConsumerGroupsAdminOption
	if len(states) > 0 {
		options = append(options, kafka.SetAdminMatchConsumerGroupStates(states))
	}
	if len(types) > 0 {
		options = append(options, kafka.SetAdminMatchConsumerGroupTypes(types))
	}

	listGroupRes, err := a.ListConsumerGroups(
		ctx,
		options...)

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
		fmt.Printf("Type: %s\n", group.Type)
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
