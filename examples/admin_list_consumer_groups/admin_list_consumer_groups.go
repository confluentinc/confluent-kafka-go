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
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func usage(message string, fs *flag.FlagSet) {
	if message != "" {
		fmt.Fprintf(os.Stderr,
			message)
	}
	fs.Usage()
	os.Exit(1)
}

func parseListConsumerGroupsArgs() (
	bootstrapServers string,
	states []kafka.ConsumerGroupState,
	types []kafka.ConsumerGroupType,
) {
	var statesString, typesString string
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&bootstrapServers, "b", "localhost:9092", "Bootstrap servers")
	fs.StringVar(&statesString, "states", "", "States to match")
	fs.StringVar(&typesString, "types", "", "Types to match")
	fs.Parse(os.Args[1:])

	if statesString != "" {
		for _, stateString := range strings.Split(statesString, ",") {
			state, _ := kafka.ConsumerGroupStateFromString(stateString)
			if state == kafka.ConsumerGroupStateUnknown {
				usage(fmt.Sprintf("Given state %s is not a valid state\n",
					stateString), fs)
			}
			states = append(states, state)
		}
	}
	if typesString != "" {
		for _, typeString := range strings.Split(typesString, ",") {
			groupType := kafka.ConsumerGroupTypeFromString(typeString)
			if groupType == kafka.ConsumerGroupTypeUnknown {
				usage(fmt.Sprintf("Given type %s is not a valid type\n",
					typeString), fs)
			}
			types = append(types, groupType)
		}
	}
	return
}

func main() {

	bootstrapServers, states, types := parseListConsumerGroupsArgs()

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
