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
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> [<timeout_seconds> = infinite]\n", os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]

	timeout_parsed := -1
	var err error
	if len(os.Args) > 2 {
		timeout_parsed, err = strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Printf("Error parsing the timeout %s\n: %s", os.Args[2], err)
		}
	}

	// Create a new AdminClient.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	var groupInfos []kafka.GroupInfo
	if timeout_parsed == -1 {
		groupInfos, err = a.ListConsumerGroups()
	} else {
		timeout := time.Duration(timeout_parsed) * time.Second
		groupInfos, err = a.ListConsumerGroups(kafka.SetListConsumerGroupsOptionRequestTimeout(timeout))
	}

	if err != nil {
		fmt.Printf("Failed to list groups: %s\n", err)
		os.Exit(1)
	}

	// Print results
	fmt.Printf("A total of %d consumer groups listed:\n", len(groupInfos))
	for _, groupInfo := range groupInfos {
		fmt.Println(groupInfo)
	}

	a.Close()
}
