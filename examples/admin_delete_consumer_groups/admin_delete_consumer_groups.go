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

// Delete consumer group
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

	if len(args) < 4 {
		fmt.Fprintf(os.Stderr,
			"Usage: %s <bootstrap_servers> <request_timeout_sec> <group1> [<group2> ...]\n", args[0])
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

	timeoutSec, err := strconv.Atoi(args[2])
	if err != nil {
		fmt.Printf("Failed to parse timeout: %s\n", err)
		os.Exit(1)
	}

	groups := args[3:]

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res, err := ac.DeleteConsumerGroups(ctx, groups,
		kafka.SetAdminRequestTimeout(time.Duration(timeoutSec)*time.Second))
	if err != nil {
		fmt.Printf("Failed to delete groups: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("DeleteConsumerGroups result: %+v\n", res)
}
