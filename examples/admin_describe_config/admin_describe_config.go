// List current configuration for a cluster resource
package main

/**
 * Copyright 2018 Confluent Inc.
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

import (
	"context"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"time"
)

func main() {

	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr,
			"Usage: %s <broker> <resource-type> <resource-name>\n"+
				"\n"+
				" <broker> - CSV list of bootstrap brokers\n"+
				" <resource-type> - any, broker, topic, group\n"+
				" <resource-name> - broker id or topic name\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	resourceType, err := kafka.ResourceTypeFromString(os.Args[2])
	if err != nil {
		fmt.Printf("Invalid resource type: %s\n", os.Args[2])
		os.Exit(1)
	}
	resourceName := os.Args[3]

	// Create a new AdminClient.
	// AdminClient can also be instantiated using an existing
	// Producer or Consumer instance, see NewAdminClientFromProducer and
	// NewAdminClientFromConsumer.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dur, _ := time.ParseDuration("20s")
	// Ask cluster for the resource's current configuration
	results, err := a.DescribeConfigs(ctx,
		[]kafka.ConfigResource{{Type: resourceType, Name: resourceName}},
		kafka.SetAdminRequestTimeout(dur))
	if err != nil {
		fmt.Printf("Failed to DescribeConfigs(%s, %s): %s\n",
			resourceType, resourceName, err)
		os.Exit(1)
	}

	// Print results
	for _, result := range results {
		fmt.Printf("%s %s: %s:\n", result.Type, result.Name, result.Error)
		for _, entry := range result.Config {
			// Truncate the value to 60 chars, if needed, for nicer formatting.
			fmt.Printf("%60s = %-60.60s   %-20s Read-only:%v Sensitive:%v\n",
				entry.Name, entry.Value, entry.Source,
				entry.IsReadOnly, entry.IsSensitive)
		}
	}

	a.Close()
}
