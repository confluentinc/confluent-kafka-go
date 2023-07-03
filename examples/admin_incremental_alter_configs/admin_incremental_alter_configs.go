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

// Incremental alter configs example
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func usage(reason string) {
	fmt.Fprintf(os.Stderr,
		"Reason: %s\n",
		reason)
	fmt.Fprintf(os.Stderr,
		"Usage: %s <bootstrap-servers> <resource-type1> <resource-name1> "+
			"<config1=op1:val1;config2=op2:val2> ...\n"+
			" <bootstrap-servers> - CSV list of bootstrap brokers\n"+
			" <resource-type> - any, broker, topic, group\n"+
			" <resource-name> - broker id or topic name\n"+
			" <config1=op1:val1;config2=op2:val2> - list of config values "+
			"to alter for the corresponding resource with the "+
			"corresponding incremental operation\n",
		os.Args[0])
	os.Exit(1)
}

func parseAlterConfigOpType(s string) kafka.AlterConfigOpType {
	switch strings.ToUpper(s) {
	case "SET":
		return kafka.AlterConfigOpTypeSet
	case "DELETE":
		return kafka.AlterConfigOpTypeDelete
	case "APPEND":
		return kafka.AlterConfigOpTypeAppend
	case "SUBTRACT":
		return kafka.AlterConfigOpTypeSubtract
	default:
		usage(fmt.Sprintf("Invalid alter config operation type: %s", s))
	}
	return kafka.AlterConfigOpTypeSet
}

func main() {

	if len(os.Args) < 5 || len(os.Args)%3 != 2 {
		usage(fmt.Sprintf("Invalid number of arguments: %d", len(os.Args)))
	}

	bootstrapServers := os.Args[1]
	var resources []kafka.ConfigResource = []kafka.ConfigResource{}

	resourceCount := 0
	for i := 2; i < len(os.Args); i += 3 {
		resourceType, err := kafka.ResourceTypeFromString(os.Args[i])
		if err != nil {
			fmt.Printf("Invalid resource type: %s\n", os.Args[2])
			os.Exit(1)
		}

		resourceName := os.Args[i+1]

		resource := kafka.ConfigResource{Type: resourceType, Name: resourceName}

		stringMap := map[string]string{}
		operationMap := map[string]kafka.AlterConfigOpType{}

		configsString := strings.Split(os.Args[i+2], ";")
		configCount := 0
		for _, configString := range configsString {
			keyOperationValue := strings.Split(configString, "=")

			if len(keyOperationValue) != 2 {
				usage(fmt.Sprintf("Missing key-value pair at resource %d, index: %d", resourceCount, configCount))
			}

			key := keyOperationValue[0]
			operationValue := strings.Split(keyOperationValue[1], ":")
			if len(operationValue) != 2 {
				usage(fmt.Sprintf("Missing operation-value pair at resource %d, index: %d", resourceCount, configCount))
			}

			op := parseAlterConfigOpType(operationValue[0])
			value := operationValue[1]

			stringMap[key] = value
			operationMap[key] = op

			configCount++
		}

		resource.Config = kafka.StringMapToIncrementalConfigEntries(stringMap, operationMap)
		resources = append(resources, resource)
		resourceCount++
	}

	// Create a new AdminClient.
	// AdminClient can also be instantiated using an existing
	// Producer or Consumer instance, see NewAdminClientFromProducer and
	// NewAdminClientFromConsumer.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dur, _ := time.ParseDuration("20s")
	// Incrementally alter configurations for specified resources.
	results, err := a.IncrementalAlterConfigs(ctx,
		resources,
		kafka.SetAdminRequestTimeout(dur))
	if err != nil {
		fmt.Printf("Failed to IncrementalAlterConfigs: %s\n",
			err)
		os.Exit(1)
	}

	// Print results
	for _, result := range results {
		fmt.Printf("%s %s: %s:\n", result.Type, result.Name, result.Error)
	}

	a.Close()
}
