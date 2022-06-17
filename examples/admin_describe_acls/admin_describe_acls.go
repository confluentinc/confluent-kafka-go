// Describe ACLs
package main

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

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Parses a list of 7n arguments to a slice of n ACLBindingFilter
func parseACLBindingFilters(args []string) (aclBindingFilters kafka.ACLBindingFilters, err error) {
	nACLBindingFilters := len(args) / 7
	parsedACLBindingFilters := make(kafka.ACLBindingFilters, nACLBindingFilters)

	for i := 0; i < nACLBindingFilters; i++ {
		start := i * 7
		resourceTypeString := args[start]
		name := args[start+1]
		resourcePatternTypeString := args[start+2]
		principal := args[start+3]
		host := args[start+4]
		operationString := args[start+5]
		permissionTypeString := args[start+6]

		var resourceType kafka.ResourceType
		var resourcePatternType kafka.ResourcePatternType
		var operation kafka.ACLOperation
		var permissionType kafka.ACLPermissionType

		resourceType, err = kafka.ResourceTypeFromString(resourceTypeString)
		if err != nil {
			fmt.Printf("Invalid resource type: %s: %v\n", resourceTypeString, err)
			return
		}
		resourcePatternType, err = kafka.ResourcePatternTypeFromString(resourcePatternTypeString)
		if err != nil {
			fmt.Printf("Invalid resource pattern type: %s: %v\n", resourcePatternTypeString, err)
			return
		}

		operation, err = kafka.ACLOperationFromString(operationString)
		if err != nil {
			fmt.Printf("Invalid operation: %s: %v\n", operationString, err)
			return
		}

		permissionType, err = kafka.ACLPermissionTypeFromString(permissionTypeString)
		if err != nil {
			fmt.Printf("Invalid permission type: %s: %v\n", permissionTypeString, err)
			return
		}

		parsedACLBindingFilters[i] = kafka.ACLBindingFilter{
			Type:                resourceType,
			Name:                name,
			ResourcePatternType: resourcePatternType,
			Principal:           principal,
			Host:                host,
			Operation:           operation,
			PermissionType:      permissionType,
		}
	}
	aclBindingFilters = parsedACLBindingFilters
	return
}

func main() {

	// 2 + 7 arguments to create an ACL binding filter
	nArgs := len(os.Args)
	aclBindingFilterArgs := nArgs - 2
	if aclBindingFilterArgs != 7 {
		fmt.Fprintf(os.Stderr,
			"Usage: %s <bootstrap-servers> <resource-type> <resource-name> <resource-pattern-type> "+
				"<principal> <host> <operation> <permission-type> ...\n",
			os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	aclBindingFilters, err := parseACLBindingFilters(os.Args[2:])
	if err != nil {
		os.Exit(1)
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

	// Describe ACLs on cluster.
	// Set Admin options to wait for the request to finish (or at most 60s)
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}
	result, err := a.DescribeACLs(
		ctx,
		aclBindingFilters[0],
		kafka.SetAdminRequestTimeout(maxDur),
	)
	if err != nil {
		fmt.Printf("Failed to describe ACLs: %v\n", err)
		os.Exit(1)
	}

	// Print results
	if result.Error.Code() == kafka.ErrNoError {
		fmt.Printf("DescribeACLs successful, result: %+v\n", result.ACLBindings)
	} else {
		fmt.Printf("DescribeACLs failed, error code: %s, message: %s\n",
			result.Error.Code(), result.Error.String())
	}

	a.Close()
}
