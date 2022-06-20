// Create ACLs
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

// Parses a list of 7n arguments to a slice of n ACLBinding
func parseACLBindings(args []string) (aclBindings kafka.ACLBindings, err error) {
	nACLBindings := len(args) / 7
	parsedACLBindings := make(kafka.ACLBindings, nACLBindings)

	for i := 0; i < nACLBindings; i++ {
		start := i * 7
		resourceTypeString := args[start]
		name := args[start+1]
		resourcePatternTypeString := args[start+2]
		principal := args[start+3]
		host := args[start+4]
		operationString := args[start+5]
		permissionTypeString := args[start+6]

		resourceType, errParse := kafka.ResourceTypeFromString(resourceTypeString)
		if errParse != nil {
			err = errParse
			fmt.Printf("Invalid resource type: %s: %v\n", resourceTypeString, err)
			return
		}

		resourcePatternType, errParse := kafka.ResourcePatternTypeFromString(resourcePatternTypeString)
		if errParse != nil {
			err = errParse
			fmt.Printf("Invalid resource pattern type: %s: %v\n", resourcePatternTypeString, err)
			return
		}

		operation, errParse := kafka.ACLOperationFromString(operationString)
		if errParse != nil {
			err = errParse
			fmt.Printf("Invalid operation: %s: %v\n", operationString, err)
			return
		}

		permissionType, errParse := kafka.ACLPermissionTypeFromString(permissionTypeString)
		if errParse != nil {
			err = errParse
			fmt.Printf("Invalid permission type: %s: %v\n", permissionTypeString, err)
			return
		}

		parsedACLBindings[i] = kafka.ACLBinding{
			Type:                resourceType,
			Name:                name,
			ResourcePatternType: resourcePatternType,
			Principal:           principal,
			Host:                host,
			Operation:           operation,
			PermissionType:      permissionType,
		}
	}
	aclBindings = parsedACLBindings
	return
}

func main() {

	// 2 + 7n arguments to create n ACL bindings
	nArgs := len(os.Args)
	aclBindingArgs := nArgs - 2
	if aclBindingArgs <= 0 || aclBindingArgs%7 != 0 {
		fmt.Fprintf(os.Stderr,
			"Usage: %s <bootstrap-servers> <resource-type1> <resource-name1> <resource-pattern-type1> "+
				"<principal1> <host1> <operation1> <permission-type1> ...\n",
			os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	aclBindings, err := parseACLBindings(os.Args[2:])
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

	// Create ACLs on cluster.
	// Set Admin options to wait for the request to finish (or at most 60s)
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}
	results, err := a.CreateACLs(
		ctx,
		aclBindings,
		kafka.SetAdminRequestTimeout(maxDur),
	)
	if err != nil {
		fmt.Printf("Failed to create ACLs: %v\n", err)
		os.Exit(1)
	}

	// Print results
	for i, result := range results {
		if result.Error.Code() == kafka.ErrNoError {
			fmt.Printf("CreateACLs %d successful\n", i)
		} else {
			fmt.Printf("CreateACLs %d failed, error code: %s, message: %s\n",
				i, result.Error.Code(), result.Error.String())
		}
	}

	a.Close()
}
