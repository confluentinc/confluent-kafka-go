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

// Describe user SCRAM credentials example
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func usage(reason string) {
	fmt.Fprintf(os.Stderr,
		"Error: %s\n",
		reason)
	fmt.Fprintf(os.Stderr,
		"Usage: %s <bootstrap-servers> <user1> <user2> ...\n",
		os.Args[0])
	os.Exit(1)
}

func main() {

	// 2 + n arguments
	nArgs := len(os.Args)

	if nArgs < 2 {
		usage("bootstrap-servers required")
	}

	mechanismString := make(map[kafka.ScramMechanism]string)
	mechanismString[kafka.ScramMechanismSHA256] = "SCRAM-SHA-256"
	mechanismString[kafka.ScramMechanismSHA512] = "SCRAM-SHA-512"
	mechanismString[kafka.ScramMechanismUnknown] = "UNKNOWN"

	bootstrapServers := os.Args[1]

	// Create new AdminClient.
	ac, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer ac.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	describeRes, describeErr := ac.DescribeUserScramCredentials(ctx, os.Args[2:])
	if describeErr != nil {
		fmt.Printf("Failed to describe user scram credentials: %s\n", describeErr)
		os.Exit(1)
	} else {
		for username, description := range describeRes {
			fmt.Printf("Username: %s \n", username)
			if description.Error.Code() == 0 {
				for i := 0; i < len(description.ScramCredentialInfos); i++ {
					fmt.Printf("	Mechanism: %s Iterations: %d\n", mechanismString[description.ScramCredentialInfos[i].Mechanism], description.ScramCredentialInfos[i].Iterations)
				}
			} else {
				fmt.Printf("	Error[%d]: %s\n", description.Error.Code(), description.Error.String())
			}
		}
	}
}
