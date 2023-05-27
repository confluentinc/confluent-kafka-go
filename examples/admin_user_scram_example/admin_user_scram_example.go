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

// Example function-based high-level Apache Kafka consumer
package main

// consumer_example implements a consumer using the non-channel Poll() API
// to retrieve messages and events.

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	// if len(os.Args) < 1 {
	// 	fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> \n",
	// 		os.Args[0])
	// 	os.Exit(1)
	// }
	var mechanismstring map[kafka.ScramMechanism]string

	mechanismstring[kafka.ScramMechanism.Scram_SHA_256] = "SCRAM-SHA-256"
	mechanismstring[kafka.ScramMechanism.Scram_SHA_512] = "SCRAM-SHA-512"
	mechanismstring[kafka.ScramMechanism.Scram_Unknown] = "UNKWOWN"
	
	bootstrapServers := "localhost:9092"
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Create new AdminClient.
	ac, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer ac.Close()

	var users []string
	users = append(users, "adhitya")
	users = append(users, "pranav")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	res, err := ac.DescribeUserScramCredentials(ctx, users)
	if err != nil {
		fmt.Printf("Failed to Describe the User Scram Credentials: %s\n", err)
		os.Exit(1)
		
	} else {
		for username, description := range res {
			fmt.Printf("Username : %s \n", username)
			for i := 0; i < len(description.scram_credential_infos); i++ {
				if description.err.code == 0 {
					fmt.Printf("	Mechansim : %s Iterations : %d\n",mechanismstring[ description.scram_credential_infos[i].mechanism ], description.scram_credential_infos[i].iterations)
				}else {
					fmt.Printf(("	Error[%d] : %s\n", err.code,err.str)
				}
			}

		}
	}
	var alterations []UserScramCredentialUpsertion
	alterations = append(alterations, UserScramCredentialUpsertion({ user : "adhitya" , salt:"salt" , password : "password" ,mechanism : kafka.ScramMechanism.SCRAM_SHA_256,iterations :10000}) )
	alterations = append(alterations, UserScramCredentialUpsertion({ user : "pranav" , salt:"salt" , password : "password" ,mechanism : kafka.ScramMechanism.SCRAM_SHA_256,iterations :10000}) )
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	res, err := ac.AlterUserScramCredentials(ctx, users)
	if err != nil {
		fmt.Printf("Failed to Alter the User Scram Credentials: %s\n", err)
		os.Exit(1)
		
	} else {
		for username, err := range res {
			fmt.Printf("Username : %s \n", username)
			if err.code == 0 {
					fmt.Printf("	Success\n")
			}else {
					fmt.Printf(("	Error[%d] : %s\n", err.code,err.str)
			}
		}
	}
	res, err := ac.DescribeUserScramCredentials(ctx, users)
	if err != nil {
		fmt.Printf("Failed to Describe the User Scram Credentials: %s\n", err)
		os.Exit(1)
		
	} else {
		for username, description := range res {
			fmt.Printf("Username : %s \n", username)
			for i := 0; i < len(description.scram_credential_infos); i++ {
				if description.err.code == 0 {
					fmt.Printf("	Mechansim : %s Iterations : %d\n",mechanismstring[ description.scram_credential_infos[i].mechanism ], description.scram_credential_infos[i].iterations)
				}else {
					fmt.Printf(("	Error[%d] : %s\n", err.code,err.str)
				}
			}

		}
	}
}
