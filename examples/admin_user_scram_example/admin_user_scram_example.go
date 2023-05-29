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
	mechanismstring := make(map[kafka.ScramMechanism]string)

	mechanismstring[kafka.Scram_SHA_256] = "SCRAM-SHA-256"
	mechanismstring[kafka.Scram_SHA_512] = "SCRAM-SHA-512"
	mechanismstring[kafka.Scram_Unknown] = "UNKWOWN"

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

	Describeres, Describeerr := ac.DescribeUserScramCredentials(ctx, users)
	if Describeerr != nil {
		fmt.Printf("Failed to Describe the User Scram Credentials: %s\n", err)
		os.Exit(1)

	} else {
		for username, description := range Describeres {
			fmt.Printf("Username : %s \n", username)
			if description.Err.Code() == 0 {
				for i := 0; i < len(description.Scram_Credential_Infos); i++ {
					fmt.Printf("	Mechansim : %s Iterations : %d\n", mechanismstring[description.Scram_Credential_Infos[i].Mechanism], description.Scram_Credential_Infos[i].Iterations)
				}
			} else {
				fmt.Printf("	Error[%d] : %s\n", description.Err.Code(), description.Err.String())
			}
		}
	}
	var alterations []kafka.UserScramCredentialUpsertion
	alterations = append(alterations, kafka.UserScramCredentialUpsertion{User: "adhitya", Salt: "salt", Password: "password", Scram_Credential_Info: kafka.ScramCredentialInfo{Mechanism: kafka.Scram_SHA_256, Iterations: 10000}})
	alterations = append(alterations, kafka.UserScramCredentialUpsertion{User: "pranav", Salt: "salt", Password: "password", Scram_Credential_Info: kafka.ScramCredentialInfo{Mechanism: kafka.Scram_SHA_256, Iterations: 10000}})

	Alterres, Altererr := ac.AlterUserScramCredentials(ctx, alterations, nil)
	if Altererr != nil {
		fmt.Printf("Failed to Alter the User Scram Credentials: %s\n", err)
		os.Exit(1)

	} else {
		for username, err := range Alterres {
			fmt.Printf("Username : %s \n", username)
			if err.Code() == 0 {
				fmt.Printf("	Success\n")
			} else {
				fmt.Printf("	Error[%d] : %s\n", err.Code(), err.String())
			}
		}
	}
	Describeres, Describeerr = ac.DescribeUserScramCredentials(ctx, users)
	if Describeerr != nil {
		fmt.Printf("Failed to Describe the User Scram Credentials: %s\n", err)
		os.Exit(1)

	} else {
		for username, description := range Describeres {
			fmt.Printf("Username : %s \n", username)
			if description.Err.Code() == 0 {
				for i := 0; i < len(description.Scram_Credential_Infos); i++ {
					fmt.Printf("	Mechansim : %s Iterations : %d\n", mechanismstring[description.Scram_Credential_Infos[i].Mechanism], description.Scram_Credential_Infos[i].Iterations)
				}
			} else {
				fmt.Printf("	Error[%d] : %s\n", description.Err.Code(), description.Err.String())
			}

		}
	}
}
