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

// Alter user SCRAM credentials example
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func usage(reason string) {
	fmt.Fprintf(os.Stderr,
		"Error: %s\n",
		reason)
	fmt.Fprintf(os.Stderr,
		"Usage: %s <bootstrap-servers> "+
			"(UPSERT <user1> <mechanism1> "+
			"<iterations1> <salt1?> <password1>|DELETE <user1> <mechanism1>) "+
			"[(UPSERT <user2> <mechanism2> <iterations2> <salt2?>"+
			" <password2>|DELETE <user2> <mechanism2>) ...]\n",
		os.Args[0])
	os.Exit(1)
}

func main() {
	// 2 + variable arguments
	nArgs := len(os.Args)

	if nArgs < 2 {
		usage("bootstrap-servers required")
	}

	if nArgs == 2 {
		usage("at least one upsert/delete required")
	}

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

	var upsertions []kafka.UserScramCredentialUpsertion
	var deletions []kafka.UserScramCredentialDeletion

	i := 2
	nAlterations := 0
	for i < nArgs {
		switch os.Args[i] {
		case "UPSERT":
			if i+5 >= nArgs {
				usage(fmt.Sprintf(
					"wrong argument count for alteration %d: expected 6, found %d",
					nAlterations, nArgs-i))
			}

			user := os.Args[i+1]
			mechanism, err := kafka.ScramMechanismFromString(os.Args[i+2])
			if err != nil {
				usage(err.Error())
			}
			iterations, err := strconv.Atoi(os.Args[i+3])
			if err != nil {
				usage(err.Error())
			}
			salt := []byte(os.Args[i+4])
			password := []byte(os.Args[i+5])
			if len(salt) == 0 {
				salt = nil
			}
			upsertions = append(upsertions,
				kafka.UserScramCredentialUpsertion{
					User:     user,
					Salt:     salt,
					Password: password,
					ScramCredentialInfo: kafka.ScramCredentialInfo{
						Mechanism:  mechanism,
						Iterations: iterations,
					},
				})
			i += 6
		case "DELETE":
			if i+2 >= nArgs {
				usage(fmt.Sprintf(
					"wrong argument count for alteration %d: expected 3, found %d",
					nAlterations, nArgs-i))
			}

			user := os.Args[i+1]
			mechanism, err := kafka.ScramMechanismFromString(os.Args[i+2])
			if err != nil {
				usage(err.Error())
			}
			deletions = append(deletions,
				kafka.UserScramCredentialDeletion{
					User:      user,
					Mechanism: mechanism,
				})
			i += 3
		default:
			usage(fmt.Sprintf("unknown alteration %s", os.Args[i]))
		}
		nAlterations++
	}

	alterRes, alterErr := ac.AlterUserScramCredentials(ctx, upsertions, deletions)
	if alterErr != nil {
		fmt.Printf("Failed to alter user scram credentials: %s\n", alterErr)
		os.Exit(1)
	}

	for username, err := range alterRes {
		fmt.Printf("Username: %s\n", username)
		if err.Code() == kafka.ErrNoError {
			fmt.Printf("	Success\n")
		} else {
			fmt.Printf("	Error[%d]: %s\n", err.Code(), err.String())
		}
	}

}
