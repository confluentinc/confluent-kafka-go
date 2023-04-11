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

// Describe topics
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(
			os.Stderr,
			"Usage: %s <bootstrap-servers> <include_cluster_authorized_operations\n",
			os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	include_cluster_authorized_operations, err_operations := strconv.ParseBool(os.Args[2])
	if err_operations != nil {
		fmt.Printf(
			"Failed to parse value of include_cluster_authorized_operations %s: %s\n", os.Args[2], err_operations)
		os.Exit(1)
	}

	// Create a new AdminClient.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer a.Close()

	// Call DescribeCluster.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	clusterDesc, err := a.DescribeCluster(
		ctx, kafka.SetAdminOptionIncludeClusterAuthorizedOperations(
			include_cluster_authorized_operations))
	if err != nil {
		fmt.Printf("Failed to describe topics: %s\n", err)
		os.Exit(1)
	}

	// Print results
	fmt.Printf("ClusterId: %s\tControllerId: %d\t\tNode count: %d\n",
		clusterDesc.ClusterID, clusterDesc.ControllerID, len(clusterDesc.Nodes))
	for nidx := 0; nidx < len(clusterDesc.Nodes); nidx++ {
		node := clusterDesc.Nodes[nidx]
		fmt.Printf("Node [id: %d, host: %s, port: %d]\n",
			node.ID, node.Host, node.Port)
	}
	if include_cluster_authorized_operations == true {
		fmt.Printf("Allowed acl operations:\n")
		for i := 0; i < len(clusterDesc.ClusterAuthorizedOperations); i++ {
			fmt.Printf("\t%s\n", clusterDesc.ClusterAuthorizedOperations[i])
		}
	}
}
