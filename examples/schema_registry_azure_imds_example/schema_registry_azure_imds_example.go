/**
 * Copyright 2026 Confluent Inc.
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

// This is a simple example demonstrating how to use Azure User-Assigned
// Managed Identity (UAMI) authentication with the Confluent Schema Registry
// client. This example is intended to be run on an Azure VM with a
// user-assigned managed identity configured.
//
// Usage:
//
//	go run schema_registry_azure_imds_example.go \
//	  -schema.registry.url=https://psrc-xxxxx.us-central1.gcp.confluent.cloud \
//	  -uami.client.id=<uami-client-id> \
//	  -uami.resource=api://<uami-resource-id> \
//	  -bearer.auth.logical.cluster=lsrc-xxxxx \
//	  -bearer.auth.identity.pool.id=pool-xxxxx \
//	  [-uami.base.url=https://custom-authority-host]
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

func main() {
	schemaRegistryURL := flag.String("schema.registry.url", "", "Schema Registry URL")
	uamiClientID := flag.String("uami.client.id", "", "UAMI client ID (Azure user-assigned managed identity client ID)")
	uamiResource := flag.String("uami.resource", "", "UAMI resource (Azure resource URI)")
	uamiBaseURL := flag.String("uami.base.url", "", "Custom Azure authority host (optional, defaults to Azure public cloud)")
	logicalCluster := flag.String("bearer.auth.logical.cluster", "", "Schema Registry logical cluster ID")
	identityPoolID := flag.String("bearer.auth.identity.pool.id", "", "Confluent identity pool ID")
	flag.Parse()

	if *schemaRegistryURL == "" || *uamiResource == "" || *logicalCluster == "" || *identityPoolID == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -schema.registry.url=<url> -uami.resource=<resource> "+
			"-bearer.auth.logical.cluster=<cluster> -bearer.auth.identity.pool.id=<pool>\n", os.Args[0])
		os.Exit(1)
	}

	conf := schemaregistry.NewConfigWithUAMIAuthentication(
		*schemaRegistryURL,
		*uamiBaseURL,   // endpointURL: custom Azure authority host (empty for Azure public cloud)
		*uamiClientID,  // endpointQuery: UAMI client ID (empty for system-assigned identity)
		*uamiResource,  // resource: Azure resource URI
		*logicalCluster,
		*identityPoolID,
	)

	client, err := schemaregistry.NewClient(conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create Schema Registry client: %s\n", err)
		os.Exit(1)
	}

	subjects, err := client.GetAllSubjects()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get subjects: %s\n", err)
		os.Exit(1)
	}

	fmt.Println("Subjects:")
	for _, subject := range subjects {
		fmt.Printf("  %s\n", subject)
	}
}
