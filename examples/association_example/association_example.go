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

// Example demonstrating Schema Registry Association API
package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <schema-registry-url>\n",
			os.Args[0])
		os.Exit(1)
	}

	url := os.Args[1]

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))
	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		os.Exit(1)
	}

	schemaStr := `
    {
		"namespace": "confluent.io.examples.serialization.avro",
		"name": "User",
		"type": "record",
		"fields": [
            {"name": "name", "type": "string", "confluent:tags": [ "PII" ]},
	        {"name": "favorite_number", "type": "long"},
	        {"name": "favorite_color", "type": "string"}
	    ]
	}`

	schema := schemaregistry.SchemaInfo{
		Schema:     schemaStr,
		SchemaType: "AVRO",
	}

	// Define the association request
	associationRequest := schemaregistry.AssociationCreateOrUpdateRequest{
		ResourceName:      "mytopic",
		ResourceNamespace: "lkc-1234",
		ResourceID:        "1234",
		ResourceType:      "topic",
		Associations: []schemaregistry.AssociationCreateOrUpdateInfo{
			{
				Subject:         "mytopic-value",
				AssociationType: "value",
				Lifecycle:       schemaregistry.STRONG,
				Schema:          &schema,
			},
		},
	}

	// Create association
	fmt.Println("Creating association...")
	createResponse, err := client.CreateAssociation(associationRequest)
	if err != nil {
		fmt.Printf("Failed to create association: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created association: %+v\n", createResponse)

	// Get the resource ID from the response
	resourceID := createResponse.ResourceID

	// Get associations by subject
	fmt.Println("\nGetting associations by subject...")
	associationsBySubject, err := client.GetAssociationsBySubject(
		"mytopic-value", // subject
		"topic",         // resourceType
		nil,             // associationTypes
		"",              // lifecycle
		0,               // offset
		100,             // limit
	)
	if err != nil {
		fmt.Printf("Failed to get associations by subject: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Associations by subject: %+v\n", associationsBySubject)

	// Get associations by resource ID
	fmt.Println("\nGetting associations by resource ID...")
	associationsByResourceID, err := client.GetAssociationsByResourceID(
		resourceID, // resourceID
		"topic",    // resourceType
		nil,        // associationTypes
		"",         // lifecycle
		0,          // offset
		100,        // limit
	)
	if err != nil {
		fmt.Printf("Failed to get associations by resource ID: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Associations by resource ID: %+v\n", associationsByResourceID)

	// Get associations by resource name
	fmt.Println("\nGetting associations by resource name...")
	associationsByResourceName, err := client.GetAssociationsByResourceName(
		"mytopic", // resourceName
		"-",       // resourceNamespace
		"topic",   // resourceType
		nil,       // associationTypes
		"",        // lifecycle
		0,         // offset
		100,       // limit
	)
	if err != nil {
		fmt.Printf("Failed to get associations by resource name: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Associations by resource name: %+v\n", associationsByResourceName)

	// Update association to WEAK lifecycle
	fmt.Println("\nUpdating association to WEAK lifecycle...")
	updateRequest := schemaregistry.AssociationCreateOrUpdateRequest{
		ResourceName:      "mytopic",
		ResourceNamespace: "lkc-1234",
		ResourceID:        resourceID,
		ResourceType:      "topic",
		Associations: []schemaregistry.AssociationCreateOrUpdateInfo{
			{
				Subject:         "mytopic-value",
				AssociationType: "value",
				Lifecycle:       schemaregistry.WEAK,
			},
		},
	}
	updateResponse, err := client.CreateOrUpdateAssociation(updateRequest)
	if err != nil {
		fmt.Printf("Failed to update association: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Updated association: %+v\n", updateResponse)

	// Get associations by resource ID to verify the update
	fmt.Println("\nGetting associations by resource ID after update...")
	associationsByResourceID, err = client.GetAssociationsByResourceID(
		resourceID, // resourceID
		"topic",    // resourceType
		nil,        // associationTypes
		"",         // lifecycle
		0,          // offset
		100,        // limit
	)
	if err != nil {
		fmt.Printf("Failed to get associations by resource ID: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Associations by resource ID (after update): %+v\n", associationsByResourceID)

	// Delete associations
	fmt.Println("\nDeleting associations...")
	err = client.DeleteAssociations(
		resourceID, // resourceID
		"topic",    // resourceType
		nil,        // associationTypes
		false,      // cascadeLifecycle
	)
	if err != nil {
		fmt.Printf("Failed to delete associations: %s\n", err)
		os.Exit(1)
	}
	fmt.Println("Associations deleted successfully")

	// Get associations by resource ID
	fmt.Println("\nGetting associations by resource ID...")
	associationsByResourceID, err = client.GetAssociationsByResourceID(
		resourceID, // resourceID
		"topic",    // resourceType
		nil,        // associationTypes
		"",         // lifecycle
		0,          // offset
		100,        // limit
	)
	if err != nil {
		fmt.Printf("Failed to get associations by resource ID: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Associations by resource ID: %+v\n", associationsByResourceID)

	fmt.Println("\nDone!")
}
