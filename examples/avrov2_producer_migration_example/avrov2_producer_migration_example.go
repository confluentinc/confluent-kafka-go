// Example function-based Apache Kafka producer
package main

/**
 * Copyright 2024 Confluent Inc.
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
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/jsonata"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
)

func main() {

	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <schema-registry> <topic>\n",
			os.Args[0])
		os.Exit(1)
	}

	// Register the KMS drivers and the field-level encryption executor
	jsonata.Register()

	bootstrapServers := os.Args[1]
	url := os.Args[2]
	topic := os.Args[3]

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

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

	schemaStr2 := `
    {
		"namespace": "confluent.io.examples.serialization.avro",
		"name": "User",
		"type": "record",
		"fields": [
            {"name": "name", "type": "string", "confluent:tags": [ "PII" ]},
	        {"name": "fave_num", "type": "long"},
	        {"name": "favorite_color", "type": "string"}
	    ]
	}`

	schema := schemaregistry.SchemaInfo{
		Schema:     schemaStr,
		SchemaType: "AVRO",
		Metadata: &schemaregistry.Metadata{
			Properties: map[string]string{
				"application.major.version": "1",
			},
		},
	}

	schema2 := schemaregistry.SchemaInfo{
		Schema:     schemaStr2,
		SchemaType: "AVRO",
		RuleSet: &schemaregistry.RuleSet{
			MigrationRules: []schemaregistry.Rule{
				schemaregistry.Rule{
					Name: "upgrade",
					Kind: "TRANSFORM",
					Mode: "UPGRADE",
					Type: "JSONATA",
					Expr: "$merge([$sift($, function($v, $k) {$k != 'favorite_number'}), {'fave_num': $.'favorite_number'}])",
				},
			},
		},
		Metadata: &schemaregistry.Metadata{
			Properties: map[string]string{
				"application.major.version": "2",
			},
		},
	}

	_, err = client.UpdateDefaultCompatibility(schemaregistry.None)
	if err != nil {
		fmt.Printf("Failed to update compatibility: %s\n", err)
		os.Exit(1)
	}

	_, err = client.Register(topic+"-value", schema, true)
	if err != nil {
		fmt.Printf("Failed to register schema: %s\n", err)
		os.Exit(1)
	}

	_, err = client.Register(topic+"-value", schema2, true)
	if err != nil {
		fmt.Printf("Failed to register schema: %s\n", err)
		os.Exit(1)
	}

	serConfig := avrov2.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestWithMetadata = map[string]string{
		"application.major.version": "1",
	}

	ser, err := avrov2.NewSerializer(client, serde.ValueSerde, serConfig)

	if err != nil {
		fmt.Printf("Failed to create serializer: %s\n", err)
		os.Exit(1)
	}

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)

	value := User{
		Name:           "First user",
		FavoriteNumber: 42,
		FavoriteColor:  "blue",
	}
	payload, err := ser.Serialize(topic, &value)
	if err != nil {
		fmt.Printf("Failed to serialize payload: %s\n", err)
		os.Exit(1)
	}

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)
	if err != nil {
		fmt.Printf("Produce failed: %v\n", err)
		os.Exit(1)
	}

	value = User{
		Name:           "Second user",
		FavoriteNumber: 42,
		FavoriteColor:  "blue",
	}
	payload, err = ser.Serialize(topic, &value)
	if err != nil {
		fmt.Printf("Failed to serialize payload: %s\n", err)
		os.Exit(1)
	}

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)
	if err != nil {
		fmt.Printf("Produce failed: %v\n", err)
		os.Exit(1)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)
}
