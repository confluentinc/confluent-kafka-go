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

// Example function-based Apache Kafka producer
package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/awskms"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/azurekms"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/gcpkms"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/hcvault"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/localkms"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
)

func main() {

	if len(os.Args) != 7 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <schema-registry> <topic> <kekName> <kmsType> <kmsKeyId>\n",
			os.Args[0])
		os.Exit(1)
	}

	// Register the KMS drivers and the field-level encryption executor
	awskms.Register()
	azurekms.Register()
	gcpkms.Register()
	hcvault.Register()
	localkms.Register()
	encryption.Register()

	bootstrapServers := os.Args[1]
	url := os.Args[2]
	topic := os.Args[3]
	kekName := os.Args[4]
	kmsType := os.Args[5] // one of aws-kms, azure-kms, gcp-kms, hcvault
	kmsKeyID := os.Args[6]

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer p.Close()

	fmt.Printf("Created Producer %v\n", p)

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))

	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		os.Exit(1)
	}

	schemaStr := `
syntax = "proto3";

option go_package="./;main";

import "confluent/meta.proto";

message User {
    string name = 1 [
        (confluent.field_meta).tags = "PII"
    ];
    int64 favorite_number = 2;
    string favorite_color = 3;
}
`
	schema := schemaregistry.SchemaInfo{
		Schema:     schemaStr,
		SchemaType: "PROTOBUF",
		RuleSet: &schemaregistry.RuleSet{
			DomainRules: []schemaregistry.Rule{
				{
					Name: "encryptPII",
					Kind: "TRANSFORM",
					Mode: "WRITEREAD",
					Type: "ENCRYPT",
					Tags: []string{"PII"},
					Params: map[string]string{
						"encrypt.kek.name":   kekName,
						"encrypt.kms.type":   kmsType,
						"encrypt.kms.key.id": kmsKeyID,
					},
					OnFailure: "ERROR,NONE",
				},
			},
		},
	}

	_, err = client.Register(topic+"-value", schema, true)
	if err != nil {
		fmt.Printf("Failed to register schema: %s\n", err)
		os.Exit(1)
	}

	serConfig := protobuf.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	// KMS properties can be passed as follows
	//serConfig.RuleConfig = map[string]string{
	//	"secret.access.key": "xxx",
	//	"access.key,id": "xxx",
	//}

	ser, err := protobuf.NewSerializer(client, serde.ValueSerde, serConfig)

	if err != nil {
		fmt.Printf("Failed to create serializer: %s\n", err)
		os.Exit(1)
	}

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

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

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
}
