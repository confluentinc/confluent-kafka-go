// This is a simple example demonstrating how to produce a message to
// a topic, and then reading it back again using a consumer. The topic
// belongs to a Apache Kafka cluster from Confluent Cloud. For more
// information about Confluent Cloud, please visit:
//
// https://www.confluent.io/confluent-cloud/

package main

/**
 * Copyright 2018 Confluent Inc.
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
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"time"
)

// In order to set the constants below, you are going to need
// to log in into your Confluent Cloud account. If you choose
// to do this via the Confluent Cloud CLI, follow these steps.

// 1) Log into Confluent Cloud:
//    $ ccloud login
//
// 2) List the environments from your account:
//    $ ccloud environment list
//
// 3) From the list displayed, select one environment:
//    $ ccloud environment use <ENVIRONMENT_ID>
//
// To retrieve the information about the bootstrap servers,
// you need to execute the following commands:
//
// 1) List the Apache Kafka clusters from the environment:
//    $ ccloud kafka cluster list
//
// 2) From the list displayed, describe your cluster:
//    $ ccloud kafka cluster describe <CLUSTER_ID>
//
// Finally, to create a new API key to be used in this program,
// you need to execute the following command:
//
// 1) Create a new API key in Confluent Cloud:
//    $ ccloud api-key create

const (
	bootstrapServers = "<BOOTSTRAP_SERVERS>"
	ccloudAPIKey     = "<CCLOUD_API_KEY>"
	ccloudAPISecret  = "<CCLOUD_API_SECRET>"
)

func main() {

	topic := "go-test-topic"
	createTopic(topic)

	// Produce a new record to the topic...
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":       bootstrapServers,
		"sasl.mechanisms":         "PLAIN",
		"security.protocol":       "SASL_SSL",
		"sasl.username":           ccloudAPIKey,
		"sasl.password":           ccloudAPISecret})

	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %s", err))
	}

	value := "golang test value"
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic,
			Partition: kafka.PartitionAny},
		Value: []byte(value)}, nil)

	// Wait for delivery report
	e := <-producer.Events()

	message := e.(*kafka.Message)
	if message.TopicPartition.Error != nil {
		fmt.Printf("failed to deliver message: %v\n",
			message.TopicPartition)
	} else {
		fmt.Printf("delivered to topic %s [%d] at offset %v\n",
			*message.TopicPartition.Topic,
			message.TopicPartition.Partition,
			message.TopicPartition.Offset)
	}

	producer.Close()

	// Now consumes the record and print its value...
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       bootstrapServers,
		"sasl.mechanisms":         "PLAIN",
		"security.protocol":       "SASL_SSL",
		"sasl.username":           ccloudAPIKey,
		"sasl.password":           ccloudAPISecret,
		"session.timeout.ms":      6000,
		"group.id":                "my-group",
		"auto.offset.reset":       "earliest"})

	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %s", err))
	}

	topics := []string{topic}
	consumer.SubscribeTopics(topics, nil)

	for {
		message, err := consumer.ReadMessage(100 * time.Millisecond)
		if err == nil {
			fmt.Printf("consumed from topic %s [%d] at offset %v: "+
				string(message.Value), *message.TopicPartition.Topic,
				message.TopicPartition.Partition, message.TopicPartition.Offset)
		}
	}

	consumer.Close()

}

func createTopic(topic string) {

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers":       bootstrapServers,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"sasl.mechanisms":         "PLAIN",
		"security.protocol":       "SASL_SSL",
		"sasl.username":           ccloudAPIKey,
		"sasl.password":           ccloudAPISecret})

	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create topics on cluster.
	// Set Admin options to wait for the operation to finish (or at most 60s)
	maxDuration, err := time.ParseDuration("60s")
	if err != nil {
		panic("time.ParseDuration(60s)")
	}

	results, err := adminClient.CreateTopics(ctx,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 3}},
		kafka.SetAdminOperationTimeout(maxDuration))

	if err != nil {
		fmt.Printf("Problem during the topic creation: %v\n", err)
		os.Exit(1)
	}

	// Check for specific topic errors
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError &&
			result.Error.Code() != kafka.ErrTopicAlreadyExists {
			fmt.Printf("Topic creation failed for %s: %v",
				result.Topic, result.Error.String())
			os.Exit(1)
		}
	}

	adminClient.Close()

}
