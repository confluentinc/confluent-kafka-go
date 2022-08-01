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

// This is a simple example demonstrating how to produce a message to
// a topic, and then reading it back again using a consumer.
// The authentication uses the OpenID Connect method of
// the OAUTHBEARER SASL mechanism.

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	bootstrapServers = "<bootstrap_servers>"
)

func main() {
	extensions := fmt.Sprintf("logicalCluster=%s,identityPoolId=%s", "<logical_cluster>", "<identity_pool_id>")
	commonProperties := kafka.ConfigMap{
		"bootstrap.servers":                   bootstrapServers,
		"security.protocol":                   "SASL_SSL",
		"sasl.mechanism":                      "OAUTHBEARER",
		"sasl.oauthbearer.method":             "OIDC",
		"sasl.oauthbearer.client.id":          "<oauthbearer_client_id>",
		"sasl.oauthbearer.client.secret":      "<oauthbearer_client_secret>",
		"sasl.oauthbearer.token.endpoint.url": "<token_endpoint_url>",
		"sasl.oauthbearer.extensions":         extensions,
		"sasl.oauthbearer.scope":              "<scope>",
	}
	producerProperties := kafka.ConfigMap{}
	consumerProperties := kafka.ConfigMap{
		"group.id":                 "oauthbearer_oidc_example",
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": "false",
	}
	for k, v := range commonProperties {
		producerProperties[k] = v
		consumerProperties[k] = v
	}

	topic := "go-test-topic"
	createTopic(&producerProperties, topic)

	producer, err := kafka.NewProducer(&producerProperties)
	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %s", err))
	}

	consumer, err := kafka.NewConsumer(&consumerProperties)
	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %s", err))
	}
	topics := []string{topic}
	consumer.SubscribeTopics(topics, nil)

	var wg sync.WaitGroup
	wg.Add(2)

	run := true
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		defer wg.Done()
		for run {
			select {
			case sig := <-signalChannel:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				run = false
			default:
				// Produce a new record to the topic...
				value := "golang test value"
				producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic,
						Partition: kafka.PartitionAny},
					Value: []byte(value)}, nil)
			loop:
				// Wait for delivery report
				for ev := range producer.Events() {
					switch e := ev.(type) {
					case *kafka.Message:
						message := e
						if message.TopicPartition.Error != nil {
							fmt.Printf("failed to deliver message: %v\n",
								message.TopicPartition)
						} else {
							fmt.Printf("delivered to topic %s [%d] at offset %v\n",
								*message.TopicPartition.Topic,
								message.TopicPartition.Partition,
								message.TopicPartition.Offset)
						}
						break loop
					default:
						// ignore other event types
					}
				}
				time.Sleep(1 * time.Second)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for run {
			// Receive one message
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				_, err := consumer.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s, %s:\n",
						e.TopicPartition, err.Error())
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}()

	wg.Wait()
	producer.Close()
	consumer.Close()
}

func createTopic(adminProperties *kafka.ConfigMap, topic string) {
	adminClient, err := kafka.NewAdminClient(adminProperties)
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
