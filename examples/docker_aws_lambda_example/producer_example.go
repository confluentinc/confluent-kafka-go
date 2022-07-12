package main

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

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var p *kafka.Producer

func main() {
	// Reference: https://github.com/aws/aws-lambda-go/issues/318#issuecomment-1019318919
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		fmt.Printf("waiting for the SIGTERM\n")
		s := <-sigs
		fmt.Printf("received signal %s\n", s)
		if p != nil {
			p.Close()
		}
		fmt.Printf("done\n")
	}()

	lambda.Start(HandleRequest)
}

// HandleRequest handles creating producer and
// producing messages.
func HandleRequest() error {
	broker := os.Getenv("BOOTSTRAP_SERVERS")
	topic := os.Getenv("TOPIC")
	ccloudAPIKey := os.Getenv("CCLOUDAPIKEY")
	ccloudAPISecret := os.Getenv("CCLOUDAPISECRET")

	var err error

	if p == nil {
		p, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": broker,
			"sasl.mechanisms":   "PLAIN",
			"security.protocol": "SASL_SSL",
			"sasl.username":     ccloudAPIKey,
			"sasl.password":     ccloudAPISecret,
		})

		if err != nil {
			fmt.Printf("Failed to create producer: %s\n", err)
			os.Exit(1)
		}

		fmt.Printf("Created Producer %v\n", p)
	}

	a, err := kafka.NewAdminClientFromProducer(p)
	defer a.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}

	// Create topics if it doesn't exist
	results, err := a.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:         topic,
			NumPartitions: 1}},
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Printf("CreateTopics request failed: %v\n", err)
		return err
	}

	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

	deliveryChan := make(chan kafka.Event)

	value := "Hello Go!"
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)

	if err != nil {
		fmt.Printf("Produce failed: %s\n", err)
		return err
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

	return nil
}
