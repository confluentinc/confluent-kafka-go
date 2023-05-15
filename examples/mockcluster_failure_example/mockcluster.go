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

// Demonstrates failure modes for mock cluster:
// 1. RTT Duration set to more than Delivery Timeout
// 2. Broker set as being down.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	mockCluster, err := kafka.NewMockCluster(1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create MockCluster: %s\n", err)
		os.Exit(1)
	}
	defer mockCluster.Close()

	// Set RTT > Delivery Timeout
	err = mockCluster.SetRoundtripDuration(1, 4*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not configure roundtrip duration: %v", err)
		return
	}
	broker := mockCluster.BootstrapServers()

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":   broker,
		"delivery.timeout.ms": 1000,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	m, err := sendTestMsg(p)

	if m.TopicPartition.Error != nil {
		fmt.Printf("EXPECTED: Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Fprintf(os.Stderr, "Message should timeout because of broker configuration")
		return
	}

	fmt.Println("'reset' broker roundtrip duration")
	err = mockCluster.SetRoundtripDuration(1, 10*time.Millisecond)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not configure roundtrip duration: %v", err)
		return
	}

	// See what happens when broker is down.
	fmt.Println("Set broker down")
	err = mockCluster.SetBrokerDown(1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Broker should now be down but got error: %v", err)
		return
	}

	m, err = sendTestMsg(p)

	if m.TopicPartition.Error != nil {
		fmt.Printf("EXPECTED: Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Fprintf(os.Stderr, "Message should timeout because of broker configuration")
		return
	}

	// Bring the broker up again.
	fmt.Println("Set broker up again")
	err = mockCluster.SetBrokerUp(1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Broker should now be up again but got error: %v", err)
		return
	}

	m, err = sendTestMsg(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "There shouldn't be an error but got: %v", err)
		return
	}

	fmt.Println("Message was sent!")

}

func sendTestMsg(p *kafka.Producer) (*kafka.Message, error) {

	topic := "Test"
	value := "Hello Go!"

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)

	if err != nil {
		return nil, err
	}

	e := <-deliveryChan
	return e.(*kafka.Message), nil
}
