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

package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
	"time"
)

func main() {

	mockCluster, err := kafka.NewMockCluster(1)
	if err != nil {
		fmt.Printf("Failed to create MockCluster: %s\n", err)
		os.Exit(1)
	}
	defer mockCluster.Close()

	err = mockCluster.SetRoundtripDuration(1, 4*time.Second)
	if err != nil {
		fmt.Errorf("could not configure roundtrip duration: %v", err)
		return
	}
	broker := mockCluster.BootstrapServers()

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":   broker,
		"delivery.timeout.ms": 1000,
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	m, err := sendTestMsg(p)

	if m.TopicPartition.Error != nil {
		fmt.Printf("EXPECTED: Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Errorf("message should timeout because of broker configuration")
		return
	}

	fmt.Println("'reset' broker roundtrip duration")
	err = mockCluster.SetRoundtripDuration(1, 10*time.Millisecond)
	if err != nil {
		fmt.Errorf("could not configure roundtrip duration: %v", err)
		return
	}

	// see what happens when broker is down
	fmt.Println("set broker down")
	err = mockCluster.SetBrokerDown(1)
	if err != nil {
		fmt.Errorf("broker should now be down but got error: %v", err)
		return
	}

	m, err = sendTestMsg(p)

	if m.TopicPartition.Error != nil {
		fmt.Printf("EXPECTED: Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Errorf("message should timeout because of broker configuration")
		return
	}

	// bring the broker up again
	fmt.Println("set broker up again")
	err = mockCluster.SetBrokerUp(1)
	if err != nil {
		fmt.Errorf("broker should now be up again but got error: %v", err)
		return
	}

	m, err = sendTestMsg(p)
	if err != nil {
		fmt.Errorf("there shouldn't be an error but got: %v", err)
		return
	}

	fmt.Println("message was sent!")

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
