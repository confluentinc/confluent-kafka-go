// This is a simple example demonstrating how to produce a message to 
// Confluent Cloud then read it back again.
//     
// https://www.confluent.io/confluent-cloud/
// 
// Confluent Cloud does not auto-create topics. You will need to use the ccloud 
// cli to create the go-test-topic topic before running this example. The 
// <ccloud bootstrap servers>, <ccloud key> and <ccloud secret> parameters are
// available via the confluent cloud web interface. For more information,
// refer to the quick-start:
//     
// https://docs.confluent.io/current/cloud-quickstart.html
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
	"time"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "<ccloud bootstrap servers>",
		"api.version.request": true,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"sasl.mechanisms": "PLAIN",
		"security.protocol": "SASL_SSL",
		"ssl.ca.location": "/usr/local/etc/openssl/cert.pem",
		"sasl.username": "<ccloud key>",
		"sasl.password": "<ccloud secret>",})

	if err != nil {
		fmt.Printf("failed to create producer: %s\n", err)
		os.Exit(1)
	}

	value := "golang test value"
	topic := "go-test-topic"
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
	}, nil)

	e := <-p.Events()
	
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		fmt.Printf("failed to deliver message: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("delivered to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	p.Close()


	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "<ccloud bootstrap servers>",
		"api.version.request": true,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"sasl.mechanisms": "PLAIN",
		"security.protocol": "SASL_SSL",
		"ssl.ca.location": "/usr/local/etc/openssl/cert.pem",
		"sasl.username": "<ccloud key>",
		"sasl.password": "<ccloud secret>",
		"session.timeout.ms": 6000,
		"group.id": "my-group",
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"},})

	if err != nil {
		fmt.Printf("failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	topics := []string { topic }
	c.SubscribeTopics(topics, nil)

	for {
		msg, err := c.ReadMessage(100 * time.Millisecond)
		if err == nil {
			fmt.Printf("consumed: %s: %s\n", msg.TopicPartition, string(msg.Value))
		}
	}

	c.Close()
}
