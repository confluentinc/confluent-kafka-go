// Example function-based Apache Kafka producer using a custom Serializer(Protobuf)
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
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/examples/protobuf_example/pb_example"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
)

var values []proto.Message = []proto.Message{
	&pb_example.Author{
		Name:  "Franz Kafka",
		Id:    19830703,
		Works: []string{"The Trial", "The Metamorphosis", "The Judgment"},
	}, &pb_example.Pizza{
		Size:     "Large",
		Toppings: []string{"Pepperoni", "Jalapenos"},
	},
}

type ProtoSerializer struct {
	kafka.AbstractSerializer
	lookup func(id string) proto.Message
}

// Serialize encodes msgObject writing the contents to the Message [Key|Value] fields
func (s *ProtoSerializer) Serialize(msg *kafka.Message) *kafka.Error {
	var err error
	if s.IsKey {
		msg.Key, err = proto.Marshal(msg.KeyObject.(proto.Message))
		return kafka.NewSerializationError(err, kafka.ErrKeySerialization)
	}

	msg.Value, err = proto.Marshal(msg.ValueObject.(proto.Message))
	return kafka.NewSerializationError(err, kafka.ErrValueSerialization)
}

func main() {

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic>\n", os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	topic := os.Args[2]

	p, err := kafka.NewSerializingProducer(kafka.ConfigMap{
		"bootstrap.servers":       broker,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0},
		&kafka.AbstractSerializer{}, &ProtoSerializer{})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	deliveryChan := make(chan kafka.Event)
	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.

	for _, value := range values {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			ValueObject:    value,
			Headers:        []kafka.Header{{Key: "proto", Value: []byte(fmt.Sprintf("%T", value))}},
		}, deliveryChan)
		log.Printf("%T", value)
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
