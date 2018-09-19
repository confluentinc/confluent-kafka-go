// Example function-based high-level Apache Kafka consumer using a custom Serializer(Protobuf)
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
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/examples/protobuf_example/pb_example"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// Protobuf serializer used to decode protobuf messages stored in Kafka.
type ProtoSerializer struct {
	kafka.AbstractSerializer
	lookup func(id string) proto.Message
}

// Deserialize decodes the message contents into a protobuf object retrieved from the local registry.
func (d *ProtoSerializer) Deserialize(msg *kafka.Message) *kafka.Error {
	var protoObj proto.Message
	if protoObj = d.GetProtoType(msg.Headers); protoObj == nil {
		return kafka.NewDeserializationError(errors.New("message object type unknown"), kafka.ErrInvalidArg)
	}

	if d.IsKey {
		msg.KeyObject = protoObj
		return kafka.NewDeserializationError(proto.Unmarshal(msg.Key, msg.KeyObject.(proto.Message)), kafka.ErrKeyDeserialization)
	}

	msg.ValueObject = protoObj
	return kafka.NewDeserializationError(proto.Unmarshal(msg.Value, msg.ValueObject.(proto.Message)), kafka.ErrValueSerialization)
}

// NewDeserializerRegistry provides a local registry to be queried on deserialization.
func (s *ProtoSerializer) NewDeserializerRegistry(new func(id string) proto.Message) {
	s.lookup = new
}

// GetProtoType searches message headers for the key proto.
// The value identified by these headers is used to retrieve objects from the local registry.
func (s *ProtoSerializer) GetProtoType(headers []kafka.Header) proto.Message {
	for _, header := range headers {
		if header.Key == "proto" {
			obj := s.lookup(string(header.Value))
			return obj
		}
	}
	return nil
}

// ProtoRegistry provides a registry for Protobuf Objects for deserialization.
func ProtoRegistry(key string) proto.Message {
	switch key {
	case "*pb_example.Author":
		return &pb_example.Author{}
	case "*pb_example.Pizza":
		return &pb_example.Pizza{}
	}
	return nil
}

// ../producer/protobuf_example.go should be run first to populate teh topic with messages we can deserialize.
func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topic := os.Args[3]

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	serde := &ProtoSerializer{}
	serde.NewDeserializerRegistry(ProtoRegistry)

	c, err := kafka.NewDeserializingConsumer(kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          group,
		"auto.offset.reset": "earliest"},
		&kafka.AbstractSerializer{}, serde)

	if err != nil {
		log.Println(err)
	}

	c.Subscribe(topic, nil)

	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
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
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
