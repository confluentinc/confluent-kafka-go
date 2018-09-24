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
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"github.com/confluentinc/confluent-kafka-go/encoding/avro"
	"os/signal"
	"syscall"
	"log"
)

func main() {

	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <schema registry url> <topic>\n", os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	schemaRegistry := os.Args[2]
	topic := os.Args[3]

	p, err := kafka.NewSerializingProducer(kafka.ConfigMap{
		"bootstrap.servers":       broker,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"schema.registry.url": schemaRegistry},
		&kafka.AbstractSerializer{}, avro.NewSerializer())

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	deliveryChan := make(chan kafka.Event)
	// Optional delivery channel, if not specified the Producer object's Events channel is used.

	schema, err := avro.ParseFile("/Users/ryan/go/src/github.com/confluentinc/confluent-kafka-go/encoding/avro/schemas/advanced.avsc")

		log.Println(p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			ValueObject:    populateRecord(avro.NewGenericRecord(schema)),
			}, deliveryChan))


	e := <-deliveryChan
	log.Println("here")
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)
	runConsumer(broker, schemaRegistry, topic, "tester")
}

func populateRecord(datum avro.GenericRecord) avro.GenericRecord {
	datum.Set("value", int32(3))
	datum.Set("union", int32(1234))

	subRecords := make([]avro.Record, 2)

	subRecord0 := avro.NewGenericRecord(datum.Schema())
	subRecord0.Set("stringValue", "Hello")
	subRecord0.Set("intValue", int32(1))
	subRecord0.Set("fruits", "apple")
	subRecords[0] = subRecord0

	subRecord1 := avro.NewGenericRecord(datum.Schema())
	subRecord1.Set("stringValue", "World")
	subRecord1.Set("intValue", int32(2))
	subRecord1.Set("fruits", "pear")
	subRecords[1] = subRecord1

	datum.Set("rec", subRecords)
	return datum
}

func runConsumer(broker string, schemaRegistry string, topic string, group string){
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewDeserializingConsumer(kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          group,
		"auto.offset.reset": "earliest",
		"schema.registry.url": schemaRegistry},
		&kafka.AbstractSerializer{}, avro.NewGenericDeserializer())

	if err != nil {
		log.Println(err)
	}
	fmt.Println(group)
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