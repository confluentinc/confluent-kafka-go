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

package main

import (
	"flag"
	"log"
	"time"

	otel "github.com/confluentinc/confluent-kafka-go/instrumentation/otel"
	"github.com/confluentinc/confluent-kafka-go/instrumentation/otel/example"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	brokers = flag.String("brokers", "localhost:9092", "The Kafka bootstrap servers to connect to, as a comma separated list")
)

func main() {
	flag.Parse()

	// Initialize an original Kafka consumer.
	confluentConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  *brokers,
		"group.id":           "example",
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Initialize OpenTelemetry trace provider and wrap the original kafka consumer.
	tracerProvider := example.InitTracer()
	consumer := otel.NewConsumerWithTracing(confluentConsumer, otel.WithTracerProvider(tracerProvider))
	defer func() { _ = consumer.Close() }()

	// Subscribe consumer to topic.
	if err := consumer.Subscribe(example.KafkaTopic, nil); err != nil {
		log.Fatal(err)
	}

	handler := func(consumer *kafka.Consumer, msg *kafka.Message) error {
		println("message received with key: " + string(msg.Key))
		return nil
	}

	// Read one message from the topic.
	_, err = consumer.ReadMessageWithHandler(10*time.Second, handler)
	if err != nil {
		log.Fatal(err)
	}

	// Or you can still use the ReadMessage(timeout) or Poll(timeoutMs) methods but you will not
	// be able to obtain the right handling duration because they will only return the Kafka message to you.
	//
	// msg, err := consumer.ReadMessage(10*time.Second)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	//
	// println("message received with key: " + string(msg.Key))
}
