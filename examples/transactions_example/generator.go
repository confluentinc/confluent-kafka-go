/**
 * Copyright 2020 Confluent Inc.
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

// This is the generator that produces input messages to the traffic light
// processor, each message is a car coming in to an intersection on
// an ingress road.

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"math/rand"
	"sync"
	"time"
)

// Intersections this application will process.
var Intersections = [...]string{
	"Stockholmsvagen,Uppsalavagen",
	"UniversityAvenue,HighStreet",
	"Sveavagen,Kungsgatan",
	"CastroStreet,WestEvelynAvenue",
	"3rdStreet,BryantStreet",
}

// Roads is our very generic per-intersection ingress road names.
var Roads = [...]string{"north", "east", "south", "west"}

// sendIngressCarEvent produces an event for a car approaching an intersection.
func sendIngressCarEvent(producer *kafka.Producer, toppar kafka.TopicPartition) {
	intersection := Intersections[rand.Intn(len(Intersections))]
	road := Roads[rand.Intn(len(Roads))]

	err := producer.Produce(&kafka.Message{
		TopicPartition: toppar,
		Key:            []byte(intersection),
		Value:          []byte(road)},
		nil)
	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrQueueFull {
			// Producer queue is full, skip this event.
			// A proper application should retry the Produce().
			addLog(fmt.Sprintf("Generator: Warning: unable to produce event: %v", err))
			return
		}
		// Treat all other errors as fatal.
		fatal(fmt.Sprintf("Generator: Failed to produce message: %v", err))
	}
}

// generateInputMessages generates a continuous stream of input messages
// for the transactional example.
// The idempotent producer is used to guarantee ordering.
// The message key is the road name and the message value is the number 1
// encoded as a Uvarint.
func generateInputMessages(wg *sync.WaitGroup, termChan chan bool) {
	defer wg.Done()

	doTerm := false
	ticker := time.NewTicker(100 * time.Millisecond)

	config := &kafka.ConfigMap{
		"client.id":              "generator",
		"bootstrap.servers":      brokers,
		"enable.idempotence":     true,
		"go.logs.channel.enable": true,
		"go.logs.channel":        logsChan,
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		fatal(err)
	}

	toppar := kafka.TopicPartition{Topic: &inputTopic, Partition: kafka.PartitionAny}

	addLog(fmt.Sprintf("Generator: producing events to topic %s", inputTopic))

	for !doTerm {
		select {
		case <-ticker.C:
			// Randomize the rate of cars by skipping 20% of ticks.
			if rand.Intn(5) == 0 {
				continue
			}

			sendIngressCarEvent(producer, toppar)

		case e := <-producer.Events():
			// Handle delivery reports
			m, ok := e.(*kafka.Message)
			if !ok {
				addLog(fmt.Sprintf("Generator: Ignoring producer event %v", e))
				continue
			}

			if m.TopicPartition.Error != nil {
				addLog(fmt.Sprintf("Generator: Message delivery failed: %v: ignoring", m.TopicPartition))
				continue
			}

		case <-termChan:
			doTerm = true
		}

	}

	addLog(fmt.Sprintf("Generator: shutting down"))
	producer.Close()
}
