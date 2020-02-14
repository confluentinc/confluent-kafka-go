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

// This is the processor, consuming the input topic of ingress car messages,
// deciding what traffic light to turn green for each handled intersection,
// and emitting the traffic light states to the output topic, all using the
// transactional API.

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sync"
	"time"
)

// The processor's consumer group id.
var processorGroupID = "go-transactions-example-processor"

// intersectionState
type intersectionState struct {
	name        string            // Name of intersection
	partition   int32             // Input partition
	lightState  map[string]string // Current light states, indexed by road
	carsWaiting map[string]int    // Numbers of cars waiting, indexed by road
	carCnt      int               // Total number of cars waiting
	currGreen   string            // Currently green road
	lastChange  time.Time         // Time of last light change
}

// intersectionStates maintains state per intersection
var intersectionStates map[string]*intersectionState

// producers map assigned consumer input partitions to Producer instances.
var producers map[int32]*kafka.Producer

// consumer is the processor consumer
var processorConsumer *kafka.Consumer

// lightStateMsg is the state representation of a traffic light and ingress road
// as sent on the output topic enveloped in intersectionStateMsg.
type lightStateMsg struct {
	Name        string // Name of intersection
	Road        string // Name of ingress road, this in combination with Name identifies a single light
	State       string // red, green
	CarsWaiting int    // Number of cars waiting at this ingress
}

// intersectionStateMsg is the state representation of an intersection
// with its traffic lights as sent on the output topic as JSON.
type intersectionStateMsg struct {
	Name   string                   // Name of intersection
	Lights map[string]lightStateMsg // Per traffic light state
}

func (m lightStateMsg) String() string {
	return fmt.Sprintf("lightState{%s: %s is %s, %d cars waiting}",
		m.Name, m.Road, m.State, m.CarsWaiting)
}

func getIntersectionState(name string, partition int32) *intersectionState {
	istate, found := intersectionStates[name]
	if found {
		return istate
	}

	istate = &intersectionState{
		name:      name,
		partition: partition,
	}

	istate.lightState = make(map[string]string)
	istate.carsWaiting = make(map[string]int)

	for _, light := range Roads {
		istate.lightState[light] = "red"
		istate.carsWaiting[light] = 0
	}

	intersectionStates[name] = istate

	return istate
}

// getNextRoad returns the next road in a cyclical fashion.
func getNextRoad(currRoad string) string {
	for i, road := range Roads {
		if currRoad == road {
			return Roads[(i+1)%len(Roads)]
		}
	}

	return Roads[0]
}

// processIngressCarMessage processes an input message of a car coming
// into an intersection.
func processIngressCarMessage(msg *kafka.Message) {

	if msg.Key == nil || msg.Value == nil {
		// Invalid message, ignore
		return
	}

	intersection := string(msg.Key)
	road := string(msg.Value)

	istate := getIntersectionState(intersection, msg.TopicPartition.Partition)
	_, found := istate.carsWaiting[road]
	if !found {
		addLog(fmt.Sprintf("Processor: %v: unknown road \"%s\" for intersection \"%s\": ignoring",
			msg.TopicPartition, road, istate.name))
		return
	}

	if istate.currGreen != road {
		istate.carsWaiting[road]++
		istate.carCnt++
	}

	// Keep track of which input partition this istate is mapped to
	// so we know which transactional producer to use.
	if istate.partition == kafka.PartitionAny {
		istate.partition = msg.TopicPartition.Partition
	}

}

// electNewGreenLight elects a new green light based and updates
// the intersection state.
// Returns true if the light changed, else false.
func electNewGreenLight(istate *intersectionState) bool {
	if istate.carCnt == 0 {
		// No cars waiting at any roads
		return false
	}

	// Created a weighted map of roads, where the weight
	// is based on the fraction of cars for each ingress road,
	// as well as the fair next road.
	nextRoad := getNextRoad(istate.currGreen)

	weightedRoads := make(map[string]float64)
	weightedRoads[nextRoad] = 1.0
	for road, cnt := range istate.carsWaiting {
		weightedRoads[road] = 1.0 + (float64(cnt) / float64(istate.carCnt))
	}

	// Sorting a map by value is not straight forward in Go,
	// so let's do iteratively instead.
	bestWeight := 0.0
	bestRoad := ""
	for road, weight := range weightedRoads {
		if weight > bestWeight {
			bestWeight = weight
			bestRoad = road
		}
	}

	_, found := weightedRoads[istate.currGreen]
	if found && istate.carsWaiting[istate.currGreen] == istate.carCnt {
		// All cars are already on the already green road, do nothing
		return false
	}

	istate.lastChange = time.Now()
	istate.currGreen = bestRoad

	if istate.currGreen != "" {
		// Let all cars on the green road pass thru
		istate.carCnt -= istate.carsWaiting[istate.currGreen]
		istate.carsWaiting[istate.currGreen] = 0
	}

	return true
}

// Run state machine for a single intersection to update light colors.
// Returns true if output messages were produced, else false.
func intersectionStateMachine(istate *intersectionState) bool {

	changed := false

	// Elect new green light
	if time.Since(istate.lastChange) > 4*time.Second {
		changed = electNewGreenLight(istate)
	}

	// Get the producer for this istate's input partition
	producer := producers[istate.partition]
	if producer == nil {
		fatal(fmt.Sprintf("BUG: No producer for intersection %s partition %v", istate.name, istate.partition))
	}

	// Produce message with current intersection light states.
	isectMsg := intersectionStateMsg{
		Name:   istate.name,
		Lights: make(map[string]lightStateMsg)}

	for road := range istate.lightState {
		if road == istate.currGreen {
			istate.lightState[road] = "green"
		} else {
			istate.lightState[road] = "red"
		}

		isectMsg.Lights[road] = lightStateMsg{
			Name:        istate.name,
			Road:        road,
			State:       istate.lightState[road],
			CarsWaiting: istate.carsWaiting[road]}
	}

	value, err := json.Marshal(isectMsg)
	if err != nil {
		fatal(err)
	}

	err = producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &outputTopic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(istate.name),
			Value: value,
		}, nil)

	if err != nil {
		fatal(fmt.Sprintf("Failed to produce message: %v", err))
	}

	return changed
}

func trafficLightProcessor(wg *sync.WaitGroup, termChan chan bool) {
	defer wg.Done()

	doTerm := false
	ticker := time.NewTicker(500 * time.Millisecond)

	// If punctuate is true the current intersection state is written to
	// the output topic every ticker interval regardless if there was an
	// intersection light state change.
	// Use this to get a more responsive visualization.
	punctuate := true

	intersectionStates = make(map[string]*intersectionState)

	// The per-partition producers are set up in groupRebalance
	producers = make(map[int32]*kafka.Producer)

	consumerConfig := &kafka.ConfigMap{
		"client.id":         "processor",
		"bootstrap.servers": brokers,
		"group.id":          processorGroupID,
		"auto.offset.reset": "earliest",
		// Consumer used for input to a transactional processor
		// must have auto-commits disabled since offsets
		// are committed with the transaction using
		// SendOffsetsToTransaction.
		"enable.auto.commit":     false,
		"go.logs.channel.enable": true,
		"go.logs.channel":        logsChan,
	}

	var err error
	processorConsumer, err = kafka.NewConsumer(consumerConfig)
	if err != nil {
		fatal(err)
	}

	err = processorConsumer.Subscribe(inputTopic, groupRebalance)
	if err != nil {
		fatal(err)
	}

	addLog(fmt.Sprintf("Processor: waiting for messages on topic %s",
		inputTopic))
	for !doTerm {
		select {

		case <-ticker.C:
			// Run intersection state machine(s) periodically
			partitionsToCommit := make(map[int32]bool)
			for _, istate := range intersectionStates {
				if intersectionStateMachine(istate) || punctuate {
					// The state machine wants its transaction committed.
					// The transaction is shared among all intersectionStates
					// that use the same input partition since the input
					// offset that is committed along with the transaction
					// applies to all intersectionStates mapped to
					// that partition.
					partitionsToCommit[istate.partition] = true
				}
			}

			// Commit transactions
			for partition := range partitionsToCommit {
				commitTransactionForInputPartition(partition)
			}

		case <-termChan:
			doTerm = true

		default:
			// Poll consumer for new messages or rebalance events.
			ev := processorConsumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				// Process ingress car event message
				processIngressCarMessage(e)
			case kafka.Error:
				// Errors are generally just informational.
				addLog(fmt.Sprintf("Consumer error: %sn", ev))
			default:
				addLog(fmt.Sprintf("Consumer event: %s: ignored", ev))
			}
		}
	}

	addLog(fmt.Sprintf("Processor: shutting down"))
	processorConsumer.Close()

	for _, producer := range producers {
		producer.AbortTransaction(nil)
		producer.Close()
	}

}
