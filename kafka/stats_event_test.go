/**
 * Copyright 2017 Confluent Inc.
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

package kafka

import (
	"encoding/json"
	"testing"
	"time"
)

// handleStatsEvent checks for stats event and signals on statsReceived
func handleStatsEvent(t *testing.T, eventCh chan Event, statsReceived chan bool) {
	for ev := range eventCh {
		switch e := ev.(type) {
		case *Stats:
			t.Logf("Stats: %v", e)

			// test if the stats string can be decoded into JSON
			var raw map[string]interface{}
			err := json.Unmarshal([]byte(e.String()), &raw) // convert string to json
			if err != nil {
				t.Fatalf("json unmarshall error: %s", err)
			}
			t.Logf("Stats['name']: %s", raw["name"])
			close(statsReceived)
			return
		default:
			t.Logf("Ignored event: %v", e)
		}
	}
}

// TestStatsEventProducerFunc dry-test stats event, no broker is needed.
func TestStatsEventProducerFunc(t *testing.T) {
	testProducerFunc(t, false)
}

func TestStatsEventProducerChannel(t *testing.T) {
	testProducerFunc(t, true)
}

func testProducerFunc(t *testing.T, withProducerChannel bool) {

	p, err := NewProducer(&ConfigMap{
		"statistics.interval.ms": 50,
		"socket.timeout.ms":      10,
		"default.topic.config":   ConfigMap{"message.timeout.ms": 10}})
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer p.Close()

	t.Logf("Producer %s", p)

	topic1 := "gotest"

	// go routine to check for stats event
	statsReceived := make(chan bool)
	go handleStatsEvent(t, p.Events(), statsReceived)

	if withProducerChannel {
		err = p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic1, Partition: 0},
			Value: []byte("Own drChan"), Key: []byte("This is my key")},
			nil)
		if err != nil {
			t.Errorf("Produce failed: %s", err)
		}
	} else {
		p.ProduceChannel() <- &Message{TopicPartition: TopicPartition{Topic: &topic1, Partition: 0},
			Value: []byte("Own drChan"), Key: []byte("This is my key")}

	}

	select {
	case <-statsReceived:
		t.Logf("Stats recevied")
	case <-time.After(time.Second * 3):
		t.Fatalf("Excepted stats but got none")
	}

	return
}

// TestStatsEventConsumerChannel dry-tests stats event for consumer, no broker is needed.
func TestStatsEventConsumerChannel(t *testing.T) {

	c, err := NewConsumer(&ConfigMap{
		"group.id":                 "gotest",
		"statistics.interval.ms":   50,
		"go.events.channel.enable": true,
		"socket.timeout.ms":        10,
		"session.timeout.ms":       10})
	if err != nil {
		t.Fatalf("%s", err)
	}

	defer c.Close()

	t.Logf("Consumer %s", c)

	// go routine to check for stats event
	statsReceived := make(chan bool)
	go handleStatsEvent(t, c.Events(), statsReceived)

	err = c.Subscribe("gotest", nil)
	if err != nil {
		t.Errorf("Subscribe failed: %s", err)
	}

	select {
	case <-statsReceived:
		t.Logf("Stats recevied")
	case <-time.After(time.Second * 3):
		t.Fatalf("Excepted stats but got none")
	}

}
