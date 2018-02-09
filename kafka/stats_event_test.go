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
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Debug function in order to find the error faster
func checkEqualityDeep(t *testing.T, o1, o2 interface{}, stack string) {
	for k, v := range o1.(map[string]interface{}) {
		switch v2 := v.(type) {
		case []interface{}:
			for i := range v2 {
				checkEqualityDeep(t, v2[i], o2.(map[string]interface{})[k].([]interface{})[i], stack+"["+strconv.Itoa(i)+"]")
			}
		case map[string]interface{}:
			checkEqualityDeep(t, v, o2.(map[string]interface{})[k], stack+"."+k)
		default:
			if !reflect.DeepEqual(v, o2.(map[string]interface{})[k]) {
				t.Logf("Unqual %s: org: %v parsed: %v", stack+"."+k, v, o2.(map[string]interface{})[k])
			}
		}
	}
}

// handleStatsEvent checks for stats event and signals on statsReceived
func handleStatsEvent(t *testing.T, eventCh chan Event, statsReceived chan bool) {
	for ev := range eventCh {
		switch e := ev.(type) {
		case *Stats:
			t.Logf("Stats: %v", e)

			// test if the stats string can be decoded into JSON
			stats, err := e.Parse() // convert string to json
			if err != nil {
				t.Fatalf("json unmarshall error: %s", err)
			}
			t.Logf("Stats['name']: %s", stats.Name)

			// test for json equality
			str, err := json.Marshal(stats)
			if err != nil {
				t.Fatalf("json marshall error: %s", err)
			}

			var o1 interface{}
			var o2 interface{}
			err = json.Unmarshal([]byte(e.String()), &o1)
			if err != nil {
				t.Fatalf("json unmarshall raw error: %s", err)
			}
			err = json.Unmarshal([]byte(str), &o2)
			if err != nil {
				t.Fatalf("json unmarshall raw2 error: %s", err)
			}

			// workarounds to make this test pass
			// cgrp might be omited by librdkafka, so remove it too
			if _, ok := o1.(map[string]interface{})["cgrp"]; !ok {
				delete(o2.(map[string]interface{}), "cgrp")
			}
			// correct that spelling mistake
			for _, v := range o1.(map[string]interface{})["topics"].(map[string]interface{}) {
				for _, v2 := range v.(map[string]interface{})["partitions"].(map[string]interface{}) {
					delete(v2.(map[string]interface{}), "commited_offset")
				}
			}
			if !reflect.DeepEqual(o1, o2) {
				// print whats actually wrong
				checkEqualityDeep(t, o1, o2, "")
				t.Fatalf("parsed json is unequal to original json")
			}

			close(statsReceived)
			return
		default:
			t.Logf("Ignored event: %v", e)
		}
	}
}

// TestStatsEventProducerFunc dry-test stats event, no broker is needed.
func TestStatsEventProducerFunc(t *testing.T) {
	testProducerFunc(t, false, false)
}

func TestStatsEventProducerChannel(t *testing.T) {
	testProducerFunc(t, true, false)
}

func TestStatsEventProducerFuncOnline(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}
	testProducerFunc(t, false, true)
}

func testProducerFunc(t *testing.T, withProducerChannel bool, online bool) {

	var conf ConfigMap
	if online {
		conf = ConfigMap{
			"bootstrap.servers":       testconf.Brokers,
			"statistics.interval.ms":  50,
			"api.version.request":     "true",
			"broker.version.fallback": "0.9.0.1",
			"default.topic.config":    ConfigMap{"acks": 1}}
		conf.updateFromTestconf()
	} else {
		conf = ConfigMap{
			"statistics.interval.ms": 50,
			"socket.timeout.ms":      10,
			"default.topic.config":   ConfigMap{"message.timeout.ms": 10}}
	}

	p, err := NewProducer(&conf)
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer p.Close()

	t.Logf("Producer %s", p)

	var topic1 string
	if online {
		topic1 = testconf.Topic
	} else {
		topic1 = "gotest"
	}

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
	testConsumerFunc(t, false)
}

func TestStatsEventConsumerChannelOnline(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}
	testConsumerFunc(t, true)
}

func testConsumerFunc(t *testing.T, online bool) {
	var conf ConfigMap
	if online {
		conf = ConfigMap{
			"bootstrap.servers":        testconf.Brokers,
			"statistics.interval.ms":   50,
			"go.events.channel.enable": true,
			"group.id":                 testconf.GroupID,
			"session.timeout.ms":       6000,
			"api.version.request":      "true",
			"debug":                    ",",
			"default.topic.config":     ConfigMap{"auto.offset.reset": "earliest"}}
		conf.updateFromTestconf()
	} else {
		conf = ConfigMap{
			"group.id":                 "gotest",
			"statistics.interval.ms":   50,
			"go.events.channel.enable": true,
			"socket.timeout.ms":        10,
			"session.timeout.ms":       10}
	}

	c, err := NewConsumer(&conf)
	if err != nil {
		t.Fatalf("%s", err)
	}

	defer c.Close()

	t.Logf("Consumer %s", c)

	// go routine to check for stats event
	statsReceived := make(chan bool)
	go handleStatsEvent(t, c.Events(), statsReceived)

	if online {
		err = c.Subscribe(testconf.Topic, nil)
	} else {
		err = c.Subscribe("gotest", nil)
	}
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
