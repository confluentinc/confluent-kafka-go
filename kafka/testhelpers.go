package kafka

/**
 * Copyright 2016 Confluent Inc.
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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

/*
#include "select_rdkafka.h"
*/
import "C"

var testconf struct {
	Docker       bool
	Semaphore    bool
	Brokers      string
	Topic        string
	GroupID      string
	PerfMsgCount int
	PerfMsgSize  int
	Config       []string
	conf         ConfigMap
}

const defaulttestconfTopic = "test"
const defaulttestconfGroupID = "testgroup"
const defaulttestconfPerfMsgCount = 2000000
const defaulttestconfPerfMsgSize = 100

var defaulttestconfConfig = [1]string{"api.version.request=true"}

const defaulttestconfBrokers = "localhost:9092"

// flag for semaphore job
var semaphoreJob = flag.Bool("clients.semaphore", false, "Tells if the job is running on Semaphore")

// Command line flags accepted by tests
var usingDocker = flag.Bool("clients.docker", false, "Decides whether a docker container be brought up automatically")

// testconfSetup does checks if will be bringing up containers for testing
// automatically, or if we will be using the bootstrap servers from the
// testconf file.
func testconfInit() {
	if (usingDocker != nil) && (*usingDocker) {
		testconf.Docker = true
	}
	if (semaphoreJob != nil) && (*semaphoreJob) {
		testconf.Semaphore = true
	}
}

// testconf_read reads the test suite config file testconf.json which must
// contain at least Brokers and Topic string properties or the defaults will be used.
// Returns true if the testconf was found and usable, false if no such file, or panics
// if the file format is wrong.
func testconfRead() bool {

	// Default values
	testconf.PerfMsgCount = defaulttestconfPerfMsgCount
	testconf.PerfMsgSize = defaulttestconfPerfMsgSize
	testconf.GroupID = defaulttestconfGroupID
	testconf.Topic = defaulttestconfTopic
	testconf.Brokers = ""

	if testconf.Docker || testconf.Semaphore {
		testconf.Brokers = defaulttestconfBrokers
		return true
	}

	cf, err := os.Open("./testconf.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%% testconf.json not found and docker compose not setup - ignoring test\n")
		return false
	}
	jp := json.NewDecoder(cf)
	err = jp.Decode(&testconf)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse testconf: %s", err))
	}

	cf.Close()

	if testconf.Brokers == "" {
		fmt.Fprintf(os.Stderr, "No Brokers provided in testconf")
		return false
	}

	if testconf.Brokers[0] == '$' {
		testconf.Brokers = os.Getenv(testconf.Brokers[1:])
	}

	return true
}

// update existing ConfigMap with key=value pairs from testconf.SerializerConfig
func (cm *ConfigMap) updateFromTestconf() error {
	if testconf.Config == nil {
		return nil
	}

	// Translate "key=value" pairs in SerializerConfig to ConfigMap
	for _, s := range testconf.Config {
		err := cm.Set(s)
		if err != nil {
			return err
		}
	}

	return nil

}

// Return the number of messages available in all partitions of a topic.
// WARNING: This uses watermark offsets so it will be incorrect for compacted topics.
func getMessageCountInTopic(topic string) (int, error) {

	// Create consumer
	config := &ConfigMap{"bootstrap.servers": testconf.Brokers,
		"group.id": testconf.GroupID}
	config.updateFromTestconf()

	c, err := NewConsumer(config)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	// get metadata for the topic to find out number of partitions

	metadata, err := c.GetMetadata(&topic, false, 5*1000)
	if err != nil {
		return 0, err
	}

	t, ok := metadata.Topics[topic]
	if !ok {
		return 0, newError(C.RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
	}

	cnt := 0
	for _, p := range t.Partitions {
		low, high, err := c.QueryWatermarkOffsets(topic, p.ID, 5*1000)
		if err != nil {
			continue
		}
		cnt += int(high - low)
	}

	return cnt, nil
}

// getBrokerList returns a list of brokers (ids) in the cluster
func getBrokerList(H Handle) (brokers []int32, err error) {
	md, err := getMetadata(H, nil, true, 15*1000)
	if err != nil {
		return nil, err
	}

	brokers = make([]int32, len(md.Brokers))
	for i, mdBroker := range md.Brokers {
		brokers[i] = mdBroker.ID
	}

	return brokers, nil
}

// waitTopicInMetadata waits for the given topic to show up in metadata
func waitTopicInMetadata(H Handle, topic string, timeoutMs int) error {
	d, _ := time.ParseDuration(fmt.Sprintf("%dms", timeoutMs))
	tEnd := time.Now().Add(d)

	for {
		remain := tEnd.Sub(time.Now()).Seconds()
		if remain < 0.0 {
			return newErrorFromString(ErrTimedOut,
				fmt.Sprintf("Timed out waiting for topic %s to appear in metadata", topic))
		}

		md, err := getMetadata(H, nil, true, int(remain*1000))
		if err != nil {
			return err
		}

		for _, t := range md.Topics {
			if t.Topic != topic {
				continue
			}
			if t.Error.Code() != ErrNoError || len(t.Partitions) < 1 {
				continue
			}
			// Proper topic found in metadata
			return nil
		}

		time.Sleep(500 * 1000) // 500ms
	}

}

func createAdminClient(t *testing.T) (a *AdminClient) {
	numver, strver := LibraryVersion()
	if numver < 0x000b0500 {
		t.Skipf("Requires librdkafka >=0.11.5 (currently on %s, 0x%x)", strver, numver)
	}

	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers}
	conf.updateFromTestconf()

	/*
	 * Create producer and produce a couple of messages with and without
	 * headers.
	 */
	a, err := NewAdminClient(&conf)
	if err != nil {
		t.Fatalf("NewAdminClient: %v", err)
	}

	return a
}

func createTestTopic(t *testing.T, suffix string, numPartitions int, replicationFactor int) string {
	rand.Seed(time.Now().Unix())

	topic := fmt.Sprintf("%s-%s-%d", testconf.Topic, suffix, rand.Intn(100000))

	a := createAdminClient(t)
	defer a.Close()

	newTopics := []TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		},
	}

	maxDuration, err := time.ParseDuration("30s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()
	result, err := a.CreateTopics(ctx, newTopics, nil)
	if err != nil {
		t.Fatalf("CreateTopics() failed: %s", err)
	}

	for _, res := range result {
		if res.Error.Code() != ErrNoError {
			t.Errorf("Failed to create topic %s: %s\n",
				res.Topic, res.Error)
			continue
		}

	}

	return topic
}
