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

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

var testconf struct {
	Brokers      string
	TopicName    string
	GroupID      string
	PerfMsgCount int
	PerfMsgSize  int
	Config       []string
	conf         ConfigMap
}

const defaulttestconfTopicName = "test"
const defaulttestconfGroupID = "testgroup"
const defaulttestconfPerfMsgCount = 2000000
const defaulttestconfPerfMsgSize = 100

// ratepdisp tracks and prints message & byte rates
type ratedisp struct {
	name      string
	start     time.Time
	lastPrint time.Time
	every     float64
	cnt       int64
	size      int64
	b         *testing.B
}

// ratedisp_start sets up a new rate displayer
func ratedispStart(b *testing.B, name string, every float64) (pf ratedisp) {
	now := time.Now()
	return ratedisp{name: name, start: now, lastPrint: now, b: b, every: every}
}

// reset start time and counters
func (rd *ratedisp) reset() {
	rd.start = time.Now()
	rd.cnt = 0
	rd.size = 0
}

// print the current (accumulated) rate
func (rd *ratedisp) print(pfx string) {
	elapsed := time.Since(rd.start).Seconds()

	rd.b.Logf("%s: %s%d messages in %fs (%.0f msgs/s), %d bytes (%.3fMb/s)",
		rd.name, pfx, rd.cnt, elapsed, float64(rd.cnt)/elapsed,
		rd.size, (float64(rd.size)/elapsed)/(1024*1024))
}

// tick adds cnt of total size size to the rate displayer and also prints
// running stats every 1s.
func (rd *ratedisp) tick(cnt, size int64) {
	rd.cnt += cnt
	rd.size += size

	if time.Since(rd.lastPrint).Seconds() >= rd.every {
		rd.print("")
		rd.lastPrint = time.Now()
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
	testconf.TopicName = defaulttestconfTopicName
	testconf.Brokers = ""

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
		return 0, NewError(ErrUnknownTopic, "", false)
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

// createAdminClient creates a new admin client, or skips the test in case it
// can't be created.
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

	a, err := NewAdminClient(&conf)
	if err != nil {
		t.Fatalf("NewAdminClient: %v", err)
	}

	return a
}

func createTestTopic(t *testing.T, suffix string, numPartitions int, replicationFactor int) string {
	rand.Seed(time.Now().Unix())

	topic := fmt.Sprintf("%s-%s-%d", testconf.TopicName, suffix, rand.Intn(100000))

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
