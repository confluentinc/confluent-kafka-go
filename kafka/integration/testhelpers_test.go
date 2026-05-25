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

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var testconf struct {
	DockerNeeded  bool
	DockerExists  bool
	Brokers       string
	BrokersSasl   string
	SaslUsername  string
	SaslPassword  string
	SaslMechanism string
	TopicName     string
	GroupID       string
	PerfMsgCount  int
	PerfMsgSize   int
	Config        []string
	conf          kafka.ConfigMap
}

const defaulttestconfTopicName = "test"
const defaulttestconfGroupID = "testgroup"
const defaulttestconfPerfMsgCount = 2000000
const defaulttestconfPerfMsgSize = 100

var defaulttestconfConfig = [1]string{"api.version.request=true"}

const defaulttestconfBrokers = "localhost:9092"
const defaulttestconfBrokersSasl = "localhost:9093"
const defaultSaslUsername = "testuser"
const defaultSaslPassword = "testpass"
const defaultSaslMechanism = "PLAIN"

// Docker cluster already exists, don't bring up automatically
var dockerExists = flag.Bool("docker.exists", false, "Docker cluster already exists, don't bring up automatically")

// Docker is needed for these tests
var dockerNeeded = flag.Bool("docker.needed", false, "Docker is needed for this test")

// testNewConsumer creates a new consumer with passed conf
// and global test configuration applied.
func testNewConsumer(t *testing.T, conf *kafka.ConfigMap) (*kafka.Consumer, error) {
	groupProtocol, found := testConsumerGroupProtocol()
	if found {
		conf.Set("group.protocol=" + groupProtocol)
	}
	// Strip classic-only properties if we are not in classic mode.
	if !testConsumerGroupProtocolClassic() {
		forbiddenProperties := []string{
			"session.timeout.ms",
			"partition.assignment.strategy",
			"heartbeat.interval.ms",
			"group.protocol.type"}
		for _, prop := range forbiddenProperties {
			if _, ok := (*conf)[prop]; !ok {
				continue
			}
			t.Logf(
				"Skipping setting forbidden configuration property \"%s\" for CONSUMER protocol",
				prop)
			delete(*conf, prop)
		}
	}
	return kafka.NewConsumer(conf)
}

// testConsumerGroupProtocol returns the value of the
// TEST_CONSUMER_GROUP_PROTOCOL environment variable.
func testConsumerGroupProtocol() (string, bool) {
	return os.LookupEnv("TEST_CONSUMER_GROUP_PROTOCOL")
}

// testConsumerGroupProtocolClassic returns true
// if the TEST_CONSUMER_GROUP_PROTOCOL environment variable
// is unset or equal to "classic"
func testConsumerGroupProtocolClassic() bool {
	groupProtocol, found := testConsumerGroupProtocol()
	if !found {
		return true
	}

	return "classic" == groupProtocol
}

// testconfInit does checks if will be bringing up containers for testing
// automatically, or if we will be using the bootstrap servers from the
// testconf file.
func testconfInit() {
	if (dockerNeeded != nil) && (*dockerNeeded) {
		testconf.DockerNeeded = true
	}
	if (dockerExists != nil) && (*dockerExists) {
		testconf.DockerExists = true
	}
}

// testconfRead reads the test suite config file testconf.json which must
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
	testconf.BrokersSasl = ""

	if testconf.DockerNeeded || testconf.DockerExists {
		testconf.Brokers = defaulttestconfBrokers
		testconf.BrokersSasl = defaulttestconfBrokersSasl
		testconf.SaslUsername = defaultSaslUsername
		testconf.SaslPassword = defaultSaslPassword
		testconf.SaslMechanism = defaultSaslMechanism
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

	if len(testconf.BrokersSasl) > 0 && testconf.BrokersSasl[0] == '$' {
		testconf.BrokersSasl = os.Getenv(testconf.BrokersSasl[1:])
	}

	return true
}

// applyTestconf updates an existing ConfigMap with key=value pairs from
// testconf.Config. Replaces the package-private (*ConfigMap).updateFromTestconf()
// method from the parent kafka package, which is unreachable from this module.
func applyTestconf(cm *kafka.ConfigMap) error {
	if testconf.Config == nil {
		return nil
	}

	for _, s := range testconf.Config {
		if err := cm.Set(s); err != nil {
			return err
		}
	}

	return nil
}

// applySaslAuth updates an existing ConfigMap with SASL settings derived from
// testconf. Replaces the package-private (*ConfigMap).updateToSaslAuthentication()
// method from the parent kafka package.
func applySaslAuth(cm *kafka.ConfigMap) error {
	if testconf.BrokersSasl == "" {
		return errors.New("BrokersSasl must be set in test config")
	}
	if len(testconf.SaslMechanism) == 0 {
		return errors.New("SaslMechanism must be set in test config")
	}
	if len(testconf.SaslPassword) == 0 {
		return errors.New("SaslPassword must be set in test config")
	}
	if len(testconf.SaslUsername) == 0 {
		return errors.New("SaslUsername must be set in test config")
	}

	cm.SetKey("bootstrap.servers", testconf.BrokersSasl)
	cm.SetKey("sasl.username", testconf.SaslUsername)
	cm.SetKey("sasl.password", testconf.SaslPassword)
	cm.SetKey("sasl.mechanisms", testconf.SaslMechanism)
	cm.SetKey("security.protocol", "SASL_PLAINTEXT")

	return nil
}

// getMessageCountInTopic returns the number of messages available in all
// partitions of a topic.
// WARNING: This uses watermark offsets so it will be incorrect for compacted topics.
func getMessageCountInTopic(topic string) (int, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": testconf.Brokers,
		"group.id":          testconf.GroupID,
	}
	if err := applyTestconf(config); err != nil {
		return 0, err
	}

	c, err := kafka.NewConsumer(config)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	metadata, err := c.GetMetadata(&topic, false, 5*1000)
	if err != nil {
		return 0, err
	}

	t, ok := metadata.Topics[topic]
	if !ok {
		return 0, kafka.NewError(kafka.ErrUnknownTopic, "", false)
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

// getBrokerList returns a list of broker ids in the cluster.
// Refactored from the parent-package helper to take a concrete *AdminClient
// (originally took the private Handle interface and called private getMetadata).
func getBrokerList(a *kafka.AdminClient) (brokers []int32, err error) {
	md, err := a.GetMetadata(nil, true, 15*1000)
	if err != nil {
		return nil, err
	}

	brokers = make([]int32, len(md.Brokers))
	for i, mdBroker := range md.Brokers {
		brokers[i] = mdBroker.ID
	}

	return brokers, nil
}

// waitTopicInMetadata waits for the given topic to show up in metadata.
// Refactored from the parent-package helper to take a concrete *AdminClient.
func waitTopicInMetadata(a *kafka.AdminClient, topic string, timeoutMs int) error {
	d, _ := time.ParseDuration(fmt.Sprintf("%dms", timeoutMs))
	tEnd := time.Now().Add(d)

	for {
		remain := tEnd.Sub(time.Now()).Seconds()
		if remain < 0.0 {
			return kafka.NewError(kafka.ErrTimedOut,
				fmt.Sprintf("Timed out waiting for topic %s to appear in metadata", topic),
				false)
		}

		md, err := a.GetMetadata(nil, true, int(remain*1000))
		if err != nil {
			return err
		}

		for _, t := range md.Topics {
			if t.Topic != topic {
				continue
			}
			if t.Error.Code() != kafka.ErrNoError || len(t.Partitions) < 1 {
				continue
			}
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}
}

// createAdminClientImpl is the implementation for createAdminClient and
// createAdminClientWithSasl. It creates a new admin client, or skips the test
// in case it can't be created.
func createAdminClientImpl(t *testing.T, withSasl bool) (a *kafka.AdminClient) {
	numver, strver := kafka.LibraryVersion()
	if numver < 0x000b0500 {
		t.Skipf("Requires librdkafka >=0.11.5 (currently on %s, 0x%x)", strver, numver)
	}

	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	conf := kafka.ConfigMap{"bootstrap.servers": testconf.Brokers}
	applyTestconf(&conf)
	if withSasl {
		if err := applySaslAuth(&conf); err != nil {
			t.Skipf("Test requires SASL Authentication, but failed to set it up: %s", err)
			return
		}
	}

	a, err := kafka.NewAdminClient(&conf)
	if err != nil {
		t.Fatalf("NewAdminClient: %v", err)
	}

	return a
}

func createAdminClient(t *testing.T) (a *kafka.AdminClient) {
	return createAdminClientImpl(t, false)
}

func createAdminClientWithSasl(t *testing.T) (a *kafka.AdminClient) {
	return createAdminClientImpl(t, true)
}

func createTestTopic(t *testing.T, suffix string, numPartitions int, replicationFactor int) string {
	rand.Seed(time.Now().Unix())

	topic := fmt.Sprintf("%s-%s-%d", testconf.TopicName, suffix, rand.Intn(100000))

	a := createAdminClient(t)
	defer a.Close()

	newTopics := []kafka.TopicSpecification{
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
		if res.Error.Code() != kafka.ErrNoError {
			t.Errorf("Failed to create topic %s: %s\n",
				res.Topic, res.Error)
			continue
		}
	}

	return topic
}
