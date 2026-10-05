package integration

/**
 * Copyright 2026 Confluent Inc.
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
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var testconf struct {
	DockerNeeded bool
	DockerExists bool
	Brokers      string
	// SchemaRegistryURL is the Schema Registry endpoint under test. When empty
	// the tests are skipped as unconfigured; when set but unreachable they fail.
	SchemaRegistryURL string
	TopicName         string
	Config            []string
}

const defaulttestconfBrokers = "localhost:9092"
const defaulttestconfSchemaRegistryURL = "http://localhost:8081"
const defaulttestconfTopicName = "test"

// Docker cluster already exists, don't bring up automatically
var dockerExists = flag.Bool("docker.exists", false,
	"Docker cluster already exists, don't bring up automatically")

// Docker is needed for these tests
var dockerNeeded = flag.Bool("docker.needed", false, "Docker is needed for this test")

func testconfInit() {
	if (dockerNeeded != nil) && (*dockerNeeded) {
		testconf.DockerNeeded = true
	}
	if (dockerExists != nil) && (*dockerExists) {
		testconf.DockerExists = true
	}
}

// testconfRead reads the test configuration, or returns false if the tests
// cannot run. Either docker flag short-circuits testconf.json and uses the
// endpoints testresources/docker-compose.yaml publishes.
func testconfRead() bool {
	testconf.TopicName = defaulttestconfTopicName
	testconf.Brokers = ""
	testconf.SchemaRegistryURL = ""

	if testconf.DockerNeeded || testconf.DockerExists {
		testconf.Brokers = defaulttestconfBrokers
		testconf.SchemaRegistryURL = defaulttestconfSchemaRegistryURL
		return true
	}

	cf, err := os.Open("./testconf.json")
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"%% testconf.json not found and docker compose not setup - ignoring test\n")
		return false
	}
	defer cf.Close()

	if err := json.NewDecoder(cf).Decode(&testconf); err != nil {
		panic(fmt.Sprintf("Failed to parse testconf: %s", err))
	}

	if testconf.Brokers == "" {
		fmt.Fprintf(os.Stderr, "No Brokers provided in testconf\n")
		return false
	}

	if testconf.Brokers[0] == '$' {
		testconf.Brokers = os.Getenv(testconf.Brokers[1:])
	}

	if len(testconf.SchemaRegistryURL) > 0 && testconf.SchemaRegistryURL[0] == '$' {
		testconf.SchemaRegistryURL = os.Getenv(testconf.SchemaRegistryURL[1:])
	}

	return true
}

// applyTestconf applies the "key=value" overrides from testconf.Config.
func applyTestconf(conf *kafka.ConfigMap) error {
	for _, s := range testconf.Config {
		key, value, found := strings.Cut(s, "=")
		if !found {
			return fmt.Errorf("invalid config property in testconf: %s", s)
		}
		if err := conf.SetKey(key, value); err != nil {
			return err
		}
	}
	return nil
}

// createAdminClient returns an admin client for the configured brokers.
func createAdminClient(t *testing.T) *kafka.AdminClient {
	t.Helper()

	conf := kafka.ConfigMap{"bootstrap.servers": testconf.Brokers}
	if err := applyTestconf(&conf); err != nil {
		t.Fatalf("Failed to update test configuration: %s", err)
	}

	a, err := kafka.NewAdminClient(&conf)
	if err != nil {
		t.Fatalf("Failed to create admin client: %s", err)
	}
	return a
}

// createTestTopic creates a topic with a name unique to this run.
func createTestTopic(t *testing.T, suffix string, numPartitions int, replicationFactor int) string {
	t.Helper()

	topic := fmt.Sprintf("%s-%s-%d", testconf.TopicName, suffix, rand.Intn(100000))

	a := createAdminClient(t)
	defer a.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := a.CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}}, nil)
	if err != nil {
		t.Fatalf("Failed to create topic %s: %s", topic, err)
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			t.Fatalf("Failed to create topic %s: %s", topic, result.Error)
		}
	}

	return topic
}

// waitSchemaRegistryReady polls the Schema Registry at url until it answers a
// listing request, or timeout elapses. Schema Registry only becomes ready once
// it has created and read back its _schemas topic, which takes appreciably
// longer than the broker itself.
func waitSchemaRegistryReady(url string, timeout time.Duration) error {
	client := &http.Client{Timeout: 5 * time.Second}
	subjectsURL := strings.TrimSuffix(url, "/") + "/subjects"
	deadline := time.Now().Add(timeout)

	var lastErr error
	for {
		resp, err := client.Get(subjectsURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("GET %s returned %s", subjectsURL, resp.Status)
		} else {
			lastErr = err
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("schema registry at %s not ready after %v: %w",
				url, timeout, lastErr)
		}
		time.Sleep(time.Second)
	}
}
