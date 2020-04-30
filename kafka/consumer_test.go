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
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestConsumerAPIs dry-tests most Consumer APIs, no broker is needed.
func TestConsumerAPIs(t *testing.T) {

	c, err := NewConsumer(&ConfigMap{})
	if err == nil {
		t.Fatalf("Expected NewConsumer() to fail without group.id")
	}

	c, err = NewConsumer(&ConfigMap{
		"group.id":                 "gotest",
		"socket.timeout.ms":        10,
		"session.timeout.ms":       10,
		"enable.auto.offset.store": false, // permit StoreOffsets()
	})
	if err != nil {
		t.Fatalf("%s", err)
	}

	t.Logf("Consumer %s", c)

	err = c.Subscribe("gotest", nil)
	if err != nil {
		t.Errorf("Subscribe failed: %s", err)
	}

	err = c.SubscribeTopics([]string{"gotest1", "gotest2"},
		func(my_c *Consumer, ev Event) error {
			t.Logf("%s", ev)
			return nil
		})
	if err != nil {
		t.Errorf("SubscribeTopics failed: %s", err)
	}

	_, err = c.Commit()
	if err != nil && err.(Error).Code() != ErrNoOffset {
		t.Errorf("Commit() failed: %s", err)
	}

	err = c.Unsubscribe()
	if err != nil {
		t.Errorf("Unsubscribe failed: %s", err)
	}

	topic := "gotest"
	stored, err := c.StoreOffsets([]TopicPartition{{Topic: &topic, Partition: 0, Offset: 1}})
	if err != nil && err.(Error).Code() != ErrUnknownPartition {
		t.Errorf("StoreOffsets() failed: %s", err)
		toppar := stored[0]
		if toppar.Error != nil && toppar.Error.(Error).Code() == ErrUnknownPartition {
			t.Errorf("StoreOffsets() TopicPartition error: %s", toppar.Error)
		}
	}
	var empty []TopicPartition
	stored, err = c.StoreOffsets(empty)
	if err != nil {
		t.Errorf("StoreOffsets(empty) failed: %s", err)
	}

	topic1 := "gotest1"
	topic2 := "gotest2"
	err = c.Assign([]TopicPartition{{Topic: &topic1, Partition: 2},
		{Topic: &topic2, Partition: 1}})
	if err != nil {
		t.Errorf("Assign failed: %s", err)
	}

	err = c.Seek(TopicPartition{Topic: &topic1, Partition: 2, Offset: -1}, 1000)
	if err != nil {
		t.Errorf("Seek failed: %s", err)
	}

	// Pause & Resume
	err = c.Pause([]TopicPartition{{Topic: &topic1, Partition: 2},
		{Topic: &topic2, Partition: 1}})
	if err != nil {
		t.Errorf("Pause failed: %s", err)
	}
	err = c.Resume([]TopicPartition{{Topic: &topic1, Partition: 2},
		{Topic: &topic2, Partition: 1}})
	if err != nil {
		t.Errorf("Resume failed: %s", err)
	}

	err = c.Unassign()
	if err != nil {
		t.Errorf("Unassign failed: %s", err)
	}

	// ConsumerGroupMetadata
	_, err = c.GetConsumerGroupMetadata()
	if err != nil {
		t.Errorf("Expected valid ConsumerGroupMetadata: %v", err)
	}

	_, err = NewTestConsumerGroupMetadata("mygroup")
	if err != nil {
		t.Errorf("Expected valid ConsumerGroupMetadata: %v", err)
	}

	topic = "mytopic"
	// OffsetsForTimes
	offsets, err := c.OffsetsForTimes([]TopicPartition{{Topic: &topic, Offset: 12345}}, 100)
	t.Logf("OffsetsForTimes() returned Offsets %s and error %s\n", offsets, err)
	if err == nil {
		t.Errorf("OffsetsForTimes() should have failed\n")
	}
	if offsets != nil {
		t.Errorf("OffsetsForTimes() failed but returned non-nil Offsets: %s\n", offsets)
	}

	// Position
	offsets, err = c.Position([]TopicPartition{
		{Topic: &topic, Partition: 10},
		{Topic: &topic, Partition: 5},
	})
	t.Logf("Position() returned Offsets %s and error %v\n", offsets, err)
	if err != nil {
		t.Errorf("Position() should not have failed\n")
	}
	if offsets == nil {
		t.Errorf("Position() should not have returned nil\n")
	}

	// Committed
	offsets, err = c.Committed([]TopicPartition{{Topic: &topic, Partition: 5}}, 10)
	t.Logf("Committed() returned Offsets %s and error %s\n", offsets, err)
	if err == nil {
		t.Errorf("Committed() should have failed\n")
	}
	if offsets != nil {
		t.Errorf("Committed() failed but returned non-nil Offsets: %s\n", offsets)
	}

	err = c.Close()
	if err != nil {
		t.Errorf("Close failed: %s", err)
	}
}

func TestConsumerSubscription(t *testing.T) {
	c, err := NewConsumer(&ConfigMap{"group.id": "gotest"})
	if err != nil {
		t.Fatalf("%s", err)
	}

	topics := []string{"gotest1", "gotest2", "gotest3"}
	sort.Strings(topics)

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		t.Fatalf("SubscribeTopics failed: %s", err)
	}

	subscription, err := c.Subscription()
	if err != nil {
		t.Fatalf("Subscription() failed: %s", err)
	}

	sort.Strings(subscription)

	t.Logf("Compare Subscription %v to original list of topics %v\n",
		subscription, topics)

	r := reflect.DeepEqual(topics, subscription)
	if r != true {
		t.Fatalf("Subscription() %v does not match original topics %v",
			subscription, topics)
	}
	c.Close()
}

func TestConsumerAssignment(t *testing.T) {
	c, err := NewConsumer(&ConfigMap{"group.id": "gotest"})
	if err != nil {
		t.Fatalf("%s", err)
	}

	topic0 := "topic0"
	topic1 := "topic1"
	partitions := TopicPartitions{
		{Topic: &topic1, Partition: 1},
		{Topic: &topic1, Partition: 3},
		{Topic: &topic0, Partition: 2}}
	sort.Sort(partitions)

	err = c.Assign(partitions)
	if err != nil {
		t.Fatalf("Assign failed: %s", err)
	}

	assignment, err := c.Assignment()
	if err != nil {
		t.Fatalf("Assignment() failed: %s", err)
	}

	sort.Sort(TopicPartitions(assignment))

	t.Logf("Compare Assignment %v to original list of partitions %v\n",
		assignment, partitions)

	// Use Logf instead of Errorf for timeout-checking errors on CI builds
	// since CI environments are unreliable timing-wise.
	tmoutFunc := t.Errorf
	_, onCi := os.LookupEnv("CI")
	if onCi {
		tmoutFunc = t.Logf
	}

	// Test ReadMessage()
	for _, tmout := range []time.Duration{0, 200 * time.Millisecond} {
		start := time.Now()
		m, err := c.ReadMessage(tmout)
		duration := time.Since(start)

		t.Logf("ReadMessage(%v) ret %v and %v in %v", tmout, m, err, duration)
		if m != nil || err == nil {
			t.Errorf("Expected ReadMessage to fail: %v, %v", m, err)
		}
		if err.(Error).Code() != ErrTimedOut {
			t.Errorf("Expected ReadMessage to fail with ErrTimedOut, not %v", err)
		}

		if tmout == 0 {
			if duration.Seconds() > 0.1 {
				tmoutFunc("Expected ReadMessage(%v) to fail after max 100ms, not %v", tmout, duration)
			}
		} else if tmout > 0 {
			if duration.Seconds() < tmout.Seconds()*0.75 || duration.Seconds() > tmout.Seconds()*1.25 {
				tmoutFunc("Expected ReadMessage() to fail after %v -+25%%, not %v", tmout, duration)
			}
		}
	}

	// reflect.DeepEqual() can't be used since TopicPartition.Topic
	// is a pointer to a string rather than a string and the pointer
	// will differ between partitions and assignment.
	// Instead do a simple stringification + string compare.
	if fmt.Sprintf("%v", assignment) != fmt.Sprintf("%v", partitions) {
		t.Fatalf("Assignment() %v does not match original partitions %v",
			assignment, partitions)
	}
	c.Close()
}

func TestConsumerOAuthBearerConfig(t *testing.T) {
	myOAuthConfig := "scope=myscope principal=gotest"

	c, err := NewConsumer(&ConfigMap{
		"group.id":                 "test",
		"security.protocol":        "SASL_PLAINTEXT",
		"go.events.channel.enable": true,
		"sasl.mechanisms":          "OAUTHBEARER",
		"sasl.oauthbearer.config":  myOAuthConfig,
	})
	if err != nil {
		t.Fatalf("NewConsumer failed: %s", err)
	}

	// Wait for initial OAuthBearerTokenRefresh and check
	// that its Config string is identical to myOAuthConfig
	for {
		ev := <-c.Events()
		oatr, ok := ev.(OAuthBearerTokenRefresh)
		if !ok {
			continue
		}

		t.Logf("Got %s with Config \"%s\"", oatr, oatr.Config)

		if oatr.Config != myOAuthConfig {
			t.Fatalf("%s: Expected .Config to be %s, not %s",
				oatr, myOAuthConfig, oatr.Config)
		}

		// Verify that we can set a token
		err = c.SetOAuthBearerToken(OAuthBearerToken{
			TokenValue: "aaaa",
			Expiration: time.Now().Add(time.Second * time.Duration(60)),
			Principal:  "gotest",
		})
		if err != nil {
			t.Fatalf("Failed to set token: %s", err)
		}

		// Verify that we can set a token refresh failure
		err = c.SetOAuthBearerTokenFailure("A token failure test")
		if err != nil {
			t.Fatalf("Failed to set token failure: %s", err)
		}

		break
	}

	c.Close()
}

func TestConsumerLog(t *testing.T) {
	logsChan := make(chan LogEvent, 1000)

	c, err := NewConsumer(&ConfigMap{
		"debug":                  "all",
		"go.logs.channel.enable": true,
		"go.logs.channel":        logsChan,
		"group.id":               "gotest"})
	if err != nil {
		t.Fatalf("%s", err)
	}

	if c.Logs() != logsChan {
		t.Fatalf("Expected c.Logs() %v == logsChan %v", c.Logs(), logsChan)
	}

	expectedLogs := map[struct {
		tag     string
		message string
	}]bool{
		{"MEMBERID", "gotest"}:  false,
		{"CGRPSTATE", "gotest"}: false,
		{"CGRPQUERY", "gotest"}: false,
	}

	go func() {
		for {
			select {
			case log, ok := <-logsChan:
				if !ok {
					return
				}

				for expectedLog, found := range expectedLogs {
					if found {
						continue
					}
					if log.Tag != expectedLog.tag {
						continue
					}
					if strings.Contains(log.Message, expectedLog.message) {
						expectedLogs[expectedLog] = true
					}
				}
			}
		}
	}()

	<-time.After(time.Second * 3)

	if err := c.Close(); err != nil {
		t.Fatal("Failed to close consumer.")
	}

	for expectedLog, found := range expectedLogs {
		if !found {
			t.Errorf(
				"Expected to find log with tag `%s' and message containing `%s',"+
					" but didn't find any.",
				expectedLog.tag,
				expectedLog.message)
		}
	}
}

func TestReadFromPartition(t *testing.T) {
	c, err := NewConsumer(&ConfigMap{
		"group.id":                             "gotest",
		"socket.timeout.ms":                    10,
		"session.timeout.ms":                   10,
		"enable.auto.offset.store":             false, // permit StoreOffsets()
		"go.enable.read.from.partition.queues": true,
		"go.application.rebalance.enable":		true,
	})
	if err != nil {
		t.Fatalf("%s", err)
	}

	t.Logf("Consumer %s", c)

	err = c.Subscribe("gotest", nil)
	if err != nil {
		t.Errorf("Subscribe failed: %s", err)
	}

	err = c.SubscribeTopics([]string{"gotest1", "gotest2"},
		func(my_c *Consumer, ev Event) error {
			t.Logf("%s", ev)
			return nil
		})
	if err != nil {
		t.Errorf("SubscribeTopics failed: %s", err)
	}

	_, err = c.Commit()
	if err != nil && err.(Error).Code() != ErrNoOffset {
		t.Errorf("Commit() failed: %s", err)
	}

	err = c.Unsubscribe()
	if err != nil {
		t.Errorf("Unsubscribe failed: %s", err)
	}

	topic := "gotest"
	stored, err := c.StoreOffsets([]TopicPartition{{Topic: &topic, Partition: 0, Offset: 1}})
	if err != nil && err.(Error).Code() != ErrUnknownPartition {
		t.Errorf("StoreOffsets() failed: %s", err)
		toppar := stored[0]
		if toppar.Error != nil && toppar.Error.(Error).Code() == ErrUnknownPartition {
			t.Errorf("StoreOffsets() TopicPartition error: %s", toppar.Error)
		}
	}
	var empty []TopicPartition
	stored, err = c.StoreOffsets(empty)
	if err != nil {
		t.Errorf("StoreOffsets(empty) failed: %s", err)
	}

	topic1 := "gotest1"
	topic2 := "gotest2"
	err = c.Assign([]TopicPartition{{Topic: &topic1, Partition: 2},
		{Topic: &topic2, Partition: 1}})
	if err != nil {
		t.Errorf("Assign failed: %s", err)
	}

	err = c.Seek(TopicPartition{Topic: &topic1, Partition: 2, Offset: -1}, 1000)
	if err != nil {
		t.Errorf("Seek failed: %s", err)
	}

	// Pause & Resume
	err = c.Pause([]TopicPartition{{Topic: &topic1, Partition: 2},
		{Topic: &topic2, Partition: 1}})
	if err != nil {
		t.Errorf("Pause failed: %s", err)
	}
	err = c.Resume([]TopicPartition{{Topic: &topic1, Partition: 2},
		{Topic: &topic2, Partition: 1}})
	if err != nil {
		t.Errorf("Resume failed: %s", err)
	}

	err = c.Unassign()
	if err != nil {
		t.Errorf("Unassign failed: %s", err)
	}

	// ConsumerGroupMetadata
	_, err = c.GetConsumerGroupMetadata()
	if err != nil {
		t.Errorf("Expected valid ConsumerGroupMetadata: %v", err)
	}

	_, err = NewTestConsumerGroupMetadata("mygroup")
	if err != nil {
		t.Errorf("Expected valid ConsumerGroupMetadata: %v", err)
	}

	topic = "mytopic"
	// OffsetsForTimes
	offsets, err := c.OffsetsForTimes([]TopicPartition{{Topic: &topic, Offset: 12345}}, 100)
	t.Logf("OffsetsForTimes() returned Offsets %s and error %s\n", offsets, err)
	if err == nil {
		t.Errorf("OffsetsForTimes() should have failed\n")
	}
	if offsets != nil {
		t.Errorf("OffsetsForTimes() failed but returned non-nil Offsets: %s\n", offsets)
	}

	// Position
	offsets, err = c.Position([]TopicPartition{
		{Topic: &topic, Partition: 10},
		{Topic: &topic, Partition: 5},
	})
	t.Logf("Position() returned Offsets %s and error %v\n", offsets, err)
	if err != nil {
		t.Errorf("Position() should not have failed\n")
	}
	if offsets == nil {
		t.Errorf("Position() should not have returned nil\n")
	}

	// Committed
	offsets, err = c.Committed([]TopicPartition{{Topic: &topic, Partition: 5}}, 10)
	t.Logf("Committed() returned Offsets %s and error %s\n", offsets, err)
	if err == nil {
		t.Errorf("Committed() should have failed\n")
	}
	if offsets != nil {
		t.Errorf("Committed() failed but returned non-nil Offsets: %s\n", offsets)
	}

	err = c.Close()
	if err != nil {
		t.Errorf("Close failed: %s", err)
	}
}
