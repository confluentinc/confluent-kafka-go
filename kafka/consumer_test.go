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
	"sync"
	"sync/atomic"
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

	// test StoreMessage doesn't fail either
	stored, err = c.StoreMessage(&Message{TopicPartition: TopicPartition{Topic: &topic, Partition: 0, Offset: 1}})
	if err != nil && err.(Error).Code() != ErrUnknownPartition {
		t.Errorf("StoreMessage() failed: %s", err)
		toppar := stored[0]
		if toppar.Error != nil && toppar.Error.(Error).Code() == ErrUnknownPartition {
			t.Errorf("StoreMessage() TopicPartition error: %s", toppar.Error)
		}
	}

	topic1 := "gotest1"
	topic2 := "gotest2"
	err = c.Assign([]TopicPartition{{Topic: &topic1, Partition: 2},
		{Topic: &topic2, Partition: 1}})
	if err != nil {
		t.Errorf("Assign failed: %s", err)
	}

	// We provide a very small timeout for Seek, to test that the timeout is
	// ignored.
	err = c.Seek(TopicPartition{Topic: &topic1, Partition: 2, Offset: -1}, 1)
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

	// Incremental Assign & Unassign
	err = c.IncrementalAssign([]TopicPartition{
		{Topic: &topic1, Partition: 9, Offset: 1},
		{Topic: &topic2, Partition: 40, Offset: OffsetEnd},
		{Topic: &topic1, Partition: 10, Offset: OffsetInvalid},
		{Topic: &topic2, Partition: 30},
	})
	if err != nil {
		t.Errorf("IncrementalAssign failed: %s", err)
	}

	err = c.IncrementalUnassign([]TopicPartition{
		{Topic: &topic2, Partition: 30},
		{Topic: &topic2, Partition: 40},
		{Topic: &topic1, Partition: 10},
	})
	if err != nil {
		t.Errorf("IncrementalUnassign failed: %s", err)
	}

	assignment, err := c.Assignment()
	if err != nil {
		t.Errorf("Assignment (after incremental) failed: %s", err)
	}

	t.Logf("(Incremental) Assignment: %s\n", assignment)
	if len(assignment) != 1 ||
		*assignment[0].Topic != topic1 ||
		assignment[0].Partition != 9 {
		t.Errorf("(Incremental) Assignment mismatch: %v", assignment)
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

	// Test timeouts using ReadMessage.
	msg, err := c.ReadMessage(time.Millisecond)
	t.Logf("ReadMessage() returned message %s and error %s\n", msg, err)

	// Check both ErrTimedOut and IsTimeout() to ensure they're consistent.
	if err == nil || !err.(Error).IsTimeout() {
		t.Errorf("ReadMessage() should time out, instead got %s\n", err)
	}
	if err == nil || err.(Error).Code() != ErrTimedOut {
		t.Errorf("ReadMessage() should time out, instead got %s\n", err)
	}
	if msg != nil {
		t.Errorf("ReadMessage() should not return a message in case of error\n")
	}

	err = c.Close()
	if err != nil {
		t.Errorf("Close failed: %s", err)
	}

	// TODO: currently it's not expected that any method is called after
	// Close, can be enabled again after merging #901

	// Tests the SetSaslCredentials call to ensure that the API does not crash.
	// c.SetSaslCredentials("username", "password")
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
		if !err.(Error).IsTimeout() {
			t.Errorf("Expected ReadMessage to fail with a timeout error, not %v", err)
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
	// that its SerializerConfig string is identical to myOAuthConfig
	for {
		ev := <-c.Events()
		oatr, ok := ev.(OAuthBearerTokenRefresh)
		if !ok {
			continue
		}

		t.Logf("Got %s with SerializerConfig \"%s\"", oatr, oatr.Config)

		if oatr.Config != myOAuthConfig {
			t.Fatalf("%s: Expected .SerializerConfig to be %s, not %s",
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

func wrapRebalanceCb(assignedEvents *int32, revokedEvents *int32, t *testing.T) func(c *Consumer, event Event) error {
	return func(c *Consumer, event Event) error {
		switch ev := event.(type) {
		case AssignedPartitions:
			atomic.AddInt32(assignedEvents, 1)

			t.Logf("%v, %s rebalance: %d new partition(s) assigned: %v\n",
				c, c.GetRebalanceProtocol(), len(ev.Partitions),
				ev.Partitions)
			err := c.Assign(ev.Partitions)
			if err != nil {
				panic(err)
			}

		case RevokedPartitions:
			atomic.AddInt32(revokedEvents, 1)

			t.Logf("%v, %s rebalance: %d partition(s) revoked: %v\n",
				c, c.GetRebalanceProtocol(), len(ev.Partitions),
				ev.Partitions)
			if c.AssignmentLost() {
				// Our consumer has been kicked out of the group and the
				// entire assignment is thus lost.
				t.Logf("%v, Current assignment lost!\n", c)
			}

			// The client automatically calls Unassign() unless
			// the callback has already called that method.
		}
		return nil
	}
}

func testPoll(c *Consumer, doneChan chan bool, t *testing.T, wg *sync.WaitGroup) {
	defer wg.Done()

	run := true
	for run {
		select {
		case <-doneChan:
			run = false

		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *Message:
				t.Logf("Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					t.Logf("Headers: %v\n", e.Headers)
				}

			case Error:
				// Errors should generally be
				// considered informational, the client
				// will try to automatically recover.
				t.Logf("Error: %v: %v for "+
					"consumer %v\n", e.Code(), e, c)

			default:
				t.Logf("Ignored %v for consumer %v\n",
					e, c)
			}
		}
	}
}

// TestConsumerCloseForStaticMember verifies the rebalance
// for static membership.
// According to KIP-345, the consumer group will not trigger rebalance unless
// 1) A new member joins
// 2) A leader rejoins (possibly due to topic assignment change)
// 3) An existing member offline time is over session timeout
// 4) Broker receives a leave group request containing alistof
//    `group.instance.id`s (details later)
//
// This test uses 3 consumers while each consumer joins after the assignment
// finished for the previous consumers.
// The expected behavior for these consumers are:
// 1) First consumer joins, AssignedPartitions happens. Assign all the
//    partitions to it.
// 2) Second consumer joins, RevokedPartitions happens from the first consumer,
//    then AssignedPartitions happens to both consumers.
// 3) Third consumer joins, RevokedPartitions happens from the previous two
//    consumers, then AssignedPartitions happens to all the three consumers.
// 4) Close the second consumer, revoke its assignments will happen, but it
//    should not notice other consumers.
// 5) Rejoin the second consumer, rebalance should not happen to all the other
//    consumers since it's not the leader, AssignedPartitions only happened
//    to this consumer to assign the partitions.
// 6) Close the third consumer, revoke its assignments will happen, but it
//    should not notice other consumers.
// 7) Close the rejoined consumer, revoke its assignments will happen,
//    but it should not notice other consumers.
// 8) Close the first consumer, revoke its assignments will happen.
//
// The total number of AssignedPartitions for the first consumer is 3,
// and the total number of RevokedPartitions for the first consumer is 3.
// The total number of AssignedPartitions for the second consumer is 2,
// and the total number of RevokedPartitions for the second consumer is 2.
// The total number of AssignedPartitions for the third consumer is 1,
// and the total number of RevokedPartitions for the third consumer is 1.
// The total number of AssignedPartitions for the rejoined consumer
// (originally second consumer) is 1,
// and the total number of RevokedPartitions for the rejoined consumer
// (originally second consumer) is 1.
func TestConsumerCloseForStaticMember(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}
	broker := testconf.Brokers
	topic := createTestTopic(t, "staticMembership", 3, 1)

	var assignedEvents1 int32
	var revokedEvents1 int32

	var assignedEvents2 int32
	var revokedEvents2 int32

	var assignedEvents3 int32
	var revokedEvents3 int32

	var assignedEvents4 int32
	var revokedEvents4 int32

	conf1 := ConfigMap{
		"bootstrap.servers":    broker,
		"group.id":             "rebalance",
		"session.timeout.ms":   "6000",
		"max.poll.interval.ms": "10000",
		"group.instance.id":    "staticmember1",
	}
	c1, err := NewConsumer(&conf1)

	conf2 := ConfigMap{
		"bootstrap.servers":    broker,
		"group.id":             "rebalance",
		"session.timeout.ms":   "6000",
		"max.poll.interval.ms": "10000",
		"group.instance.id":    "staticmember2",
	}
	c2, err := NewConsumer(&conf2)
	if err != nil {
		t.Fatalf("%s", err)
	}

	conf3 := ConfigMap{
		"bootstrap.servers":    broker,
		"group.id":             "rebalance",
		"session.timeout.ms":   "6000",
		"max.poll.interval.ms": "10000",
		"group.instance.id":    "staticmember3",
	}

	c3, err := NewConsumer(&conf3)
	if err != nil {
		t.Fatalf("%s", err)
	}
	wrapRebalancecb1 := wrapRebalanceCb(&assignedEvents1, &revokedEvents1, t)
	err = c1.Subscribe(topic, wrapRebalancecb1)
	if err != nil {
		t.Fatalf("Failed to subscribe to topic %s: %s\n", topic, err)
	}

	wg := sync.WaitGroup{}
	doneChan := make(chan bool, 3)

	wg.Add(1)
	go testPoll(c1, doneChan, t, &wg)
	testConsumerWaitAssignment(c1, t)

	closeChan := make(chan bool)
	wrapRebalancecb2 := wrapRebalanceCb(&assignedEvents2, &revokedEvents2, t)
	err = c2.Subscribe(topic, wrapRebalancecb2)
	if err != nil {
		t.Fatalf("Failed to subscribe to topic %s: %s\n", topic, err)
	}
	wg.Add(1)
	go testPoll(c2, closeChan, t, &wg)
	testConsumerWaitAssignment(c2, t)

	wrapRebalancecb3 := wrapRebalanceCb(&assignedEvents3, &revokedEvents3, t)
	err = c3.Subscribe(topic, wrapRebalancecb3)
	if err != nil {
		t.Fatalf("Failed to subscribe to topic %s: %s\n", topic, err)
	}
	wg.Add(1)
	go testPoll(c3, doneChan, t, &wg)
	testConsumerWaitAssignment(c3, t)

	closeChan <- true
	close(closeChan)
	c2.Close()

	c2, err = NewConsumer(&conf2)
	if err != nil {
		t.Fatalf("%s", err)
	}

	wrapRebalancecb4 := wrapRebalanceCb(&assignedEvents4, &revokedEvents4, t)
	err = c2.Subscribe(topic, wrapRebalancecb4)
	if err != nil {
		t.Fatalf("Failed to subscribe to topic %s: %s\n", topic, err)
	}

	wg.Add(1)
	go testPoll(c2, doneChan, t, &wg)
	testConsumerWaitAssignment(c2, t)

	doneChan <- true
	close(doneChan)

	c3.Close()
	c2.Close()
	c1.Close()

	wg.Wait()

	// Wait 2 * session.timeout.ms to make sure no revokedEvents happens
	time.Sleep(2 * 6000 * time.Millisecond)

	if atomic.LoadInt32(&assignedEvents1) != 3 {
		t.Fatalf("3 assignedEvents are Expected to happen for the first consumer, but %d happened\n",
			atomic.LoadInt32(&assignedEvents1))
	}

	if atomic.LoadInt32(&revokedEvents1) != 3 {
		t.Fatalf("3 revokedEvents are Expected to happen for the first consumer, but %d happened\n",
			atomic.LoadInt32(&revokedEvents1))
	}

	if atomic.LoadInt32(&assignedEvents2) != 2 {
		t.Fatalf("2 assignedEvents are Expected to happen for the second consumer, but %d happened\n",
			atomic.LoadInt32(&assignedEvents2))
	}
	if atomic.LoadInt32(&revokedEvents2) != 2 {
		t.Fatalf("2 revokedEvents is Expected to happen for the second consumer, but %d happened\n",
			atomic.LoadInt32(&revokedEvents2))
	}

	if atomic.LoadInt32(&assignedEvents3) != 1 {
		t.Fatalf("1 assignedEvents is Expected to happen for the third consumer, but %d happened\n",
			atomic.LoadInt32(&assignedEvents3))
	}
	if atomic.LoadInt32(&revokedEvents3) != 1 {
		t.Fatalf("1 revokedEvents is Expected to happen for the third consumer, but %d happened\n",
			atomic.LoadInt32(&revokedEvents3))
	}

	if atomic.LoadInt32(&assignedEvents4) != 1 {
		t.Fatalf("1 assignedEvents is Expected to happen for the rejoined consumer(originally second consumer), but %d happened\n",
			atomic.LoadInt32(&assignedEvents4))
	}
	if atomic.LoadInt32(&revokedEvents4) != 1 {
		t.Fatalf("1 revokedEvents is Expected to happen for the rejoined consumer(originally second consumer), but %d happened\n",
			atomic.LoadInt32(&revokedEvents4))
	}
}

func testConsumerWaitAssignment(c *Consumer, t *testing.T) {
	run := true
	for run {
		assignment, err := c.Assignment()
		if err != nil {
			t.Fatalf("Assignment failed: %s\n", err)
		}

		if len(assignment) != 0 {
			t.Logf("%v Assigned partitions are: %v\n", c, assignment)
			run = false
		}
	}
}
