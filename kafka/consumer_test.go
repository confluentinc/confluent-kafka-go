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
	"reflect"
	"sort"
	"testing"
)

// TestConsumerAPIs dry-tests most Consumer APIs, no broker is needed.
func TestConsumerAPIs(t *testing.T) {

	c, err := NewConsumer(&ConfigMap{})
	if err == nil {
		t.Fatalf("Expected NewConsumer() to fail without group.id")
	}

	c, err = NewConsumer(&ConfigMap{
		"group.id":           "gotest",
		"socket.timeout.ms":  10,
		"session.timeout.ms": 10})
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
		if toppar.Error.(Error).Code() == ErrUnknownPartition {
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

	err = c.Unassign()
	if err != nil {
		t.Errorf("Unassign failed: %s", err)
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
