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

	topic1 := "gotest1"
	topic2 := "gotest2"
	err = c.Assign([]TopicPartition{{Topic: &topic1, Partition: 2},
		{Topic: &topic2, Partition: 1}})
	if err != nil {
		t.Errorf("Assign failed: %s", err)
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
