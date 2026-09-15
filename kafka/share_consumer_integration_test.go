//go:build sharegroup

/**
 * Copyright 2024 Confluent Inc.
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

// Integration tests for the ShareConsumer (Kafka Queues / KIP-932).
//
// These require a broker with share groups enabled (Apache Kafka 4.2.0+). Bring
// one up with:
//
//	docker compose -f integration/testresources/docker-compose-sharegroup.yaml up -d
//
// then run:
//
//	SHARE_GROUP_BROKER=localhost:9092 go test -tags sharegroup ./kafka/ -run Share -v
//
// The tests are skipped unless SHARE_GROUP_BROKER is set.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func shareBroker(t *testing.T) string {
	b := os.Getenv("SHARE_GROUP_BROKER")
	if b == "" {
		t.Skip("SHARE_GROUP_BROKER not set; skipping share group integration test")
	}
	return b
}

// createShareTestTopic creates a fresh topic and returns its name.
func createShareTestTopic(t *testing.T, broker string, partitions int) string {
	t.Helper()
	admin, err := NewAdminClient(&ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		t.Fatalf("NewAdminClient: %s", err)
	}
	defer admin.Close()

	topic := fmt.Sprintf("share-it-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := admin.CreateTopics(ctx, []TopicSpecification{{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}})
	if err != nil {
		t.Fatalf("CreateTopics: %s", err)
	}
	for _, r := range results {
		if r.Error.Code() != ErrNoError {
			t.Fatalf("CreateTopics(%s): %s", r.Topic, r.Error)
		}
	}
	// Give the broker a moment to make the topic fully available.
	time.Sleep(2 * time.Second)
	return topic
}

// setShareGroupEarliest sets the share group's start offset to earliest so a
// share group reads from the beginning of the log regardless of when it joins.
// This makes the tests independent of produce/subscribe ordering.
func setShareGroupEarliest(t *testing.T, broker, groupID string) {
	t.Helper()
	admin, err := NewAdminClient(&ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		t.Fatalf("NewAdminClient: %s", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := admin.IncrementalAlterConfigs(ctx, []ConfigResource{{
		Type: ResourceGroup,
		Name: groupID,
		Config: []ConfigEntry{{
			Name:                 "share.auto.offset.reset",
			Value:                "earliest",
			IncrementalOperation: AlterConfigOpTypeSet,
		}},
	}})
	if err != nil {
		t.Fatalf("IncrementalAlterConfigs(group %s): %s", groupID, err)
	}
	for _, r := range results {
		if r.Error.Code() != ErrNoError {
			t.Fatalf("IncrementalAlterConfigs(group %s): %s", r.Name, r.Error)
		}
	}
}

// produceShareTestMessages produces n messages with keys "0".."n-1".
func produceShareTestMessages(t *testing.T, broker, topic string, n int) {
	t.Helper()
	p, err := NewProducer(&ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		t.Fatalf("NewProducer: %s", err)
	}
	defer p.Close()

	dr := make(chan Event, n)
	for i := 0; i < n; i++ {
		err := p.Produce(&Message{
			TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
			Key:            []byte(fmt.Sprintf("%d", i)),
			Value:          []byte(fmt.Sprintf("value-%d", i)),
		}, dr)
		if err != nil {
			t.Fatalf("Produce: %s", err)
		}
	}
	for i := 0; i < n; i++ {
		ev := <-dr
		m := ev.(*Message)
		if m.TopicPartition.Error != nil {
			t.Fatalf("delivery failed: %s", m.TopicPartition.Error)
		}
	}
	if remaining := p.Flush(15000); remaining > 0 {
		t.Fatalf("Flush: %d messages still queued", remaining)
	}
}

// TestShareConsumerImplicitIntegration produces N messages and consumes them
// with a single implicit-mode share consumer, verifying every key is received.
func TestShareConsumerImplicitIntegration(t *testing.T) {
	broker := shareBroker(t)
	const n = 100

	topic := createShareTestTopic(t, broker, 4)
	groupID := fmt.Sprintf("share-it-implicit-%d", time.Now().UnixNano())
	setShareGroupEarliest(t, broker, groupID)
	produceShareTestMessages(t, broker, topic, n)

	c, err := NewShareConsumer(&ConfigMap{
		"bootstrap.servers":          broker,
		"group.id":                   groupID,
		"share.acknowledgement.mode": "implicit",
	})
	if err != nil {
		t.Fatalf("NewShareConsumer: %s", err)
	}
	defer c.Close()

	if err := c.Subscribe(topic); err != nil {
		t.Fatalf("Subscribe: %s", err)
	}

	seen := map[string]bool{}
	deadline := time.Now().Add(60 * time.Second)
	for len(seen) < n && time.Now().Before(deadline) {
		set, err := c.Poll(1000)
		if err != nil {
			t.Fatalf("Poll: %s", err)
		}
		if set == nil {
			continue
		}
		for _, m := range set.Messages() {
			if m.TopicPartition.Error != nil {
				t.Logf("record error: %s", m.TopicPartition.Error)
				continue
			}
			seen[string(m.Key)] = true
		}
	}

	if len(seen) != n {
		t.Fatalf("implicit share consumer received %d/%d distinct keys", len(seen), n)
	}
	t.Logf("implicit share consumer received all %d keys", n)
}

// TestShareConsumerExplicitAckIntegration produces N messages and consumes them
// cooperatively with two explicit-ack share consumers in the same share group,
// accepting each record and verifying the union covers every key.
func TestShareConsumerExplicitAckIntegration(t *testing.T) {
	broker := shareBroker(t)
	const n = 200

	topic := createShareTestTopic(t, broker, 4)
	groupID := fmt.Sprintf("share-it-explicit-%d", time.Now().UnixNano())
	setShareGroupEarliest(t, broker, groupID)
	produceShareTestMessages(t, broker, topic, n)

	newExplicit := func() *ShareConsumer {
		c, err := NewShareConsumer(&ConfigMap{
			"bootstrap.servers":          broker,
			"group.id":                   groupID,
			"share.acknowledgement.mode": "explicit",
		})
		if err != nil {
			t.Fatalf("NewShareConsumer: %s", err)
		}
		if err := c.Subscribe(topic); err != nil {
			t.Fatalf("Subscribe: %s", err)
		}
		return c
	}

	type result struct {
		keys  map[string]bool
		total int
	}
	results := make(chan result, 2)

	consume := func(c *ShareConsumer) {
		defer c.Close()
		keys := map[string]bool{}
		total := 0
		idle := 0
		for idle < 8 { // stop after ~8s of empty polls
			set, err := c.Poll(1000)
			if err != nil {
				t.Errorf("Poll: %s", err)
				break
			}
			if set == nil {
				idle++
				continue
			}
			idle = 0
			for _, m := range set.Messages() {
				if m.TopicPartition.Error != nil {
					continue
				}
				keys[string(m.Key)] = true
				total++
			}
			if err := set.AcknowledgeAll(ShareAcknowledgeTypeAccept); err != nil {
				t.Errorf("AcknowledgeAll: %s", err)
			}
			if _, err := c.CommitSync(5000); err != nil {
				t.Errorf("CommitSync: %s", err)
			}
		}
		results <- result{keys: keys, total: total}
	}

	c1 := newExplicit()
	c2 := newExplicit()
	go consume(c1)
	go consume(c2)

	union := map[string]bool{}
	grandTotal := 0
	for i := 0; i < 2; i++ {
		r := <-results
		grandTotal += r.total
		for k := range r.keys {
			union[k] = true
		}
	}

	if len(union) != n {
		t.Fatalf("explicit share consumers received %d/%d distinct keys (total records %d)",
			len(union), n, grandTotal)
	}
	t.Logf("two explicit share consumers received all %d keys (total records delivered: %d)",
		n, grandTotal)
}
