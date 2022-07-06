/**
 * Copyright 2020 Confluent Inc.
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

// Integration tests for the transactional producer

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// expectDeliveryReports expects numDrs delivery reports on drChan
func expectDeliveryReports(t *testing.T, drChan chan Event, numDrs int, expectErr bool) {
	for numDrs > 0 {
		ev := <-drChan
		m, ok := ev.(*Message)
		if !ok {
			t.Fatalf("Unexpected event type %t: %v\n", ev, ev)
		}

		if (m.TopicPartition.Error != nil) != expectErr {
			t.Fatalf("Delivery status %v != expectErr %v\n",
				m.TopicPartition, expectErr)
		}

		numDrs--
	}
}

// TestTransactionalProducer verifies the transactional producer in
// different calling combinations.
func TestTransactionalProducer(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	rand.Seed(time.Now().Unix())

	config := &ConfigMap{"bootstrap.servers": testconf.Brokers,
		"transactional.id": fmt.Sprintf("go-txnid-%d", rand.Intn(100000))}
	if err := config.updateFromTestconf(); err != nil {
		t.Fatalf("Failed to update test configuration: %s\n", err)
	}

	producer, err := NewProducer(config)
	if err != nil {
		t.Fatalf("Failed to create Producer client: %s\n", err)
	}

	err = producer.InitTransactions(nil)
	if err != nil {
		t.Fatalf("InitTransactions() failed: %v\n", err)
	}

	// Transactions to run, with options:
	//   produce = produce messages
	//   send = SendOffsetsToTransaction
	//   flush = explicit Flush() prior to Commit/Abort
	//   commit = CommitTransaction
	//   abort = AbortTransaction
	txns := []string{
		"produce,send,commit",
		"produce,flush,send,commit",
		"produce,commit",
		"send,commit",
		"produce,flush,send,abort",
		"produce,abort",
		"send,abort",
	}

	fakedInputTopic := createTestTopic(t, "txnFakedInput", 3, 1)
	outputTopic := createTestTopic(t, "txnOutput", 3, 1)
	const msgCnt int = 10

	for _, how := range txns {
		t.Logf("Running transaction with options %s\n", how)

		err = producer.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction() failed: %v\n", err)
		}

		drChan := make(chan Event, msgCnt)

		if strings.Contains(how, "produce") {
			for i := 0; i < msgCnt; i++ {
				err = producer.Produce(
					&Message{
						TopicPartition: TopicPartition{Topic: &outputTopic,
							Partition: PartitionAny},
						Value: []byte(fmt.Sprintf("value%d", i)),
					}, drChan)
				if err != nil {
					t.Fatalf("Failed to produce message: %v\n", err)
				}
			}
		}

		if strings.Contains(how, "send") {
			cgmd, err := NewTestConsumerGroupMetadata(fmt.Sprintf("%s-group", fakedInputTopic))
			if err != nil {
				t.Fatalf("Failed to create group metadata: %v", err)
			}

			err = producer.SendOffsetsToTransaction(nil,
				[]TopicPartition{
					{Topic: &fakedInputTopic, Partition: 0, Offset: 123},
					{Topic: &fakedInputTopic, Partition: 1, Offset: 456},
				},
				cgmd)
			if err != nil {
				t.Fatalf("Failed to send offsets to transactions: %v\n", err)
			}
		}

		if strings.Contains(how, "flush") {
			remaining := producer.Flush(-1)
			if remaining > 0 {
				t.Fatalf("Flush() failed: %d messages still not delivered\n", remaining)
			}

			if strings.Contains(how, "produce") && strings.Contains(how, "produce") {
				expectDeliveryReports(t, drChan, msgCnt, false)
			}
		}

		if strings.Contains(how, "commit") {
			err = producer.CommitTransaction(nil)
			if err != nil {
				t.Fatalf("CommitTransaction() failed: %v\n", err)
			}
		} else {
			err = producer.AbortTransaction(nil)
			if err != nil {
				t.Fatalf("AbortTransaction() failed: %v\n", err)
			}
		}

		if strings.Contains(how, "produce") && !strings.Contains(how, "flush") {
			// In a proper application this delivery channel
			// must be served before commit/abort, or in
			// a separate Go routine.
			// We're only allowed to do this after Commit/Abort
			// here if the delivery channel buffer size
			// is large enough to never be filled up, which
			// is generally an unknown in the real world.
			expectDeliveryReports(t, drChan, msgCnt,
				strings.Contains(how, "abort"))
		}

	}

	producer.Close()
}

// TestTransactionalSendOffsets verifies that SendOffsetsToTransaction works.
func TestTransactionalSendOffsets(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	rand.Seed(time.Now().Unix())

	// Producer config
	config := &ConfigMap{"bootstrap.servers": testconf.Brokers,
		"transactional.id": fmt.Sprintf("go-txnid-%d", rand.Intn(100000))}
	if err := config.updateFromTestconf(); err != nil {
		t.Fatalf("Failed to update test configuration: %s\n", err)
	}

	// Consumer config
	groupID := fmt.Sprintf("go-txngroup-%d", rand.Intn(10000))
	consumerConfig := &ConfigMap{"bootstrap.servers": testconf.Brokers, "group.id": groupID}

	if err := consumerConfig.updateFromTestconf(); err != nil {
		t.Fatalf("Failed to update test configuration: %s\n", err)
	}

	fakedInputTopic := createTestTopic(t, "txnFakedInput", 3, 1)
	outputTopic := createTestTopic(t, "txnOutput", 3, 1)
	const msgCnt int = 10

	partitions := []TopicPartition{
		{Topic: &fakedInputTopic, Partition: 0},
		{Topic: &fakedInputTopic, Partition: 1},
		{Topic: &fakedInputTopic, Partition: 2},
	}
	goodOffsets := []TopicPartition{
		{Topic: &fakedInputTopic, Partition: 0, Offset: 102030},
		{Topic: &fakedInputTopic, Partition: 1, Offset: OffsetInvalid},
		{Topic: &fakedInputTopic, Partition: 2, Offset: 2},
	}
	badOffsets := []TopicPartition{
		{Topic: &fakedInputTopic, Partition: 0, Offset: 5},
		{Topic: &fakedInputTopic, Partition: 1, Offset: 6},
		{Topic: &fakedInputTopic, Partition: 2, Offset: 99999999},
	}

	for _, how := range []string{"commit", "abort"} {

		producer, err := NewProducer(config)
		if err != nil {
			t.Fatalf("Failed to create Producer client: %s\n", err)
		}

		err = producer.InitTransactions(nil)
		if err != nil {
			t.Fatalf("InitTransactions() failed: %v\n", err)
		}

		err = producer.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction() failed: %v\n", err)
		}

		expectDrErr := how == "abort"

		termChan := make(chan bool)
		doneChan := make(chan bool)
		go func() {
			doTerm := false
			for doTerm {
				select {
				case e := <-producer.Events():
					m, ok := e.(*Message)
					if !ok {
						t.Logf("Ignoring %v event\n", e)
						continue
					}
					if (m.TopicPartition.Error != nil) == expectDrErr {
						t.Fatalf("Unexpected %v, expectDrErr %v\n",
							m.TopicPartition, expectDrErr)
					}

				case <-termChan:
					doTerm = true
				}
			}

			close(doneChan)
		}()

		for i := 0; i < msgCnt; i++ {
			err = producer.Produce(
				&Message{
					TopicPartition: TopicPartition{
						Topic:     &outputTopic,
						Partition: int32(msgCnt % 3)},
					Value: []byte(fmt.Sprintf("value%d", i)),
				}, nil)

			if err != nil {
				t.Fatalf("Failed to produce message: %v\n", err)
			}
		}

		offsets := goodOffsets
		if how == "abort" {
			offsets = badOffsets
		}

		cgmd, err := NewTestConsumerGroupMetadata(groupID)
		if err != nil {
			t.Fatalf("Failed to create consumer group metadata: %v", err)
		}
		err = producer.SendOffsetsToTransaction(nil, offsets, cgmd)
		if err != nil {
			t.Fatalf("Failed to send offsets to transactions: %v\n", err)
		}

		if how == "commit" {
			err = producer.CommitTransaction(nil)
			if err != nil {
				t.Fatalf("CommitTransaction() failed: %v\n", err)
			}
		} else {
			err = producer.AbortTransaction(nil)
			if err != nil {
				t.Fatalf("AbortTransaction() failed: %v\n", err)
			}
		}

		// Create consumer (to read committed offsets) prior to closing the
		// consumer to trigger the race condition where the transaction is
		// not fully committed by the time consumer.Committed() is called.
		// Prior to KIP-447 this would result in the committed offsets not
		// showing up, but with KIP-447 the consumer automatically retries
		// the offset retrieval.
		t.Logf("Creating consumer for (later) offset retrieval\n")
		consumer, err := NewConsumer(consumerConfig)
		if err != nil {
			t.Fatalf("Failed to create Consumer client: %s\n", err)
		}

		// Close producer
		// signal go-routine to finish
		close(termChan)
		// wait for go-routine to finish
		_ = <-doneChan

		producer.Close()

		t.Logf("Retrieving committed offsets\n")
		committed, err := consumer.Committed(partitions, -1)
		if err != nil {
			t.Fatalf("Failed to get committed offsets: %s\n", err)
		}
		t.Logf("Committed offsets: %v\n", committed)

		// reflect.DeepEqual() can't be used since TopicPartition.Topic
		// is a pointer to a string rather than a string and the pointer
		// will differ between partitions and assignment.
		// Instead do a simple stringification + string compare.
		if fmt.Sprintf("%v", committed) != fmt.Sprintf("%v", goodOffsets) {
			t.Fatalf("Committed offsets %v does not match expected good offsets %v",
				committed, goodOffsets)
		}

		consumer.Close()
	}
}
