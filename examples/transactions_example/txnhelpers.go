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

package main

// Transaction helper methods

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

// createTransactionalProducer creates a transactional producer for the given
// input partition.
func createTransactionalProducer(toppar kafka.TopicPartition) error {
	producerConfig := &kafka.ConfigMap{
		"client.id":              fmt.Sprintf("txn-p%d", toppar.Partition),
		"bootstrap.servers":      brokers,
		"transactional.id":       fmt.Sprintf("go-transactions-example-p%d", int(toppar.Partition)),
		"go.logs.channel.enable": true,
		"go.logs.channel":        logsChan,
	}

	producer, err := kafka.NewProducer(producerConfig)
	if err != nil {
		return err
	}

	maxDuration, err := time.ParseDuration("10s")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	err = producer.InitTransactions(ctx)
	if err != nil {
		return err
	}

	err = producer.BeginTransaction()
	if err != nil {
		return err
	}

	producers[toppar.Partition] = producer
	addLog(fmt.Sprintf("Processor: created producer %s for partition %v",
		producers[toppar.Partition], toppar.Partition))
	return nil
}

// destroyTransactionalProducer aborts the current transaction and destroys the producer.
func destroyTransactionalProducer(producer *kafka.Producer) error {
	maxDuration, err := time.ParseDuration("10s")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	err = producer.AbortTransaction(ctx)
	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrState {
			// No transaction in progress, ignore the error.
			err = nil
		} else {
			addLog(fmt.Sprintf("Failed to abort transaction for %s: %s",
				producer, err))
		}
	}

	producer.Close()

	return err
}

// groupRebalance is triggered on consumer rebalance.
//
// For each assigned partition a transactional producer is created, this is
// required to guarantee per-partition offset commit state prior to
// KIP-447 being supported.
func groupRebalance(consumer *kafka.Consumer, event kafka.Event) error {
	addLog(fmt.Sprintf("Processor: rebalance event %v", event))

	switch e := event.(type) {
	case kafka.AssignedPartitions:
		// Create a producer per input partition.
		for _, tp := range e.Partitions {
			err := createTransactionalProducer(tp)
			if err != nil {
				fatal(err)
			}
		}

		err := consumer.Assign(e.Partitions)
		if err != nil {
			fatal(err)
		}

	case kafka.RevokedPartitions:
		// Abort any current transactions and close the
		// per-partition producers.
		for _, producer := range producers {
			err := destroyTransactionalProducer(producer)
			if err != nil {
				fatal(err)
			}
		}

		// Clear producer and intersection states
		producers = make(map[int32]*kafka.Producer)
		intersectionStates = make(map[string]*intersectionState)

		err := consumer.Unassign()
		if err != nil {
			fatal(err)
		}
	}

	return nil
}

// rewindConsumerPosition rewinds the consumer to the last committed offset or
// the beginning of the partition if there is no committed offset.
// This is to be used when the current transaction is aborted.
func rewindConsumerPosition(partition int32) {
	committed, err := processorConsumer.Committed([]kafka.TopicPartition{{Topic: &inputTopic, Partition: partition}}, 10*1000 /* 10s */)
	if err != nil {
		fatal(err)
	}

	for _, tp := range committed {
		if tp.Offset < 0 {
			// No committed offset, reset to earliest
			tp.Offset = kafka.OffsetBeginning
		}

		addLog(fmt.Sprintf("Processor: rewinding input partition %v to offset %v",
			tp.Partition, tp.Offset))

		err = processorConsumer.Seek(tp, -1)
		if err != nil {
			fatal(err)
		}
	}
}

// getConsumerPosition gets the current position (next offset) for a given input partition.
func getConsumerPosition(partition int32) []kafka.TopicPartition {
	position, err := processorConsumer.Position([]kafka.TopicPartition{{Topic: &inputTopic, Partition: partition}})
	if err != nil {
		fatal(err)
	}

	return position
}

// commitTransactionForInputPartition sends the consumer offsets for
// the given input partition and commits the current transaction.
// A new transaction will be started when done.
func commitTransactionForInputPartition(partition int32) {
	producer, found := producers[partition]
	if !found || producer == nil {
		fatal(fmt.Sprintf("BUG: No producer for input partition %v", partition))
	}

	position := getConsumerPosition(partition)
	consumerMetadata, err := processorConsumer.GetConsumerGroupMetadata()
	if err != nil {
		fatal(fmt.Sprintf("Failed to get consumer group metadata: %v", err))
	}

	err = producer.SendOffsetsToTransaction(nil, position, consumerMetadata)
	if err != nil {
		addLog(fmt.Sprintf(
			"Processor: Failed to send offsets to transaction for input partition %v: %s: aborting transaction",
			partition, err))

		err = producer.AbortTransaction(nil)
		if err != nil {
			fatal(err)
		}

		// Rewind this input partition to the last committed offset.
		rewindConsumerPosition(partition)
	} else {
		err = producer.CommitTransaction(nil)
		if err != nil {
			addLog(fmt.Sprintf(
				"Processor: Failed to commit transaction for input partition %v: %s",
				partition, err))

			err = producer.AbortTransaction(nil)
			if err != nil {
				fatal(err)
			}

			// Rewind this input partition to the last committed offset.
			rewindConsumerPosition(partition)
		}
	}

	// Start a new transaction
	err = producer.BeginTransaction()
	if err != nil {
		fatal(err)
	}
}
