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
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// createTransactionalProducer creates a single transactional producer.
func createTransactionalProducer() (*kafka.Producer, error) {
	producerConfig := &kafka.ConfigMap{
		"client.id":              "txn-producer",
		"bootstrap.servers":      bootstrapServers,
		"transactional.id":       "go-transactions-example",
		"go.logs.channel.enable": true,
		"go.logs.channel":        logsChan,
	}

	p, err := kafka.NewProducer(producerConfig)
	if err != nil {
		return nil, err
	}

	maxDuration, err := time.ParseDuration("10s")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	err = p.InitTransactions(ctx)
	if err != nil {
		return nil, err
	}

	err = p.BeginTransaction()
	if err != nil {
		return nil, err
	}

	addLog(fmt.Sprintf("Processor: created transactional producer %s", p))
	return p, nil
}

// groupRebalance is triggered on consumer rebalance.
func groupRebalance(consumer *kafka.Consumer, event kafka.Event) error {
	addLog(fmt.Sprintf("Processor: rebalance event %v", event))

	switch e := event.(type) {
	case kafka.AssignedPartitions:
		err := consumer.Assign(e.Partitions)
		if err != nil {
			fatal(err)
		}

	case kafka.RevokedPartitions:
		// Abort the current transaction since the partition assignment
		// is changing. Offsets for the revoked partitions will not be
		// committed.
		if producer != nil {
			maxDuration, err := time.ParseDuration("10s")
			if err != nil {
				fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
			defer cancel()

			err = producer.AbortTransaction(ctx)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrState {
					// No transaction in progress, ignore.
					err = nil
				} else {
					addLog(fmt.Sprintf("Failed to abort transaction: %s", err))
				}
			}
		}

		intersectionStates = make(map[string]*intersectionState)

		err := consumer.Unassign()
		if err != nil {
			fatal(err)
		}

		// Begin a new transaction for future messages.
		if producer != nil {
			err = producer.BeginTransaction()
			if err != nil {
				fatal(err)
			}
		}
	}

	return nil
}

// rewindConsumerPosition rewinds the consumer to the last committed offset or
// the beginning of the partition if there is no committed offset.
// This is to be used when the current transaction is aborted.
func rewindConsumerPosition() {
	assignment, err := processorConsumer.Assignment()
	if err != nil {
		fatal(err)
	}

	committed, err := processorConsumer.Committed(assignment, 10*1000 /* 10s */)
	if err != nil {
		fatal(err)
	}

	for _, tp := range committed {
		if tp.Offset < 0 {
			tp.Offset = kafka.OffsetBeginning
			tp.LeaderEpoch = nil
		}

		addLog(fmt.Sprintf("Processor: rewinding input partition %v to offset %v",
			tp.Partition, tp.Offset))

		err = processorConsumer.Seek(tp, -1)
		if err != nil {
			fatal(err)
		}
	}
}

// commitTransaction sends the consumer offsets for all assigned partitions
// and commits the current transaction. A new transaction will be started
// when done.
func commitTransaction() {
	if producer == nil {
		return
	}

	assignment, err := processorConsumer.Assignment()
	if err != nil {
		fatal(err)
	}

	if len(assignment) == 0 {
		return
	}

	positions, err := processorConsumer.Position(assignment)
	if err != nil {
		fatal(err)
	}

	consumerMetadata, err := processorConsumer.GetConsumerGroupMetadata()
	if err != nil {
		fatal(fmt.Sprintf("Failed to get consumer group metadata: %v", err))
	}

	err = producer.SendOffsetsToTransaction(nil, positions, consumerMetadata)
	if err != nil {
		addLog(fmt.Sprintf(
			"Processor: Failed to send offsets to transaction: %s: aborting transaction",
			err))

		err = producer.AbortTransaction(nil)
		if err != nil {
			fatal(err)
		}

		rewindConsumerPosition()
	} else {
		err = producer.CommitTransaction(nil)
		if err != nil {
			addLog(fmt.Sprintf(
				"Processor: Failed to commit transaction: %s",
				err))

			err = producer.AbortTransaction(nil)
			if err != nil {
				fatal(err)
			}

			rewindConsumerPosition()
		}
	}

	// Start a new transaction
	err = producer.BeginTransaction()
	if err != nil {
		fatal(err)
	}
}
