/**
 * Copyright 2022 Confluent Inc.
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

// Example high-level Apache Kafka consumer demonstrating use of rebalance
// callback along with manual store and/or commit.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// offsetStorageKey is the key that we use for our external offset storage.
type offsetStorageKey struct {
	topic     string
	partition int32
}

// externalOffsetStorage represents an external (non-Kafka) store to which we
// are writing offsets, before using some arbitrary logic to decide when we
// write them to Kafka.
type externalOffsetStorage map[offsetStorageKey]kafka.Offset

// makeKey is a convenience function to create keys for externalOffsetStorage.
func makeKey(tp *kafka.TopicPartition) offsetStorageKey {
	return offsetStorageKey{
		topic:     *tp.Topic,
		partition: tp.Partition,
	}
}

// store is the instance of externalOffsetStorage used to store offsets.
var store externalOffsetStorage

// autocommit, autostore store command line options.
var autocommit, autostore bool

func main() {
	if len(os.Args) < 6 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <enable_auto_commit> <enable_auto_offset_store> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	var err error
	bootstrapServers := os.Args[1]
	if autocommit, err = strconv.ParseBool(os.Args[2]); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing `enable_auto_commit` as boolean: %s\n", err)
		os.Exit(1)
	}
	if autostore, err = strconv.ParseBool(os.Args[3]); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing `enable_auto_offset_store` as boolean: %s\n", err)
		os.Exit(1)
	}
	group := os.Args[4]
	topics := os.Args[5:]

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		// Consumer group ID
		"group.id": group,
		// Start reading from the first message of each assigned
		// partition if there are no previously committed offsets
		// for this group.
		"auto.offset.reset": "earliest",
		// Whether or not we commit offsets automatically.
		"enable.auto.commit": autocommit,
		// Whether or not we store offsets automatically.
		"enable.auto.offset.store": autostore,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("%% Created Consumer %v\n", c)

	// Initialize external storage for offsets in case
	// enable.auto.offset.store is unset.
	if !autostore {
		store = make(map[offsetStorageKey]kafka.Offset)
	}

	// Subscribe to topics, call the rebalanceCallback on assignment/revoke.
	// The rebalanceCallback is triggered from c.Poll() and c.Close().
	err = c.SubscribeTopics(topics, rebalanceCallback)

	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("%% Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			if err = processEvent(c, ev); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to process event: %s\n", err)
			}

		}
	}

	fmt.Printf("%% Closing consumer\n")
	c.Close()
}

// processEvent processes the message/error received from the kafka Consumer's
// Poll() method.
func processEvent(c *kafka.Consumer, ev kafka.Event) error {
	switch e := ev.(type) {

	case *kafka.Message:
		fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))

		// Handle manual offset storage if enable.auto.store.offset is unset.
		if !autostore {
			if err := maybeStoreOffset(c, e.TopicPartition); err != nil {
				return err
			}
		}

		// Handle manual commit if enable.auto.commit is unset.
		if !autocommit {
			if err := maybeCommit(c, e.TopicPartition); err != nil {
				return err
			}
		}

	case kafka.Error:
		// Errors should generally be considered informational, the client
		// will try to automatically recover.
		fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n",
			e.Code(), e)

	default:
		fmt.Printf("Ignored %v\n", e)
	}

	return nil
}

// maybeStoreOffset is called for each message we receive from a Kafka topic if
// enable.auto.store.offset is unset.
// This method can be used to apply some arbitary logic/processing to the
// offsets, write the offsets into some external storage, and finally, to
// decide when we want to store the offsets into the Kafka library.
func maybeStoreOffset(c *kafka.Consumer, topicPartition kafka.TopicPartition) error {
	// Write offsets to a local store outside Kafka.
	key := makeKey(&topicPartition)
	store[key] = topicPartition.Offset + 1

	// If offset is divisible by 10, store offset to Kafka.
	// This logic is completely arbitrary. We can use any other internal or
	// external variables to decide when we store the offset.
	if store[key]%10 == 0 {
		var storedOffsets []kafka.TopicPartition
		storedOffsets, err := c.StoreOffsets([]kafka.TopicPartition{
			{
				Topic:     topicPartition.Topic,
				Partition: topicPartition.Partition,
				// While storing to the library, make sure to store the next
				// offset we want to read, and not the offset we have just read.
				Offset: topicPartition.Offset + 1,
			},
		})
		if err != nil {
			return err
		}
		fmt.Printf("%% Stored offsets to Kafka: %v\n", storedOffsets)
	}

	return nil
}

// maybeCommit is called for each message we receive from a Kafka topic if
// enable.auto.commit is unset.
// This method can be used to apply some arbitary logic/processing to the
// offsets, write the offsets into some external storage, and finally, to
// decide when we want to commit already-stored offsets into Kafka.
func maybeCommit(c *kafka.Consumer, topicPartition kafka.TopicPartition) error {
	// Always commit the already-stored offsets to Kafka.
	// This logic is completely arbitrary. We can use any other internal or
	// external variables to decide when we commit the already-stored offsets.
	commitedOffsets, err := c.Commit()

	// ErrNoOffset occurs when there are no stored offsets to commit. This
	// can happen if we haven't stored anything since the last commit.
	// It is informational, rather than being an error.
	if err != nil && err.(kafka.Error).Code() != kafka.ErrNoOffset {
		return err
	}

	fmt.Printf("%% Commited offsets to Kafka: %v\n", commitedOffsets)
	return nil
}

// rebalanceCallback is called on each group rebalance to assign additional
// partitions, or remove existing partitions, from the consumer's current
// assignment.
//
// The application may use this optional callback to inspect the assignment,
// alter the initial start offset (the .Offset field of each assigned partition),
// and read/write offsets to commit to an alternative store outside of Kafka.
func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		fmt.Printf("%% %s rebalance: %d new partition(s) assigned: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)

		// If enable.auto.store.offset is unset, we need to initialize the
		// external store outside of Kafka in which we will store offsets.
		if !autostore {
			for _, partition := range ev.Partitions {
				store[makeKey(&partition)] = kafka.OffsetInvalid
			}
		}

		// The application may update the start .Offset of each assigned
		// partition and then call Assign(). It is optional to call Assign
		// in case the application is not modifying any start .Offsets. In
		// that case we don't, the library takes care of it.
		// It is called here despite not modifying any .Offsets for illustrative
		// purposes.
		err := c.Assign(ev.Partitions)
		if err != nil {
			return err
		}

	case kafka.RevokedPartitions:
		fmt.Printf("%% %s rebalance: %d partition(s) revoked: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)

		// Usually, the rebalance callback for `RevokedPartitions` is called
		// just before the partitions are revoked. We can be certain that a
		// partition being revoked is not yet owned by any other consumer.
		// This way, logic like storing any pending offsets or committing
		// offsets can be handled.
		// However, there can be cases where the assignment is lost
		// involuntarily. In this case, the partition might already be owned
		// by another consumer, and operations including committing
		// offsets may not work.
		if c.AssignmentLost() {
			// Our consumer has been kicked out of the group and the
			// entire assignment is thus lost.
			fmt.Fprintln(os.Stderr, "Assignment lost involuntarily, store/commit may fail")
		}

		// If enable.auto.store.offset is unset, we need to move any pending
		// offsets to be stored from the external store to the Kafka library
		// store.
		if !autostore {
			partitionsToStore := make([]kafka.TopicPartition, 0)
			for _, partition := range ev.Partitions {
				key := makeKey(&partition)
				value, ok := store[key]
				if !ok {
					fmt.Fprintf(
						os.Stderr, "Partition not found in external storage: %v\n", partition)
					continue
				}
				partitionsToStore = append(partitionsToStore,
					kafka.TopicPartition{
						Partition: partition.Partition,
						Topic:     partition.Topic,
						Offset:    value})
			}
			storedOffsets, err := c.StoreOffsets(partitionsToStore)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to store offsets: %s\n", err)
				return err
			}
			fmt.Printf("%% Stored offsets to Kafka: %v\n", storedOffsets)

			// Remove entries for the revoked partitions from the external store.
			for _, partition := range ev.Partitions {
				key := makeKey(&partition)
				delete(store, key)
			}
		}

		// If enable.auto.commit is unset, we need to commit offsets manually
		// before the partition is revoked.
		if !autocommit {
			commitedOffsets, err := c.Commit()
			// ErrNoOffset occurs when there are no stored offsets to commit. This
			// can happen if we haven't stored anything since the last commit.
			// It is informational, rather than being an error.
			if err != nil && err.(kafka.Error).Code() != kafka.ErrNoOffset {
				fmt.Fprintf(os.Stderr, "Failed to commit offsets: %s\n", err)
				return err
			}
			fmt.Printf("%% Commited offsets to Kafka: %v\n", commitedOffsets)
		}

		// Similar to Assign, client automatically calls Unassign() unless the
		// callback has already called that method. Here, we don't call it.

	default:
		fmt.Fprintf(os.Stderr, "Unxpected event type: %v\n", event)
	}

	return nil
}
