/**
 * Copyright 2023 Confluent Inc.
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
// callback along with manual commit.
// It processes a batch of messages in parallel and pairs Kafka commits with
// a simulated user transaction. Random failures are present
// to show how it must behave in each case.
package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var processing []chan error

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	var err error
	bootstrapServers := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3:]
	exitCode := 0

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		// Consumer group ID
		"group.id": group,
		// Start reading from the first message of each assigned
		// partition if there are no previously committed offsets
		// for this group.
		"auto.offset.reset": "earliest",
		// Whether or not we commit offsets automatically.
		"enable.auto.commit":   false,
		"enable.partition.eof": true,
	})
	defer func() {
		fmt.Printf("%% Closing consumer\n")
		c.Close()
		os.Exit(exitCode)
	}()

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		exitCode = 1
		return
	}

	fmt.Printf("%% Created Consumer %v\n", c)

	// Subscribe to topics, call the rebalanceCallback on assignment/revoke.
	// The rebalanceCallback can be triggered from c.Poll() and c.Close().
	err = c.SubscribeTopics(topics, rebalanceCallback)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed subscribing to topics: %s\n", err)
		exitCode = 1
		return
	}

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("%% Caught signal %v: terminating\n", sig)
			commit(c)
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
}

// processEvent processes the message/error received from the kafka Consumer's
// Poll() method.
func processEvent(c *kafka.Consumer, ev kafka.Event) error {
	switch e := ev.(type) {

	case *kafka.Message:
		fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
		processMessage(e)

		// Handle manual commit since enable.auto.commit is unset.
		if err := maybeCommit(c, e.TopicPartition); err != nil {
			return err
		}

	case kafka.PartitionEOF:
		fmt.Printf("%% EOF for topic %s partition %d, committing\n", *e.Topic,
			e.Partition)
		if err := commit(c); err != nil {
			return err
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

// processMessage starts parallel processing of a message and
// appends the returned channel to the processing slice.
func processMessage(message *kafka.Message) error {
	processing = append(processing, parallelProcessMessage(message))
	return nil
}

// parallelProcessMessage starts parallel processing of a message and
// returns a channel were the corresponding error will be produced.
func parallelProcessMessage(message *kafka.Message) chan error {
	channel := make(chan error)
	go func() {
		time.Sleep(100 * time.Millisecond)
		channel <- randomFailure()
		close(channel)
	}()
	return channel
}

// completeProcessing awaits all processing tasks and
// returns an error if at least one of them failed.
func completeProcessing() error {
	fmt.Printf("%% Complete pending tasks\n")
	var err error = nil
	// Awaits a result from all the processing tasks
	for _, channel := range processing {
		currentErr := <-channel
		if err == nil {
			err = currentErr
		}
	}
	// Clear the processing slice
	processing = processing[:0]
	return err
}

// maybeCommit is called for each message we receive from a Kafka topic.
// This method can be used to apply some arbitary logic/processing to the
// offsets, write the offsets into some external storage, and finally, to
// decide when we want to commit already-stored offsets into Kafka.
func maybeCommit(c *kafka.Consumer, topicPartition kafka.TopicPartition) error {
	// Commit the already-stored offsets to Kafka whenever the offset is divisible
	// by 10, otherwise return early.
	// This logic is completely arbitrary. We can use any other internal or
	// external variables to decide when we commit the already-stored offsets.
	if topicPartition.Offset%10 != 0 {
		return nil
	}

	fmt.Printf("%% maybeCommit: do commit\n")
	return commit(c)
}

// commit completes current parallel processing,
// commits user transaction and offsets on Kafka.
// If something fails, calls abort to abort the transaction.
func commit(c *kafka.Consumer) error {
	fmt.Printf("%% Committing transaction\n")

	var err error
	err = completeProcessing()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Processing tasks failed, aborting\n")
		abort(c)
		return err
	}

	err = userTransactionCommit()
	if err != nil {
		fmt.Fprintf(os.Stderr, "User commit failed, aborting\n")
		abort(c)
		return err
	}

	committedOffsets, err := c.Commit()

	// ErrNoOffset occurs when there are no stored offsets to commit. This
	// can happen if we haven't stored anything since the last commit.
	// While this will never happen for this example since we call this method
	// per-message, and thus, always have something to commit, the error
	// handling is illustrative of how to handle it in cases we call Commit()
	// in another way, for example, every N seconds.
	if err != nil && err.(kafka.Error).Code() != kafka.ErrNoOffset {
		fmt.Fprintf(os.Stderr, "Fatal: Committed offsets to user transaction but not to Kafka: %s\n", err.Error())
		return err
	}

	fmt.Printf("%% Committed offsets to Kafka: %v\n", committedOffsets)
	return nil
}

// userTransactionCommit models committing a user transaction, that can be a
// DBMS transaction of something else.
// Returns a random failure
func userTransactionCommit() error {
	fmt.Printf("%% Committing user transaction\n")
	return randomFailure()
}

// userTransactionAbort models aborting a user transaction, that can be a
// DBMS transaction of something else.
// Returns a random failure
func userTransactionAbort() error {
	fmt.Printf("%% Aborting user transaction\n")
	return randomFailure()
}

// Returns an error with probability 0.05 or no error with probability 0.95
func randomFailure() error {
	if rand.Float64() < 0.05 {
		return fmt.Errorf("random failure")
	}
	return nil
}

// abort completes current parallel processing,
// aborts user transaction and rewinds consumer to committed offsets.
func abort(c *kafka.Consumer) {
	fmt.Printf("%% Aborting transaction\n")
	completeProcessing()
	// Ignore error, transaction is aborting anyway

	var err error
	// Continue retrying, if it cannot abort a transaction or seek assigned
	// partitions, probably it cannot communicate with one of the two components,
	// so it's not worth trying anything else.
	for {
		err = userTransactionAbort()
		if err != nil {
			fmt.Fprintf(os.Stderr, "userTransactionAbort failed, retry: %s\n", err.Error())
			continue
		}
		err = rewindConsumerPosition(c)
		if err == nil {
			return
		}
		fmt.Fprintf(os.Stderr, "rewindConsumerPosition failed, retry: %s\n", err.Error())
		// Pause between retries
		time.Sleep(3 * time.Second)
	}
}

// rewindConsumerPosition Rewinds consumer's position to the
// previous committed offset
func rewindConsumerPosition(c *kafka.Consumer) error {
	fmt.Printf("%% Rewind to committed offsets\n")
	assignment, err := c.Assignment()
	if err != nil {
		return err
	}

	committed, err := c.Committed(assignment, 30*1000 /* 30s */)
	if err != nil {
		return err
	}

	for _, tp := range committed {
		if tp.Offset < 0 {
			tp.Offset = kafka.OffsetBeginning
			tp.LeaderEpoch = nil
		}
		err := c.Seek(tp, 1)
		if err != nil {
			return err
		}
	}
	return nil
}

// rebalanceCallback is called on each group rebalance to assign additional
// partitions, or remove existing partitions, from the consumer's current
// assignment.
//
// A rebalance occurs when a consumer joins or leaves a consumer group, if it
// changes the topic(s) it's subscribed to, or if there's a change in one of
// the topics it's subscribed to, for example, the total number of partitions
// increases.
//
// The application may use this optional callback to inspect the assignment,
// alter the initial start offset (the .Offset field of each assigned partition),
// and read/write offsets to commit to an alternative store outside of Kafka.
func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		fmt.Printf("%% %s rebalance: %d new partition(s) assigned: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)

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

		if c.IsClosed() {
			// This is last revoke before closing, cannot commit here
			// must be done before closing.
			return nil
		}
		// Usually, the rebalance callback for `RevokedPartitions` is called
		// just before the partitions are revoked. We can be certain that a
		// partition being revoked is not yet owned by any other consumer.
		// This way, logic like storing any pending offsets or committing
		// offsets can be handled.
		// However, there can be cases where the assignment is lost
		// involuntarily. In this case, the partition might already be owned
		// by another consumer, and operations including committing
		// offsets may not work. We abort user transaction too in this case.
		if c.AssignmentLost() {
			// Our consumer has been kicked out of the group and the
			// entire assignment is thus lost.
			abort(c)
		} else {
			return commit(c)
		}

		// Similar to Assign, client automatically calls Unassign() unless the
		// callback has already called that method. Here, we don't call it.

	default:
		fmt.Fprintf(os.Stderr, "Unexpected event type: %v\n", event)
	}

	return nil
}
