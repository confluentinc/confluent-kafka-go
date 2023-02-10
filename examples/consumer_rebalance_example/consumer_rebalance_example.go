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
// callback along with manual store and/or commit.
package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

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
		"enable.auto.commit": false,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("%% Created Consumer %v\n", c)

	// Subscribe to topics, call the rebalanceCallback on assignment/revoke.
	// The rebalanceCallback can be triggered from c.Poll() and c.Close().
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

		// Handle manual commit since enable.auto.commit is unset.
		if err := maybeCommit(c, e.TopicPartition); err != nil {
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

	commitedOffsets, err := c.Commit()

	// ErrNoOffset occurs when there are no stored offsets to commit. This
	// can happen if we haven't stored anything since the last commit.
	// While this will never happen for this example since we call this method
	// per-message, and thus, always have something to commit, the error
	// handling is illustrative of how to handle it in cases we call Commit()
	// in another way, for example, every N seconds.
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
			fmt.Fprintln(os.Stderr, "Assignment lost involuntarily, commit may fail")
		}

		// Since enable.auto.commit is unset, we need to commit offsets manually
		// before the partition is revoked.
		commitedOffsets, err := c.Commit()

		if err != nil && err.(kafka.Error).Code() != kafka.ErrNoOffset {
			fmt.Fprintf(os.Stderr, "Failed to commit offsets: %s\n", err)
			return err
		}
		fmt.Printf("%% Commited offsets to Kafka: %v\n", commitedOffsets)

		// Similar to Assign, client automatically calls Unassign() unless the
		// callback has already called that method. Here, we don't call it.

	default:
		fmt.Fprintf(os.Stderr, "Unxpected event type: %v\n", event)
	}

	return nil
}
