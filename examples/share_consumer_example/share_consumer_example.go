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

// Example share consumer (Kafka Queues / KIP-932) with explicit acknowledgement
package main

// share_consumer_example subscribes to one or more topics as a member of a
// share group and processes each polled batch with explicit acknowledgement:
// every record is acknowledged Accept, Release (redeliver later) or Reject
// (drop). Share groups require a broker with the feature enabled (Apache Kafka
// 4.2.0+).

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3:]
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewShareConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          group,
		// "implicit" (the default) auto-accepts the previous poll's records on
		// the next Poll(). "explicit" requires the application to acknowledge
		// every record of a batch before the next Poll(), as shown below.
		"share.acknowledgement.mode": "explicit",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create share consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created share consumer %v\n", c)

	err = c.SubscribeTopics(topics)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to topics: %s\n", err)
		os.Exit(1)
	}

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			set, err := c.Poll(100)
			if err != nil {
				// Poll errors on a share consumer are typically fatal
				// (misconfiguration, authorization); log and keep polling.
				fmt.Fprintf(os.Stderr, "Poll error: %v\n", err)
				continue
			}
			if set == nil {
				// Timeout with no messages.
				continue
			}

			for _, msg := range set.Messages() {
				fmt.Printf("%% Message on %s:\n%s\n",
					msg.TopicPartition, string(msg.Value))
				// Acknowledge each record: Accept marks it processed, Release
				// makes it available for redelivery, Reject drops it.
				if err := set.Acknowledge(msg, kafka.ShareAcknowledgeTypeAccept); err != nil {
					fmt.Fprintf(os.Stderr, "Acknowledge failed: %v\n", err)
				}
			}

			// Flush the acknowledgements for this batch to the broker.
			if _, err := c.CommitSync(5000); err != nil {
				fmt.Fprintf(os.Stderr, "CommitSync failed: %v\n", err)
			}
		}
	}

	fmt.Printf("Closing share consumer\n")
	c.Close()
}
