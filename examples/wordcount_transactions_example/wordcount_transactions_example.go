/**
 * Copyright 2025 Confluent Inc.
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

// Simple transactional word-count example.
//
// Demonstrates Kafka transactions by reading sentences from an input topic,
// splitting them into words, counting each word, and producing the counts
// to an output topic — all within a single atomic transaction.
//
// The program is self-contained: it creates the topics, seeds sample
// sentences, runs the transactional word count, verifies the results,
// and cleans up.
//
// Usage:
//
//	go run . [bootstrap-servers]
//
// Defaults to localhost:9092 if no argument is given.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	broker := "localhost:9092"
	if len(os.Args) > 1 {
		broker = os.Args[1]
	}

	suffix := rand.Intn(100000)
	inputTopic := fmt.Sprintf("wordcount-input-%d", suffix)
	outputTopic := fmt.Sprintf("wordcount-output-%d", suffix)
	groupID := fmt.Sprintf("wordcount-group-%d", suffix)

	fmt.Printf("Input topic:  %s\n", inputTopic)
	fmt.Printf("Output topic: %s\n", outputTopic)

	// --- Create topics ---
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		fatal("create admin", err)
	}
	results, err := admin.CreateTopics(
		context.Background(),
		[]kafka.TopicSpecification{
			{Topic: inputTopic, NumPartitions: 1, ReplicationFactor: 1},
			{Topic: outputTopic, NumPartitions: 1, ReplicationFactor: 1},
		},
		kafka.SetAdminOperationTimeout(10*time.Second),
	)
	if err != nil {
		fatal("create topics", err)
	}
	for _, r := range results {
		if r.Error.Code() != kafka.ErrNoError {
			fatal("create topic "+r.Topic, r.Error)
		}
	}
	admin.Close()
	fmt.Println("Topics created.")

	// --- Seed input topic with sentences ---
	sentences := []string{
		"the quick brown fox",
		"the fox jumped over the lazy dog",
		"the dog barked at the fox",
	}

	seedProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
	})
	if err != nil {
		fatal("create seed producer", err)
	}

	for _, sentence := range sentences {
		err = seedProducer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &inputTopic, Partition: kafka.PartitionAny},
			Value:          []byte(sentence),
		}, nil)
		if err != nil {
			fatal("seed produce", err)
		}
	}
	seedProducer.Flush(10 * 1000)
	seedProducer.Close()
	fmt.Printf("Seeded %d sentences.\n", len(sentences))

	// --- Create transactional producer ---
	txnProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"transactional.id":  fmt.Sprintf("wordcount-txn-%d", suffix),
	})
	if err != nil {
		fatal("create txn producer", err)
	}

	ctx10s, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = txnProducer.InitTransactions(ctx10s)
	if err != nil {
		fatal("init transactions", err)
	}

	err = txnProducer.BeginTransaction()
	if err != nil {
		fatal("begin transaction", err)
	}
	fmt.Println("Transaction started.")

	// --- Create consumer ---
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		fatal("create consumer", err)
	}

	err = consumer.Subscribe(inputTopic, nil)
	if err != nil {
		fatal("subscribe", err)
	}

	// --- Consume sentences and count words ---
	wordCounts := make(map[string]int)
	sentencesRead := 0
	emptyPolls := 0
	maxEmptyPolls := 20

	for emptyPolls < maxEmptyPolls {
		ev := consumer.Poll(500)
		if ev == nil {
			emptyPolls++
			continue
		}
		emptyPolls = 0

		msg, ok := ev.(*kafka.Message)
		if !ok {
			continue
		}

		sentence := strings.TrimSpace(string(msg.Value))
		words := strings.Fields(strings.ToLower(sentence))
		for _, word := range words {
			wordCounts[word]++
		}
		sentencesRead++
		fmt.Printf("  Read: %q (%d words)\n", sentence, len(words))
	}

	fmt.Printf("Consumed %d sentences, found %d unique words.\n", sentencesRead, len(wordCounts))

	// --- Produce word counts within the transaction ---
	for word, count := range wordCounts {
		payload, err := json.Marshal(map[string]interface{}{
			"word":  word,
			"count": count,
		})
		if err != nil {
			fatal("marshal", err)
		}

		err = txnProducer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &outputTopic, Partition: kafka.PartitionAny},
			Key:            []byte(word),
			Value:          payload,
		}, nil)
		if err != nil {
			fatal("txn produce", err)
		}
	}

	// --- Commit transaction with consumer offsets ---
	assignment, err := consumer.Assignment()
	if err != nil {
		fatal("assignment", err)
	}

	positions, err := consumer.Position(assignment)
	if err != nil {
		fatal("position", err)
	}

	consumerMetadata, err := consumer.GetConsumerGroupMetadata()
	if err != nil {
		fatal("group metadata", err)
	}

	err = txnProducer.SendOffsetsToTransaction(nil, positions, consumerMetadata)
	if err != nil {
		fatal("send offsets", err)
	}

	err = txnProducer.CommitTransaction(nil)
	if err != nil {
		fatal("commit transaction", err)
	}
	fmt.Println("Transaction committed.")

	consumer.Close()
	txnProducer.Close()

	// --- Verify: read word counts from output topic ---
	verifyConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           fmt.Sprintf("verify-%d", suffix),
		"auto.offset.reset":  "earliest",
		"isolation.level":    "read_committed",
		"enable.auto.commit": false,
	})
	if err != nil {
		fatal("create verify consumer", err)
	}

	err = verifyConsumer.Subscribe(outputTopic, nil)
	if err != nil {
		fatal("verify subscribe", err)
	}

	type wordEntry struct {
		Word  string `json:"word"`
		Count int    `json:"count"`
	}

	var verified []wordEntry
	emptyPolls = 0
	for emptyPolls < 20 {
		ev := verifyConsumer.Poll(500)
		if ev == nil {
			emptyPolls++
			continue
		}
		emptyPolls = 0
		msg, ok := ev.(*kafka.Message)
		if !ok {
			continue
		}

		var entry wordEntry
		if err := json.Unmarshal(msg.Value, &entry); err != nil {
			fmt.Printf("  WARN: bad message at %v: %v\n", msg.TopicPartition, err)
			continue
		}
		verified = append(verified, entry)
	}
	verifyConsumer.Close()

	// --- Print results ---
	sort.Slice(verified, func(i, j int) bool {
		if verified[i].Count != verified[j].Count {
			return verified[i].Count > verified[j].Count
		}
		return verified[i].Word < verified[j].Word
	})

	fmt.Printf("\nWord counts (%d unique words):\n", len(verified))
	for _, e := range verified {
		fmt.Printf("  %-10s %d\n", e.Word, e.Count)
	}

	if len(verified) == len(wordCounts) {
		fmt.Println("\nSUCCESS")
	} else {
		fmt.Printf("\nMISMATCH: expected %d words, got %d\n", len(wordCounts), len(verified))
		os.Exit(1)
	}

	// --- Cleanup topics ---
	admin2, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err == nil {
		admin2.DeleteTopics(context.Background(), []string{inputTopic, outputTopic})
		admin2.Close()
		fmt.Println("Topics cleaned up.")
	}
}

func fatal(context string, err error) {
	fmt.Fprintf(os.Stderr, "FATAL [%s]: %v\n", context, err)
	os.Exit(1)
}
