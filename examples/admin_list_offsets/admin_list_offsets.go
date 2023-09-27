/**
 * Copyright 2018 Confluent Inc.
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

// Delete topics
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	bootstrapServers := "localhost:9092"

	// Create a new AdminClient.
	// AdminClient can also be instantiated using an existing
	// Producer or Consumer instance, see NewAdminClientFromProducer and
	// NewAdminClientFromConsumer.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requests := make(map[kafka.TopicPartition]int64)
	goTopic := "topicname"
	tp1 := kafka.TopicPartition{Topic: &goTopic, Partition: 0}
	requests[tp1] = int64(kafka.EarliestOffsetSpec)

	results, err := a.ListOffsets(ctx, requests, kafka.SetAdminIsolationLevel(kafka.ReadCommitted))
	if err != nil {
		fmt.Printf("Failed to List offsets: %v\n", err)
		os.Exit(1)
	}
	// map[TopicPartition]ListOffsetResultInfo
	// Print results
	for tp, info := range results {
		fmt.Printf("Topic: %s Partition_Index : %d\n", *tp.Topic, tp.Partition)
		if info.Err.Code() != 0 {
			fmt.Printf("	ErrorCode : %d ErrorMessage : %s\n\n", info.Err.Code(), info.Err.String())
		} else {
			fmt.Printf("	Offset : %d Timestamp : %d\n\n", info.Offset, info.Timestamp)
		}
	}
	var topics []kafka.TopicSpecification
	topics = append(topics, kafka.TopicSpecification{Topic: goTopic, NumPartitions: 1, ReplicationFactor: 1})
	create_topic_result, create_topic_err := a.CreateTopics(ctx, topics)
	if create_topic_err != nil {
		fmt.Printf("Failed to create topic : %v\n", err)
		os.Exit(1)
	} else {
		if create_topic_result[0].Error.Code() != 0 {
			fmt.Printf("Failed to create Topic : %s with error : %s \n", create_topic_result[0].Topic, create_topic_result[0].Error.String())
		} else {
			fmt.Printf("Topic : %s created!\n", create_topic_result[0].Topic)
		}
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)
	timestamp := time.Now()
	t1 := timestamp.Add(time.Second * 100)
	t2 := timestamp.Add(time.Second * 300)
	t3 := timestamp.Add(time.Second * 200)
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &goTopic, Partition: 0},
		Value:          []byte("Message-1"),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		Timestamp:      t1,
	}, nil)

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &goTopic, Partition: 0},
		Value:          []byte("Message-2"),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		Timestamp:      t2,
	}, nil)

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &goTopic, Partition: 0},
		Value:          []byte("Message-3"),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		Timestamp:      t3,
	}, nil)

	p.Flush(5 * 1000)
	requests[tp1] = int64(kafka.EarliestOffsetSpec)
	results, err = a.ListOffsets(ctx, requests, kafka.SetAdminIsolationLevel(kafka.ReadCommitted))
	if err != nil {
		fmt.Printf("Failed to List offsets: %v\n", err)
		os.Exit(1)
	}
	for tp, info := range results {
		fmt.Printf("Topic: %s Partition_Index : %d\n", *tp.Topic, tp.Partition)
		if info.Err.Code() != 0 {
			fmt.Printf("	ErrorCode : %d ErrorMessage : %s\n\n", info.Err.Code(), info.Err.String())
		} else {
			fmt.Printf("	Offset : %d Timestamp : %d\n\n", info.Offset, info.Timestamp)
		}
	}
	requests[tp1] = int64(kafka.LatestOffsetSpec)
	results, err = a.ListOffsets(ctx, requests, kafka.SetAdminIsolationLevel(kafka.ReadCommitted))
	if err != nil {
		fmt.Printf("Failed to List offsets: %v\n", err)
		os.Exit(1)
	}
	for tp, info := range results {
		fmt.Printf("Topic: %s Partition_Index : %d\n", *tp.Topic, tp.Partition)
		if info.Err.Code() != 0 {
			fmt.Printf("	ErrorCode : %d ErrorMessage : %s\n\n", info.Err.Code(), info.Err.String())
		} else {
			fmt.Printf("	Offset : %d Timestamp : %d\n\n", info.Offset, info.Timestamp)
		}
	}
	requests[tp1] = int64(kafka.MaxTimestampOffsetSpec)
	results, err = a.ListOffsets(ctx, requests, kafka.SetAdminIsolationLevel(kafka.ReadCommitted))
	if err != nil {
		fmt.Printf("Failed to List offsets: %v\n", err)
		os.Exit(1)
	}
	for tp, info := range results {
		fmt.Printf("Topic: %s Partition_Index : %d\n", *tp.Topic, tp.Partition)
		if info.Err.Code() != 0 {
			fmt.Printf("	ErrorCode : %d ErrorMessage : %s\n\n", info.Err.Code(), info.Err.String())
		} else {
			fmt.Printf("	Offset : %d Timestamp : %d\n\n", info.Offset, info.Timestamp)
		}
	}
	var del_topics []string
	del_topics = append(del_topics, goTopic)
	_, err = a.DeleteTopics(ctx, del_topics)
	if err != nil {
		fmt.Printf("Failed to delete topic: %s\n", err)
		os.Exit(1)
	}
	a.Close()
	p.Close()
}
