/**
 * Copyright 2016 Confluent Inc.
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

// Apache Kafka kafkatest VerifiableConsumer implemented in Go
// This implementation is functionally equivalent to the Java VerifiableConsumer
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	verbosity              = 0
	sigs                   chan os.Signal
	lastConsumerState      string
	lastAssignmentReported bool
)

// ConsumerState holds the state of the consumer
type ConsumerState struct {
	consumer                 *kafka.Consumer
	topic                    string
	maxMessages              int  // -1 for infinite
	consumedMessages         int  // total messages consumed
	useAutoCommit            bool // whether auto-commit is enabled
	useAsyncCommit           bool // whether to use async commit
	verbose                  bool // whether to output individual record data
	run                      bool // flag to control main loop
	currentAssignment        map[string]*PartitionState
	consumedMsgsLastReported int
	shuttingDown             bool
	isCooperative            bool // whether using cooperative rebalancing protocol
}

// PartitionState tracks state for each assigned partition
type PartitionState struct {
	topic      string
	partition  int32
	count      int64 // messages consumed from this partition in current batch
	minOffset  int64
	maxOffset  int64
	totalCount int64 // total messages consumed from this partition
}

var state ConsumerState

// Event types for JSON output
type ConsumerEvent struct {
	Name      string `json:"name"`
	Timestamp int64  `json:"timestamp"`
}

type StartupCompleteEvent struct {
	ConsumerEvent
}

type ShutdownCompleteEvent struct {
	ConsumerEvent
}

type TopicPartitionInfo struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

type PartitionsAssignedEvent struct {
	ConsumerEvent
	Partitions []TopicPartitionInfo `json:"partitions"`
}

type PartitionsRevokedEvent struct {
	ConsumerEvent
	Partitions []TopicPartitionInfo `json:"partitions"`
}

type RecordSetSummary struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Count     int64  `json:"count"`
	MinOffset int64  `json:"minOffset"`
	MaxOffset int64  `json:"maxOffset"`
}

type RecordsConsumedEvent struct {
	ConsumerEvent
	Count      int64              `json:"count"`
	Partitions []RecordSetSummary `json:"partitions"`
}

type RecordDataEvent struct {
	ConsumerEvent
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
}

type CommitData struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type OffsetsCommittedEvent struct {
	ConsumerEvent
	Offsets []CommitData `json:"offsets"`
	Success bool         `json:"success"`
	Error   string       `json:"error,omitempty"`
}

// send prints a JSON event to stdout
func send(event interface{}) {
	b, err := json.Marshal(event)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal JSON: %v\n", err)
		return
	}
	fmt.Println(string(b))
}

// getPartitionKey returns a unique key for a partition
func getPartitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

// topicPartitionsToInfo converts kafka.TopicPartition slice to TopicPartitionInfo slice
func topicPartitionsToInfo(partitions []kafka.TopicPartition) []TopicPartitionInfo {
	infos := make([]TopicPartitionInfo, len(partitions))
	for i, tp := range partitions {
		infos[i] = TopicPartitionInfo{
			Topic:     *tp.Topic,
			Partition: tp.Partition,
		}
	}
	return infos
}

// hasMessageLimit returns true if there's a max message limit
func hasMessageLimit() bool {
	return state.maxMessages >= 0
}

// isFinished returns true if we've consumed enough messages
func isFinished() bool {
	return hasMessageLimit() && state.consumedMessages >= state.maxMessages
}

// onPartitionsAssigned handles partition assignment
func onPartitionsAssigned(partitions []kafka.TopicPartition) {
	if verbosity >= 1 {
		fmt.Fprintf(os.Stderr, "%% Partitions assigned: %v\n", partitions)
	}

	// Clear current assignment and create new state
	state.currentAssignment = make(map[string]*PartitionState)
	for _, tp := range partitions {
		key := getPartitionKey(*tp.Topic, tp.Partition)
		state.currentAssignment[key] = &PartitionState{
			topic:     *tp.Topic,
			partition: tp.Partition,
			minOffset: -1,
			maxOffset: -1,
		}
	}

	send(PartitionsAssignedEvent{
		ConsumerEvent: ConsumerEvent{
			Name:      "partitions_assigned",
			Timestamp: time.Now().UnixMilli(),
		},
		Partitions: topicPartitionsToInfo(partitions),
	})
}

// onPartitionsRevoked handles partition revocation
func onPartitionsRevoked(partitions []kafka.TopicPartition) {
	if verbosity >= 1 {
		fmt.Fprintf(os.Stderr, "%% Partitions revoked: %v\n", partitions)
	}

	send(PartitionsRevokedEvent{
		ConsumerEvent: ConsumerEvent{
			Name:      "partitions_revoked",
			Timestamp: time.Now().UnixMilli(),
		},
		Partitions: topicPartitionsToInfo(partitions),
	})

	// Clear current assignment
	state.currentAssignment = make(map[string]*PartitionState)
}

// onRecordsReceived processes consumed records and returns offsets to commit
func onRecordsReceived(records []*kafka.Message) []kafka.TopicPartition {
	offsetsMap := make(map[string]kafka.TopicPartition)

	// Group records by partition
	partitionRecords := make(map[string][]*kafka.Message)
	for _, record := range records {
		key := getPartitionKey(*record.TopicPartition.Topic, record.TopicPartition.Partition)
		partitionRecords[key] = append(partitionRecords[key], record)
	}

	summaries := []RecordSetSummary{}

	for key, partRecords := range partitionRecords {
		partState, ok := state.currentAssignment[key]
		if !ok {
			fmt.Fprintf(os.Stderr, "%% Received records for unassigned partition: %s\n", key)
			continue
		}

		// Check message limit
		if hasMessageLimit() && state.consumedMessages+len(partRecords) > state.maxMessages {
			partRecords = partRecords[0 : state.maxMessages-state.consumedMessages]
		}

		if len(partRecords) == 0 {
			continue
		}

		minOffset := int64(partRecords[0].TopicPartition.Offset)
		maxOffset := int64(partRecords[len(partRecords)-1].TopicPartition.Offset)

		// Update partition state
		if partState.minOffset == -1 || minOffset < partState.minOffset {
			partState.minOffset = minOffset
		}
		if maxOffset > partState.maxOffset {
			partState.maxOffset = maxOffset
		}
		partState.count += int64(len(partRecords))
		partState.totalCount += int64(len(partRecords))

		// Add to offsets to commit (next offset to read)
		topicCopy := partState.topic
		tp := kafka.TopicPartition{
			Topic:     &topicCopy,
			Partition: partState.partition,
			Offset:    kafka.Offset(maxOffset + 1),
		}
		offsetsMap[key] = tp

		// Output individual record data if verbose
		if state.verbose {
			for _, record := range partRecords {
				event := RecordDataEvent{
					ConsumerEvent: ConsumerEvent{
						Name:      "record_data",
						Timestamp: time.Now().UnixMilli(),
					},
					Topic:     *record.TopicPartition.Topic,
					Partition: record.TopicPartition.Partition,
					Offset:    int64(record.TopicPartition.Offset),
					Value:     string(record.Value),
				}
				if record.Key != nil {
					event.Key = string(record.Key)
				}
				send(event)
			}
		}

		// Add summary
		summaries = append(summaries, RecordSetSummary{
			Topic:     partState.topic,
			Partition: partState.partition,
			Count:     int64(len(partRecords)),
			MinOffset: minOffset,
			MaxOffset: maxOffset,
		})

		state.consumedMessages += len(partRecords)

		if isFinished() {
			break
		}
	}

	// Send records_consumed event
	send(RecordsConsumedEvent{
		ConsumerEvent: ConsumerEvent{
			Name:      "records_consumed",
			Timestamp: time.Now().UnixMilli(),
		},
		Count:      int64(len(records)),
		Partitions: summaries,
	})

	// Convert map to slice
	offsets := make([]kafka.TopicPartition, 0, len(offsetsMap))
	for _, tp := range offsetsMap {
		offsets = append(offsets, tp)
	}

	// Store offsets for auto-commit or manual commit
	// This ensures offsets are only stored after messages are processed and reported
	// With enable.auto.offset.store=false, we must explicitly store offsets
	if len(offsets) > 0 {
		_, err := state.consumer.StoreOffsets(offsets)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%% Error storing offsets: %v\n", err)
		}
	}

	return offsets
}

// commitSync performs synchronous commit
func commitSync(offsets []kafka.TopicPartition) {
	if len(offsets) == 0 {
		return
	}

	committedOffsets, err := state.consumer.CommitOffsets(offsets)

	// Prepare commit data
	commitData := []CommitData{}
	for _, tp := range committedOffsets {
		commitData = append(commitData, CommitData{
			Topic:     *tp.Topic,
			Partition: tp.Partition,
			Offset:    int64(tp.Offset),
		})
	}

	event := OffsetsCommittedEvent{
		ConsumerEvent: ConsumerEvent{
			Name:      "offsets_committed",
			Timestamp: time.Now().UnixMilli(),
		},
		Offsets: commitData,
		Success: err == nil,
	}

	if err != nil {
		event.Error = err.Error()
		if verbosity >= 1 {
			fmt.Fprintf(os.Stderr, "%% Commit failed: %v\n", err)
		}
	}

	send(event)
}

// commitAsync performs asynchronous commit
func commitAsync(offsets []kafka.TopicPartition) {
	if len(offsets) == 0 {
		return
	}

	// Async commit - callback will be received as an event
	_, err := state.consumer.CommitOffsets(offsets)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%% Async commit error: %v\n", err)
	}
}

// rebalanceCallback is called on each group rebalance
func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch e := event.(type) {
	case kafka.AssignedPartitions:
		fmt.Fprintf(os.Stderr, "%% AssignedPartitions event received with %d partitions\n", len(e.Partitions))
		onPartitionsAssigned(e.Partitions)
		lastAssignmentReported = true

		// Use incremental assign for cooperative rebalancing
		// (consumer protocol OR classic protocol with cooperative assignment strategy)
		// Use regular assign for eager rebalancing (classic protocol with eager strategies)
		var err error
		if state.isCooperative {
			err = c.IncrementalAssign(e.Partitions)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%% Error incrementally assigning partitions: %v\n", err)
				return err
			}
		} else {
			err = c.Assign(e.Partitions)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%% Error assigning partitions: %v\n", err)
				return err
			}
		}

	case kafka.RevokedPartitions:
		fmt.Fprintf(os.Stderr, "%% RevokedPartitions event received with %d partitions\n", len(e.Partitions))
		onPartitionsRevoked(e.Partitions)

		// Use incremental unassign for cooperative rebalancing
		// For eager rebalancing, unassign is called automatically by the library
		if state.isCooperative {
			err := c.IncrementalUnassign(e.Partitions)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%% Error incrementally unassigning partitions: %v\n", err)
				return err
			}
		}

	default:
		fmt.Fprintf(os.Stderr, "%% Unexpected rebalance event type: %T\n", e)
	}
	return nil
}

// statsCallback processes statistics from librdkafka
func statsCallback(stats string) {
	// Only apply workaround for consumer protocol (KIP-848)
	// The classic protocol correctly invokes callbacks for empty assignments
	if !state.isCooperative {
		return
	}

	// Parse the stats JSON to check consumer state and assignment
	var statsData map[string]interface{}
	err := json.Unmarshal([]byte(stats), &statsData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%% Failed to parse stats: %v\n", err)
		return
	}

	// Check consumer group state
	cgrp, ok := statsData["cgrp"].(map[string]interface{})
	if !ok {
		return
	}

	cgrpState, _ := cgrp["state"].(string)
	cgrpStateAge, _ := cgrp["stateage"].(float64)

	// Track state changes
	if cgrpState != lastConsumerState {
		fmt.Fprintf(os.Stderr, "%% Consumer state changed: %s -> %s\n", lastConsumerState, cgrpState)
		lastConsumerState = cgrpState
	}

	// WORKAROUND for librdkafka bug in consumer protocol (KIP-848):
	// When a consumer joins with an empty assignment, librdkafka skips the rebalance
	// callback because it considers the assignment "unchanged" (comparing two empty lists).
	// This is a bug in librdkafka's optimization logic at rdkafka_cgrp.c:2844-2865.
	// The Java consumer always invokes the callback on join, even with empty assignments.
	// This workaround ensures consistent behavior by manually triggering the callback.
	if cgrpState == "up" && cgrpStateAge < 2000 && !lastAssignmentReported {
		// Consumer just joined and is stable, check if we have assignment
		assignment, err := state.consumer.Assignment()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%% Failed to get assignment: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "%% Consumer joined group with assignment: %v\n", assignment)
			// If no partitions assigned and callback wasn't called, trigger it manually
			if len(assignment) == 0 {
				fmt.Fprintf(os.Stderr, "%% Manually triggering onPartitionsAssigned with empty assignment (consumer protocol workaround)\n")
				onPartitionsAssigned([]kafka.TopicPartition{})
				lastAssignmentReported = true
			}
		}
	}
}

// handleEvent processes consumer events from Poll()
func handleEvent(ev kafka.Event) {
	switch e := ev.(type) {
	case *kafka.Message:
		if e.TopicPartition.Error != nil {
			fmt.Fprintf(os.Stderr, "%% Message error: %v\n", e.TopicPartition.Error)
			return
		}

		// Batch messages for processing
		records := []*kafka.Message{e}
		offsets := onRecordsReceived(records)

		// Commit if not using auto-commit
		if !state.useAutoCommit && len(offsets) > 0 {
			if state.useAsyncCommit {
				commitAsync(offsets)
			} else {
				commitSync(offsets)
			}
		}

		// Check if finished
		if isFinished() {
			state.run = false
		}

	case kafka.OffsetsCommitted:
		// This event is received for async commits
		if state.useAsyncCommit {
			commitData := []CommitData{}
			for _, tp := range e.Offsets {
				commitData = append(commitData, CommitData{
					Topic:     *tp.Topic,
					Partition: tp.Partition,
					Offset:    int64(tp.Offset),
				})
			}

			event := OffsetsCommittedEvent{
				ConsumerEvent: ConsumerEvent{
					Name:      "offsets_committed",
					Timestamp: time.Now().UnixMilli(),
				},
				Offsets: commitData,
				Success: e.Error == nil,
			}

			if e.Error != nil {
				event.Error = e.Error.Error()
			}

			send(event)
		}

	case *kafka.Stats:
		// Process statistics to track consumer state
		statsCallback(e.String())

	case kafka.Error:
		// Check if it's a fatal error
		if e.Code() == kafka.ErrUnknownTopicOrPart {
			fmt.Fprintf(os.Stderr, "%% Ignorable error: %v\n", e)
		} else {
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			// Some errors are fatal
			if e.IsFatal() {
				fmt.Fprintf(os.Stderr, "%% Fatal error detected, terminating immediately\n")
				// Send shutdown event and exit immediately
				// Don't call Close() as it may hang trying to leave the consumer group
				send(ShutdownCompleteEvent{
					ConsumerEvent: ConsumerEvent{
						Name:      "shutdown_complete",
						Timestamp: time.Now().UnixMilli(),
					},
				})
				os.Exit(1)
			}
		}

	default:
		if verbosity >= 2 {
			fmt.Fprintf(os.Stderr, "%% Ignored event: %T %v\n", e, e)
		}
	}
}

// run is the main consumer loop
func run() {
	defer func() {
		state.consumer.Close()
		send(ShutdownCompleteEvent{
			ConsumerEvent: ConsumerEvent{
				Name:      "shutdown_complete",
				Timestamp: time.Now().UnixMilli(),
			},
		})
	}()

	// Subscribe to topic with rebalance callback
	err := state.consumer.SubscribeTopics([]string{state.topic}, rebalanceCallback)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to topic: %v\n", err)
		return
	}

	send(StartupCompleteEvent{
		ConsumerEvent: ConsumerEvent{
			Name:      "startup_complete",
			Timestamp: time.Now().UnixMilli(),
		},
	})

	state.run = true

	// Main poll loop
	for state.run {
		select {
		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			state.run = false
			return

		default:
			// Poll with timeout (note: -1 would block indefinitely)
			ev := state.consumer.Poll(100)
			if ev != nil {
				handleEvent(ev)
				// Check if a fatal error was encountered and exit immediately
				if !state.run {
					fmt.Fprintf(os.Stderr, "%% Exiting main loop due to fatal error\n")
					return
				}
			}
		}
	}
}

// loadPropertiesFile loads a properties file into the config map
func loadPropertiesFile(filename string, config *kafka.ConfigMap) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Simple properties file parser
	var lines []string
	buf := make([]byte, 1024)
	content := ""
	for {
		n, err := file.Read(buf)
		if n > 0 {
			content += string(buf[:n])
		}
		if err != nil {
			break
		}
	}

	lines = strings.Split(content, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Skip empty lines and comments
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			(*config)[key] = value
		}
	}

	return nil
}

func main() {
	sigs = make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Default config
	conf := kafka.ConfigMap{}

	/* Required options */
	topic := kingpin.Flag("topic", "Consumes messages from this topic").Required().String()
	brokers := kingpin.Flag("bootstrap-server", "The server(s) to connect to").String()
	brokerList := kingpin.Flag("broker-list", "The server(s) to connect to (alias for --bootstrap-server)").String()
	groupId := kingpin.Flag("group-id", "The group id of the consumer group").Required().String()

	/* Optional options */
	groupProtocol := kingpin.Flag("group-protocol", "Group protocol (classic or consumer)").Default("classic").String()
	groupRemoteAssignor := kingpin.Flag("group-remote-assignor", "Group remote assignor").String()
	groupInstanceId := kingpin.Flag("group-instance-id", "A unique identifier of the consumer instance").String()
	maxMessages := kingpin.Flag("max-messages", "Consume this many messages. If -1, consume until killed externally").Default("-1").Int()
	sessionTimeout := kingpin.Flag("session-timeout", "Set the consumer's session timeout in ms").Default("-1").Int()
	verbose := kingpin.Flag("verbose", "Enable to log individual consumed records").Default("false").Bool()
	enableAutocommit := kingpin.Flag("enable-autocommit", "Enable offset auto-commit on consumer").Default("false").Bool()
	resetPolicy := kingpin.Flag("reset-policy", "Set reset policy (earliest, latest, or none)").Default("earliest").String()
	assignmentStrategy := kingpin.Flag("assignment-strategy", "Set assignment strategy (Java class name)").Default("org.apache.kafka.clients.consumer.RangeAssignor").String()
	consumerConfig := kingpin.Flag("consumer.config", "(DEPRECATED) Consumer config properties file").String()
	commandConfig := kingpin.Flag("command-config", "Config properties file").String()
	debug := kingpin.Flag("debug", "Debug flags").String()
	xconf := kingpin.Flag("property", "CSV separated key=value librdkafka configuration properties").Short('X').String()

	kingpin.Parse()

	// Handle broker-list and bootstrap-server flags (broker-list is for backward compatibility)
	var bootstrapServers string
	if *brokers != "" {
		bootstrapServers = *brokers
	} else if *brokerList != "" {
		bootstrapServers = *brokerList
	} else {
		fmt.Fprintf(os.Stderr, "Error: either --bootstrap-server or --broker-list must be specified\n")
		os.Exit(1)
	}

	// Set required config
	conf["bootstrap.servers"] = bootstrapServers
	conf["group.id"] = *groupId
	conf["enable.auto.commit"] = *enableAutocommit
	conf["auto.offset.reset"] = *resetPolicy
	conf["group.protocol"] = *groupProtocol

	// Disable automatic offset store to match Java consumer behavior
	//
	// IMPORTANT: Java KafkaConsumer does not have enable.auto.offset.store config.
	// In Java, offsets are only committed for messages from previous poll() cycles,
	// never for messages from the current poll() that haven't been processed yet.
	//
	// librdkafka default (enable.auto.offset.store=true) is MORE aggressive:
	// - Offsets are stored immediately when poll() returns, before processing
	// - If consumer crashes after poll() but before processing, those offsets may be auto-committed
	// - This causes message loss as next consumer resumes from the stored offset
	//
	// By setting enable.auto.offset.store=false and calling StoreOffsets() after processing,
	// we replicate Java's behavior where offsets are only committed for successfully
	// processed messages. This prevents message loss during consumer failures.
	conf["enable.auto.offset.store"] = false

	// Enable rebalance callback
	conf["go.application.rebalance.enable"] = true
	conf["go.events.channel.enable"] = false

	// Enable statistics callback to track consumer state
	conf["statistics.interval.ms"] = 1000

	// Load config files if specified
	if *consumerConfig != "" && *commandConfig != "" {
		fmt.Fprintf(os.Stderr, "Error: Options --consumer.config and --command-config are mutually exclusive\n")
		os.Exit(1)
	}

	if *consumerConfig != "" {
		fmt.Fprintf(os.Stderr, "Option --consumer.config has been deprecated and will be removed in a future version. Use --command-config instead.\n")
		err := loadPropertiesFile(*consumerConfig, &conf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load consumer config: %v\n", err)
			os.Exit(1)
		}
	}

	if *commandConfig != "" {
		err := loadPropertiesFile(*commandConfig, &conf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load command config: %v\n", err)
			os.Exit(1)
		}
	}

	// Set group remote assignor if using consumer protocol
	if *groupProtocol == "consumer" && *groupRemoteAssignor != "" {
		conf["group.remote.assignor"] = *groupRemoteAssignor
	}

	// Set assignment strategy if using classic protocol
	if *groupProtocol == "classic" && *assignmentStrategy != "" {
		// Convert Java class name to librdkafka strategy name
		var strats []string
		for _, jstrat := range strings.Split(*assignmentStrategy, ",") {
			parts := strings.Split(jstrat, ".")
			stratName := parts[len(parts)-1]
			stratName = strings.TrimSuffix(stratName, "Assignor")
			stratName = strings.ToLower(stratName)

			// Note: librdkafka supports: range, roundrobin, cooperative-sticky
			if stratName == "cooperativesticky" {
				stratName = "cooperative-sticky"
			}

			strats = append(strats, stratName)
		}
		conf["partition.assignment.strategy"] = strings.Join(strats, ",")
		if verbosity >= 1 {
			fmt.Fprintf(os.Stderr, "%% Mapped assignment strategy %s -> %s\n",
				*assignmentStrategy, conf["partition.assignment.strategy"])
		}
	}

	// Set session timeout if specified
	if *sessionTimeout > 0 {
		conf["session.timeout.ms"] = *sessionTimeout
	}

	// Set group instance id if specified
	if *groupInstanceId != "" {
		conf["group.instance.id"] = *groupInstanceId
	}

	if len(*debug) > 0 {
		conf["debug"] = *debug
	}

	// Handle -X properties
	if len(*xconf) > 0 {
		for _, kv := range strings.Split(*xconf, ",") {
			x := strings.Split(kv, "=")
			if len(x) != 2 {
				fmt.Fprintf(os.Stderr, "-X expects a ,-separated list of confprop=val pairs\n")
				os.Exit(1)
			}
			conf[x[0]] = x[1]
		}
	}

	if verbosity >= 1 {
		fmt.Fprintf(os.Stderr, "Config: %v\n", conf)
	}

	// Create consumer
	consumer, err := kafka.NewConsumer(&conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %v\n", err)
		os.Exit(1)
	}

	_, verstr := kafka.LibraryVersion()
	fmt.Fprintf(os.Stderr, "%% Created Consumer %v (%s)\n", consumer, verstr)

	// Initialize state
	state.consumer = consumer
	state.topic = *topic
	state.maxMessages = *maxMessages
	state.useAutoCommit = *enableAutocommit
	state.useAsyncCommit = false // Java implementation uses sync by default
	state.verbose = *verbose
	state.currentAssignment = make(map[string]*PartitionState)

	// Determine if we need to use cooperative rebalancing methods (incremental assign/unassign)
	// This is required when:
	// 1. Using consumer protocol (KIP-848), OR
	// 2. Using classic protocol with a cooperative assignment strategy (e.g., cooperative-sticky)
	state.isCooperative = (*groupProtocol == "consumer")
	if !state.isCooperative {
		// Check if using a cooperative assignment strategy in classic protocol
		if strategyVal, ok := conf["partition.assignment.strategy"]; ok {
			if strategy, ok := strategyVal.(string); ok {
				state.isCooperative = strings.Contains(strategy, "cooperative")
			}
		}
	}

	// Run consumer
	run()
}
