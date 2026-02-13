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

// Apache Kafka kafkatest VerifiableProducer implemented in Go
// This implementation is functionally equivalent to the Java VerifiableProducer
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	verbosity = 1
	sigs      chan os.Signal
)

// ProducerState holds the state of the producer
type ProducerState struct {
	maxMessages   int64  // messages to send (-1 for infinite)
	msgCnt        int64  // messages sent (attempts)
	deliveryCnt   int64  // messages delivered successfully
	errCnt        int64  // messages failed to deliver
	valuePrefix   *int   // prefix for message values
	throughput    int64  // target throughput (msgs/sec), -1 for unlimited
	repeatingKeys *int   // if set, use keys 0 to repeatingKeys-1 in rotation
	keyCounter    int    // current key value
	createTime    *int64 // custom create time in ms since epoch
	startTime     int64  // start time in ms since epoch
	p             *kafka.Producer
	stopProducing bool // flag to stop producing
}

var state ProducerState

// Event types for JSON output
type ProducerEvent struct {
	Name      string `json:"name"`
	Timestamp int64  `json:"timestamp"`
}

type StartupCompleteEvent struct {
	ProducerEvent
}

type ShutdownCompleteEvent struct {
	ProducerEvent
}

type SuccessfulSendEvent struct {
	ProducerEvent
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
}

type FailedSendEvent struct {
	ProducerEvent
	Topic     string `json:"topic"`
	Partition int32  `json:"partition,omitempty"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
	Exception string `json:"exception,omitempty"`
	Message   string `json:"message"`
}

type ToolDataEvent struct {
	ProducerEvent
	Sent             int64   `json:"sent"`
	Acked            int64   `json:"acked"`
	TargetThroughput int64   `json:"target_throughput"`
	AvgThroughput    float64 `json:"avg_throughput"`
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

// getValue returns the message value for a given counter
func (s *ProducerState) getValue(counter int64) string {
	if s.valuePrefix != nil {
		return fmt.Sprintf("%d.%d", *s.valuePrefix, counter)
	}
	return fmt.Sprintf("%d", counter)
}

// getKey returns the message key based on repeatingKeys setting
func (s *ProducerState) getKey() *string {
	if s.repeatingKeys == nil {
		return nil
	}
	key := strconv.Itoa(s.keyCounter)
	s.keyCounter++
	if s.keyCounter >= *s.repeatingKeys {
		s.keyCounter = 0
	}
	return &key
}

// getCreateTime returns the timestamp to use for the message
func (s *ProducerState) getCreateTime() *time.Time {
	if s.createTime == nil {
		return nil
	}
	// Calculate the timestamp based on elapsed time since start
	elapsed := time.Now().UnixMilli() - s.startTime
	timestamp := time.UnixMilli(*s.createTime + elapsed)
	return &timestamp
}

// handleDeliveryReport handles delivery reports from the producer
// returns false when producer should terminate, else true to keep running
func handleDeliveryReport(m *kafka.Message) {
	if verbosity >= 2 {
		fmt.Fprintf(os.Stderr, "%% DR: %v:\n", m.TopicPartition)
	}

	if m.TopicPartition.Error != nil {
		state.errCnt++
		event := FailedSendEvent{
			ProducerEvent: ProducerEvent{
				Name:      "producer_send_error",
				Timestamp: time.Now().UnixMilli(),
			},
			Topic:     *m.TopicPartition.Topic,
			Partition: m.TopicPartition.Partition,
			Message:   m.TopicPartition.Error.Error(),
		}
		if m.Key != nil {
			event.Key = string(m.Key)
		}
		if m.Value != nil {
			event.Value = string(m.Value)
		}
		send(event)
	} else {
		state.deliveryCnt++
		event := SuccessfulSendEvent{
			ProducerEvent: ProducerEvent{
				Name:      "producer_send_success",
				Timestamp: time.Now().UnixMilli(),
			},
			Topic:     *m.TopicPartition.Topic,
			Partition: m.TopicPartition.Partition,
			Offset:    int64(m.TopicPartition.Offset),
			Value:     string(m.Value),
		}
		if m.Key != nil {
			event.Key = string(m.Key)
		}
		send(event)
	}
}

// ThroughputThrottler manages message throughput
type ThroughputThrottler struct {
	targetThroughput int64
	startMs          int64
	sleepDeficitNs   int64
}

// NewThroughputThrottler creates a new throttler
func NewThroughputThrottler(targetThroughput int64, startMs int64) *ThroughputThrottler {
	return &ThroughputThrottler{
		targetThroughput: targetThroughput,
		startMs:          startMs,
		sleepDeficitNs:   0,
	}
}

// shouldThrottle determines if throttling is needed
func (t *ThroughputThrottler) shouldThrottle(recordsSent int64, sendStartMs int64) bool {
	if t.targetThroughput < 0 {
		return false
	}

	elapsedMs := sendStartMs - t.startMs
	if elapsedMs <= 0 {
		return false
	}

	// Calculate expected number of messages at this time
	expectedRecords := (t.targetThroughput * elapsedMs) / 1000
	return recordsSent >= expectedRecords
}

// throttle performs the actual throttling
func (t *ThroughputThrottler) throttle() {
	if t.targetThroughput < 0 {
		return
	}

	// Calculate how long to sleep to maintain target throughput
	sleepTimeNs := (1_000_000_000 / t.targetThroughput) - t.sleepDeficitNs
	if sleepTimeNs > 0 {
		sleepStart := time.Now().UnixNano()
		time.Sleep(time.Duration(sleepTimeNs) * time.Nanosecond)
		sleepEnd := time.Now().UnixNano()
		// Track deficit/surplus for next iteration
		t.sleepDeficitNs = (sleepEnd - sleepStart) - sleepTimeNs
	} else {
		t.sleepDeficitNs = -sleepTimeNs
	}
}

// produceMessage sends a single message
func produceMessage(topic string, key *string, value string, timestamp *time.Time) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(value),
	}

	if key != nil {
		msg.Key = []byte(*key)
	}

	if timestamp != nil {
		msg.Timestamp = *timestamp
	}

	state.msgCnt++

	err := state.p.Produce(msg, nil)
	if err != nil {
		// Synchronous error - send failed immediately
		state.errCnt++
		event := FailedSendEvent{
			ProducerEvent: ProducerEvent{
				Name:      "producer_send_error",
				Timestamp: time.Now().UnixMilli(),
			},
			Topic:     topic,
			Message:   err.Error(),
			Exception: fmt.Sprintf("%T", err),
		}
		if key != nil {
			event.Key = *key
		}
		event.Value = value
		send(event)
		return err
	}

	return nil
}

// runProducer runs the main producer loop
func runProducer(config *kafka.ConfigMap, topic string) {
	p, err := kafka.NewProducer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	_, verstr := kafka.LibraryVersion()
	fmt.Fprintf(os.Stderr, "%% Created Producer %v (%s)\n", p, verstr)
	state.p = p

	// Send startup complete event
	send(StartupCompleteEvent{
		ProducerEvent: ProducerEvent{
			Name:      "startup_complete",
			Timestamp: time.Now().UnixMilli(),
		},
	})

	// Determine max messages to send
	maxMessages := state.maxMessages
	if maxMessages < 0 {
		maxMessages = int64(^uint64(0) >> 1) // Max int64
	}

	throttler := NewThroughputThrottler(state.throughput, time.Now().UnixMilli())

	// Start event handler goroutine
	eventDone := make(chan bool)
	go func() {
		for ev := range p.Events() {
			switch e := ev.(type) {
			case *kafka.Message:
				handleDeliveryReport(e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			default:
				if verbosity >= 2 {
					fmt.Fprintf(os.Stderr, "%% Ignored event: %v\n", e)
				}
			}
		}
		eventDone <- true
	}()

	// Main production loop
	for i := int64(0); i < maxMessages; i++ {
		if state.stopProducing {
			break
		}

		// Check for signals
		select {
		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			state.stopProducing = true
			goto done
		default:
		}

		sendStartMs := time.Now().UnixMilli()

		key := state.getKey()
		value := state.getValue(i)
		timestamp := state.getCreateTime()

		produceMessage(topic, key, value, timestamp)

		// Throttle if necessary
		if throttler.shouldThrottle(i+1, sendStartMs) {
			throttler.throttle()
		}
	}

done:
	fmt.Fprintf(os.Stderr, "%% Waiting for remaining delivery reports...\n")

	// Wait for all outstanding messages to be delivered
	// Check every 100ms if all messages have been delivered
	for {
		remaining := state.p.Len()
		if remaining == 0 {
			break
		}
		if verbosity >= 2 {
			fmt.Fprintf(os.Stderr, "%% Waiting for %d messages to be delivered\n", remaining)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Flush any remaining messages
	state.p.Flush(30000) // 30 second timeout

	fmt.Fprintf(os.Stderr, "%% Closing, %d/%d messages delivered, %d failed\n",
		state.deliveryCnt, state.msgCnt, state.errCnt)

	state.p.Close()
	<-eventDone

	// Send shutdown complete event
	send(ShutdownCompleteEvent{
		ProducerEvent: ProducerEvent{
			Name:      "shutdown_complete",
			Timestamp: time.Now().UnixMilli(),
		},
	})

	// Send tool_data event
	stopMs := time.Now().UnixMilli()
	elapsedSec := float64(stopMs-state.startTime) / 1000.0
	avgThroughput := 0.0
	if elapsedSec > 0 {
		avgThroughput = float64(state.deliveryCnt) / elapsedSec
	}

	send(ToolDataEvent{
		ProducerEvent: ProducerEvent{
			Name:      "tool_data",
			Timestamp: time.Now().UnixMilli(),
		},
		Sent:             state.msgCnt,
		Acked:            state.deliveryCnt,
		TargetThroughput: state.throughput,
		AvgThroughput:    avgThroughput,
	})
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
	topic := kingpin.Flag("topic", "Produce messages to this topic").Required().String()
	brokers := kingpin.Flag("bootstrap-server", "REQUIRED: The server(s) to connect to").Required().String()

	/* Optional options */
	maxMessages := kingpin.Flag("max-messages", "Produce this many messages. If -1, produce until killed externally").Default("-1").Int64()
	throughput := kingpin.Flag("throughput", "If set >= 0, throttle maximum message throughput to approximately THROUGHPUT messages/sec").Default("-1").Int64()
	acks := kingpin.Flag("acks", "Acks required on each produced message").Default("-1").String()
	valuePrefix := kingpin.Flag("value-prefix", "If specified, each produced value will have this prefix with a dot separator").Default("-999999").Int()
	repeatingKeys := kingpin.Flag("repeating-keys", "If specified, each produced record will have a key starting at 0 increment by 1 up to the number specified (exclusive), then the key is set to 0 again").Default("-1").Int()
	createTime := kingpin.Flag("message-create-time", "Send messages with creation time starting at the arguments value, in milliseconds since epoch").Default("-1").Int64()
	producerConfig := kingpin.Flag("producer.config", "(DEPRECATED) Producer config properties file").String()
	commandConfig := kingpin.Flag("command-config", "Config properties file").String()
	debug := kingpin.Flag("debug", "Debug flags").String()
	xconf := kingpin.Flag("property", "CSV separated key=value librdkafka configuration properties").Short('X').String()

	kingpin.Parse()

	// Set bootstrap servers and acks
	conf["bootstrap.servers"] = *brokers
	conf["acks"] = *acks

	// Set retries to 0 (like Java implementation)
	conf["retries"] = "0"

	// Load config files if specified
	if *producerConfig != "" && *commandConfig != "" {
		fmt.Fprintf(os.Stderr, "Error: Options --producer.config and --command-config are mutually exclusive\n")
		os.Exit(1)
	}

	if *producerConfig != "" {
		fmt.Fprintf(os.Stderr, "Option --producer.config has been deprecated and will be removed in a future version. Use --command-config instead.\n")
		err := loadPropertiesFile(*producerConfig, &conf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load producer config: %v\n", err)
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

	// Initialize state
	state.maxMessages = *maxMessages
	state.throughput = *throughput
	state.startTime = time.Now().UnixMilli()

	// Check if value-prefix was explicitly set (we use -999999 as sentinel)
	if *valuePrefix != -999999 {
		state.valuePrefix = valuePrefix
	}

	// Check if repeating-keys was explicitly set
	if *repeatingKeys != -1 {
		state.repeatingKeys = repeatingKeys
	}

	// Check if message-create-time was explicitly set
	if *createTime != -1 {
		state.createTime = createTime
	}

	// Run producer
	runProducer(&conf, *topic)
}
