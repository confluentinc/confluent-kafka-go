// Apache Kafka kafkatest VerifiableProducer implemented in Go
package main

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

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	verbosity = 1
	sigs      chan os.Signal
)

func send(name string, msg map[string]interface{}) {
	if msg == nil {
		msg = make(map[string]interface{})
	}
	msg["name"] = name
	msg["_time"] = time.Now().Format("2006-01-02 15:04:05.000")
	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
}

func partitionsToMap(partitions []kafka.TopicPartition) []map[string]interface{} {
	parts := make([]map[string]interface{}, len(partitions))
	for i, tp := range partitions {
		parts[i] = map[string]interface{}{"topic": *tp.Topic, "partition": tp.Partition}
	}
	return parts
}

func sendPartitions(name string, partitions []kafka.TopicPartition) {

	msg := make(map[string]interface{})
	msg["partitions"] = partitionsToMap(partitions)

	send(name, msg)
}

type commState struct {
	maxMessages int // messages to send
	msgCnt      int // messages produced
	deliveryCnt int // messages delivered
	errCnt      int // messages failed to deliver
	valuePrefix string
	throughput  int
	p           *kafka.Producer
}

var state commState

// handle_dr handles delivery reports
// returns false when producer should terminate, else true to keep running.
func handleDr(m *kafka.Message) bool {
	if verbosity >= 2 {
		fmt.Fprintf(os.Stderr, "%% DR: %v:\n", m.TopicPartition)
	}

	if m.TopicPartition.Error != nil {
		state.errCnt++
		errmsg := make(map[string]interface{})
		errmsg["message"] = m.TopicPartition.Error.Error()
		errmsg["topic"] = *m.TopicPartition.Topic
		errmsg["partition"] = m.TopicPartition.Partition
		errmsg["key"] = (string)(m.Key)
		errmsg["value"] = (string)(m.Value)
		send("producer_send_error", errmsg)
	} else {
		state.deliveryCnt++
		drmsg := make(map[string]interface{})
		drmsg["topic"] = *m.TopicPartition.Topic
		drmsg["partition"] = m.TopicPartition.Partition
		drmsg["offset"] = m.TopicPartition.Offset
		drmsg["key"] = (string)(m.Key)
		drmsg["value"] = (string)(m.Value)
		send("producer_send_success", drmsg)
	}

	if state.deliveryCnt+state.errCnt >= state.maxMessages {
		// we're done
		return false
	}

	return true

}

func runProducer(config *kafka.ConfigMap, topic string) {
	p, err := kafka.NewProducer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	_, verstr := kafka.LibraryVersion()
	fmt.Fprintf(os.Stderr, "%% Created Producer %v (%s)\n", p, verstr)
	state.p = p

	send("startup_complete", nil)
	run := true

	throttle := time.NewTicker(time.Second / (time.Duration)(state.throughput))
	for run == true {
		select {
		case <-throttle.C:
			// produce a message (async) on each throttler tick
			value := fmt.Sprintf("%s%d", state.valuePrefix, state.msgCnt)
			state.msgCnt++
			err := p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny},
				Value: []byte(value)}, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%% Produce failed: %v\n", err)
				state.errCnt++
			}

			if state.msgCnt == state.maxMessages {
				// all messages sent, now wait for deliveries
				throttle.Stop()
			}

		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			run = false

		case ev := <-p.Events():
			switch e := ev.(type) {
			case *kafka.Message:
				run = handleDr(e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			default:
				fmt.Fprintf(os.Stderr, "%% Unhandled event %T ignored: %v\n", e, e)
			}
		}
	}

	fmt.Fprintf(os.Stderr, "%% Closing, %d/%d messages delivered, %d failed\n", state.deliveryCnt, state.msgCnt, state.errCnt)

	p.Close()

	send("shutdown_complete", nil)
}

func main() {
	sigs = make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Default config
	conf := kafka.ConfigMap{"default.topic.config": kafka.ConfigMap{
		"auto.offset.reset":     "earliest",
		"produce.offset.report": true}}

	/* Required options */
	topic := kingpin.Flag("topic", "Topic").Required().String()
	brokers := kingpin.Flag("broker-list", "Bootstrap broker(s)").Required().String()

	/* Optionals */
	throughput := kingpin.Flag("throughput", "Msgs/s").Default("1000000").Int()
	maxMessages := kingpin.Flag("max-messages", "Max message count").Default("1000000").Int()
	valuePrefix := kingpin.Flag("value-prefix", "Payload value string prefix").Default("").String()
	acks := kingpin.Flag("acks", "Required acks").Default("all").String()
	configFile := kingpin.Flag("producer.config", "Config file").File()
	debug := kingpin.Flag("debug", "Debug flags").String()
	xconf := kingpin.Flag("--property", "CSV separated key=value librdkafka configuration properties").Short('X').String()

	kingpin.Parse()

	conf["bootstrap.servers"] = *brokers
	conf["default.topic.config"].(kafka.ConfigMap).SetKey("acks", *acks)

	if len(*debug) > 0 {
		conf["debug"] = *debug
	}

	if len(*xconf) > 0 {
		for _, kv := range strings.Split(*xconf, ",") {
			x := strings.Split(kv, "=")
			if len(x) != 2 {
				panic("-X expects a ,-separated list of confprop=val pairs")
			}
			conf[x[0]] = x[1]
		}
	}
	fmt.Println("Config: ", conf)

	if *configFile != nil {
		fmt.Fprintf(os.Stderr, "%% Ignoring config file %v\n", *configFile)
	}

	if len(*valuePrefix) > 0 {
		state.valuePrefix = fmt.Sprintf("%s.", *valuePrefix)
	} else {
		state.valuePrefix = ""
	}

	state.throughput = *throughput
	state.maxMessages = *maxMessages
	runProducer((*kafka.ConfigMap)(&conf), *topic)

}
