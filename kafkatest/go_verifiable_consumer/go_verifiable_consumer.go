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
package main

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
	verbosity     = 1
	exit_eof      = false
	eof_cnt       = 0
	partition_cnt = 0
	key_delim     = ""
	sigs          chan os.Signal
)

func fatal(why string) {
	fmt.Fprintf(os.Stderr, "%% FATAL ERROR: %s", why)
	panic(why)
}

func send(name string, msg map[string]interface{}) {
	if msg == nil {
		msg = make(map[string]interface{})
	}
	msg["name"] = name
	msg["_time"] = time.Now().Unix()
	b, err := json.Marshal(msg)
	if err != nil {
		fatal(fmt.Sprintf("json.Marshal failed: %v", err))
	}
	fmt.Println(string(b))
}

func partitions_to_map(partitions []kafka.TopicPartition) []map[string]interface{} {
	parts := make([]map[string]interface{}, len(partitions))
	for i, tp := range partitions {
		parts[i] = map[string]interface{}{"topic": *tp.Topic, "partition": tp.Partition, "offset": tp.Offset}
	}
	return parts
}

func send_offsets_committed(offsets []kafka.TopicPartition, err error) {
	if len(state.curr_assignment) == 0 {
		// Dont emit offsets_committed if there is no current assignment
		// This happens when auto_commit is enabled since we also
		// force a manual commit on rebalance to make sure
		// offsets_committed is emitted prior to partitions_revoked,
		// so the builtin auto committer will also kick in and post
		// this later OffsetsCommitted event which we simply ignore..
		fmt.Fprintf(os.Stderr, "%% Ignore OffsetsCommitted(%v) without a valid assignment\n", err)
		return
	}
	msg := make(map[string]interface{})

	if err != nil {
		msg["success"] = false
		msg["error"] = fmt.Sprintf("%v", err)

		kerr, ok := err.(kafka.KafkaError)
		if ok && kerr.Code() == kafka.ERR__NO_OFFSET {
			fmt.Fprintf(os.Stderr, "%% No offsets to commit\n")
			return
		}

		fmt.Fprintf(os.Stderr, "%% Commit failed: %v", msg["error"])
	} else {
		msg["success"] = true

	}

	if offsets != nil {
		msg["offsets"] = partitions_to_map(offsets)
	}

	// Make sure we report consumption before commit,
	// otherwise tests may fail because of commit > consumed
	send_records_consumed(true)

	send("offsets_committed", msg)
}

func send_partitions(name string, partitions []kafka.TopicPartition) {

	msg := make(map[string]interface{})
	msg["partitions"] = partitions_to_map(partitions)

	send(name, msg)
}

type assigned_partition struct {
	tp            kafka.TopicPartition
	consumed_msgs int
	min_offset    int64
	max_offset    int64
}

func assignment_key(tp kafka.TopicPartition) string {
	return fmt.Sprintf("%s-%d", *tp.Topic, tp.Partition)
}

func find_assignment(tp kafka.TopicPartition) *assigned_partition {
	a, ok := state.curr_assignment[assignment_key(tp)]
	if !ok {
		return nil
	}
	return a
}

func add_assignment(tp kafka.TopicPartition) {
	state.curr_assignment[assignment_key(tp)] = &assigned_partition{tp: tp, min_offset: -1, max_offset: -1}
}

func clear_curr_assignment() {
	state.curr_assignment = make(map[string]*assigned_partition)
}

type comm_state struct {
	run                          bool
	consumed_msgs                int
	consumed_msgs_last_reported  int
	consumed_msgs_at_last_commit int
	curr_assignment              map[string]*assigned_partition
	max_messages                 int
	auto_commit                  bool
	async_commit                 bool
	c                            *kafka.Consumer
}

var state comm_state

func send_records_consumed(immediate bool) {
	if len(state.curr_assignment) == 0 ||
		(!immediate && state.consumed_msgs_last_reported+1000 > state.consumed_msgs) {
		return
	}

	msg := map[string]interface{}{}
	msg["count"] = state.consumed_msgs - state.consumed_msgs_last_reported
	parts := make([]map[string]interface{}, len(state.curr_assignment))
	i := 0
	for _, a := range state.curr_assignment {
		if a.min_offset == -1 {
			// Skip partitions that havent had any messages since last time.
			// This is to circumvent some minOffset checks in kafkatest.
			continue
		}
		parts[i] = map[string]interface{}{"topic": *a.tp.Topic,
			"partition":     a.tp.Partition,
			"consumed_msgs": a.consumed_msgs,
			"minOffset":     a.min_offset,
			"maxOffset":     a.max_offset}
		a.min_offset = -1
		i += 1
	}
	msg["partitions"] = parts[0:i]

	send("records_consumed", msg)

	state.consumed_msgs_last_reported = state.consumed_msgs
}

// do_commit commits every 1000 messages or whenever there is a consume timeout, or when immediate==true
func do_commit(immediate bool, async bool) {
	if !immediate &&
		(state.auto_commit ||
			state.consumed_msgs_at_last_commit+1000 > state.consumed_msgs) {
		return
	}

	async = state.async_commit

	fmt.Fprintf(os.Stderr, "%% Committing %d messages (async=%v)\n",
		state.consumed_msgs-state.consumed_msgs_at_last_commit, async)

	state.consumed_msgs_at_last_commit = state.consumed_msgs

	var wait_committed chan bool

	if !async {
		wait_committed = make(chan bool)
	}

	go func() {
		offsets, err := state.c.Commit()

		send_offsets_committed(offsets, err)

		if !async {
			close(wait_committed)
		}
	}()

	if !async {
		_, _ = <-wait_committed
	}
}

// returns false when consumer should terminate, else true to keep running.
func handle_msg(m *kafka.Message) bool {
	if verbosity >= 2 {
		fmt.Fprintf(os.Stderr, "%% Message receved: %v:\n", m.TopicPartition)
	}

	a := find_assignment(m.TopicPartition)
	if a == nil {
		fmt.Fprintf(os.Stderr, "%% Received message on unassigned partition: %v\n", m.TopicPartition)
		return true
	}

	a.consumed_msgs += 1
	offset := int64(m.TopicPartition.Offset)
	if a.min_offset == -1 {
		a.min_offset = offset
	}
	if a.max_offset < offset {
		a.max_offset = offset
	}

	state.consumed_msgs += 1

	send_records_consumed(false)
	do_commit(false, state.async_commit)

	if state.max_messages > 0 && state.consumed_msgs >= state.max_messages {
		// ignore extra messages
		return false
	}

	return true

}

// handle_event handles an event as returned by Poll().
func handle_event(c *kafka.Consumer, ev kafka.Event) {
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		if len(state.curr_assignment) > 0 {
			fatal(fmt.Sprintf("Assign: curr_assignment should have been empty: %v", state.curr_assignment))
		}
		state.curr_assignment = make(map[string]*assigned_partition)
		for _, tp := range e.Partitions {
			add_assignment(tp)
		}
		send_partitions("partitions_assigned", e.Partitions)
		c.Assign(e.Partitions)

	case kafka.RevokedPartitions:
		send_records_consumed(true)
		do_commit(true, false)
		send_partitions("partitions_revoked", e.Partitions)
		clear_curr_assignment()
		c.Unassign()

	case kafka.OffsetsCommitted:
		send_offsets_committed(e.Offsets, e.Error)

	case *kafka.Message:
		state.run = handle_msg(e)

	case kafka.KafkaError:
		if e.Code() == kafka.ERR_UNKNOWN_TOPIC_OR_PART {
			fmt.Fprintf(os.Stderr,
				"%% Ignoring transient error: %v\n", e)
		} else {
			fatal(fmt.Sprintf("%% KafkaError: %v\n", e))
		}

	default:
		fmt.Fprintf(os.Stderr, "%% Unhandled event %T ignored: %v\n", e, e)
	}
}

// main_loop serves consumer events, signals, etc.
// will run for at most (roughly) \p timeout seconds.
func main_loop(c *kafka.Consumer, timeout int) {
	tmout := time.NewTicker(time.Duration(timeout) * time.Second)
	every1s := time.NewTicker(1 * time.Second)

out:
	for state.run == true {
		select {

		case _ = <-tmout.C:
			tmout.Stop()
			break out

		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			state.run = false

		case _ = <-every1s.C:
			// Report consumed messages
			send_records_consumed(true)
			// Commit on timeout as well (not just every 1000 messages)
			do_commit(false, state.async_commit)

		case _ = <-time.After(100000 * time.Microsecond):
			for true {
				ev := c.Poll(0)
				if ev == nil {
					break
				}
				handle_event(c, ev)
			}
		}
	}
}

func run_consumer(config *kafka.ConfigMap, topic string) {
	c, err := kafka.NewConsumer(config)
	if err != nil {
		fatal(fmt.Sprintf("Failed to create consumer: %v", err))
	}

	_, verstr := kafka.LibraryVersion()
	fmt.Fprintf(os.Stderr, "%% Created Consumer %v (%s)\n", c, verstr)
	state.c = c

	c.Subscribe(topic, nil)

	send("startup_complete", nil)
	state.run = true

	main_loop(c, 10*60)
	t_term_begin := time.Now()
	fmt.Fprintf(os.Stderr, "%% Consumer shutting down\n")

	send_records_consumed(true)

	// Final commit (if auto commit is disabled)
	do_commit(false, false)

	c.Unsubscribe()

	// Wait for rebalance, final offset commits, etc.
	state.run = true
	main_loop(c, 10)

	fmt.Fprintf(os.Stderr, "%% Closing consumer\n")

	c.Close()

	msg := make(map[string]interface{})
	msg["_shutdown_duration"] = time.Since(t_term_begin).Seconds()
	send("shutdown_complete", msg)
}

func main() {
	sigs = make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Default config
	conf := kafka.ConfigMap{"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"}}

	/* Required options */
	group := kingpin.Flag("group-id", "Consumer group").Required().String()
	topic := kingpin.Flag("topic", "Topic to consume").Required().String()
	brokers := kingpin.Flag("broker-list", "Bootstrap broker(s)").Required().String()
	session_timeout := kingpin.Flag("session-timeout", "Session timeout").Required().Int()

	/* Optionals */
	enable_autocommit := kingpin.Flag("enable-autocommit", "Enable auto-commit").Default("true").Bool()
	max_messages := kingpin.Flag("max-messages", "Max messages to consume").Default("10000000").Int()
	java_assignment_strategy := kingpin.Flag("assignment-strategy", "Assignment strategy (Java class name)").String()
	config_file := kingpin.Flag("consumer.config", "Config file").File()
	debug := kingpin.Flag("debug", "Debug flags").String()
	xconf := kingpin.Flag("--property", "CSV separated key=value librdkafka configuration properties").Short('X').String()

	kingpin.Parse()

	conf["bootstrap.servers"] = *brokers
	conf["group.id"] = *group
	conf["session.timeout.ms"] = *session_timeout
	conf["enable.auto.commit"] = *enable_autocommit

	if len(*debug) > 0 {
		conf["debug"] = *debug
	}

	/* Convert Java assignment strategy to librdkafka one.
	 * "[java.class.path.]Strategy[Assignor]" -> "strategy" */
	if java_assignment_strategy != nil && len(*java_assignment_strategy) > 0 {
		s := strings.Split(*java_assignment_strategy, ".")
		strategy := strings.ToLower(strings.TrimSuffix(s[len(s)-1], "Assignor"))
		conf["partition.assignment.strategy"] = strategy
		fmt.Fprintf(os.Stderr, "%% Mapped %s -> %s\n",
			*java_assignment_strategy, conf["partition.assignment.strategy"])
	}

	if *config_file != nil {
		fmt.Fprintf(os.Stderr, "%% Ignoring config file %v\n", *config_file)
	}

	conf["go.events.channel.enable"] = false
	conf["go.application.rebalance.enable"] = true

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

	state.auto_commit = *enable_autocommit
	state.max_messages = *max_messages
	run_consumer((*kafka.ConfigMap)(&conf), *topic)

}
