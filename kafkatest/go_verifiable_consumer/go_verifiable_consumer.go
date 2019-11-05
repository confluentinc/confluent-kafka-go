// Apache Kafka kafkatest VerifiableConsumer implemented in Go
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
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
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

func fatal(why string) {
	fmt.Fprintf(os.Stderr, "%% FATAL ERROR: %s", why)
	panic(why)
}

func send(name string, msg map[string]interface{}) {
	if msg == nil {
		msg = make(map[string]interface{})
	}
	msg["name"] = name
	msg["_time"] = time.Now().Format("2006-01-02 15:04:05.000")
	b, err := json.Marshal(msg)
	if err != nil {
		fatal(fmt.Sprintf("json.Marshal failed: %v", err))
	}
	fmt.Println(string(b))
}

func partitionsToMap(partitions []kafka.TopicPartition) []map[string]interface{} {
	parts := make([]map[string]interface{}, len(partitions))
	for i, tp := range partitions {
		parts[i] = map[string]interface{}{"topic": *tp.Topic, "partition": tp.Partition, "offset": tp.Offset}
	}
	return parts
}

func sendOffsetsCommitted(offsets []kafka.TopicPartition, err error) {
	if len(state.currAssignment) == 0 {
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

		kerr, ok := err.(kafka.Error)
		if ok && kerr.Code() == kafka.ErrNoOffset {
			fmt.Fprintf(os.Stderr, "%% No offsets to commit\n")
			return
		}

		fmt.Fprintf(os.Stderr, "%% Commit failed: %v", msg["error"])
	} else {
		msg["success"] = true

	}

	if offsets != nil {
		msg["offsets"] = partitionsToMap(offsets)
	}

	// Make sure we report consumption before commit,
	// otherwise tests may fail because of commit > consumed
	sendRecordsConsumed(true)

	send("offsets_committed", msg)
}

func sendPartitions(name string, partitions []kafka.TopicPartition) {

	msg := make(map[string]interface{})
	msg["partitions"] = partitionsToMap(partitions)

	send(name, msg)
}

type assignedPartition struct {
	tp           kafka.TopicPartition
	consumedMsgs int
	minOffset    int64
	maxOffset    int64
}

func assignmentKey(tp kafka.TopicPartition) string {
	return fmt.Sprintf("%s-%d", *tp.Topic, tp.Partition)
}

func findAssignment(tp kafka.TopicPartition) *assignedPartition {
	a, ok := state.currAssignment[assignmentKey(tp)]
	if !ok {
		return nil
	}
	return a
}

func addAssignment(tp kafka.TopicPartition) {
	state.currAssignment[assignmentKey(tp)] = &assignedPartition{tp: tp, minOffset: -1, maxOffset: -1}
}

func clearCurrAssignment() {
	state.currAssignment = make(map[string]*assignedPartition)
}

type commState struct {
	run                      bool
	consumedMsgs             int
	consumedMsgsLastReported int
	consumedMsgsAtLastCommit int
	currAssignment           map[string]*assignedPartition
	maxMessages              int
	autoCommit               bool
	asyncCommit              bool
	c                        *kafka.Consumer
	termOnRevoke             bool
}

var state commState

func sendRecordsConsumed(immediate bool) {
	if len(state.currAssignment) == 0 ||
		(!immediate && state.consumedMsgsLastReported+1000 > state.consumedMsgs) {
		return
	}

	msg := map[string]interface{}{}
	msg["count"] = state.consumedMsgs - state.consumedMsgsLastReported
	parts := make([]map[string]interface{}, len(state.currAssignment))
	i := 0
	for _, a := range state.currAssignment {
		if a.minOffset == -1 {
			// Skip partitions that havent had any messages since last time.
			// This is to circumvent some minOffset checks in kafkatest.
			continue
		}
		parts[i] = map[string]interface{}{"topic": *a.tp.Topic,
			"partition":     a.tp.Partition,
			"consumed_msgs": a.consumedMsgs,
			"minOffset":     a.minOffset,
			"maxOffset":     a.maxOffset}
		a.minOffset = -1
		i++
	}
	msg["partitions"] = parts[0:i]

	send("records_consumed", msg)

	state.consumedMsgsLastReported = state.consumedMsgs
}

// do_commit commits every 1000 messages or whenever there is a consume timeout, or when immediate==true
func doCommit(immediate bool, async bool) {
	if !immediate &&
		(state.autoCommit ||
			state.consumedMsgsAtLastCommit+1000 > state.consumedMsgs) {
		return
	}

	async = state.asyncCommit

	fmt.Fprintf(os.Stderr, "%% Committing %d messages (async=%v)\n",
		state.consumedMsgs-state.consumedMsgsAtLastCommit, async)

	state.consumedMsgsAtLastCommit = state.consumedMsgs

	var waitCommitted chan bool

	if !async {
		waitCommitted = make(chan bool)
	}

	go func() {
		offsets, err := state.c.Commit()

		sendOffsetsCommitted(offsets, err)

		if !async {
			close(waitCommitted)
		}
	}()

	if !async {
		_, _ = <-waitCommitted
	}
}

// returns false when consumer should terminate, else true to keep running.
func handleMsg(m *kafka.Message) bool {
	if verbosity >= 2 {
		fmt.Fprintf(os.Stderr, "%% Message receved: %v:\n", m.TopicPartition)
	}

	a := findAssignment(m.TopicPartition)
	if a == nil {
		fmt.Fprintf(os.Stderr, "%% Received message on unassigned partition: %v\n", m.TopicPartition)
		return true
	}

	a.consumedMsgs++
	offset := int64(m.TopicPartition.Offset)
	if a.minOffset == -1 {
		a.minOffset = offset
	}
	if a.maxOffset < offset {
		a.maxOffset = offset
	}

	state.consumedMsgs++

	sendRecordsConsumed(false)
	doCommit(false, state.asyncCommit)

	if state.maxMessages > 0 && state.consumedMsgs >= state.maxMessages {
		// ignore extra messages
		return false
	}

	return true

}

// handle_event handles an event as returned by Poll().
func handleEvent(c *kafka.Consumer, ev kafka.Event) {
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		if len(state.currAssignment) > 0 {
			fatal(fmt.Sprintf("Assign: currAssignment should have been empty: %v", state.currAssignment))
		}
		state.currAssignment = make(map[string]*assignedPartition)
		for _, tp := range e.Partitions {
			addAssignment(tp)
		}
		sendPartitions("partitions_assigned", e.Partitions)
		c.Assign(e.Partitions)

	case kafka.RevokedPartitions:
		sendRecordsConsumed(true)
		doCommit(true, false)
		sendPartitions("partitions_revoked", e.Partitions)
		clearCurrAssignment()
		c.Unassign()
		if state.termOnRevoke {
			state.run = false
		}

	case kafka.OffsetsCommitted:
		sendOffsetsCommitted(e.Offsets, e.Error)

	case *kafka.Message:
		state.run = handleMsg(e)

	case kafka.Error:
		if e.Code() == kafka.ErrUnknownTopicOrPart {
			fmt.Fprintf(os.Stderr,
				"%% Ignoring transient error: %v\n", e)
		} else {
			fatal(fmt.Sprintf("%% Error: %v\n", e))
		}

	default:
		fmt.Fprintf(os.Stderr, "%% Unhandled event %T ignored: %v\n", e, e)
	}
}

// main_loop serves consumer events, signals, etc.
// will run for at most (roughly) \p timeout seconds.
func mainLoop(c *kafka.Consumer, timeout int) {
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
			sendRecordsConsumed(true)
			// Commit on timeout as well (not just every 1000 messages)
			doCommit(false, state.asyncCommit)

		default:
			//case _ = <-time.After(100000 * time.Microsecond):
			for true {
				ev := c.Poll(0)
				if ev == nil {
					break
				}
				handleEvent(c, ev)
			}
		}
	}
}

func runConsumer(config *kafka.ConfigMap, topic string) {
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

	mainLoop(c, 10*60)

	tTermBegin := time.Now()
	fmt.Fprintf(os.Stderr, "%% Consumer shutting down\n")

	sendRecordsConsumed(true)

	// Final commit (if auto commit is disabled)
	doCommit(false, false)

	c.Unsubscribe()

	// Wait for rebalance, final offset commits, etc.
	state.run = true
	state.termOnRevoke = true
	mainLoop(c, 10)

	fmt.Fprintf(os.Stderr, "%% Closing consumer\n")

	c.Close()

	msg := make(map[string]interface{})
	msg["_shutdown_duration"] = time.Since(tTermBegin).Seconds()
	send("shutdown_complete", msg)
}

func main() {
	sigs = make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Default config
	conf := kafka.ConfigMap{"auto.offset.reset": "earliest"}

	/* Required options */
	group := kingpin.Flag("group-id", "Consumer group").Required().String()
	topic := kingpin.Flag("topic", "Topic to consume").Required().String()
	brokers := kingpin.Flag("broker-list", "Bootstrap broker(s)").Required().String()
	sessionTimeout := kingpin.Flag("session-timeout", "Session timeout").Required().Int()

	/* Optionals */
	enableAutocommit := kingpin.Flag("enable-autocommit", "Enable auto-commit").Default("true").Bool()
	maxMessages := kingpin.Flag("max-messages", "Max messages to consume").Default("10000000").Int()
	javaAssignmentStrategy := kingpin.Flag("assignment-strategy", "Assignment strategy (Java class name)").String()
	configFile := kingpin.Flag("consumer.config", "Config file").File()
	debug := kingpin.Flag("debug", "Debug flags").String()
	xconf := kingpin.Flag("--property", "CSV separated key=value librdkafka configuration properties").Short('X').String()

	kingpin.Parse()

	conf["bootstrap.servers"] = *brokers
	conf["group.id"] = *group
	conf["session.timeout.ms"] = *sessionTimeout
	conf["enable.auto.commit"] = *enableAutocommit

	if len(*debug) > 0 {
		conf["debug"] = *debug
	}

	/* Convert Java assignment strategy(s) (CSV) to librdkafka one.
	 * "[java.class.path.]Strategy[Assignor],.." -> "strategy,.." */
	if javaAssignmentStrategy != nil && len(*javaAssignmentStrategy) > 0 {
		var strats []string
		for _, jstrat := range strings.Split(*javaAssignmentStrategy, ",") {
			s := strings.Split(jstrat, ".")
			strats = append(strats, strings.ToLower(strings.TrimSuffix(s[len(s)-1], "Assignor")))
		}
		conf["partition.assignment.strategy"] = strings.Join(strats, ",")
		fmt.Fprintf(os.Stderr, "%% Mapped %s -> %s\n",
			*javaAssignmentStrategy, conf["partition.assignment.strategy"])
	}

	if *configFile != nil {
		fmt.Fprintf(os.Stderr, "%% Ignoring config file %v\n", *configFile)
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

	state.autoCommit = *enableAutocommit
	state.maxMessages = *maxMessages
	runConsumer((*kafka.ConfigMap)(&conf), *topic)

}
