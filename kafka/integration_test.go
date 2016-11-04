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

package kafka

import (
	"fmt"
	"os"
	"testing"
)

type testmsgType struct {
	Partition     int32
	Key           []byte
	Value         []byte
	ExpectedError string
}

var testmsgs = []testmsgType{
	{0, []byte("key1"), []byte("value1"), ""},
	{0, nil, nil, ""},
	{1, nil, []byte("value2"), ErrUnknownPartition.String()},
	{1, []byte("key3"), nil, ErrUnknownPartition.String()},
	{PartitionAny, []byte("key4"), []byte("value4"), ""},
	{100000, []byte("key5"), []byte("value5"), ErrUnknownPartition.String()},
}

var groupId = "integration-test-group.go"
//var topic = "integration-test.go"

//Test LibraryVersion()
func TestLibraryVersion(t *testing.T) {
	ver, verstr := LibraryVersion()
	t.Logf("Library version %d: %s\n", ver, verstr)
}

//Test TimestampType
func TestTimestampType(t *testing.T) {
	timestamp_map := map[TimestampType]string{TimestampCreateTime: "CreateTime",
		TimestampLogAppendTime: "LogAppendTime",
		TimestampNotAvailable:  "NotAvailable"}
	for ts, desc := range timestamp_map {
		if ts.String() != desc {
			t.Errorf("Wrong description")
		}
	}
}

//Test Offset APIs
func TestOffsetAPIs(t *testing.T) {
	offsets := []Offset{OffsetBeginning, OffsetEnd, OffsetInvalid, OffsetStored, 1001}
	for _, offset := range offsets {
		t.Logf("Offset: %s\n", offset.String())
	}

	// test known offset strings
	testOffsets := map[string]Offset{"beginning": OffsetBeginning,
		"earliest": OffsetBeginning,
		"end":      OffsetEnd,
		"latest":   OffsetEnd,
		"unset":    OffsetInvalid,
		"invalid":  OffsetInvalid,
		"stored":   OffsetStored}
	for key, expected_offset := range testOffsets {
		offset, err := NewOffset(key)
		if err != nil {
			t.Errorf("Cannot create offset for %s\n", key)
		} else {
			if offset != expected_offset {
				t.Errorf("Offset does not equal expected: %s != %s\n", offset, expected_offset)
			}
		}
	}

	// test numeric string conversion
	offset, err := NewOffset("10")
	if err != nil {
		t.Errorf("Cannot create offset for 10\n")
	} else {
		if offset != Offset(10) {
			t.Errorf("Offset does not equal expected: %s != %s\n", offset, Offset(10))
		}
	}

	// test integer offset
	var int_offset int = 10
	offset, err = NewOffset(int_offset)
	if err != nil {
		t.Errorf("Cannot create offset for 10\n")
	} else {
		if offset != Offset(10) {
			t.Errorf("Offset does not equal expected: %s != %s\n", offset, Offset(10))
		}
	}

	// test int64 offset
	var int64_offset int64 = 10
	offset, err = NewOffset(int64_offset)
	if err != nil {
		t.Errorf("Cannot create offset for 10\n")
	} else {
		if offset != Offset(10) {
			t.Errorf("Offset does not equal expected: %s != %s\n", offset, Offset(10))
		}
	}

	// test invalid string offset
	offset, err = NewOffset("what is this offset")
	if err == nil {
		t.Errorf("Expected error for this string offset\n")
	}

	// test double offset
	offset, err = NewOffset(12.15)
	if err == nil {
		t.Errorf("Expected error for this double offset: 12.15\n")
	}

	// test change offset via Set()
	offset, err = NewOffset("beginning")
	if err != nil {
		t.Errorf("Cannot create offset for 'beginning'\n")
	}
	err = offset.Set("latest")
	if err != nil {
		t.Errorf("Cannot set offset to 'latest'\n")
	}

	// test OffsetTail()
	tail := OffsetTail(offset)
	t.Logf("offset tail %v\n", tail)

}

//Test config map APIs
func TestConfigMapAPIs(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	kConfig := &ConfigMap{}

	// set a good key via SetKey()
	err := kConfig.SetKey("bootstrap.servers", testconf.Brokers)
	if err != nil {
		t.Errorf("Failed to set key via SetKey()")
	}

	err = kConfig.SetKey("{topic}.produce.offset.report", true)
	if err != nil {
		t.Errorf("Failed to set key via SetKey()")
	}

	// set a good key-value pair via Set()
	err = kConfig.Set("group.id=test.id")
	if err != nil {
		t.Errorf("Failed to set key-value pair via Set()")
	}

	// negative test cases
	// set a bad key-value pair via Set()
	err = kConfig.Set("group.id:test.id")
	if err == nil {
		t.Errorf("Expected failure when setting invalid key-value pair via Set()")
	}

}

//Test consumer QueryWatermarkOffsets API
func TestConsumerQueryWatermarkOffsets(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	kConfig := &ConfigMap{"bootstrap.servers": testconf.Brokers,
		"group.id": groupId}

	// Create consumer
	c, err := NewConsumer(kConfig)
	if err != nil {
		t.Errorf("Failed to create consumer: %s\n", err)
		return
	}
	low, high, err := c.QueryWatermarkOffsets(testconf.Topic, 0, 5*1000)
	if err != nil {
		t.Errorf("Failed to query watermark offsets for topic %s\n", testconf.Topic)
		return
	} else {
		t.Logf("Watermark offsets fo topic %s: low=%d, high=%d\n", testconf.Topic, low, high)
	}
}

//Test producer QueryWatermarkOffsets API
func TestProducerQueryWatermarkOffsets(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	kConfig := &ConfigMap{"bootstrap.servers": testconf.Brokers}

	// Create producer
	p, err := NewProducer(kConfig)
	if err != nil {
		t.Errorf("Failed to create producer: %s\n", err)
		return
	}
	low, high, err := p.QueryWatermarkOffsets(testconf.Topic, 0, 5*1000)
	if err != nil {
		t.Errorf("Failed to query watermark offsets for topic %s\n", testconf.Topic)
		return
	} else {
		t.Logf("Watermark offsets fo topic %s: low=%d, high=%d\n", testconf.Topic, low, high)
	}
}

// TestProducerFuncBasedAPI test function-based API, requires broker
func TestProducerFuncBasedAPI(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	kConfig := &ConfigMap{
		"bootstrap.servers":    testconf.Brokers,
		"default.topic.config": ConfigMap{"produce.offset.report": true},
	}

	p, err := NewProducer(kConfig)
	if err != nil {
		t.Fatalf("Failed to create producer: %s\n", err)
	}

	t.Logf("Created Producer %v\n", p)

	deliveryChan := make(chan Event)

	for _, testmsg := range testmsgs {
		m := &Message{
			TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: testmsg.Partition},
			Value:          testmsg.Value,
			Key:            testmsg.Key}

		// coverage test for message String() API
		msgContent := m.String()
		t.Logf("Producing message %s\n", msgContent)

		err = p.Produce(m, deliveryChan)
		if err != nil {
			if testmsg.ExpectedError != "" && err.Error() == testmsg.ExpectedError {
				fmt.Fprintf(os.Stderr, "OK: Expected error: %s\n", testmsg.ExpectedError)
				continue
			} else {
				t.Errorf("Encountered error: %s\n", err)
				continue
			}
		}

		e := <-deliveryChan
		verifyEvent(e, testmsg, t)
	}
	p.Close()
}

// TestProducerChannelBasedAPI test channel-based API, requires broker
func TestProducerChannelBasedAPI(t *testing.T) {
	runProducerChannelBasedAPI(t)
}

// run the prodcuer using channel based API
func runProducerChannelBasedAPI(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	kConfig := &ConfigMap{
		"bootstrap.servers":    testconf.Brokers,
		"default.topic.config": ConfigMap{"produce.offset.report": true}}

	p, err := NewProducer(kConfig)
	if err != nil {
		t.Fatalf("Failed to create producer: %s\n", err)
	}

	t.Logf("Created Producer %v\n", p)

	var doneChan chan bool

	for _, testmsg := range testmsgs {

		doneChan = make(chan bool)

		// delivery handler
		go func() {
			e := <-p.Events()
			verifyEvent(e, testmsg, t)
			close(doneChan)
		}()

		m := &Message{
			TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: testmsg.Partition},
			Value:          testmsg.Value,
			Key:            testmsg.Key}
		p.ProduceChannel() <- m

		// wait for delivery report goroutine to finish
		_ = <-doneChan

	}

	p.Close()

}

// verify event info
func verifyEvent(e Event, testmsg testmsgType, t *testing.T) {
	switch e.(type) {
	case *Message:
		m := e.(*Message)
		if m.TopicPartition.Error != nil {
			err := m.TopicPartition.Error
			if testmsg.ExpectedError != "" && err.Error() == testmsg.ExpectedError {
				fmt.Fprintf(os.Stderr, "OK: Expected error: %s\n", testmsg.ExpectedError)
			} else {
				t.Errorf("Encountered unexepected error: %s\n", err)
			}
			return

		}
		// check partition
		if !((testmsg.Partition == PartitionAny && m.TopicPartition.Partition >= 0) ||
			(testmsg.Partition >= 0 && m.TopicPartition.Partition == testmsg.Partition)) {
			t.Errorf("Partition check failed for test message %v. Got back {[%d] [%s] [%s]}\n",
				testmsg, m.TopicPartition.Partition, m.Key, m.Value)
			return

		}

		// check Key
		if testmsg.Key == nil {
			if !(m.Key == nil || len(m.Key) == 0) {
				t.Errorf("Key verification failed. Expected %s, got %s\n", testmsg.Key, m.Key)
				return
			}
		} else {
			if string(m.Key) != string(testmsg.Key) {
				t.Errorf("Key verification failed. Expected %s, got %s\n", testmsg.Key, m.Key)
				return
			}
		}

		// check Value
		if testmsg.Value == nil {
			if !(m.Value == nil || len(m.Value) == 0) {
				t.Errorf("Value verification failed. Expected %s, got %s\n", testmsg.Value, m.Value)
				return
			}
		} else {
			if string(m.Value) != string(testmsg.Value) {
				t.Errorf("Value verification failed. Expected %s, got %s\n", testmsg.Value, m.Value)
				return
			}
		}

		fmt.Printf("%% Message delivered to topic %s [%d] at offset %v: key=(%s), value=(%s)\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset, m.Key, m.Value)

	default:
		fmt.Printf("Ignored event: %s\n", e)
	}
}

// TestConsumerFuncBasedAPI test function-based API, requires broker
func TestConsumerFuncBasedAPI(t *testing.T) {
	runConsumerFuncBasedAPI(t, "CommitMessage")
	runConsumerFuncBasedAPI(t, "CommitOffsets")
	runProducerChannelBasedAPI(t)
	runConsumerFuncBasedAPI(t, "Commit")
}

func runConsumerFuncBasedAPI(t *testing.T, commitMode string) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}
	c, err := NewConsumer(&ConfigMap{
		"bootstrap.servers":    testconf.Brokers,
		"group.id":             groupId,
		"enable.auto.commit":   false,
		"session.timeout.ms":   6000,
		"default.topic.config": ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	t.Logf("Created Consumer %v\n", c)

	err = c.Subscribe(testconf.Topic, nil)

	maxMsgCnt := 3
	msgcnt := 0
	run := true

	savedMessages := make([]*Message, maxMsgCnt)

	for run == true {
		select {
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *Message:
				fmt.Printf("%% Message on %s: key=(%s), value=(%s)\n",
					e.TopicPartition, e.Key, e.Value)
				savedMessages[msgcnt] = e
				msgcnt++
				if msgcnt >= maxMsgCnt {
					run = false
				}
			case PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
				run = false
			case Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	switch commitMode {
	case "CommitMessage":
		// verify CommitMessage() API
		for _, message := range savedMessages {
			_, commit_err := c.CommitMessage(message)
			if commit_err != nil {
				t.Errorf("Cannot commit message\n")
			}
		}
	case "CommitOffsets":
		// verify CommitOffset
		partitions := make([]TopicPartition, len(savedMessages))
		for index, message := range savedMessages {
			partitions[index] = message.TopicPartition
		}
		_, commit_err := c.CommitOffsets(partitions)
		if commit_err != nil {
			t.Errorf("Failed to commit using CommitOffsets\n")
		}
	default:
		// verify Commit() API
		_, commit_err := c.Commit()
		if commit_err != nil {
			t.Errorf("Failed to commit: %s", commit_err)
		}

	}

	t.Logf("Closing consumer\n")
	c.Close()
}

// TestConsumerChannelBasedAPI test channel-based API, requires broker
func TestConsumerChannelBasedAPI(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}
	c, err := NewConsumer(&ConfigMap{
		"bootstrap.servers":        testconf.Brokers,
		"group.id":                 groupId,
		"enable.auto.commit":       false,
		"session.timeout.ms":       6000,
		"go.events.channel.enable": true,
		"default.topic.config":     ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		t.Fatalf("Failed to create consumer: %s\n", err)
	}

	t.Logf("Created Consumer %v\n", c)

	err = c.Subscribe(testconf.Topic, nil)

	maxMsgCnt := 3
	msgcnt := 0
	run := true

	for run == true {
		select {
		case ev := <-c.Events():
			switch e := ev.(type) {
			case *Message:
				fmt.Printf("%% Message on %s: key=(%s), value=(%s)\n",
					e.TopicPartition, e.Key, e.Value)
				msgcnt++
				if msgcnt >= maxMsgCnt {
					run = false
				}
			case PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
				run = false
			case Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	t.Logf("Closing consumer\n")
	c.Close()
}
