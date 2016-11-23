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
	"testing"
)

// producer test control
type PC struct {
	silent        bool
	withDr        bool // use delivery channel
	batchProducer bool // enable batch producer
}

// define commitMode with constants
type commitMode string

const (
	ViaCommitMessageAPI = "CommitMessage"
	ViaCommitOffsetsAPI = "CommitOffsets"
	ViaCommitAPI        = "Commit"
)

// consumer test control
type CC struct {
	autoCommit bool // set enable.auto.commit property
	useChannel bool
	commitMode commitMode // which commit api to use
}

type testmsgType struct {
	msg           Message
	expectedError Error
}

// msgtracker tracks messages
type msgtracker struct {
	t      *testing.T
	msgcnt int64
	errcnt int64 // count of failed messages
	msgs   []*Message
}

// msgtrackerStart sets up a new message tracker
func msgtrackerStart(t *testing.T, expectedCnt int) (mt msgtracker) {
	mt = msgtracker{t: t}
	mt.msgs = make([]*Message, expectedCnt)
	return mt
}

var testMsgsInit  = false
var p0TestMsgs []*testmsgType // partition 0 test messages
// pAllTestMsgs holds messages for various partitions including PartitionAny and  invalid partitions
var pAllTestMsgs []*testmsgType

// createTestMessages populates p0TestMsgs and pAllTestMsgs
func createTestMessages() {

	if testMsgsInit {
		return
	}
	defer func() { testMsgsInit = true }()

	testmsgs := make([]*testmsgType, 100)
	i := 0

	/*
		// a test message with default initialization
		testmsgs[i] = &testmsgType{}
		i++
	*/

	// a test message for partition 0 with only Opaque specified
	testmsgs[i] = &testmsgType{msg: Message{TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: 0},
		Opaque: fmt.Sprintf("Op%d", i),
	}}
	i++

	// a test message for partition 0 with empty Value and Keys
	testmsgs[i] = &testmsgType{msg: Message{TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: 0},
		Value:  []byte(""),
		Key:    []byte(""),
		Opaque: fmt.Sprintf("Op%d", i),
	}}
	i++

	// a test message for partition 0 with Value, Key, and Opaque
	testmsgs[i] = &testmsgType{msg: Message{TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: 0},
		Value:  []byte(fmt.Sprintf("value%d", i)),
		Key:    []byte(fmt.Sprintf("key%d", i)),
		Opaque: fmt.Sprintf("Op%d", i),
	}}
	i++

	// a test message for partition 0 without  Value
	testmsgs[i] = &testmsgType{msg: Message{TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: 0},
		Key:    []byte(fmt.Sprintf("key%d", i)),
		Opaque: fmt.Sprintf("Op%d", i),
	}}
	i++

	// a test message for partition 0 without Key
	testmsgs[i] = &testmsgType{msg: Message{TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: 0},
		Value:  []byte(fmt.Sprintf("value%d", i)),
		Opaque: fmt.Sprintf("Op%d", i),
	}}
	i++

	p0TestMsgs = testmsgs[:i]

	// a test message for PartitonAny with Value, Key, and Opaque
	testmsgs[i] = &testmsgType{msg: Message{TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: PartitionAny},
		Value:  []byte(fmt.Sprintf("value%d", i)),
		Key:    []byte(fmt.Sprintf("key%d", i)),
		Opaque: fmt.Sprintf("Op%d", i),
	}}
	i++

	// a test message for a non-existent partition with Value, Key, and Opaque.
	// It should generate ErrUnknownPartition
	testmsgs[i] = &testmsgType{expectedError: Error{ErrUnknownPartition, ""},
		msg: Message{TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: int32(10000)},
			Value:  []byte(fmt.Sprintf("value%d", i)),
			Key:    []byte(fmt.Sprintf("key%d", i)),
			Opaque: fmt.Sprintf("Op%d", i),
		}}
	i++

	pAllTestMsgs = testmsgs[:i]
}

//Test LibraryVersion()
func TestLibraryVersion(t *testing.T) {
	ver, verstr := LibraryVersion()
	if ver >= 0x00090200 {
		t.Logf("Library version %d: %s\n", ver, verstr)
	} else {
		t.Errorf("Unexpected Library version %d: %s\n", ver, verstr)
	}
}

//Test TimestampType
func TestTimestampType(t *testing.T) {
	timestampMap := map[TimestampType]string{TimestampCreateTime: "CreateTime",
		TimestampLogAppendTime: "LogAppendTime",
		TimestampNotAvailable:  "NotAvailable"}
	for ts, desc := range timestampMap {
		if ts.String() != desc {
			t.Errorf("Wrong timestamp description for %s, expected %s\n", desc, ts.String())
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

	for key, expectedOffset := range testOffsets {
		offset, err := NewOffset(key)
		if err != nil {
			t.Errorf("Cannot create offset for %s, error: %s\n", key, err)
		} else {
			if offset != expectedOffset {
				t.Errorf("Offset does not equal expected: %s != %s\n", offset, expectedOffset)
			}
		}
	}

	// test numeric string conversion
	offset, err := NewOffset("10")
	if err != nil {
		t.Errorf("Cannot create offset for 10, error: %s\n", err)
	} else {
		if offset != Offset(10) {
			t.Errorf("Offset does not equal expected: %s != %s\n", offset, Offset(10))
		}
	}

	// test integer offset
	var intOffset = 10
	offset, err = NewOffset(intOffset)
	if err != nil {
		t.Errorf("Cannot create offset for int 10, Error: %s\n", err)
	} else {
		if offset != Offset(10) {
			t.Errorf("Offset does not equal expected: %s != %s\n", offset, Offset(10))
		}
	}

	// test int64 offset
	var int64Offset int64 = 10
	offset, err = NewOffset(int64Offset)
	if err != nil {
		t.Errorf("Cannot create offset for int64 10, Error: %s \n", err)
	} else {
		if offset != Offset(10) {
			t.Errorf("Offset does not equal expected: %s != %s\n", offset, Offset(10))
		}
	}

	// test invalid string offset
	invalidOffsetString := "what is this offset"
	offset, err = NewOffset(invalidOffsetString)
	if err == nil {
		t.Errorf("Expected error for this string offset. Error: %s\n", err)
	} else if offset != Offset(0) {
		t.Errorf("Expected offset (%v), got (%v)\n", Offset(0), offset)
	}
	t.Logf("Offset for string (%s): %v\n", invalidOffsetString, offset)

	// test double offset
	doubleOffset := 12.15
	offset, err = NewOffset(doubleOffset)
	if err == nil {
		t.Errorf("Expected error for this double offset: %f. Error: %s\n", doubleOffset, err)
	} else if offset != OffsetInvalid {
		t.Errorf("Expected offset (%v), got (%v)\n", OffsetInvalid, offset)
	}
	t.Logf("Offset for double (%f): %v\n", doubleOffset, offset)

	// test change offset via Set()
	offset, err = NewOffset("beginning")
	if err != nil {
		t.Errorf("Cannot create offset for 'beginning'. Error: %s\n", err)
	}

	// test change to a logical offset
	err = offset.Set("latest")
	if err != nil {
		t.Errorf("Cannot set offset to 'latest'. Error: %s \n", err)
	} else if offset != OffsetEnd {
		t.Errorf("Failed to change offset. Expect (%v), got (%v)\n", OffsetEnd, offset)
	}

	// test change to an integer offset
	err = offset.Set(int(10))
	if err != nil {
		t.Errorf("Cannot set offset to (%v). Error: %s \n", 10, err)
	} else if offset != 10 {
		t.Errorf("Failed to change offset. Expect (%v), got (%v)\n", 10, offset)
	}

	// test OffsetTail()
	tail := OffsetTail(offset)
	t.Logf("offset tail %v\n", tail)

}

// A custom type with Stringer interface to be used to test config map APIs
type HostPortType struct {
	Host string
	Port int
}

// implements String() interface
func (hp HostPortType) String() string {
	return fmt.Sprintf("%s:%d", hp.Host, hp.Port)
}

//Test config map APIs
func TestConfigMapAPIs(t *testing.T) {
	config := &ConfigMap{}

	// set a good key via SetKey()
	err := config.SetKey("bootstrap.servers", testconf.Brokers)
	if err != nil {
		t.Errorf("Failed to set key via SetKey(). Error: %s\n", err)
	}

	// test custom Stringer type
	hostPort := HostPortType{Host: "localhost", Port: 9092}
	err = config.SetKey("bootstrap.servers", hostPort)
	if err != nil {
		t.Errorf("Failed to set custom Stringer type via SetKey(). Error: %s\n", err)
	}

	err = config.SetKey("{topic}.produce.offset.report", true)
	if err != nil {
		t.Errorf("Failed to set key via SetKey(). Error: %s\n", err)
	}

	// test offset literal string
	err = config.SetKey("{topic}.auto.offset.reset", "earliest")
	if err != nil {
		t.Errorf("Failed to set key via SetKey(). Error: %s\n", err)
	}

	//test offset constant
	err = config.SetKey("{topic}.auto.offset.reset", OffsetBeginning)
	if err != nil {
		t.Errorf("Failed to set key via SetKey(). Error: %s\n", err)
	}

	//test integer offset
	err = config.SetKey("{topic}.auto.offset.reset", 10)
	if err != nil {
		t.Errorf("Failed to set integer value via SetKey(). Error: %s\n", err)
	}

	// set a good key-value pair via Set()
	err = config.Set("group.id=test.id")
	if err != nil {
		t.Errorf("Failed to set key-value pair via Set(). Error: %s\n", err)
	}

	// negative test cases
	// set a bad key-value pair via Set()
	err = config.Set("group.id:test.id")
	if err == nil {
		t.Errorf("Expected failure when setting invalid key-value pair via Set()")
	}

}

// consume messages through the Poll() interface
func eventTestPollConsumer(c *Consumer, mt *msgtracker, expCnt int) {
	for true {
		ev := c.Poll(100)
		if ev == nil {
			// timeout
			continue
		}
		if !handleTestEvent(c, mt, expCnt, ev) {
			break
		}
	}
}

// consume messages through the Events channel
func eventTestChannelConsumer(c *Consumer, mt *msgtracker, expCnt int) {
	for ev := range c.Events() {
		if !handleTestEvent(c, mt, expCnt, ev) {
			break
		}
	}
}

// handleTestEvent returns false if processing should stop, else true. Tracks the message received
func handleTestEvent(c *Consumer, mt *msgtracker, expCnt int, ev Event) bool {
	switch e := ev.(type) {
	case *Message:
		if e.TopicPartition.Error != nil {
			mt.t.Errorf("Error: %v", e.TopicPartition)
		}
		mt.msgs[mt.msgcnt] = e
		mt.msgcnt++
		if mt.msgcnt >= int64(expCnt) {
			return false
		}
	case PartitionEOF:
		break // silence
	default:
		mt.t.Fatalf("Consumer error: %v", e)
	}
	return true

}

// delivery event handler. Tracks the message received
func deliveryTestHandler(t *testing.T, expCnt int64, deliveryChan chan Event, mt *msgtracker, doneChan chan int64) {

	for ev := range deliveryChan {
		m, ok := ev.(*Message)
		if !ok {
			continue
		}

		mt.msgs[mt.msgcnt] = m
		mt.msgcnt++

		if m.TopicPartition.Error != nil {
			mt.errcnt++
			// log it and check it later
			t.Logf("Message delivery error: %v", m.TopicPartition)
		}

		t.Logf("Delivered %d/%d to %s, error count %d", mt.msgcnt, expCnt, m.TopicPartition, mt.errcnt)

		if mt.msgcnt >= expCnt {
			break
		}

	}

	doneChan <- mt.msgcnt
	close(doneChan)
}

// producerTest produces messages in <testmsgs> to topic. Verifies delivered messages
func producerTest(t *testing.T, testname string, testmsgs []*testmsgType, pc PC, produceFunc func(p *Producer, m *Message, drChan chan Event)) {

	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	if testmsgs == nil {
		createTestMessages()
		testmsgs = pAllTestMsgs
	}

	//get the number of messages prior to producing more messages
	prerunMsgCnt, err := getMessageCountInTopic(testconf.Topic)
	if err != nil {
		t.Fatalf("Cannot get message count, Error: %s\n", err)
	}

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"go.batch.producer":            pc.batchProducer,
		"go.delivery.reports":          pc.withDr,
		"queue.buffering.max.messages": len(testmsgs),
		"api.version.request":          "true",
		"broker.version.fallback":      "0.9.0.1",
		"default.topic.config":         ConfigMap{"acks": 1}}

	conf.updateFromTestconf()

	p, err := NewProducer(&conf)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	mt := msgtrackerStart(t, len(testmsgs))

	var doneChan chan int64
	var drChan chan Event

	if pc.withDr {
		doneChan = make(chan int64)
		drChan = p.Events()
		go deliveryTestHandler(t, int64(len(testmsgs)), p.Events(), &mt, doneChan)
	}

	if !pc.silent {
		t.Logf("%s: produce %d messages", testname, len(testmsgs))
	}

	for i := 0; i < len(testmsgs); i++ {
		t.Logf("producing message %d: %v\n", i, testmsgs[i].msg)
		produceFunc(p, &testmsgs[i].msg, drChan)
	}

	if !pc.silent {
		t.Logf("produce done")
	}

	// Wait for messages in-flight and in-queue to get delivered.
	if !pc.silent {
		t.Logf("%s: %d messages in queue", testname, p.Len())
	}

	r := p.Flush(10000)
	if r > 0 {
		t.Errorf("%s: %d messages remains in queue after Flush()", testname, r)
	}

	if pc.withDr {
		mt.msgcnt = <-doneChan
	} else {
		mt.msgcnt = int64(len(testmsgs))
	}

	if !pc.silent {
		t.Logf("delivered %d messages\n", mt.msgcnt)
	}

	//get the number of messages afterward
	postrunMsgCnt, err := getMessageCountInTopic(testconf.Topic)
	if err != nil {
		t.Fatalf("Cannot get message count, Error: %s\n", err)
	}

	if !pc.silent {
		t.Logf("prerun message count: %d,  postrun count %d, delta: %d\n", prerunMsgCnt, postrunMsgCnt, postrunMsgCnt-prerunMsgCnt)
		t.Logf("deliveried message count: %d,  error message count %d\n", mt.msgcnt, mt.errcnt)

	}

	// verify the count and messages only if we get the delivered messages
	if pc.withDr {
		if int64(postrunMsgCnt-prerunMsgCnt) != (mt.msgcnt - mt.errcnt) {
			t.Errorf("Expected topic message count %d, got %d\n", prerunMsgCnt+int(mt.msgcnt-mt.errcnt), postrunMsgCnt)
		}

		verifyMessages(t, mt.msgs, testmsgs)
	}
}

// consumerTest consumes messages from a pre-primed (produced to) topic
func consumerTest(t *testing.T, testname string, msgcnt int, cc CC, consumeFunc func(c *Consumer, mt *msgtracker, expCnt int), rebalanceCb func(c *Consumer, event Event) error) {

	if msgcnt == 0 {
		createTestMessages()
		producerTest(t, "Priming producer", p0TestMsgs, PC{},
			func(p *Producer, m *Message, drChan chan Event) {
				p.ProduceChannel() <- m
			})
		msgcnt = len(p0TestMsgs)
	}

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"go.events.channel.enable": cc.useChannel,
		"group.id":                 testconf.GroupID,
		"session.timeout.ms":       6000,
		"api.version.request":      "true",
		"enable.auto.commit":       cc.autoCommit,
		"debug":                    ",",
		"default.topic.config":     ConfigMap{"auto.offset.reset": "earliest"}}

	conf.updateFromTestconf()

	c, err := NewConsumer(&conf)

	if err != nil {
		panic(err)
	}
	defer c.Close()

	expCnt := msgcnt
	mt := msgtrackerStart(t, expCnt)

	t.Logf("%s, expecting %d messages", testname, expCnt)
	c.Subscribe(testconf.Topic, rebalanceCb)

	consumeFunc(c, &mt, expCnt)

	//test commits
	switch cc.commitMode {
	case ViaCommitMessageAPI:
		// verify CommitMessage() API
		for _, message := range mt.msgs {
			_, commitErr := c.CommitMessage(message)
			if commitErr != nil {
				t.Errorf("Cannot commit message. Error: %s\n", commitErr)
			}
		}
	case ViaCommitOffsetsAPI:
		// verify CommitOffset
		partitions := make([]TopicPartition, len(mt.msgs))
		for index, message := range mt.msgs {
			partitions[index] = message.TopicPartition
		}
		_, commitErr := c.CommitOffsets(partitions)
		if commitErr != nil {
			t.Errorf("Failed to commit using CommitOffsets. Error: %s\n", commitErr)
		}
	case ViaCommitAPI:
		// verify Commit() API
		_, commitErr := c.Commit()
		if commitErr != nil {
			t.Errorf("Failed to commit. Error: %s", commitErr)
		}

	}

}

//Test consumer QueryWatermarkOffsets API
func TestConsumerQueryWatermarkOffsets(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	// getMessageCountInTopic() uses consumer QueryWatermarkOffsets() API to
	// get the number of messages in a topic
	msgcnt, err := getMessageCountInTopic(testconf.Topic)
	if err != nil {
		t.Errorf("Cannot get message size. Error: %s\n", err)
	}

	// Prime topic with test messages
	createTestMessages()
	producerTest(t, "Priming producer", p0TestMsgs, PC{silent: true},
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})

	// getMessageCountInTopic() uses consumer QueryWatermarkOffsets() API to
	// get the number of messages in a topic
	newmsgcnt, err := getMessageCountInTopic(testconf.Topic)
	if err != nil {
		t.Errorf("Cannot get message size. Error: %s\n", err)
	}

	if newmsgcnt-msgcnt != len(p0TestMsgs) {
		t.Errorf("Incorrect offsets. Expected message count %d, got %d\n", len(p0TestMsgs), newmsgcnt-msgcnt)
	}

}

// test consumer GetMetadata API
func TestConsumerGetMetadata(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	config := &ConfigMap{"bootstrap.servers": testconf.Brokers,
		"group.id": testconf.GroupID}

	// Create consumer
	c, err := NewConsumer(config)
	if err != nil {
		t.Errorf("Failed to create consumer: %s\n", err)
		return
	}
	defer c.Close()

	metaData, err := c.GetMetadata(&testconf.Topic, false, 5*1000)
	if err != nil {
		t.Errorf("Failed to get meta data for topic %s. Error: %s\n", testconf.Topic, err)
		return
	}
	t.Logf("Meta data for topic %s: %v\n", testconf.Topic, metaData)

	metaData, err = c.GetMetadata(nil, true, 5*1000)
	if err != nil {
		t.Errorf("Failed to get meta data, Error: %s\n", err)
		return
	}
	t.Logf("Meta data for consumer: %v\n", metaData)
}

//Test producer QueryWatermarkOffsets API
func TestProducerQueryWatermarkOffsets(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	config := &ConfigMap{"bootstrap.servers": testconf.Brokers}

	// Create producer
	p, err := NewProducer(config)
	if err != nil {
		t.Errorf("Failed to create producer: %s\n", err)
		return
	}
	defer p.Close()

	low, high, err := p.QueryWatermarkOffsets(testconf.Topic, 0, 5*1000)
	if err != nil {
		t.Errorf("Failed to query watermark offsets for topic %s. Error: %s\n", testconf.Topic, err)
		return
	}
	cnt := high - low
	t.Logf("Watermark offsets fo topic %s: low=%d, high=%d\n", testconf.Topic, low, high)

	createTestMessages()
	producerTest(t, "Priming producer", p0TestMsgs, PC{silent: true},
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})

	low, high, err = p.QueryWatermarkOffsets(testconf.Topic, 0, 5*1000)
	if err != nil {
		t.Errorf("Failed to query watermark offsets for topic %s. Error: %s\n", testconf.Topic, err)
		return
	}
	t.Logf("Watermark offsets fo topic %s: low=%d, high=%d\n", testconf.Topic, low, high)
	newcnt := high - low
	t.Logf("count = %d, New count = %d\n", cnt, newcnt)
	if newcnt-cnt != int64(len(p0TestMsgs)) {
		t.Errorf("Incorrect offsets. Expected message count %d, got %d\n", len(p0TestMsgs), newcnt-cnt)
	}
}

//Test producer GetMetadata API
func TestProducerGetMetadata(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	config := &ConfigMap{"bootstrap.servers": testconf.Brokers}

	// Create producer
	p, err := NewProducer(config)
	if err != nil {
		t.Errorf("Failed to create producer: %s\n", err)
		return
	}
	defer p.Close()

	metaData, err := p.GetMetadata(&testconf.Topic, false, 5*1000)
	if err != nil {
		t.Errorf("Failed to get meta data for topic %s. Error: %s\n", testconf.Topic, err)
		return
	}
	t.Logf("Meta data for topic %s: %v\n", testconf.Topic, metaData)

	metaData, err = p.GetMetadata(nil, true, 5*1000)
	if err != nil {
		t.Errorf("Failed to get meta data, Error: %s\n", err)
		return
	}
	t.Logf("Meta data for producer: %v\n", metaData)

}

// test producer function-based API without delivery report
func TestProducerFunc(t *testing.T) {
	producerTest(t, "Function producer (without DR)",
		nil, PC{},
		func(p *Producer, m *Message, drChan chan Event) {
			err := p.Produce(m, drChan)
			if err != nil {
				t.Errorf("Produce() failed: %v", err)
			}
		})
}

// test producer function-based API with delivery report
func TestProducerFuncDR(t *testing.T) {
	producerTest(t, "Function producer (with DR)",
		nil, PC{withDr: true},
		func(p *Producer, m *Message, drChan chan Event) {
			err := p.Produce(m, drChan)
			if err != nil {
				t.Errorf("Produce() failed: %v", err)
			}
		})
}

// test producer channel-based API without delivery report
func TestProducerChannel(t *testing.T) {
	producerTest(t, "Channel producer (without DR)",
		nil, PC{},
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})
}

// test producer channel-based API with delivery report
func TestProducerChannelDR(t *testing.T) {
	producerTest(t, "Channel producer (with DR)",
		nil, PC{withDr: true},
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})

}

// test batch producer channel-based API without delivery report
func xTestProducerBatchChannel(t *testing.T) {
	producerTest(t, "Channel producer (without DR, batch channel)",
		nil, PC{batchProducer: true},
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})
}

// test batch producer channel-based API with delivery report
func xTestProducerBatchChannelDR(t *testing.T) {
	producerTest(t, "Channel producer (DR, batch channel)",
		nil, PC{withDr: true, batchProducer: true},
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})
}

// use opaque string to locate the matching test message for message verification
func findExpectedMessage(expected []*testmsgType, opaque string) *testmsgType {
	for i, m := range expected {
		if expected[i].msg.Opaque.(string) == opaque {
			return m
		}
	}
	return nil
}

// verify the message content against the expected
func verifyMessages(t *testing.T, msgs []*Message, expected []*testmsgType) {
	if len(msgs) != len(expected) {
		t.Errorf("Expected %d messages, got %d instead\n", len(expected), len(msgs))
		return
	}
	for _, m := range msgs {
		if m.Opaque == nil {
			continue // No way to look up the corresponding expected message, let it go
		}
		testmsg := findExpectedMessage(expected, m.Opaque.(string))
		if testmsg == nil {
			t.Errorf("Cannot find a matching expected message for message %v\n", m)
			continue
		}
		em := testmsg.msg
		if m.TopicPartition.Error != nil {
			if m.TopicPartition.Error != testmsg.expectedError {
				t.Errorf("Expected error %s, but got error %s\n", testmsg.expectedError, m.TopicPartition.Error)
			}
			continue
		}

		// check partition
		if em.TopicPartition.Partition == PartitionAny {
			if m.TopicPartition.Partition < 0 {
				t.Errorf("Expected partition %d, got %d\n", em.TopicPartition.Partition, m.TopicPartition.Partition)
			}
		} else if em.TopicPartition.Partition != m.TopicPartition.Partition {
			t.Errorf("Expected partition %d, got %d\n", em.TopicPartition.Partition, m.TopicPartition.Partition)
		}

		//check Key, Value, and Opaque
		if string(m.Key) != string(em.Key) {
			t.Errorf("Expected Key %v, got %v\n", m.Key, em.Key)
		}
		if string(m.Value) != string(em.Value) {
			t.Errorf("Expected Value %v, got %v\n", m.Value, em.Value)
		}
		if m.Opaque.(string) != em.Opaque.(string) {
			t.Errorf("Expected Opaque %v, got %v\n", m.Opaque, em.Opaque)
		}

	}
}

// test consumer APIs with various message commit modes
func consumerTestWithCommits(t *testing.T, testname string, msgcnt int, useChannel bool, consumeFunc func(c *Consumer, mt *msgtracker, expCnt int), rebalanceCb func(c *Consumer, event Event) error) {
	consumerTest(t, testname+" auto commit",
		msgcnt, CC{useChannel: useChannel, autoCommit: true}, consumeFunc, rebalanceCb)

	consumerTest(t, testname+" using CommitMessage() API",
		msgcnt, CC{useChannel: useChannel, commitMode: ViaCommitMessageAPI}, consumeFunc, rebalanceCb)

	consumerTest(t, testname+" using CommitOffsets() API",
		msgcnt, CC{useChannel: useChannel, commitMode: ViaCommitOffsetsAPI}, consumeFunc, rebalanceCb)

	consumerTest(t, testname+" using Commit() API",
		msgcnt, CC{useChannel: useChannel, commitMode: ViaCommitAPI}, consumeFunc, rebalanceCb)

}

// test consumer channel-based API
func TestConsumerChannel(t *testing.T) {
	consumerTestWithCommits(t, "Channel Consumer", 0, true, eventTestChannelConsumer, nil)
}

// test consumer poll-based API
func TestConsumerPoll(t *testing.T) {
	consumerTestWithCommits(t, "Poll Consumer", 0, false, eventTestPollConsumer, nil)
}

// test consumer poll-based API with rebalance callback
func TestConsumerPollRebalance(t *testing.T) {
	consumerTestWithCommits(t, "Poll Consumer (rebalance callback)",
		0, false, eventTestPollConsumer,
		func(c *Consumer, event Event) error {
			t.Logf("Rebalanced: %s", event)
			return nil
		})
}
