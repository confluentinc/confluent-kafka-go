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
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// producer test control
type producerCtrl struct {
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
type consumerCtrl struct {
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

var testMsgsInit = false
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

	// a test message with default initialization
	testmsgs[i] = &testmsgType{msg: Message{TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: 0}}}
	i++

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
func producerTest(t *testing.T, testname string, testmsgs []*testmsgType, pc producerCtrl, produceFunc func(p *Producer, m *Message, drChan chan Event)) {

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

	p.Close()

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
func consumerTest(t *testing.T, testname string, msgcnt int, cc consumerCtrl, consumeFunc func(c *Consumer, mt *msgtracker, expCnt int), rebalanceCb func(c *Consumer, event Event) error) {

	if msgcnt == 0 {
		createTestMessages()
		producerTest(t, "Priming producer", p0TestMsgs, producerCtrl{},
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

	// Trigger RevokePartitions
	c.Unsubscribe()

	// Handle RevokePartitions
	c.Poll(500)

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
	producerTest(t, "Priming producer", p0TestMsgs, producerCtrl{silent: true},
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

//TestConsumerOffsetsForTimes
func TestConsumerOffsetsForTimes(t *testing.T) {
	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"group.id":            testconf.GroupID,
		"api.version.request": true}

	conf.updateFromTestconf()

	c, err := NewConsumer(&conf)

	if err != nil {
		panic(err)
	}
	defer c.Close()

	// Prime topic with test messages
	createTestMessages()
	producerTest(t, "Priming producer", p0TestMsgs, producerCtrl{silent: true},
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})

	times := make([]TopicPartition, 1)
	times[0] = TopicPartition{Topic: &testconf.Topic, Partition: 0, Offset: 12345}
	offsets, err := c.OffsetsForTimes(times, 5000)
	if err != nil {
		t.Errorf("OffsetsForTimes() failed: %s\n", err)
		return
	}

	if len(offsets) != 1 {
		t.Errorf("OffsetsForTimes() returned wrong length %d, expected 1\n", len(offsets))
		return
	}

	if *offsets[0].Topic != testconf.Topic || offsets[0].Partition != 0 {
		t.Errorf("OffsetsForTimes() returned wrong topic/partition\n")
		return
	}

	if offsets[0].Error != nil {
		t.Errorf("OffsetsForTimes() returned error for partition 0: %s\n", err)
		return
	}

	low, _, err := c.QueryWatermarkOffsets(testconf.Topic, 0, 5*1000)
	if err != nil {
		t.Errorf("Failed to query watermark offsets for topic %s. Error: %s\n", testconf.Topic, err)
		return
	}

	t.Logf("OffsetsForTimes() returned offset %d for timestamp %d\n", offsets[0].Offset, times[0].Offset)

	// Since we're using a phony low timestamp it is assumed that the returned
	// offset will be oldest message.
	if offsets[0].Offset != Offset(low) {
		t.Errorf("OffsetsForTimes() returned invalid offset %d for timestamp %d, expected %d\n", offsets[0].Offset, times[0].Offset, low)
		return
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
	producerTest(t, "Priming producer", p0TestMsgs, producerCtrl{silent: true},
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
		nil, producerCtrl{},
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
		nil, producerCtrl{withDr: true},
		func(p *Producer, m *Message, drChan chan Event) {
			err := p.Produce(m, drChan)
			if err != nil {
				t.Errorf("Produce() failed: %v", err)
			}
		})
}

// test producer with bad messages
func TestProducerWithBadMessages(t *testing.T) {
	conf := ConfigMap{"bootstrap.servers": testconf.Brokers}
	p, err := NewProducer(&conf)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// producing a nil message should return an error without crash
	err = p.Produce(nil, p.Events())
	if err == nil {
		t.Errorf("Producing a nil message should return error\n")
	} else {
		t.Logf("Producing a nil message returns expected error: %s\n", err)
	}

	// producing a blank message (with nil Topic) should return an error without crash
	err = p.Produce(&Message{}, p.Events())
	if err == nil {
		t.Errorf("Producing a blank message should return error\n")
	} else {
		t.Logf("Producing a blank message returns expected error: %s\n", err)
	}
}

// test producer channel-based API without delivery report
func TestProducerChannel(t *testing.T) {
	producerTest(t, "Channel producer (without DR)",
		nil, producerCtrl{},
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})
}

// test producer channel-based API with delivery report
func TestProducerChannelDR(t *testing.T) {
	producerTest(t, "Channel producer (with DR)",
		nil, producerCtrl{withDr: true},
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})

}

// test batch producer channel-based API without delivery report
func TestProducerBatchChannel(t *testing.T) {
	producerTest(t, "Channel producer (without DR, batch channel)",
		nil, producerCtrl{batchProducer: true},
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})
}

// test batch producer channel-based API with delivery report
func TestProducerBatchChannelDR(t *testing.T) {
	producerTest(t, "Channel producer (DR, batch channel)",
		nil, producerCtrl{withDr: true, batchProducer: true},
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})
}

// use opaque string to locate the matching test message for message verification
func findExpectedMessage(expected []*testmsgType, opaque string) *testmsgType {
	for i, m := range expected {
		if expected[i].msg.Opaque != nil && expected[i].msg.Opaque.(string) == opaque {
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
		msgcnt, consumerCtrl{useChannel: useChannel, autoCommit: true}, consumeFunc, rebalanceCb)

	consumerTest(t, testname+" using CommitMessage() API",
		msgcnt, consumerCtrl{useChannel: useChannel, commitMode: ViaCommitMessageAPI}, consumeFunc, rebalanceCb)

	consumerTest(t, testname+" using CommitOffsets() API",
		msgcnt, consumerCtrl{useChannel: useChannel, commitMode: ViaCommitOffsetsAPI}, consumeFunc, rebalanceCb)

	consumerTest(t, testname+" using Commit() API",
		msgcnt, consumerCtrl{useChannel: useChannel, commitMode: ViaCommitAPI}, consumeFunc, rebalanceCb)

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

// Test Committed() API
func TestConsumerCommitted(t *testing.T) {
	consumerTestWithCommits(t, "Poll Consumer (rebalance callback, verify Committed())",
		0, false, eventTestPollConsumer,
		func(c *Consumer, event Event) error {
			t.Logf("Rebalanced: %s", event)
			rp, ok := event.(RevokedPartitions)
			if ok {
				offsets, err := c.Committed(rp.Partitions, 5000)
				if err != nil {
					t.Errorf("Failed to get committed offsets: %s\n", err)
					return nil
				}

				t.Logf("Retrieved Committed offsets: %s\n", offsets)

				if len(offsets) != len(rp.Partitions) || len(rp.Partitions) == 0 {
					t.Errorf("Invalid number of partitions %d, should be %d (and >0)\n", len(offsets), len(rp.Partitions))
				}

				// Verify proper offsets: at least one partition needs
				// to have a committed offset.
				validCnt := 0
				for _, p := range offsets {
					if p.Error != nil {
						t.Errorf("Committed() partition error: %v: %v", p, p.Error)
					} else if p.Offset >= 0 {
						validCnt++
					}
				}

				if validCnt == 0 {
					t.Errorf("Committed(): no partitions with valid offsets: %v", offsets)
				}
			}
			return nil
		})
}

// TestProducerConsumerTimestamps produces messages with timestamps
// and verifies them on consumption.
// Requires librdkafka >=0.9.4 and Kafka >=0.10.0.0
func TestProducerConsumerTimestamps(t *testing.T) {
	numver, strver := LibraryVersion()
	if numver < 0x00090400 {
		t.Skipf("Requires librdkafka >=0.9.4 (currently on %s)", strver)
	}

	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"api.version.request":      true,
		"go.events.channel.enable": true,
		"group.id":                 testconf.Topic,
	}

	conf.updateFromTestconf()

	/* Create consumer and find recognizable message, verify timestamp.
	 * The consumer is started before the producer to make sure
	 * the message isn't missed. */
	t.Logf("Creating consumer")
	c, err := NewConsumer(&conf)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	t.Logf("Assign %s [0]", testconf.Topic)
	err = c.Assign([]TopicPartition{{Topic: &testconf.Topic, Partition: 0,
		Offset: OffsetEnd}})
	if err != nil {
		t.Fatalf("Assign: %v", err)
	}

	/* Wait until EOF is reached so we dont miss the produced message */
	for ev := range c.Events() {
		t.Logf("Awaiting initial EOF")
		_, ok := ev.(PartitionEOF)
		if ok {
			break
		}
	}

	/*
	 * Create producer and produce one recognizable message with timestamp
	 */
	t.Logf("Creating producer")
	conf.SetKey("{topic}.produce.offset.report", true)
	p, err := NewProducer(&conf)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	drChan := make(chan Event, 1)

	/* Offset the timestamp to avoid comparison with system clock */
	future, _ := time.ParseDuration("87658h") // 10y
	timestamp := time.Now().Add(future)
	key := fmt.Sprintf("TS: %v", timestamp)
	t.Logf("Producing message with timestamp %v", timestamp)
	err = p.Produce(&Message{
		TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: 0},
		Key:            []byte(key),
		Timestamp:      timestamp},
		drChan)

	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Wait for delivery
	t.Logf("Awaiting delivery report")
	ev := <-drChan
	m, ok := ev.(*Message)
	if !ok {
		t.Fatalf("drChan: Expected *Message, got %v", ev)
	}
	if m.TopicPartition.Error != nil {
		t.Fatalf("Delivery failed: %v", m.TopicPartition)
	}
	t.Logf("Produced message to %v", m.TopicPartition)
	producedOffset := m.TopicPartition.Offset

	p.Close()

	/* Now consume messages, waiting for that recognizable one. */
	t.Logf("Consuming messages")
outer:
	for ev := range c.Events() {
		switch m := ev.(type) {
		case *Message:
			if m.TopicPartition.Error != nil {
				continue
			}
			if m.Key == nil || string(m.Key) != key {
				continue
			}

			t.Logf("Found message at %v with timestamp %s %s",
				m.TopicPartition,
				m.TimestampType, m.Timestamp)

			if m.TopicPartition.Offset != producedOffset {
				t.Fatalf("Produced Offset %d does not match consumed offset %d", producedOffset, m.TopicPartition.Offset)
			}

			if m.TimestampType != TimestampCreateTime {
				t.Fatalf("Expected timestamp CreateTime, not %s",
					m.TimestampType)
			}

			/* Since Kafka timestamps are milliseconds we need to
			 * shave off some precision for the comparison */
			if m.Timestamp.UnixNano()/1000000 !=
				timestamp.UnixNano()/1000000 {
				t.Fatalf("Expected timestamp %v (%d), not %v (%d)",
					timestamp, timestamp.UnixNano(),
					m.Timestamp, m.Timestamp.UnixNano())
			}
			break outer
		default:
		}
	}

	c.Close()
}

// TestProducerConsumerHeaders produces messages with headers
// and verifies them on consumption.
// Requires librdkafka >=0.11.4 and Kafka >=0.11.0.0
func TestProducerConsumerHeaders(t *testing.T) {
	numver, strver := LibraryVersion()
	if numver < 0x000b0400 {
		t.Skipf("Requires librdkafka >=0.11.4 (currently on %s, 0x%x)", strver, numver)
	}

	if !testconfRead() {
		t.Skipf("Missing testconf.json")
	}

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"api.version.request": true,
		"enable.auto.commit":  false,
		"group.id":            testconf.Topic,
	}

	conf.updateFromTestconf()

	/*
	 * Create producer and produce a couple of messages with and without
	 * headers.
	 */
	t.Logf("Creating producer")
	p, err := NewProducer(&conf)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	drChan := make(chan Event, 1)

	// prepare some header values
	bigBytes := make([]byte, 2500)
	for i := 0; i < len(bigBytes); i++ {
		bigBytes[i] = byte(i)
	}

	myVarint := make([]byte, binary.MaxVarintLen64)
	myVarintLen := binary.PutVarint(myVarint, 12345678901234)

	expMsgHeaders := [][]Header{
		{
			{"msgid", []byte("1")},
			{"a key with SPACES ", bigBytes[:15]},
			{"BIGONE!", bigBytes},
		},
		{
			{"msgid", []byte("2")},
			{"myVarint", myVarint[:myVarintLen]},
			{"empty", []byte("")},
			{"theNullIsNil", nil},
		},
		nil, // no headers
		{
			{"msgid", []byte("4")},
			{"order", []byte("1")},
			{"order", []byte("2")},
			{"order", nil},
			{"order", []byte("4")},
		},
	}

	t.Logf("Producing %d messages", len(expMsgHeaders))
	for _, hdrs := range expMsgHeaders {
		err = p.Produce(&Message{
			TopicPartition: TopicPartition{Topic: &testconf.Topic, Partition: 0},
			Headers:        hdrs},
			drChan)
	}

	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	var firstOffset Offset = OffsetInvalid
	for range expMsgHeaders {
		ev := <-drChan
		m, ok := ev.(*Message)
		if !ok {
			t.Fatalf("drChan: Expected *Message, got %v", ev)
		}
		if m.TopicPartition.Error != nil {
			t.Fatalf("Delivery failed: %v", m.TopicPartition)
		}
		t.Logf("Produced message to %v", m.TopicPartition)
		if firstOffset == OffsetInvalid {
			firstOffset = m.TopicPartition.Offset
		}
	}

	p.Close()

	/* Now consume the produced messages and verify the headers */
	t.Logf("Creating consumer starting at offset %v", firstOffset)
	c, err := NewConsumer(&conf)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	err = c.Assign([]TopicPartition{{Topic: &testconf.Topic, Partition: 0,
		Offset: firstOffset}})
	if err != nil {
		t.Fatalf("Assign: %v", err)
	}

	for n, hdrs := range expMsgHeaders {
		m, err := c.ReadMessage(-1)
		if err != nil {
			t.Fatalf("Expected message #%d, not error %v", n, err)
		}

		if m.Headers == nil {
			if hdrs == nil {
				continue
			}
			t.Fatalf("Expected message #%d to have headers", n)
		}

		if hdrs == nil {
			t.Fatalf("Expected message #%d not to have headers, but found %v", n, m.Headers)
		}

		// Compare headers
		if !reflect.DeepEqual(hdrs, m.Headers) {
			t.Fatalf("Expected message #%d headers to match %v, but found %v", n, hdrs, m.Headers)
		}

		t.Logf("Message #%d headers matched: %v", n, m.Headers)
	}

	c.Close()
}
