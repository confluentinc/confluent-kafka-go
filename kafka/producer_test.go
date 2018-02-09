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
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// TestProducerAPIs dry-tests all Producer APIs, no broker is needed.
func TestProducerAPIs(t *testing.T) {

	// expected message dr count on events channel
	expMsgCnt := 0
	p, err := NewProducer(&ConfigMap{
		"socket.timeout.ms":    10,
		"default.topic.config": ConfigMap{"message.timeout.ms": 10}})
	if err != nil {
		t.Fatalf("%s", err)
	}

	t.Logf("Producer %s", p)

	drChan := make(chan Event, 10)

	topic1 := "gotest"
	topic2 := "gotest2"

	// Produce with function, DR on passed drChan
	err = p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic1, Partition: 0},
		Value: []byte("Own drChan"), Key: []byte("This is my key")},
		drChan)
	if err != nil {
		t.Errorf("Produce failed: %s", err)
	}

	// Produce with function, use default DR channel (Events)
	err = p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0},
		Value: []byte("Events DR"), Key: []byte("This is my key")},
		nil)
	if err != nil {
		t.Errorf("Produce failed: %s", err)
	}
	expMsgCnt++

	// Produce with function and timestamp,
	// success depends on librdkafka version
	err = p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0}, Timestamp: time.Now()}, nil)
	numver, strver := LibraryVersion()
	t.Logf("Produce with timestamp on %s returned: %s", strver, err)
	if numver < 0x00090400 {
		if err == nil || err.(Error).Code() != ErrNotImplemented {
			t.Errorf("Expected Produce with timestamp to fail with ErrNotImplemented on %s (0x%x), got: %s", strver, numver, err)
		}
	} else {
		if err != nil {
			t.Errorf("Produce with timestamp failed on %s: %s", strver, err)
		}
	}
	if err == nil {
		expMsgCnt++
	}

	// Produce through ProducerChannel, uses default DR channel (Events),
	// pass Opaque object.
	myOpq := "My opaque"
	p.ProduceChannel() <- &Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0},
		Opaque: &myOpq,
		Value:  []byte("ProducerChannel"), Key: []byte("This is my key")}
	expMsgCnt++

	// Len() will not report messages on private delivery report chans (our drChan for example),
	// so expect at least 2 messages, not 3.
	// And completely ignore the timestamp message.
	if p.Len() < 2 {
		t.Errorf("Expected at least 2 messages (+requests) in queue, only %d reported", p.Len())
	}

	// Message Headers
	varIntHeader := make([]byte, binary.MaxVarintLen64)
	varIntLen := binary.PutVarint(varIntHeader, 123456789)

	myHeaders := []Header{
		{"thisHdrIsNullOrNil", nil},
		{"empty", []byte("")},
		{"MyVarIntHeader", varIntHeader[:varIntLen]},
		{"mystring", []byte("This is a simple string")},
	}

	p.ProduceChannel() <- &Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0},
		Value:   []byte("Headers"),
		Headers: myHeaders}
	expMsgCnt++

	//
	// Now wait for messages to time out so that delivery reports are triggered
	//

	// drChan (1 message)
	ev := <-drChan
	m := ev.(*Message)
	if string(m.Value) != "Own drChan" {
		t.Errorf("DR for wrong message (wanted 'Own drChan'), got %s",
			string(m.Value))
	} else if m.TopicPartition.Error == nil {
		t.Errorf("Expected error for message")
	} else {
		t.Logf("Message %s", m.TopicPartition)
	}
	close(drChan)

	// Events chan (3 messages and possibly events)
	for msgCnt := 0; msgCnt < expMsgCnt; {
		ev = <-p.Events()
		switch e := ev.(type) {
		case *Message:
			msgCnt++
			if (string)(e.Value) == "ProducerChannel" {
				s := e.Opaque.(*string)
				if s != &myOpq {
					t.Errorf("Opaque should point to %v, not %v", &myOpq, s)
				}
				if *s != myOpq {
					t.Errorf("Opaque should be \"%s\", not \"%v\"",
						myOpq, *s)
				}
				t.Logf("Message \"%s\" with opaque \"%s\"\n",
					(string)(e.Value), *s)

			} else if (string)(e.Value) == "Headers" {
				if e.Opaque != nil {
					t.Errorf("Message opaque should be nil, not %v", e.Opaque)
				}
				if !reflect.DeepEqual(e.Headers, myHeaders) {
					// FIXME: Headers are currently not available on the delivery report.
					// t.Errorf("Message headers should be %v, not %v", myHeaders, e.Headers)
				}
			} else {
				if e.Opaque != nil {
					t.Errorf("Message opaque should be nil, not %v", e.Opaque)
				}
			}
		default:
			t.Logf("Ignored event %s", e)
		}
	}

	r := p.Flush(2000)
	if r > 0 {
		t.Errorf("Expected empty queue after Flush, still has %d", r)
	}

	// OffsetsForTimes
	offsets, err := p.OffsetsForTimes([]TopicPartition{{Topic: &topic2, Offset: 12345}}, 100)
	t.Logf("OffsetsForTimes() returned Offsets %s and error %s\n", offsets, err)
	if err == nil {
		t.Errorf("OffsetsForTimes() should have failed\n")
	}
	if offsets != nil {
		t.Errorf("OffsetsForTimes() failed but returned non-nil Offsets: %s\n", offsets)
	}
}

// TestProducerBufferSafety verifies issue #24, passing any type of memory backed buffer
// (JSON in this case) to Produce()
func TestProducerBufferSafety(t *testing.T) {

	p, err := NewProducer(&ConfigMap{
		"socket.timeout.ms":    10,
		"default.topic.config": ConfigMap{"message.timeout.ms": 10}})
	if err != nil {
		t.Fatalf("%s", err)
	}

	topic := "gotest"
	value, _ := json.Marshal(struct{ M string }{M: "Hello Go!"})
	empty := []byte("")

	// Try combinations of Value and Key: json value, empty, nil
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: value, Key: nil}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: value, Key: value}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: nil, Key: value}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: nil, Key: nil}, nil)

	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: empty, Key: nil}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: empty, Key: empty}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: nil, Key: empty}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: value, Key: empty}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: value, Key: value}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: empty, Key: value}, nil)

	// And Headers
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: empty, Key: value,
		Headers: []Header{{"hdr", value}, {"hdr2", empty}, {"hdr3", nil}}}, nil)

	p.Flush(100)

	p.Close()
}
