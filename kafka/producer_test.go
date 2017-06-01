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
	"encoding/json"
	"testing"
	"time"
)

type Key struct {
	K string
}

type Envelope struct {
	M string
}

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

	key := Key{K: "This is my key"}
	keyData, err := json.Marshal(key)

	if err != nil {
		t.Error(err)
	}

	envelope := Envelope{M: "Own drChan"}
	envelopeData, err := json.Marshal(envelope)

	if err != nil {
		t.Error(err)
	}

	// Produce with function, DR on passed drChan
	err = p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic1, Partition: 0},
		Value: envelopeData, Key: keyData},
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

	//
	// Now wait for messages to time out so that delivery reports are triggered
	//

	// drChan (1 message)
	ev := <-drChan
	m := ev.(*Message)

	var envelope2 Envelope

	err = json.Unmarshal(m.Value, &envelope2)

	if err != nil {
		t.Error(err)
	}

	if envelope2.M != "Own drChan" {
		t.Errorf("DR for wrong message (wanted 'Own drChan'), got %s",
			string(envelope2.M))
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
}
