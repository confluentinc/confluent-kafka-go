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
	"testing"
)

// TestProducerAPIs dry-tests all Producer APIs, no broker is needed.
func TestProducerAPIs(t *testing.T) {

	p, err := NewProducer(&ConfigMap{
		"socket.timeout.ms":    10,
		"default.topic.config": ConfigMap{"message.timeout.ms": 10}})
	if err != nil {
		t.Fatalf("%s", err)
	}

	t.Logf("Producer %s", p)

	dr_chan := make(chan Event, 10)

	topic1 := "gotest"
	topic2 := "gotest2"

	// Produce with function, DR on passed dr_chan
	err = p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic1, Partition: 0},
		Value: []byte("Own dr_chan"), Key: []byte("This is my key")},
		dr_chan, nil)
	if err != nil {
		t.Errorf("Produce failed: %s", err)
	}

	// Produce with function, use default DR channel (Events)
	err = p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0},
		Value: []byte("Events DR"), Key: []byte("This is my key")},
		nil, nil)
	if err != nil {
		t.Errorf("Produce failed: %s", err)
	}

	// Produce through ProducerChannel, uses default DR channel (Events)
	p.ProduceChannel <- &Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0},
		Value: []byte("ProducerChannel"), Key: []byte("This is my key")}

	// Len() will not report messages on private delivery report chans (our dr_chan for example),
	// so expect at least 2 messages, not 3.
	if p.Len() < 2 {
		t.Errorf("Expected at least 2 messages (+requests) in queue, only %d reported", p.Len())
	}

	//
	// Now wait for messages to time out so that delivery reports are triggered
	//

	// dr_chan (1 message)
	ev := <-dr_chan
	m := ev.(*Message)
	if string(m.Value) != "Own dr_chan" {
		t.Errorf("DR for wrong message (wanted 'Own dr_chan'), got %s",
			string(m.Value))
	} else if m.TopicPartition.Error == nil {
		t.Errorf("Expected error for message")
	} else {
		t.Logf("Message %s", m.TopicPartition)
	}
	close(dr_chan)

	// Events chan (2 messages and possibly events)
	for msg_cnt := 0; msg_cnt < 2; {
		ev = <-p.Events
		switch e := ev.(type) {
		case *Message:
			msg_cnt += 1
		default:
			t.Logf("Ignored event %s", e)
		}
	}

	r := p.Flush(2000)
	if r > 0 {
		t.Errorf("Expected empty queue after Flush, still has %d", r)
	}
}
