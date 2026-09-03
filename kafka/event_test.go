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

// TestEventAPIs dry-tests the public event related APIs, no broker is needed.
func TestEventAPIs(t *testing.T) {
	assignedPartitions := AssignedPartitions{}
	t.Logf("%s\n", assignedPartitions.String())

	revokedPartitions := RevokedPartitions{}
	t.Logf("%s\n", revokedPartitions.String())

	topic := "test"
	partition := PartitionEOF{Topic: &topic}
	t.Logf("%s\n", partition.String())

	partition = PartitionEOF{}
	t.Logf("%s\n", partition.String())

	committedOffsets := OffsetsCommitted{}
	t.Logf("%s\n", committedOffsets.String())

	stats := Stats{"{\"name\": \"Producer-1\"}"}
	t.Logf("Stats: %s\n", stats.String())

	oauthBearerTokenRefresh := OAuthBearerTokenRefresh{"some=config"}
	t.Logf("%s\n", oauthBearerTokenRefresh.String())
}

// TestHandleSendToChannel tests the default delivery channel handler used by
// eventPoll(), which a SerializingProducer replaces with its own.
func TestHandleSendToChannel(t *testing.T) {
	p, err := NewProducer(&ConfigMap{"socket.timeout.ms": 10})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	topic := "gotest"
	msg := &Message{TopicPartition: TopicPartition{Topic: &topic}}

	deliveryChan := make(chan Event, 1)
	termChan := make(chan bool)
	if term := p.handle.sendToChannel(msg, &deliveryChan, termChan); term {
		t.Errorf("Expected the event to be sent, not terminated")
	}
	if ev := <-deliveryChan; ev != any(msg) {
		t.Errorf("Expected the message on the channel, got %v", ev)
	}

	// A termination signal on a channel nobody reads from is reported back.
	blockedChan := make(chan Event)
	close(termChan)
	if term := p.handle.sendToChannel(msg, &blockedChan, termChan); !term {
		t.Errorf("Expected termination to be reported")
	}
}
