/**
 * Copyright 2024 Confluent Inc.
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

// TestShareConsumerAPIs dry-tests the ShareConsumer APIs; no broker is needed.
func TestShareConsumerAPIs(t *testing.T) {
	// group.id is required.
	_, err := NewShareConsumer(&ConfigMap{})
	if err == nil {
		t.Fatalf("Expected NewShareConsumer() to fail without group.id")
	}

	c, err := NewShareConsumer(&ConfigMap{
		"group.id":          "gotest-share",
		"socket.timeout.ms": 10,
	})
	if err != nil {
		t.Fatalf("NewShareConsumer failed: %s", err)
	}
	t.Logf("ShareConsumer %s", c)

	if c.IsClosed() {
		t.Errorf("Expected IsClosed()==false for a fresh consumer")
	}

	// Exercise the APIs on an open consumer (no broker: async/timeout paths).
	testShareConsumerAPIs(t, c, false)

	if err := c.Close(); err != nil {
		t.Fatalf("Close() failed: %s", err)
	}
	if !c.IsClosed() {
		t.Errorf("Expected IsClosed()==true after Close()")
	}

	// Every API must reject calls on a closed consumer.
	testShareConsumerAPIs(t, c, true)

	// Close() again should report the closed-client error, not panic.
	if err := c.Close(); err == nil {
		t.Errorf("Expected Close() on a closed consumer to return an error")
	}
}

// testShareConsumerAPIs calls each ShareConsumer method. When closed is true,
// every call must return the closed-client error.
func testShareConsumerAPIs(t *testing.T, c *ShareConsumer, closed bool) {
	wantClosed := getOperationNotAllowedErrorForClosedClient()

	check := func(name string, err error) {
		if closed {
			if err != wantClosed {
				t.Errorf("%s on closed consumer: want %v, got %v", name, wantClosed, err)
			}
		}
	}

	check("Subscribe", c.Subscribe("gotest-share-topic"))
	check("SubscribeTopics", c.SubscribeTopics([]string{"t1", "t2"}))

	_, err := c.Subscription()
	check("Subscription", err)

	// Poll with a short timeout returns (nil, nil) on timeout when open.
	set, err := c.Poll(10)
	check("Poll", err)
	if !closed {
		if err != nil {
			t.Errorf("Poll() on open consumer (no broker) should time out, got err: %s", err)
		}
		if set != nil {
			t.Errorf("Poll() timeout should return a nil set, got %d messages", set.Len())
		}
	}

	_, err = c.CommitSync(10)
	check("CommitSync", err)

	check("CommitAsync", c.CommitAsync())

	check("Unsubscribe", c.Unsubscribe())
}

// TestShareAcknowledgeTypeString verifies the acknowledgement-type string forms
// and that the constants map to the librdkafka enum values.
func TestShareAcknowledgeTypeString(t *testing.T) {
	cases := []struct {
		t    ShareAcknowledgeType
		want string
	}{
		{ShareAcknowledgeTypeAccept, "accept"},
		{ShareAcknowledgeTypeRelease, "release"},
		{ShareAcknowledgeTypeReject, "reject"},
	}
	for _, c := range cases {
		if got := c.t.String(); got != c.want {
			t.Errorf("ShareAcknowledgeType(%d).String() = %q, want %q", int(c.t), got, c.want)
		}
	}

	// librdkafka enum values: ACCEPT=1, RELEASE=2, REJECT=3.
	if ShareAcknowledgeTypeAccept != 1 || ShareAcknowledgeTypeRelease != 2 || ShareAcknowledgeTypeReject != 3 {
		t.Errorf("ShareAcknowledgeType constants do not match the librdkafka enum: %d,%d,%d",
			ShareAcknowledgeTypeAccept, ShareAcknowledgeTypeRelease, ShareAcknowledgeTypeReject)
	}
}
