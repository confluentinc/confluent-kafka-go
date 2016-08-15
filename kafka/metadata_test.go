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

// TestMetadataAPIs dry-tests the Metadata APIs, no broker is needed.
func TestMetadataAPIs(t *testing.T) {

	p, err := NewProducer(&ConfigMap{"socket.timeout.ms": 10})
	if err != nil {
		t.Fatalf("%s", err)
	}

	metadata, err := p.GetMetadata(nil, true, 10)
	if err == nil {
		t.Errorf("Expected GetMetadata to fail")
	}

	topic := "gotest"
	metadata, err = p.GetMetadata(&topic, false, 10)
	if err == nil {
		t.Errorf("Expected GetMetadata to fail")
	}

	metadata, err = p.GetMetadata(nil, false, 10)
	if err == nil {
		t.Errorf("Expected GetMetadata to fail")
	}

	p.Close()

	c, err := NewConsumer(&ConfigMap{"group.id": "gotest"})
	if err != nil {
		t.Fatalf("%s", err)
	}

	metadata, err = c.GetMetadata(nil, true, 10)
	if err == nil {
		t.Errorf("Expected GetMetadata to fail")
	}
	if metadata != nil {
		t.Errorf("Return value should be nil")
	}

	c.Close()

}
