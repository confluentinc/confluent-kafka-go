/**
 * Copyright 2023 Confluent Inc.
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

// Integration tests using the mock cluster.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestImmediateFlush tests that producer.Flush ignores
// "queue.buffering.max.ms". (Issue #1013).
func TestImmediateFlush(t *testing.T) {
	assert := assert.New(t)

	// Large queue.buffering.max.ms
	queueBufferingMs := 5000
	// Make sure that flush timeout exceeds queueBufferingMs, so we know if we
	// are waiting for queueBufferingMs while flushing rather than timing out
	// prematurely.
	flushTimeoutMs := queueBufferingMs + 1000
	expectedFlushMs := 1000

	// Create mock cluster
	mockCluster, err := NewMockCluster(2)
	assert.NoError(err, "Mock cluster creation should succeed")
	defer mockCluster.Close()

	// Create producer and send message
	cfg := &ConfigMap{
		"bootstrap.servers":      mockCluster.BootstrapServers(),
		"queue.buffering.max.ms": queueBufferingMs,
	}
	p, err := NewProducer(cfg)
	assert.NoError(err, "Producer creation should succeed")
	defer p.Close()

	topic := "topic"
	msg := Message{
		TopicPartition: TopicPartition{
			Topic:     &topic,
			Partition: PartitionAny,
		},
		Value: []byte("value"),
	}
	err = p.Produce(&msg, nil)
	assert.NoError(err, "Message should be produced")

	// Consume all producer events and discard them.
	go func() {
		for range p.Events() {
		}
	}()

	// Flush messages.
	startTime := time.Now()
	n := p.Flush(flushTimeoutMs)
	elapsed := time.Since(startTime)
	assert.Less(
		elapsed, time.Second,
		"Flush should not take more than %dms, took %dms",
		expectedFlushMs, elapsed.Milliseconds())
	assert.Zero(n, "Nothing should be unflushed")
}
