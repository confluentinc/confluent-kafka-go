/**
 * Copyright 2018 Confluent Inc.
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

package internal

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
)

func TestAttributes_KafkaMessageHeaders(t *testing.T) {
	testCases := []struct {
		name     string
		msg      []kafka.Header
		expected []attribute.KeyValue
	}{
		{
			name:     "No header",
			msg:      nil,
			expected: nil,
		},
		{
			name: "Single header",
			msg: []kafka.Header{
				{Key: "header-1", Value: []byte("value-1")},
			},
			expected: []attribute.KeyValue{
				{Key: "messaging.headers.header-1", Value: attribute.StringValue("value-1")},
			},
		},
		{
			name: "Multiple headers",
			msg: []kafka.Header{
				{Key: "header-1", Value: []byte("value-1")},
				{Key: "header-2", Value: []byte("value-2")},
			},
			expected: []attribute.KeyValue{
				{Key: "messaging.headers.header-1", Value: attribute.StringValue("value-1")},
				{Key: "messaging.headers.header-2", Value: attribute.StringValue("value-2")},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			attributes := KafkaMessageHeaders(testCase.msg)
			assert.Equal(t, testCase.expected, attributes)
		})
	}
}
