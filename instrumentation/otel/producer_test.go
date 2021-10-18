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

package otel

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib"
	"go.opentelemetry.io/otel/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var (
	topic = "test-topic"
)

func TestProducer(t *testing.T) {
	// Given
	confluentProducer, err := kafka.NewProducer(&kafka.ConfigMap{})
	require.NoError(t, err)

	producer := NewProducerWithTracing(confluentProducer)

	// Then
	assert.Equal(t, confluentProducer, producer.Producer)
	assert.NotNil(t, producer.tracer)
}

func TestProducer_WithTracerProvider(t *testing.T) {
	// Given
	provider := trace.NewNoopTracerProvider()

	confluentProducer, err := kafka.NewProducer(&kafka.ConfigMap{})
	require.NoError(t, err)

	producer := NewProducerWithTracing(confluentProducer, WithTracerProvider(provider))

	// Then
	assert.Equal(t, confluentProducer, producer.Producer)
	assert.Equal(t, provider.Tracer(
		tracerName,
		oteltrace.WithInstrumentationVersion(contrib.SemVersion()),
	), producer.tracer)
}

func TestProducer_Produce(t *testing.T) {
	// Given
	provider := trace.NewNoopTracerProvider()

	confluentProducer, err := kafka.NewProducer(&kafka.ConfigMap{})
	require.NoError(t, err)

	producer := NewProducerWithTracing(confluentProducer, WithTracerProvider(provider))

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic,
		},
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	deliveryChan := make(chan kafka.Event)

	// When
	err = producer.Produce(message, deliveryChan)

	// Then
	assert.Nil(t, err)

	assert.Nil(t, message.Headers)
}

func TestProducer_ProduceChannel(t *testing.T) {
	// Given
	provider := trace.NewNoopTracerProvider()

	confluentProducer, err := kafka.NewProducer(&kafka.ConfigMap{})
	require.NoError(t, err)

	producer := NewProducerWithTracing(confluentProducer, WithTracerProvider(provider))

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic,
		},
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	// When
	producer.ProduceChannel() <- message

	// Then
	assert.Nil(t, message.Headers)
}
