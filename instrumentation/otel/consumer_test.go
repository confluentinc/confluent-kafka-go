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
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib"
	"go.opentelemetry.io/otel/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestConsumer(t *testing.T) {
	// Given
	confluentConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"group.id": "consumer.test",
	})
	require.NoError(t, err)

	consumer := NewConsumerWithTracing(confluentConsumer)

	// Then
	assert.Equal(t, confluentConsumer, consumer.Consumer)
	assert.NotNil(t, consumer.tracer)
}

func TestConsumer_WithTracerProvider(t *testing.T) {
	// Given
	provider := trace.NewNoopTracerProvider()

	confluentConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"group.id": "consumer.test",
	})
	require.NoError(t, err)

	consumer := NewConsumerWithTracing(confluentConsumer, WithTracerProvider(provider))

	// Then
	assert.Equal(t, confluentConsumer, consumer.Consumer)
	assert.Equal(t, provider.Tracer(
		tracerName,
		oteltrace.WithInstrumentationVersion(contrib.SemVersion()),
	), consumer.tracer)
}

func TestConsumer_WithConsumerGroupID(t *testing.T) {
	// Given
	consumerGroupID := "my.test.consumer.group"

	confluentConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"group.id": consumerGroupID,
	})
	require.NoError(t, err)

	consumer := NewConsumerWithTracing(confluentConsumer, WithConsumerGroupID(consumerGroupID))

	// Then
	assert.Equal(t, confluentConsumer, consumer.Consumer)
	assert.Equal(t, consumerGroupID, consumer.consumerGroupID)
}

func TestConsumer_ReadMessage(t *testing.T) {
	// Given
	provider := trace.NewNoopTracerProvider()

	confluentConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"group.id": "consumer.test",
	})
	require.NoError(t, err)

	consumer := NewConsumerWithTracing(confluentConsumer, WithTracerProvider(provider))

	// When
	timeout := 1 * time.Second
	msg, err := consumer.ReadMessage(timeout)

	// Then
	assert.Nil(t, msg)
	assert.Equal(t, err.(kafka.Error).Code(), kafka.ErrTimedOut)
}

func TestConsumer_ReadMessageWithHandler(t *testing.T) {
	// Given
	provider := trace.NewNoopTracerProvider()

	confluentConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"group.id": "consumer.test",
	})
	require.NoError(t, err)

	consumer := NewConsumerWithTracing(confluentConsumer, WithTracerProvider(provider))

	handler := func(consumer *kafka.Consumer, msg *kafka.Message) error {
		// Nothing to do here...
		return nil
	}

	// When
	timeout := 1 * time.Second
	msg, err := consumer.ReadMessageWithHandler(timeout, handler)

	// Then
	assert.Nil(t, msg)
	assert.Equal(t, err.(kafka.Error).Code(), kafka.ErrTimedOut)
}

func TestConsumer_PollWithHandler(t *testing.T) {
	// Given
	provider := trace.NewNoopTracerProvider()

	confluentConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"group.id": "consumer.test",
	})
	require.NoError(t, err)

	consumer := NewConsumerWithTracing(confluentConsumer, WithTracerProvider(provider))

	handler := func(consumer *kafka.Consumer, msg *kafka.Message) error {
		// Nothing to do here...
		return nil
	}

	// When
	event := consumer.PollWithHandler(1000, handler)

	// Then
	assert.Nil(t, event)
}

func TestConsumer_Poll(t *testing.T) {
	// Given
	provider := trace.NewNoopTracerProvider()

	confluentConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"group.id": "consumer.test",
	})
	require.NoError(t, err)

	consumer := NewConsumerWithTracing(confluentConsumer, WithTracerProvider(provider))

	// When
	event := consumer.Poll(1000)

	// Then
	assert.Nil(t, event)
}
