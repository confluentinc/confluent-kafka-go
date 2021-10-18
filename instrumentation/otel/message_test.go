package otel

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/propagation"
)

func TestNewMessageCarrier(t *testing.T) {
	// Given
	msg := &kafka.Message{}

	// When
	carrier := NewMessageCarrier(msg)

	// Then
	assert.Implements(t, (*propagation.TextMapCarrier)(nil), carrier)
	assert.Equal(t, msg, carrier.msg)
}

func TestMessageCarrier_Get(t *testing.T) {
	// Given
	testCases := []struct {
		name     string
		msg      *kafka.Message
		key      string
		expected string
	}{
		{
			name:     "Headers is not defined",
			msg:      &kafka.Message{},
			key:      "taceparent",
			expected: "",
		},
		{
			name: "Value is absent",
			msg: &kafka.Message{
				Headers: []kafka.Header{},
			},
			key:      "taceparent",
			expected: "",
		},
		{
			name: "Value is present",
			msg: &kafka.Message{
				Headers: []kafka.Header{
					{Key: "traceparent", Value: []byte("123456789123456789123456789123-1234567891234567")},
				},
			},
			key:      "traceparent",
			expected: "123456789123456789123456789123-1234567891234567",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			carrier := NewMessageCarrier(testCase.msg)
			value := carrier.Get(testCase.key)

			assert.Equal(t, testCase.expected, value)
		})
	}
}

func TestMessageCarrier_Set(t *testing.T) {
	// Given
	testCases := []struct {
		name     string
		msg      *kafka.Message
		key      string
		value    string
		expected []kafka.Header
	}{
		{
			name:     "Empty values",
			msg:      &kafka.Message{},
			key:      "",
			value:    "",
			expected: nil,
		},
		{
			name:  "Set a new value",
			msg:   &kafka.Message{},
			key:   "traceparent",
			value: "123456789123456789123456789123-1234567891234567",
			expected: []kafka.Header{
				{Key: "traceparent", Value: []byte("123456789123456789123456789123-1234567891234567")},
			},
		},
		{
			name: "Set a new value with other values set",
			msg: &kafka.Message{
				Headers: []kafka.Header{
					{Key: "my-test-key", Value: []byte("my-value-key")},
				},
			},
			key:   "traceparent",
			value: "123456789123456789123456789123-1234567891234567",
			expected: []kafka.Header{
				{Key: "my-test-key", Value: []byte("my-value-key")},
				{Key: "traceparent", Value: []byte("123456789123456789123456789123-1234567891234567")},
			},
		},
		{
			name: "Replace value when already defined",
			msg: &kafka.Message{
				Headers: []kafka.Header{
					{Key: "traceparent", Value: []byte("an-old-value-that-should-be-overriden")},
				},
			},
			key:   "traceparent",
			value: "123456789123456789123456789123-1234567891234567",
			expected: []kafka.Header{
				{Key: "traceparent", Value: []byte("123456789123456789123456789123-1234567891234567")},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			carrier := NewMessageCarrier(testCase.msg)
			carrier.Set(testCase.key, testCase.value)

			assert.Equal(t, testCase.expected, testCase.msg.Headers)
		})
	}
}

func TestMessageCarrier_Keys(t *testing.T) {
	// Given
	testCases := []struct {
		name     string
		msg      *kafka.Message
		expected []string
	}{
		{
			name:     "No headers defined",
			msg:      &kafka.Message{},
			expected: []string{},
		},
		{
			name: "Empty values",
			msg: &kafka.Message{
				Headers: []kafka.Header{},
			},
			expected: []string{},
		},
		{
			name: "Having some values",
			msg: &kafka.Message{
				Headers: []kafka.Header{
					{Key: "traceparent", Value: []byte("123456789123456789123456789123-1234567891234567")},
					{Key: "my-test-key", Value: []byte("my-test-value")},
				},
			},
			expected: []string{"traceparent", "my-test-key"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			carrier := NewMessageCarrier(testCase.msg)
			assert.Equal(t, testCase.expected, carrier.Keys())
		})
	}
}
