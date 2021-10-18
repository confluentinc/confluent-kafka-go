package otel

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.opentelemetry.io/otel/propagation"
)

var _ propagation.TextMapCarrier = (*MessageCarrier)(nil)

// MessageCarrier injects and extracts traces from a sarama.Message.
type MessageCarrier struct {
	msg *kafka.Message
}

// NewMessageCarrier creates a new MessageCarrier.
func NewMessageCarrier(msg *kafka.Message) MessageCarrier {
	return MessageCarrier{msg: msg}
}

// Get retrieves a single value for a given key from Kafka message headers.
func (c MessageCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set sets a header on Kafka message.
func (c MessageCarrier) Set(key, value string) {
	if key == "" || value == "" {
		return
	}

	// Ensure uniqueness of keys
	for i := 0; i < len(c.msg.Headers); i++ {
		if c.msg.Headers[i].Key == key {
			c.msg.Headers = append(c.msg.Headers[:i], c.msg.Headers[i+1:]...)
			i--
		}
	}

	c.msg.Headers = append(c.msg.Headers, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

// Keys returns all keys identifiers from the Kafka message headers.
func (c MessageCarrier) Keys() []string {
	out := make([]string, len(c.msg.Headers))
	for i, h := range c.msg.Headers {
		out[i] = h.Key
	}
	return out
}
