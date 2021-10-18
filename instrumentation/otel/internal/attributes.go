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
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

type Operation string

const (
	OperationProduce Operation = "produce"
	OperationConsume Operation = "consume"

	MessagingSystemKeyValue = "kafka"
)

func KafkaSystemKey() attribute.KeyValue {
	return semconv.MessagingSystemKey.String(MessagingSystemKeyValue)
}

func KafkaOperation(operationName Operation) attribute.KeyValue {
	return semconv.MessagingOperationKey.String(string(operationName))
}

func KafkaDestinationTopic(topic string) attribute.KeyValue {
	return semconv.MessagingDestinationKey.String(topic)
}

func KafkaMessageKey(messageID string) attribute.KeyValue {
	return semconv.MessagingMessageIDKey.String(messageID)
}

func KafkaMessageHeaders(headers []kafka.Header) []attribute.KeyValue {
	var attributes []attribute.KeyValue

	for _, header := range headers {
		attributes = append(attributes, attribute.Key("messaging.headers."+header.Key).String(string(header.Value)))
	}

	return attributes
}

func KafkaConsumerGroupID(consumerGroupID string) attribute.KeyValue {
	return semconv.MessagingKafkaConsumerGroupKey.String(consumerGroupID)
}
