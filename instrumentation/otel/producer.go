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
	"context"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/instrumentation/otel/internal"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"go.opentelemetry.io/contrib"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type Producer struct {
	*kafka.Producer
	ctx        context.Context
	tracer     oteltrace.Tracer
	propagator propagation.TextMapPropagator
	spans      *sync.Map
	events     chan kafka.Event
}

func NewProducerWithTracing(producer *kafka.Producer, opts ...Option) *Producer {
	cfg := &config{
		tracerProvider: otel.GetTracerProvider(),
		propagator:     otel.GetTextMapPropagator(),
		tracerName:     tracerName,
	}

	for _, o := range opts {
		o.apply(cfg)
	}

	p := &Producer{
		Producer: producer,
		ctx:      context.Background(),
		tracer: cfg.tracerProvider.Tracer(
			cfg.tracerName,
			oteltrace.WithInstrumentationVersion(contrib.SemVersion()),
		),
		propagator: cfg.propagator,
		spans:      &sync.Map{},
		events:     make(chan kafka.Event, cap(producer.Events())),
	}

	go p.listenEvents()

	return p
}

func (p *Producer) listenEvents() {
	for event := range p.Producer.Events() {
		p.events <- event

		switch ev := event.(type) {
		case *kafka.Message:
			msg := ev
			if msg == nil {
				continue
			}

			spanID := msg.Opaque

			if spanID == "" {
				continue
			}

			if s, ok := p.spans.LoadAndDelete(spanID); ok {
				span := s.(oteltrace.Span)
				endSpan(span, msg.TopicPartition.Error)
			}
		}
	}
}

// Events returns the channel events
func (p *Producer) Events() chan kafka.Event {
	return p.events
}

func (p *Producer) attrsByOperationAndMessage(operation internal.Operation, msg *kafka.Message) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		internal.KafkaSystemKey(),
		internal.KafkaOperation(operation),
		semconv.MessagingDestinationKindTopic,
	}

	if msg != nil {
		attributes = append(attributes, internal.KafkaMessageKey(string(msg.Key)))
		attributes = append(attributes, internal.KafkaMessageHeaders(msg.Headers)...)
		attributes = append(attributes, semconv.MessagingKafkaPartitionKey.Int(int(msg.TopicPartition.Partition)))

		if topic := msg.TopicPartition.Topic; topic != nil {
			attributes = append(attributes, internal.KafkaDestinationTopic(*topic))
		}
	}

	return attributes
}

func (p *Producer) startSpan(operationName internal.Operation, msg *kafka.Message) oteltrace.Span {
	opts := []oteltrace.SpanStartOption{
		oteltrace.WithSpanKind(oteltrace.SpanKindProducer),
	}

	carrier := NewMessageCarrier(msg)
	ctx := p.propagator.Extract(p.ctx, carrier)

	ctx, span := p.tracer.Start(ctx, string(operationName), opts...)

	p.propagator.Inject(ctx, carrier)

	span.SetAttributes(p.attrsByOperationAndMessage(operationName, msg)...)

	return span
}

// Produce creates a new span and produces the given Kafka message synchronously
// using the original producer.
func (p *Producer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	s := p.startSpan(internal.OperationProduce, msg)
	err := p.Producer.Produce(msg, deliveryChan)
	endSpan(s, err)
	return err
}

// ProduceChannel creates a new span for every messages sent into the channel and
// forwards to the original producer channel.
func (p *Producer) ProduceChannel() chan *kafka.Message {
	wrappedProduceChannel := make(chan *kafka.Message, 0)

	go func() {
		for msg := range wrappedProduceChannel {
			s := p.startSpan(internal.OperationProduce, msg)

			// Store span identifier in message in order to retrieve it later when
			// events will confirm that it is produced to the brokers.
			spanID := s.SpanContext().SpanID().String()
			msg.Opaque = spanID
			p.spans.Store(spanID, s)

			p.Producer.ProduceChannel() <- msg
		}
	}()

	return wrappedProduceChannel
}

func (p *Producer) Close() {
	p.Producer.Close()
	close(p.events)
}
