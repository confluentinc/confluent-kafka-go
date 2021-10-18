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
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type config struct {
	tracerName      string
	tracerProvider  oteltrace.TracerProvider
	propagator      propagation.TextMapPropagator
	consumerGroupID string
}

// Option is used to configure the client.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (o optionFunc) apply(c *config) {
	o(c)
}

// WithTracerName specifies a specific name for the current tracer.
func WithTracerName(name string) Option {
	return optionFunc(func(cfg *config) {
		cfg.tracerName = name
	})
}

// WithTracerProvider specifies a tracer provider to use for creating a tracer.
// If none is specified, the global provider is used.
func WithTracerProvider(provider oteltrace.TracerProvider) Option {
	return optionFunc(func(cfg *config) {
		if provider != nil {
			cfg.tracerProvider = provider
		}
	})
}

// WithPropagator specifies a custom TextMapPropagator.
func WithPropagator(propagator propagation.TextMapPropagator) Option {
	return optionFunc(func(cfg *config) {
		cfg.propagator = propagator
	})
}

// WithConsumerGroupID specifies the consumer group ID that is used for creating a consumer.
func WithConsumerGroupID(consumerGroupID string) Option {
	return optionFunc(func(cfg *config) {
		cfg.consumerGroupID = consumerGroupID
	})
}
