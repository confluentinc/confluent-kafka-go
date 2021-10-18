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

// Package otel instruments github.com/confluentinc/confluent-kafka-go.
//
// This instrumentation provided is tracing instrumentation for the kafka
// client.
//
// The instrumentation works by wrapping the kafka producer or consumer by calling
// `NewProducerWithTracing` or `NewConsumerWithTracing` and tracing it's every operation.

package otel // import "github.com/confluentinc/confluent-kafka-go/instrumentation/otel"
