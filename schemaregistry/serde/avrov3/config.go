/**
 * Copyright 2026 Confluent Inc.
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

package avrov3

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

// SerializerConfig is used to pass multiple configuration options to the serializers.
type SerializerConfig struct {
	serde.SerializerConfig

	// UnionResolutionError, if true, causes encoding to return an error when
	// a union type cannot be resolved. Forwarded to the underlying
	// confluent-avro-go Config of the same name.
	UnionResolutionError bool

	// PartialUnionTypeResolution, if true, lets union type resolution succeed
	// when only some of the union's variants have been registered: registered
	// variants use their typed Go struct, unregistered variants fall back to
	// map[string]any. Without this, a single unregistered variant in a union
	// downgrades every variant to map[string]any. Forwarded to the underlying
	// confluent-avro-go Config of the same name.
	PartialUnionTypeResolution bool
}

// NewSerializerConfig returns a new configuration instance with sane defaults.
func NewSerializerConfig() *SerializerConfig {
	c := &SerializerConfig{
		SerializerConfig: *serde.NewSerializerConfig(),
	}

	return c
}

// DeserializerConfig is used to pass multiple configuration options to the deserializers.
type DeserializerConfig struct {
	serde.DeserializerConfig

	// UnionResolutionError, if true, causes decoding to return an error when
	// a union type cannot be resolved. Forwarded to the underlying
	// confluent-avro-go Config of the same name.
	UnionResolutionError bool

	// PartialUnionTypeResolution, if true, lets union type resolution succeed
	// when only some of the union's variants have been registered: registered
	// variants decode into their typed Go struct, unregistered variants fall
	// back to map[string]any. Without this, a single unregistered variant in a
	// union downgrades every variant to map[string]any. Forwarded to the
	// underlying confluent-avro-go Config of the same name.
	PartialUnionTypeResolution bool
}

// NewDeserializerConfig returns a new configuration instance with sane defaults.
func NewDeserializerConfig() *DeserializerConfig {
	c := &DeserializerConfig{
		DeserializerConfig: *serde.NewDeserializerConfig(),
	}

	return c
}
