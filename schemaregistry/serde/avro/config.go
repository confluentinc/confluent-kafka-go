/**
 * Copyright 2022 Confluent Inc.
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

package avro

import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"

// SerializerConfig is used to pass multiple configuration options to the serializers.
type SerializerConfig struct {
	serde.SerializerConfig
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
}

// NewDeserializerConfig returns a new configuration instance with sane defaults.
func NewDeserializerConfig() *DeserializerConfig {
	c := &DeserializerConfig{
		DeserializerConfig: *serde.NewDeserializerConfig(),
	}

	return c
}
