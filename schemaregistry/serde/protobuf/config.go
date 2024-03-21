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

package protobuf

import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"

// SerializerConfig is used to pass multiple configuration options to the serializers.
type SerializerConfig struct {
	serde.SerializerConfig
	// CacheSchemas will cache serialization results based on the name of the protobuf file
	// corresponding to the message being serialized. This will drastically improve serialization
	// performance if you are only ever using a _single_ version of a specific protobuf schema
	// during any given run of your application. This should be the case for most applications,
	// but might not apply if you're not creating proto messages based on generated files (e.g.
	// you are proxying or reading raw protobuf messages from a data source), or if for some reason
	// you are including multiple versions of the same schema/protobuf in your application.
	CacheSchemas bool
}

// NewSerializerConfig returns a new configuration instance with sane defaults.
func NewSerializerConfig() *SerializerConfig {
	c := &SerializerConfig{
		SerializerConfig: *serde.NewSerializerConfig(),
		CacheSchemas:     false,
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
