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

package serde

// SerializerConfig is used to pass multiple configuration options to the serializers.
type SerializerConfig struct {
	// AutoRegisterSchemas determines whether to automatically register schemas during serialization
	AutoRegisterSchemas bool
	// NormalizeSchemas determines whether to normalize schemas during serialization
	NormalizeSchemas bool
	// RuleConfig specifies configuration options to the rules
	RuleConfig map[string]string
}

// NewSerializerConfig returns a new configuration instance with sane defaults.
func NewSerializerConfig() *SerializerConfig {
	c := &SerializerConfig{}

	c.AutoRegisterSchemas = true
	c.NormalizeSchemas = false

	return c
}

// DeserializerConfig is used to pass multiple configuration options to the deserializers.
type DeserializerConfig struct {
	// RuleConfig specifies configuration options to the rules
	RuleConfig map[string]string
}

// NewDeserializerConfig returns a new configuration instance with sane defaults.
func NewDeserializerConfig() *DeserializerConfig {
	c := &DeserializerConfig{}

	return c
}

type SerializeHint struct {
	// UseSchemaID specifies a schema ID to use during serialization
	UseSchemaID int
	// UseVersion specifies a specific schema version to use during serialization
	UseSpecificVersion int
	// UseLatestVersion specifies whether to use the latest schema version during serialization
	UseLatestVersion bool
	// UseLatestWithMetadata specifies whether to use the latest schema with metadata during serialization
	UseLatestWithMetadata map[string]string
}

func NewSerializeHint() *SerializeHint {
	c := &SerializeHint{
		UseSchemaID:           -1,
		UseSpecificVersion:    -1,
		UseLatestVersion:      false,
		UseLatestWithMetadata: make(map[string]string),
	}

	return c
}

type DeserializeHint struct {
	// UseLatestVersion specifies whether to use the latest schema version during deserialization
	UseLatestVersion bool
	// UseLatestWithMetadata specifies whether to use the latest schema with metadata during deserialization
	UseLatestWithMetadata map[string]string
}

func NewDeserializeHint() *DeserializeHint {
	c := &DeserializeHint{
		UseLatestVersion:      false,
		UseLatestWithMetadata: make(map[string]string),
	}

	return c
}
