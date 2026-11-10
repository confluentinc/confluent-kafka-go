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
	// UseSchemaID specifies a schema ID to use during serialization
	UseSchemaID int
	// UseLatestVersion specifies whether to use the latest schema version during serialization
	UseLatestVersion bool
	// UseLatestWithMetadata specifies whether to use the latest schema with metadata during serialization
	UseLatestWithMetadata map[string]string
	// NormalizeSchemas determines whether to normalize schemas during serialization
	NormalizeSchemas bool
	// RuleConfig specifies configuration options to the rules
	RuleConfig map[string]string
	// RuleRegistry supplies an alternative registry of rule executors, actions, and
	// overrides for this serializer to use instead of the global one. When nil (the
	// default), the global registry is used - the same one the rules packages' Register
	// functions populate. Provide a dedicated *RuleRegistry (see NewRuleRegistry) to
	// isolate this serializer's rule executors from the process-wide global state, so
	// that concurrently-constructed serializers do not share and reconfigure the same
	// executor instances. Populate it via RegisterExecutor/RegisterAction; nothing is
	// linked in on your behalf, exactly as with the global registry.
	RuleRegistry *RuleRegistry
	// SubjectNameStrategyType specifies the subject name strategy type
	SubjectNameStrategyType SubjectNameStrategyType
	// SubjectNameStrategyConfig specifies configuration options for the subject name strategy
	SubjectNameStrategyConfig map[string]string
	// ValidationRulesExecution determines when inline validation rules run, relative to
	// domain rule transformations. Defaults to ValidationRulesDisabled.
	ValidationRulesExecution ValidationRulesExecution
	// ValidationRulesFailFast stops validation at the first failed rule and reports only
	// that violation. When false (the default), every node is visited and all violations
	// are reported.
	ValidationRulesFailFast bool
	// ValidationRuleExecutor is the executor used to evaluate inline validation rules.
	// When nil, the executor is taken from the global registry, which importing
	// schemaregistry/rules/cel populates with the CEL-backed one - as with every other
	// rule executor here, nothing is linked in on your behalf. Setting this field is the
	// alternative to that import; with neither, enabling validation is an error.
	ValidationRuleExecutor ValidationRuleExecutor
}

// NewSerializerConfig returns a new configuration instance with sane defaults.
func NewSerializerConfig() *SerializerConfig {
	c := &SerializerConfig{}

	c.AutoRegisterSchemas = true
	c.UseSchemaID = -1
	c.UseLatestVersion = false
	c.NormalizeSchemas = false
	c.ValidationRulesExecution = ValidationRulesDisabled

	return c
}

// DeserializerConfig is used to pass multiple configuration options to the deserializers.
type DeserializerConfig struct {
	// UseLatestVersion specifies whether to use the latest schema version during deserialization
	UseLatestVersion bool
	// UseLatestWithMetadata specifies whether to use the latest schema with metadata during serialization
	UseLatestWithMetadata map[string]string
	// RuleConfig specifies configuration options to the rules
	RuleConfig map[string]string
	// RuleRegistry supplies an alternative registry of rule executors, actions, and
	// overrides for this deserializer to use instead of the global one. When nil (the
	// default), the global registry is used - the same one the rules packages' Register
	// functions populate. Provide a dedicated *RuleRegistry (see NewRuleRegistry) to
	// isolate this deserializer's rule executors from the process-wide global state, so
	// that concurrently-constructed deserializers do not share and reconfigure the same
	// executor instances. Populate it via RegisterExecutor/RegisterAction; nothing is
	// linked in on your behalf, exactly as with the global registry.
	RuleRegistry *RuleRegistry
	// SubjectNameStrategyType specifies the subject name strategy type
	SubjectNameStrategyType SubjectNameStrategyType
	// SubjectNameStrategyConfig specifies configuration options for the subject name strategy
	SubjectNameStrategyConfig map[string]string
}

// NewDeserializerConfig returns a new configuration instance with sane defaults.
func NewDeserializerConfig() *DeserializerConfig {
	c := &DeserializerConfig{}

	return c
}
