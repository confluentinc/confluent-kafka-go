/**
 * Copyright 2024 Confluent Inc.
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

package cel

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

// FieldExecutor is a CEL field rule executor
type FieldExecutor struct {
	serde.AbstractFieldRuleExecutor
	executor Executor
}

// Type returns the type of the executor
func (f *FieldExecutor) Type() string {
	return "CEL_FIELD"
}

// Configure configures the executor
func (f *FieldExecutor) Configure(clientConfig *schemaregistry.Config, config map[string]string) error {
	return f.executor.Configure(clientConfig, config)
}

// NewTransform creates a new transform
func (f *FieldExecutor) NewTransform(ctx serde.RuleContext) (serde.FieldTransform, error) {
	transform := FieldExecutorTransform{
		executor: f.executor,
	}
	return &transform, nil
}

// Close closes the executor
func (f *FieldExecutor) Close() error {
	return f.executor.Close()
}

// FieldExecutorTransform is a CEL field rule executor transform
type FieldExecutorTransform struct {
	executor Executor
}

// Transform transforms the field value using the rule
func (f *FieldExecutorTransform) Transform(ctx serde.RuleContext, fieldCtx serde.FieldContext, fieldValue interface{}) (interface{}, error) {
	if fieldValue == nil {
		return nil, nil
	}
	if !fieldCtx.IsPrimitive() {
		return fieldValue, nil
	}
	args := map[string]interface{}{
		"value":    fieldValue,
		"fullName": fieldCtx.FullName,
		"name":     fieldCtx.Name,
		"typeName": fieldCtx.TypeName(),
		"tags":     fieldCtx.Tags,
		"message":  fieldCtx.ContainingMessage,
	}
	return f.executor.execute(ctx, fieldValue, args)
}
