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

package encryption

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

// RegisterFieldExecutorWithClock registers the encryption rule executor with a given clock
func RegisterFieldExecutorWithClock(c Clock) *FieldEncryptionExecutor {
	f := NewFieldExecutorWithClock(c)
	serde.RegisterRuleExecutor(f)
	return f
}

// NewFieldExecutor creates a new encryption rule executor
func NewFieldExecutor() serde.RuleExecutor {
	c := clock{}
	return NewFieldExecutorWithClock(&c)
}

// NewFieldExecutorWithClock creates a new encryption rule executor with a given clock
func NewFieldExecutorWithClock(c Clock) *FieldEncryptionExecutor {
	a := &serde.AbstractFieldRuleExecutor{}
	f := &FieldEncryptionExecutor{*a, *NewExecutorWithClock(c)}
	f.FieldRuleExecutor = f
	return f
}

// FieldEncryptionExecutor is a field encryption executor
type FieldEncryptionExecutor struct {
	serde.AbstractFieldRuleExecutor
	Executor Executor
}

// Configure configures the executor
func (f *FieldEncryptionExecutor) Configure(clientConfig *schemaregistry.Config, config map[string]string) error {
	return f.Executor.Configure(clientConfig, config)
}

// Type returns the type of the executor
func (f *FieldEncryptionExecutor) Type() string {
	return "ENCRYPT"
}

// NewTransform creates a new transform
func (f *FieldEncryptionExecutor) NewTransform(ctx serde.RuleContext) (serde.FieldTransform, error) {
	executorTransform, err := f.Executor.NewTransform(ctx)
	if err != nil {
		return nil, err
	}
	transform := FieldEncryptionExecutorTransform{
		ExecutorTransform: *executorTransform,
	}
	return &transform, nil
}

// Close closes the executor
func (f *FieldEncryptionExecutor) Close() error {
	return f.Executor.Close()
}

// FieldEncryptionExecutorTransform is a field encryption executor transform
type FieldEncryptionExecutorTransform struct {
	ExecutorTransform ExecutorTransform
}

// Transform transforms the field value using the rule
func (f *FieldEncryptionExecutorTransform) Transform(ctx serde.RuleContext, fieldCtx serde.FieldContext, fieldValue interface{}) (interface{}, error) {
	return f.ExecutorTransform.Transform(ctx, fieldCtx.Type, fieldValue)
}
