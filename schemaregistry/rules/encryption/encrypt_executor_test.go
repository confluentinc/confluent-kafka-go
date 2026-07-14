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
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

func TestEncryptionExecutor_Configure(t *testing.T) {
	maybeFail = initFailFunc(t)

	executor := NewExecutor()
	clientConfig := schemaregistry.NewConfig("mock://")
	config := map[string]string{
		"key": "value",
	}
	err := executor.Configure(clientConfig, config)
	maybeFail(err)
	// configure with same args is fine
	err = executor.Configure(clientConfig, config)
	maybeFail(err)
	config2 := map[string]string{
		"key2": "value2",
	}
	// configure with additional config keys is fine
	err = executor.Configure(clientConfig, config2)
	maybeFail(err)

	clientConfig2 := schemaregistry.NewConfig("mock://")
	clientConfig2.BasicAuthUserInfo = "foo"
	err = executor.Configure(clientConfig2, config)
	maybeFail(expect(err != nil, true))

	config3 := map[string]string{
		"key": "value2",
	}
	err = executor.Configure(clientConfig, config3)
	maybeFail(expect(err != nil, true))
}

func TestGetOrCreateKekUsesContextFromSubject(t *testing.T) {
	maybeFail = initFailFunc(t)

	executor := NewExecutorWithClock(&clock{})
	clientConfig := schemaregistry.NewConfig("mock://")
	err := executor.Configure(clientConfig, map[string]string{})
	maybeFail(err)

	// Pre-register the same kek name under two different contexts, with a
	// different kmsKeyID each, so a wrong (or dropped) context shows up as a
	// mismatched kmsKeyID rather than just "it didn't error".
	_, err = executor.Client.RegisterKek("kek1", "local-kms", "myctxkey", nil, "", false, ".myctx")
	maybeFail(err)
	_, err = executor.Client.RegisterKek("kek1", "local-kms", "defaultkey", nil, "", false, "")
	maybeFail(err)

	rule := &schemaregistry.Rule{
		Name:   "rule1",
		Kind:   "TRANSFORM",
		Mode:   "WRITE",
		Type:   "ENCRYPT_PAYLOAD",
		Params: map[string]string{EncryptKekName: "kek1"},
	}
	target := &schemaregistry.SchemaInfo{}

	// Context-qualified subject: the context should be parsed out of the
	// subject and threaded through to the dek registry client, not dropped.
	ctx := serde.RuleContext{
		Target:   target,
		Subject:  ":.myctx:widget-value",
		RuleMode: schemaregistry.Write,
		Rule:     rule,
	}
	transform, err := executor.NewTransform(ctx)
	maybeFail(err)
	maybeFail(expect(transform.Kek.KmsKeyID, "myctxkey"))

	// Unqualified subject (default context): the context should normalize to
	// "" rather than being looked up under the literal "." context.
	ctx2 := serde.RuleContext{
		Target:   target,
		Subject:  "widget-value",
		RuleMode: schemaregistry.Write,
		Rule:     rule,
	}
	transform2, err := executor.NewTransform(ctx2)
	maybeFail(err)
	maybeFail(expect(transform2.Kek.KmsKeyID, "defaultkey"))

	// Explicitly-qualified default context (":.:subject"): should behave
	// identically to an unqualified subject, not be looked up under the
	// literal "." context.
	ctx3 := serde.RuleContext{
		Target:   target,
		Subject:  ":.:widget-value",
		RuleMode: schemaregistry.Write,
		Rule:     rule,
	}
	transform3, err := executor.NewTransform(ctx3)
	maybeFail(err)
	maybeFail(expect(transform3.Kek.KmsKeyID, "defaultkey"))
}

type failFunc func(...error)

var maybeFail failFunc

func initFailFunc(t *testing.T) failFunc {
	tester := t
	return func(errors ...error) {
		for _, err := range errors {
			if err != nil {
				pc := make([]uintptr, 1)
				runtime.Callers(2, pc)
				caller := runtime.FuncForPC(pc[0])
				_, line := caller.FileLine(caller.Entry())

				tester.Fatalf("%s:%d failed: %s", caller.Name(), line, err)
			}
		}
	}
}

func expect(actual, expected interface{}) error {
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("expected: %v, Actual: %v", expected, actual)
	}

	return nil
}
