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

package cel

// A field entered with no containing message must not take the executor down.
//
// buildProgram walks reflect.TypeOf(msg) to the type that carries fields and then asks it for
// its Kind. For a nil message that type is nil, and the call was a nil dereference - a panic
// out of a rule, before any expression could even compile. FieldContext is exported and its
// ContainingMessage is an interface, so nil is a value a walker can legitimately supply.

import (
	"testing"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

func nilMessageRun(t *testing.T, expr string) (result interface{}, err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("expr %q panicked: %v", expr, r)
		}
	}()
	rule := schemaregistry.Rule{
		Name: "r", Kind: "TRANSFORM", Mode: "WRITE", Type: "CEL_FIELD", Expr: expr,
	}
	ctx := serde.RuleContext{
		Rule:   &rule,
		Target: &schemaregistry.SchemaInfo{Schema: `"string"`, SchemaType: "AVRO"},
		Rules:  []schemaregistry.Rule{rule},
	}
	transform, terr := NewFieldExecutor().(*FieldExecutor).NewTransform(ctx)
	if terr != nil {
		t.Fatal(terr)
	}
	fieldCtx := serde.FieldContext{
		ContainingMessage: nil,
		FullName:          "R.f",
		Name:              "f",
		Type:              serde.TypeString,
		Tags:              map[string]bool{},
	}
	return transform.Transform(ctx, fieldCtx, "hi")
}

func TestFieldRuleSurvivesANilContainingMessage(t *testing.T) {
	got, err := nilMessageRun(t, `value + "!"`)
	if err != nil {
		t.Fatalf("a rule that does not read the message must still run: %v", err)
	}
	if got != "hi!" {
		t.Errorf("value = %v, want \"hi!\"", got)
	}
}

// The discriminator: without the guard this never got as far as compiling, so a rule that
// *does* name the message could not report anything at all.
func TestNilContainingMessageBindsAsNull(t *testing.T) {
	got, err := nilMessageRun(t, `message == null ? "absent" : "present"`)
	if err != nil {
		t.Fatalf("message == null must compile and evaluate: %v", err)
	}
	if got != "absent" {
		t.Errorf("message = %v, want \"absent\"", got)
	}
}
