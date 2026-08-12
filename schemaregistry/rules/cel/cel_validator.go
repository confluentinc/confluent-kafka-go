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

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/google/cel-go/cel"
	"google.golang.org/protobuf/proto"
)

// NewValidator creates a new CEL validation rule executor
func NewValidator() serde.ValidationRuleExecutor {
	env, _ := DefaultEnv()

	return &Validator{
		env:   env,
		cache: map[string]cel.Program{},
	}
}

// Validator is a validation rule executor backed by CEL. Each rule expression is
// evaluated with `this` bound to the value being validated and `now` bound to the current
// time, and must return either a bool (false = failed) or a string (non-empty = failed,
// with that string as the failure message).
//
// Fields of a native Go struct are addressed by their schema name rather than their Go
// name, so a field declared as Age with an avro tag of "age" is read as this.age, matching
// the other clients. See schemaFieldName for how the mapping is resolved.
type Validator struct {
	env       *cel.Env
	cache     map[string]cel.Program
	cacheLock sync.RWMutex
}

// Execute evaluates a single validation rule against a value
func (v *Validator) Execute(rule serde.ValidationRule, schema interface{}, msg interface{}) (interface{}, error) {
	name := rule.Name
	if name == "" {
		name = "unnamed"
	}
	if msg == nil {
		// Walkers are expected to enforce skip-on-null before invoking the executor; a nil
		// here means a non-compliant caller. Surface the contract violation explicitly
		// rather than trip a confusing CEL evaluation error.
		return nil, fmt.Errorf("validation rule '%s' received a null value; walkers must "+
			"enforce skip-on-null before invoking the executor", name)
	}
	if rule.Expr == "" {
		return nil, fmt.Errorf("validation rule '%s' has no expression", name)
	}

	// Dereference pointers for CEL evaluation, but only for non-proto types since
	// proto.Message is implemented on the pointer receiver.
	celMsg := msg
	if _, ok := msg.(proto.Message); !ok {
		if val := reflect.ValueOf(msg); val.Kind() == reflect.Ptr && !val.IsNil() {
			celMsg = val.Elem().Interface()
		}
	}

	thisType := findType(celMsg)
	// Native Go structs are all declared as CEL's dyn type, so the declaration name alone
	// does not identify the environment a program was compiled against: buildProgram
	// registers the concrete struct type, and a program built for one struct cannot adapt
	// a value of another. Include the concrete type so the two do not share an entry.
	cacheKey := rule.Expr + "\n" + thisType.TypeName() + "\n" + concreteTypeName(celMsg)
	v.cacheLock.RLock()
	program, ok := v.cache[cacheKey]
	v.cacheLock.RUnlock()
	if !ok {
		decls := []cel.EnvOption{
			cel.Variable("this", thisType),
			cel.Variable("now", cel.TimestampType),
		}
		var err error
		program, err = buildProgram(v.env, rule.Expr, celMsg, decls)
		if err != nil {
			return nil, fmt.Errorf("could not compile validation rule '%s': %w", name, err)
		}
		v.cacheLock.Lock()
		v.cache[cacheKey] = program
		v.cacheLock.Unlock()
	}

	args := map[string]interface{}{
		"this": celMsg,
		"now":  time.Now().UTC(),
	}
	result, err := evalProgram(rule.Expr, program, args)
	if err != nil {
		if rule.Doc != "" {
			return nil, fmt.Errorf("could not execute validation rule '%s' (%s): %w", name, rule.Doc, err)
		}
		return nil, fmt.Errorf("could not execute validation rule '%s': %w", name, err)
	}
	switch result.(type) {
	case bool, string:
		return result, nil
	default:
		return nil, fmt.Errorf("validation rule '%s' must return bool or string; got %T", name, result)
	}
}

// concreteTypeName identifies the Go type of a value for cache-key purposes. Protobuf
// messages that share a Go type (dynamic messages) are still distinguished by the
// descriptor name carried in the CEL declaration type.
func concreteTypeName(msg interface{}) string {
	typ := reflect.TypeOf(msg)
	if typ == nil {
		return ""
	}
	return typ.PkgPath() + "." + typ.String()
}

// Close closes the validator
func (v *Validator) Close() error {
	return nil
}
