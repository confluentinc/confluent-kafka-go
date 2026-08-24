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
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/ext"
	"google.golang.org/protobuf/proto"
	"reflect"
	"strings"
	"sync"
)

func init() {
	Register()
}

// Register registers the CEL rule executor
func Register() {
	serde.RegisterRuleExecutor(NewExecutor())
	serde.RegisterRuleExecutor(NewFieldExecutor())
	serde.RegisterValidationRuleExecutor(NewValidator())
}

// NewExecutor creates a new CEL rule executor
func NewExecutor() serde.RuleExecutor {
	env, _ := DefaultEnv()

	return &Executor{
		env:   env,
		cache: map[string]cel.Program{},
	}
}

// Executor is a CEL rule executor
type Executor struct {
	Config    map[string]string
	env       *cel.Env
	cache     map[string]cel.Program
	cacheLock sync.RWMutex
}

// Configure configures the executor
func (c *Executor) Configure(clientConfig *schemaregistry.Config, config map[string]string) error {
	c.Config = config
	return nil
}

// Type returns the type of the executor
func (c *Executor) Type() string {
	return "CEL"
}

// Transform transforms the message using the rule
func (c *Executor) Transform(ctx serde.RuleContext, msg interface{}) (interface{}, error) {
	// Dereference pointer for CEL evaluation, but only for non-proto types
	// since proto.Message is implemented on the pointer receiver.
	// Use a separate celMsg for args so that the original msg is preserved
	// for return semantics in execute().
	celMsg := msg
	if _, ok := msg.(proto.Message); !ok {
		if v := reflect.ValueOf(msg); v.Kind() == reflect.Ptr && !v.IsNil() {
			celMsg = v.Elem().Interface()
		}
	}
	args := map[string]interface{}{
		"message": celMsg,
	}
	return c.execute(ctx, msg, args)
}

func (c *Executor) execute(ctx serde.RuleContext, msg interface{}, args map[string]interface{}) (interface{}, error) {
	expr := ctx.Rule.Expr
	index := strings.Index(expr, ";")
	if index >= 0 {
		guard := expr[0:index]
		if len(strings.TrimSpace(guard)) != 0 {
			guardResult, err := c.executeRule(ctx, guard, msg, args)
			if err != nil {
				guardResult = false
			}
			guardBool, ok := guardResult.(bool)
			if ok && !guardBool {
				// Skip the expr
				if ctx.Rule.Kind == "CONDITION" {
					return true, nil
				}
				return msg, nil
			}
		}
		expr = expr[index+1:]
	}
	return c.executeRule(ctx, expr, msg, args)
}

func (c *Executor) executeRule(ctx serde.RuleContext, expr string, obj interface{}, args map[string]interface{}) (interface{}, error) {
	args = boundaryArgs(args)
	msg, ok := args["message"]
	if !ok {
		msg = obj
	}
	schema := ctx.Target.Schema
	scriptType := ctx.Target.SchemaType
	declTypeNames := toDeclTypeNames(args)
	rule := ruleWithArgs{
		Rule:          expr,
		ScriptType:    scriptType,
		DeclTypeNames: declTypeNames,
		Schema:        schema,
	}
	ruleJSON, err := rule.MarshalJSON()
	if err != nil {
		return nil, err
	}
	c.cacheLock.RLock()
	program, ok := c.cache[string(ruleJSON)]
	c.cacheLock.RUnlock()
	if !ok {
		decls := toDecls(args)
		var err error
		program, err = c.newProgram(expr, msg, decls)
		if err != nil {
			return nil, err
		}
		c.cacheLock.Lock()
		c.cache[string(ruleJSON)] = program
		c.cacheLock.Unlock()
	}
	return c.eval(expr, program, args)
}

func toDecls(args map[string]interface{}) []cel.EnvOption {
	var vars []cel.EnvOption
	for name, typ := range args {
		vars = append(vars, cel.Variable(name, findType(typ)))
	}
	return vars
}

func toDeclTypeNames(args map[string]interface{}) map[string]string {
	declTypeNames := map[string]string{}
	for name, typ := range args {
		declTypeNames[name] = findType(typ).TypeName()
	}
	return declTypeNames
}

func findType(arg interface{}) *cel.Type {
	if arg == nil {
		return cel.NullType
	}
	// Before the proto.Message branch: a confluent.type.Decimal message would otherwise be
	// declared as an object type, which is a different CEL type from the opaque decimalType
	// that decimal(...) returns even though both carry the same name. See
	// decimalBoundaryValue.
	if _, ok := decimalBoundaryValue(arg); ok {
		return decimalType
	}
	msg, ok := arg.(proto.Message)
	if ok {
		return cel.ObjectType(string(msg.ProtoReflect().Descriptor().FullName()))
	}
	return typeToCELType(arg)
}

// boundaryArgs presents each bound value the way this client's CEL surface expects. Only
// decimal-shaped leaf values are rewritten; a record or map passes through untouched, so this is
// a no-op for every binding other than a decimal field's value.
func boundaryArgs(args map[string]interface{}) map[string]interface{} {
	var out map[string]interface{}
	for name, v := range args {
		dv, ok := decimalBoundaryValue(v)
		if !ok {
			continue
		}
		if out == nil {
			out = make(map[string]interface{}, len(args))
			for n, val := range args {
				out[n] = val
			}
		}
		out[name] = dv
	}
	if out == nil {
		return args
	}
	return out
}

func typeToCELType(arg interface{}) *cel.Type {
	if arg == nil {
		return cel.NullType
	}
	switch arg.(type) {
	case bool:
		return cel.BoolType
	case int, int8, int16, int32, int64:
		return cel.IntType
	// Unsigned values are declared unsigned, so that arithmetic on them resolves an
	// overload: the value bound at evaluation time is a CEL uint either way, and a
	// declaration of int leaves `this % 2` or `this + 1` with no matching overload.
	case uint, uint8, uint16, uint32, uint64, uintptr:
		return cel.UintType
	case []byte:
		return cel.BytesType
	case float32, float64:
		return cel.DoubleType
	case string:
		return cel.StringType
	}
	kind := reflect.TypeOf(arg).Kind()
	switch kind {
	case reflect.Map:
		return cel.MapType(cel.DynType, cel.DynType)
	case reflect.Array, reflect.Slice:
		return cel.ListType(cel.DynType)
	case reflect.Struct:
		return cel.DynType
	default:
		return cel.DynType
	}
}

func (c *Executor) newProgram(expr string, msg interface{}, decls []cel.EnvOption) (cel.Program, error) {
	// Domain rules address a Go struct's fields by their Go names, which is how they have
	// always been written; only validation rules, which come from the schema, use the
	// schema names.
	return buildProgram(c.env, expr, msg, decls, nil)
}

// schemaFieldName resolves the CEL name of a Go struct field to the field's schema name,
// so that validation rules address fields the same way they do in the other clients
// (`this.age` rather than `this.Age`). Inline validation rules are written against the
// schema, so the schema's field names are the only ones they can use.
//
// This applies to validation rules alone: a domain rule's expression is written by the
// user against the Go type, so the Executor keeps addressing fields by their Go names.
//
// Avro structs carry `avro` tags and JSON Schema structs carry `json` tags; either is
// consulted, with the Go field name as the fallback for untagged fields. Protobuf messages
// do not go through this path — they are registered with cel.Types, which already exposes
// proto field names.
func schemaFieldName(field reflect.StructField) string {
	for _, tagName := range []string{"avro", "json"} {
		tag, found := field.Tag.Lookup(tagName)
		if !found {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name != "" && name != "-" {
			return name
		}
	}
	return field.Name
}

// buildProgram compiles expr against env, extended with the declarations in decls and
// with the type of msg registered so that field access on it resolves.
//
// fieldName, when non-nil, names a Go struct's fields for the expression; the Go field
// names are used when it is nil.
func buildProgram(baseEnv *cel.Env, expr string, msg interface{}, decls []cel.EnvOption,
	fieldName func(reflect.StructField) string) (cel.Program, error) {
	typ := reflect.TypeOf(msg)
	if typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Interface {
		typ = typ.Elem()
	}
	protoType, ok := msg.(proto.Message)
	var declType cel.EnvOption
	if ok {
		declType = cel.Types(protoType)
	} else if typ.Kind() == reflect.Struct {
		if fieldName != nil {
			declType = ext.NativeTypes(typ, ext.ParseStructField(fieldName))
		} else {
			declType = ext.NativeTypes(typ)
		}
	}
	envOptions := decls
	if declType != nil {
		envOptions = make([]cel.EnvOption, len(decls))
		copy(envOptions, decls)
		envOptions = append(envOptions, declType)
	}
	env, err := baseEnv.Extend(envOptions...)
	if err != nil {
		return nil, err
	}
	// After the type registrations above, so the wrapped adapter is the one that knows them.
	env, err = env.Extend(cel.CustomTypeAdapter(decimalAdapter{inner: env.CELTypeAdapter()}))
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	return prg, nil
}

func (c *Executor) eval(expr string, program cel.Program, args map[string]interface{}) (interface{}, error) {
	return evalProgram(expr, program, args)
}

// evalProgram evaluates program against args, converting the result to a native value.
func evalProgram(expr string, program cel.Program, args map[string]interface{}) (interface{}, error) {
	out, _, err := program.Eval(args)
	if err != nil {
		return nil, fmt.Errorf("CEL expr %s failed: %w", expr, err)
	}
	if out.Type() == types.ErrType {
		return nil, out.Value().(error)
	}
	if out.Type() == types.UnknownType {
		return out.Value(), nil
	}
	var want interface{}
	// Want type of type.Interface
	// See https://stackoverflow.com/questions/18306151/in-go-which-value-s-kind-is-reflect-interface
	wantType := reflect.ValueOf(&want).Type().Elem()
	return out.ConvertToNative(wantType)
}

// Close closes the executor
func (c *Executor) Close() error {
	return nil
}

type ruleWithArgs struct {
	Rule          string
	ScriptType    string
	DeclTypeNames map[string]string
	Schema        string
}

// MarshalJSON implements the json.Marshaler interface
func (r *ruleWithArgs) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Rule          string            `json:"rule,omitempty"`
		ScriptType    string            `json:"scriptType,omitempty"`
		DeclTypeNames map[string]string `json:"declTypeNames,omitempty"`
		Schema        string            `json:"schema,omitempty"`
	}{
		r.Rule,
		r.ScriptType,
		r.DeclTypeNames,
		r.Schema,
	})

}

// UnmarshalJSON implements the json.Unmarshaller interface
func (r *ruleWithArgs) UnmarshalJSON(b []byte) error {
	var err error
	var tmp struct {
		Rule          string            `json:"rule,omitempty"`
		ScriptType    string            `json:"scriptType,omitempty"`
		DeclTypeNames map[string]string `json:"declTypeNames,omitempty"`
		Schema        string            `json:"schema,omitempty"`
	}

	err = json.Unmarshal(b, &tmp)

	r.Rule = tmp.Rule
	r.ScriptType = tmp.ScriptType
	r.DeclTypeNames = tmp.DeclTypeNames
	r.Schema = tmp.Schema

	return err
}
