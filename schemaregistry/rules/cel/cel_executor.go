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
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/ext"
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
	env, _ := DefaultEnv()

	e := &Executor{
		env:   env,
		cache: map[string]cel.Program{},
	}
	serde.RegisterRuleExecutor(e)

	a := &serde.AbstractFieldRuleExecutor{}
	f := &FieldExecutor{
		AbstractFieldRuleExecutor: *a,
		executor: Executor{
			env:   env,
			cache: map[string]cel.Program{},
		},
	}
	f.FieldRuleExecutor = f
	serde.RegisterRuleExecutor(f)
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
	args := map[string]interface{}{
		"message": msg,
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
	return c.eval(program, args)
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
	msg, ok := arg.(proto.Message)
	if ok {
		return cel.ObjectType(string(msg.ProtoReflect().Descriptor().FullName()))
	}
	return typeToCELType(arg)
}

func typeToCELType(arg interface{}) *cel.Type {
	if arg == nil {
		return cel.NullType
	}
	switch arg.(type) {
	case bool:
		return cel.BoolType
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		return cel.IntType
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
	typ := reflect.TypeOf(msg)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	protoType, ok := msg.(proto.Message)
	var declType cel.EnvOption
	if ok {
		declType = cel.Types(protoType)
	} else {
		declType = ext.NativeTypes(typ)
	}
	envOptions := make([]cel.EnvOption, len(decls))
	copy(envOptions, decls)
	envOptions = append(envOptions, declType)
	env, err := c.env.Extend(envOptions...)
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

func (c *Executor) eval(program cel.Program, args map[string]interface{}) (interface{}, error) {
	out, _, err := program.Eval(args)
	if err != nil {
		return nil, err
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
