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

package jsonschema

import (
	"fmt"
	"reflect"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	jsonschema2 "github.com/santhosh-tekuri/jsonschema/v5"
)

// validateMessage walks msg against schema, evaluating every inline "confluent:rules"
// constraint encountered and collecting all failures. Read-only — the message is not
// modified.
//
// Two kinds of rules are evaluated:
//   - Object-level ("confluent:rules" on an object schema) — `this` is the object.
//   - Property-level ("confluent:rules" on a property schema) — `this` is the property
//     value. Honors the skip-on-null contract: a property that is absent or nil does not
//     have its rules invoked.
//
// Failures are returned with their location, rooted at "$" to match the JVM client (e.g.
// $.addr.zip, $.tags[3]). The walk continues after each failure unless failFast is set.
func validateMessage(executor serde.ValidationRuleExecutor, schema *jsonschema2.Schema, msg *reflect.Value,
	failFast bool) ([]serde.ValidationRuleError, error) {
	var violations []serde.ValidationRuleError
	if executor == nil || schema == nil || msg == nil {
		return violations, nil
	}
	err := validateWithRules(executor, schema, "$", msg, failFast, &violations)
	if err != nil {
		return nil, err
	}
	return violations, nil
}

// validateWithRules evaluates the rules declared on schema against msg, then walks into
// whatever schema describes.
//
// This is the only place rules are read. A property's schema and the schema the walk
// recurses into for that property are the same schema, so reading them in the property loop
// as well would charge every rule on an object-valued property twice - which is why
// validateProperties only recurses. Matches the JVM client.
func validateWithRules(executor serde.ValidationRuleExecutor, schema *jsonschema2.Schema, path string,
	msg *reflect.Value, failFast bool, out *[]serde.ValidationRuleError) error {
	if msg == nil || (msg.Kind() == reflect.Pointer && msg.IsNil()) || schema == nil {
		return nil
	}
	// Skip-on-null: an absent or nil value does not invoke the executor. The walk below
	// still runs and no-ops.
	if isPresent(msg) {
		if val := deref(msg); val.IsValid() && val.CanInterface() {
			for _, rule := range getInlineValidationRules(schema) {
				if err := serde.EvaluateValidationRule(
					executor, rule, schema, val.Interface(), path, out); err != nil {
					return err
				}
				if failFast && len(*out) > 0 {
					return nil
				}
			}
		}
	}
	return validateSchemaBody(executor, schema, path, msg, failFast, out)
}

// validateSchemaBody mirrors transform's dispatch shape: type arrays, then the combined
// keywords (allOf/anyOf/oneOf) with their sibling properties/items, then items, then $ref,
// then object properties. Rules for this node have already been evaluated by
// validateWithRules.
func validateSchemaBody(executor serde.ValidationRuleExecutor, schema *jsonschema2.Schema, path string,
	msg *reflect.Value, failFast bool, out *[]serde.ValidationRuleError) error {
	if len(schema.Types) > 1 {
		// Narrow to the type the value actually matches. Unlike transform, which mutates
		// schema.Types in place, this walks a shallow copy: the compiled schema is cached
		// and shared across serializations, so mutating it would race with concurrent use.
		subschema, err := matchSubtype(schema, msg)
		if err != nil {
			return err
		}
		if subschema != nil {
			// The narrowed schema is a copy of this one and carries the same rules, so
			// only its body is walked.
			return validateSchemaBody(executor, subschema, path, msg, failFast, out)
		}
		return nil
	}
	if len(schema.AllOf) > 0 || len(schema.AnyOf) > 0 || len(schema.OneOf) > 0 {
		if len(schema.AllOf) > 0 {
			for _, subschema := range schema.AllOf {
				if err := validateWithRules(executor, subschema, path, msg, failFast, out); err != nil {
					return err
				}
				if failFast && len(*out) > 0 {
					return nil
				}
			}
		} else if len(schema.OneOf) > 0 {
			for _, subschema := range schema.OneOf {
				valid, err := validate(subschema, deref(msg))
				if err != nil {
					return err
				}
				if valid {
					if err := validateWithRules(executor, subschema, path, msg, failFast, out); err != nil {
						return err
					}
					break
				}
			}
		} else { // AnyOf
			for _, subschema := range schema.AnyOf {
				valid, err := validate(subschema, deref(msg))
				if err != nil {
					return err
				}
				if valid {
					if err := validateWithRules(executor, subschema, path, msg, failFast, out); err != nil {
						return err
					}
					if failFast && len(*out) > 0 {
						return nil
					}
				}
			}
		}
		if failFast && len(*out) > 0 {
			return nil
		}
		// Also visit sibling properties/items at this level
		// (siblings to allOf/anyOf/oneOf).
		if err := validateProperties(executor, schema, path, msg, failFast, out); err != nil {
			return err
		}
		if failFast && len(*out) > 0 {
			return nil
		}
		if itemSchema := itemsSchema(schema); itemSchema != nil {
			return validateArray(executor, itemSchema, path, msg, failFast, out)
		}
		return nil
	}
	if itemSchema := itemsSchema(schema); itemSchema != nil {
		return validateArray(executor, itemSchema, path, msg, failFast, out)
	}
	if schema.Ref != nil {
		return validateWithRules(executor, schema.Ref, path, msg, failFast, out)
	}
	return validateProperties(executor, schema, path, msg, failFast, out)
}

// itemsSchema returns the array item schema for either JSON Schema draft family.
func itemsSchema(schema *jsonschema2.Schema) *jsonschema2.Schema {
	if isModernJSONSchema(schema.Draft) {
		return schema.Items2020
	}
	if sch, ok := schema.Items.(*jsonschema2.Schema); ok {
		return sch
	}
	return nil
}

func validateArray(executor serde.ValidationRuleExecutor, sch *jsonschema2.Schema, path string,
	msg *reflect.Value, failFast bool, out *[]serde.ValidationRuleError) error {
	val := deref(msg)
	if val.Kind() != reflect.Slice {
		return nil
	}
	for i := 0; i < val.Len(); i++ {
		item := val.Index(i)
		err := validateWithRules(executor, sch, fmt.Sprintf("%s[%d]", path, i), &item, failFast, out)
		if err != nil {
			return err
		}
		if failFast && len(*out) > 0 {
			return nil
		}
	}
	return nil
}

// validateProperties recurses into each declared property value. Undeclared properties are
// not walked, matching the JVM client. Rules are not read here - see validateWithRules,
// which each property value goes through.
func validateProperties(executor serde.ValidationRuleExecutor, schema *jsonschema2.Schema, path string,
	msg *reflect.Value, failFast bool, out *[]serde.ValidationRuleError) error {
	val := deref(msg)
	if val.Kind() != reflect.Struct && val.Kind() != reflect.Map {
		return nil
	}
	var fieldsByName map[string]*reflect.Value
	if val.Kind() == reflect.Struct {
		fieldsByName = fieldByNames(val)
	}
	for propName, propSchema := range schema.Properties {
		var propVal *reflect.Value
		if val.Kind() == reflect.Struct {
			structField, ok := fieldsByName[propName]
			if !ok {
				continue
			}
			propVal = structField
		} else {
			key, ok := serde.MapKeyForName(*val, propName)
			if !ok {
				continue
			}
			mapField := val.MapIndex(key)
			if !mapField.IsValid() {
				continue
			}
			propVal = &mapField
		}
		fullName := path + "." + propName
		if err := validateWithRules(executor, propSchema, fullName, propVal, failFast, out); err != nil {
			return err
		}
		if failFast && len(*out) > 0 {
			return nil
		}
	}
	return nil
}

// getInlineValidationRules reads the "confluent:rules" keyword off a schema. The keyword is
// compiled into the schema by validationRulesCompiler.
func getInlineValidationRules(schema *jsonschema2.Schema) []serde.ValidationRule {
	if schema == nil {
		return nil
	}
	ext, ok := schema.Extensions[serde.ValidationRulesProp]
	if !ok {
		return nil
	}
	rules, ok := ext.(validationRulesSchema)
	if !ok {
		return nil
	}
	return rules
}

// matchSubtype returns a copy of schema narrowed to the first of its declared types that
// the value satisfies, or nil when it satisfies none. The copy is what keeps both walkers
// read-only with respect to the shared compiled schema.
func matchSubtype(schema *jsonschema2.Schema, msg *reflect.Value) (*jsonschema2.Schema, error) {
	for _, typ := range schema.Types {
		candidate := *schema
		candidate.Types = []string{typ}
		valid, err := validate(&candidate, deref(msg))
		if err != nil {
			return nil, err
		}
		if valid {
			return &candidate, nil
		}
	}
	return nil, nil
}

// isPresent reports whether a value is set, i.e. neither invalid nor a nil pointer or
// interface.
func isPresent(val *reflect.Value) bool {
	if val == nil || !val.IsValid() {
		return false
	}
	switch val.Kind() {
	case reflect.Pointer, reflect.Interface:
		return !val.IsNil()
	default:
		return true
	}
}
