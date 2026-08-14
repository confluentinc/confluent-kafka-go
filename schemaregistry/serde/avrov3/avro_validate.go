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

package avrov3

import (
	"fmt"
	"reflect"

	avro "github.com/confluentinc/confluent-avro-go/v2"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

// validateMessage walks msg against schema, evaluating every inline "confluent:rules"
// CHECK constraint encountered and collecting all failures. Read-only — the message is not
// modified.
//
// Two kinds of rules are evaluated:
//   - Record-level ("confluent:rules" on a record schema) — `this` is the record.
//   - Field-level ("confluent:rules" on a record's field) — `this` is the field value.
//     Honors the skip-on-null contract: a field that is absent or nil does not have its
//     rules invoked.
//
// Failures are returned with their dotted-path location (e.g. addr.zip, tags[3],
// scores["foo"]). The walk continues after each failure so callers see the full set rather
// than only the first, unless failFast is set.
func validateMessage(executor serde.ValidationRuleExecutor, resolver *avro.TypeResolver, schema avro.Schema,
	msg *reflect.Value, failFast bool) ([]serde.ValidationRuleError, error) {
	var violations []serde.ValidationRuleError
	if executor == nil || schema == nil || msg == nil {
		return violations, nil
	}
	err := validate(executor, resolver, schema, "", msg, failFast, &violations)
	if err != nil {
		return nil, err
	}
	return violations, nil
}

// validate mirrors transform's switch-on-schema-type dispatch shape.
func validate(executor serde.ValidationRuleExecutor, resolver *avro.TypeResolver, schema avro.Schema,
	path string, msg *reflect.Value, failFast bool, out *[]serde.ValidationRuleError) error {
	if msg == nil || (msg.Kind() == reflect.Pointer && msg.IsNil()) || schema == nil {
		return nil
	}
	switch schema.(type) {
	case *avro.UnionSchema:
		val := deref(msg)
		subschema, submsg, err := resolveUnion(resolver, schema, val)
		if err != nil {
			// An unresolvable union is not a rule failure; leave it to the writer to report.
			return nil
		}
		if subschema == nil || subschema.Type() == avro.Null {
			return nil
		}
		return validate(executor, resolver, subschema, path, submsg, failFast, out)
	case *avro.ArraySchema:
		val := deref(msg)
		if val.Kind() != reflect.Slice {
			return nil
		}
		subschema := schema.(*avro.ArraySchema).Items()
		for i := 0; i < val.Len(); i++ {
			item := val.Index(i)
			err := validate(executor, resolver, subschema, fmt.Sprintf("%s[%d]", path, i), &item, failFast, out)
			if err != nil {
				return err
			}
			if failFast && len(*out) > 0 {
				return nil
			}
		}
		return nil
	case *avro.MapSchema:
		val := deref(msg)
		if val.Kind() != reflect.Map {
			return nil
		}
		subschema := schema.(*avro.MapSchema).Values()
		iter := val.MapRange()
		for iter.Next() {
			k := iter.Key()
			v := iter.Value()
			err := validate(executor, resolver, subschema, fmt.Sprintf("%s[%q]", path, k), &v, failFast, out)
			if err != nil {
				return err
			}
			if failFast && len(*out) > 0 {
				return nil
			}
		}
		return nil
	case *avro.RecordSchema:
		val := deref(msg)
		recordSchema := schema.(*avro.RecordSchema)
		if val.Kind() != reflect.Struct && val.Kind() != reflect.Map {
			return nil
		}
		// Record-level rules: this = the record value.
		if val.IsValid() && val.CanInterface() {
			for _, rule := range serde.ParseValidationRules(recordSchema.Prop(serde.ValidationRulesProp)) {
				err := serde.EvaluateValidationRule(executor, rule, recordSchema, val.Interface(), path, out)
				if err != nil {
					return err
				}
				if failFast && len(*out) > 0 {
					return nil
				}
			}
		}
		var fieldByNames map[string]*reflect.Value
		if val.Kind() == reflect.Struct {
			fieldByNames = fieldsByNames(val)
		}
		for _, avroField := range recordSchema.Fields() {
			var fieldVal *reflect.Value
			if val.Kind() == reflect.Struct {
				structField, ok := fieldByNames[avroField.Name()]
				if !ok {
					continue
				}
				fieldVal = structField
			} else {
				key, ok := serde.MapKeyForName(*val, avroField.Name())
				if !ok {
					continue
				}
				mapField := val.MapIndex(key)
				fieldVal = &mapField
			}
			childPath := avroField.Name()
			if path != "" {
				childPath = path + "." + avroField.Name()
			}
			// Skip-on-null: an absent or nil field value does not invoke the executor. The
			// recursion below still runs but no-ops for nil.
			if isPresent(fieldVal) {
				value := deref(fieldVal)
				if value.IsValid() && value.CanInterface() {
					for _, rule := range serde.ParseValidationRules(avroField.Prop(serde.ValidationRulesProp)) {
						err := serde.EvaluateValidationRule(
							executor, rule, avroField.Type(), value.Interface(), childPath, out)
						if err != nil {
							return err
						}
						if failFast && len(*out) > 0 {
							return nil
						}
					}
				}
			}
			err := validate(executor, resolver, avroField.Type(), childPath, fieldVal, failFast, out)
			if err != nil {
				return err
			}
			if failFast && len(*out) > 0 {
				return nil
			}
		}
		return nil
	default:
		// primitive leaf — field-level rules were evaluated by the parent record case
		return nil
	}
}

// isPresent reports whether a field value is set, i.e. neither invalid nor a nil pointer
// or interface.
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

// fieldsByNames maps Avro field names to the corresponding struct fields. Mirrors
// fieldByNames in avro_util.go, which is not reused so that the walker cannot be affected
// by changes to the transform path.
func fieldsByNames(value *reflect.Value) map[string]*reflect.Value {
	result := make(map[string]*reflect.Value, value.NumField())
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		structField := value.Type().Field(i)
		fieldName := structField.Name
		if tag, ok := structField.Tag.Lookup("avro"); ok {
			fieldName = tag
		}
		result[fieldName] = &field
	}
	return result
}
