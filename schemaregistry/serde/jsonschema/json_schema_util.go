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

package jsonschema

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	jsonschema2 "github.com/santhosh-tekuri/jsonschema/v5"
	"reflect"
	"strings"
)

func transform(ctx serde.RuleContext, schema *jsonschema2.Schema, path string, msg *reflect.Value,
	fieldTransform serde.FieldTransform) (*reflect.Value, error) {
	if msg == nil || (msg.Kind() == reflect.Pointer && msg.IsNil()) || schema == nil {
		return msg, nil
	}
	fieldCtx := ctx.CurrentField()
	if fieldCtx != nil {
		fieldCtx.Type = getType(schema)
	}
	if len(schema.Types) > 1 {
		// Narrow to the type the value actually matches, on a shallow copy: the compiled
		// schema is cached and shared across serializations, so mutating it - even
		// temporarily - races with concurrent use. Same as the validation walk.
		subschema, err := matchSubtype(schema, msg)
		if err != nil {
			return nil, err
		}
		if subschema != nil {
			return transform(ctx, subschema, path, msg, fieldTransform)
		}
	}
	if len(schema.AllOf) > 0 || len(schema.AnyOf) > 0 || len(schema.OneOf) > 0 {
		if len(schema.AllOf) > 0 {
			for _, subschema := range schema.AllOf {
				result, err := transform(ctx, subschema, path, msg, fieldTransform)
				if err != nil {
					return nil, err
				}
				msg = result
			}
		} else if len(schema.OneOf) > 0 {
			for _, subschema := range schema.OneOf {
				valid, err := validate(subschema, deref(msg))
				if err != nil {
					return nil, err
				}
				if valid {
					result, err := transform(ctx, subschema, path, msg, fieldTransform)
					if err != nil {
						return nil, err
					}
					msg = result
					break
				}
			}
		} else { // AnyOf
			for _, subschema := range schema.AnyOf {
				valid, err := validate(subschema, deref(msg))
				if err != nil {
					return nil, err
				}
				if valid {
					result, err := transform(ctx, subschema, path, msg, fieldTransform)
					if err != nil {
						return nil, err
					}
					msg = result
				}
			}
		}
		// Also visit sibling properties/items at this level
		// (siblings to allOf/anyOf/oneOf).
		if len(schema.Properties) > 0 {
			result, err := transformProperties(ctx, schema, path, msg, fieldTransform)
			if err != nil {
				return nil, err
			}
			msg = result
		}
		var itemSchema *jsonschema2.Schema
		if isModernJSONSchema(schema.Draft) {
			itemSchema = schema.Items2020
		} else if sch, ok := schema.Items.(*jsonschema2.Schema); ok {
			itemSchema = sch
		}
		if itemSchema != nil {
			result, err := transformArray(ctx, msg, itemSchema, path, fieldTransform)
			if err != nil {
				return nil, err
			}
			msg = result
		}
		return msg, nil
	}
	if isModernJSONSchema(schema.Draft) {
		sch := schema.Items2020
		if sch != nil {
			return transformArray(ctx, msg, sch, path, fieldTransform)
		}
	} else {
		sch, ok := schema.Items.(*jsonschema2.Schema)
		if ok {
			return transformArray(ctx, msg, sch, path, fieldTransform)
		}
	}
	if schema.Ref != nil {
		return transform(ctx, schema.Ref, path, msg, fieldTransform)
	}
	typ := getType(schema)
	switch typ {
	case serde.TypeRecord:
		val := deref(msg)
		if val.Kind() != reflect.Struct && val.Kind() != reflect.Map {
			return nil, fmt.Errorf("message of kind %s is not a struct or map", val.Kind())
		}
		return transformProperties(ctx, schema, path, msg, fieldTransform)
	case serde.TypeEnum, serde.TypeString, serde.TypeInt, serde.TypeDouble, serde.TypeBoolean:
		if fieldCtx != nil {
			ruleTags := ctx.Rule.Tags
			if len(ruleTags) == 0 || !disjoint(ruleTags, fieldCtx.Tags) {
				val := deref(msg)
				newVal, err := fieldTransform.Transform(ctx, *fieldCtx, val.Interface())
				if err != nil {
					return nil, err
				}
				result := reflect.ValueOf(newVal)
				return &result, nil
			}
		}
		return msg, nil
	default:
		return msg, nil
	}
}

func fieldByNames(value *reflect.Value) map[string]*reflect.Value {
	fieldByNames := make(map[string]*reflect.Value, value.NumField())
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		structField := value.Type().Field(i)
		fieldName := structField.Name
		if tag, ok := structField.Tag.Lookup("json"); ok {
			// A json tag carries options after the name, e.g. `json:"age,omitempty"`, so
			// index by the encoded name; otherwise schema properties never line up with
			// the struct fields. Mirrors schemaFieldName on the CEL side.
			if name := strings.Split(tag, ",")[0]; name != "" && name != "-" {
				fieldName = name
			}
		}
		fieldByNames[fieldName] = &field
	}
	return fieldByNames
}

func transformProperties(ctx serde.RuleContext, schema *jsonschema2.Schema, path string, msg *reflect.Value,
	fieldTransform serde.FieldTransform) (*reflect.Value, error) {
	val := deref(msg)
	switch val.Kind() {
	case reflect.Struct:
		fieldByNames := fieldByNames(val)
		for propName, propSchema := range schema.Properties {
			structField, ok := fieldByNames[propName]
			if !ok {
				continue
			}
			if err := transformField(ctx, path, propName, structField, val, propSchema, fieldTransform); err != nil {
				return nil, err
			}
		}
	case reflect.Map:
		for propName, propSchema := range schema.Properties {
			key, ok := serde.MapKeyForName(*val, propName)
			if !ok {
				continue
			}
			mapField := val.MapIndex(key)
			if !mapField.IsValid() {
				continue
			}
			if err := transformField(ctx, path, propName, &mapField, val, propSchema, fieldTransform); err != nil {
				return nil, err
			}
		}
	}
	return msg, nil
}

func transformField(ctx serde.RuleContext, path string, propName string, structField *reflect.Value, val *reflect.Value,
	propSchema *jsonschema2.Schema, fieldTransform serde.FieldTransform) error {
	fullName := path + "." + propName
	defer ctx.LeaveField()
	ctx.EnterField(val.Interface(), fullName, propName, getType(propSchema), getInlineTags(propSchema))
	newVal, err := transform(ctx, propSchema, fullName, structField, fieldTransform)
	if err != nil {
		return err
	}
	if ctx.Rule.Kind == "CONDITION" {
		newBool := deref(newVal)
		if newBool.Kind() == reflect.Bool && !newBool.Bool() {
			return serde.RuleConditionErr{
				Rule: ctx.Rule,
			}
		}
	} else {
		if val.Kind() == reflect.Struct {
			err = setField(structField, newVal)
			if err != nil {
				return err
			}
		} else if val.Kind() == reflect.Map {
			if key, ok := serde.MapKeyForName(*val, propName); ok {
				val.SetMapIndex(key, *newVal)
			}
		}
	}
	return nil
}

func transformArray(ctx serde.RuleContext, msg *reflect.Value, sch *jsonschema2.Schema, path string,
	fieldTransform serde.FieldTransform) (*reflect.Value, error) {
	val := deref(msg)
	if val.Kind() != reflect.Slice {
		return msg, nil
	}
	for i := 0; i < val.Len(); i++ {
		item := val.Index(i)
		newVal, err := transform(ctx, sch, path, &item, fieldTransform)
		if err != nil {
			return nil, err
		}
		item.Set(*newVal)
	}
	return msg, nil
}

func isModernJSONSchema(draft *jsonschema2.Draft) bool {
	u := draft.URL()
	return u == "https://json-schema.org/draft/2020-12/schema" ||
		u == "https://json-schema.org/draft/2019-09/schema"
}

func getType(schema *jsonschema2.Schema) serde.FieldType {
	types := schema.Types
	// An enumeration is typed by its values, and JSON Schema does not require it to declare a
	// type as well - {"enum": ["a", "b"]} is the ordinary form. Checked before the typeless
	// case so that form is not read as a typeless node: the JVM client answers ENUM for it,
	// and ENUM is not primitive, so a field rule that would otherwise be charged against it
	// is skipped there and has to be here too.
	if len(schema.Constant) > 0 || len(schema.Enum) > 0 {
		return serde.TypeEnum
	}
	if len(types) == 0 {
		if len(schema.Properties) > 0 {
			return serde.TypeRecord
		}
		return serde.TypeNull
	}
	if len(types) > 1 || len(schema.AllOf) > 0 || len(schema.AnyOf) > 0 || len(schema.OneOf) > 0 {
		return serde.TypeCombined
	}
	typ := types[0]
	switch typ {
	case "object":
		if len(schema.Properties) == 0 {
			return serde.TypeMap
		}
		return serde.TypeRecord
	case "array":
		return serde.TypeArray
	case "string":
		return serde.TypeString
	// The JSON Schema keyword is "integer"; "int" is not a JSON Schema type, and mapping
	// only that left every integer field typed NULL - which the transform walk skips, so
	// no rule ever reached an integer field even though the validation walk visits it.
	case "integer":
		return serde.TypeInt
	case "number":
		return serde.TypeDouble
	case "boolean":
		return serde.TypeBoolean
	case "null":
		return serde.TypeNull
	default:
		return serde.TypeNull
	}
}

func getInlineTags(schema *jsonschema2.Schema) []string {
	ext, ok := schema.Extensions["confluent:tags"]
	if !ok {
		return nil
	}
	return ext.(tagsSchema)
}

func disjoint(slice1 []string, map1 map[string]bool) bool {
	for _, v := range slice1 {
		if map1[v] {
			return false
		}
	}
	return true
}

func getField(msg *reflect.Value, name string) (*reflect.Value, error) {
	if msg.Kind() != reflect.Struct {
		return nil, fmt.Errorf("message is not a struct")
	}
	fieldVal := msg.FieldByName(name)
	return &fieldVal, nil
}

// See https://stackoverflow.com/questions/64138199/how-to-set-a-struct-member-that-is-a-pointer-to-an-arbitrary-value-using-reflect
func setField(field *reflect.Value, value *reflect.Value) error {
	if !field.CanSet() {
		return fmt.Errorf("cannot assign to the given field")
	}
	if field.Kind() == reflect.Pointer && value.Kind() != reflect.Pointer {
		x := reflect.New(field.Type().Elem())
		x.Elem().Set(*value)
		field.Set(x)
	} else {
		field.Set(*value)
	}
	return nil
}

func validate(schema *jsonschema2.Schema, msg *reflect.Value) (bool, error) {
	var obj interface{}
	if msg.IsValid() && msg.CanInterface() {
		raw, err := json.Marshal(msg.Interface())
		if err != nil {
			return false, err
		}
		// Need to unmarshal to pure interface
		err = json.Unmarshal(raw, &obj)
		if err != nil {
			return false, err
		}
	}
	err := schema.Validate(obj)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// deref unwraps every pointer and interface layer, not just one. A value read out of a
// map[string]interface{} arrives as an interface, so a nested record held as a pointer
// needs two unwraps to reach the struct; stopping at one leaves a reflect.Pointer, which
// every caller's Kind check rejects, and the record's fields are never walked.
//
// Terminates on nil without a guard: Elem() of a nil pointer or nil interface is the zero
// Value, whose Kind is Invalid.
func deref(val *reflect.Value) *reflect.Value {
	v := *val
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return &v
}
