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
	jsonschema2 "github.com/santhosh-tekuri/jsonschema/v6"
	"reflect"
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
	if len(schema.AllOf) > 0 {
		subschema, err := validateSubschemas(schema.AllOf, msg)
		if err != nil {
			return nil, err
		}
		if subschema != nil {
			return transform(ctx, subschema, path, msg, fieldTransform)
		}
	}
	if len(schema.AnyOf) > 0 {
		subschema, err := validateSubschemas(schema.AnyOf, msg)
		if err != nil {
			return nil, err
		}
		if subschema != nil {
			return transform(ctx, subschema, path, msg, fieldTransform)
		}
	}
	if len(schema.OneOf) > 0 {
		subschema, err := validateSubschemas(schema.OneOf, msg)
		if err != nil {
			return nil, err
		}
		if subschema != nil {
			return transform(ctx, subschema, path, msg, fieldTransform)
		}
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
		if val.Kind() == reflect.Struct {
			fieldByNames := fieldByNames(val)
			for propName, propSchema := range schema.Properties {
				structField, ok := fieldByNames[propName]
				if !ok {
					return nil, fmt.Errorf("json: missing field %s", propName)
				}
				err := transformField(ctx, path, propName, structField, val, propSchema, fieldTransform)
				if err != nil {
					return nil, err
				}
			}
			return msg, nil
		} else if val.Kind() == reflect.Map {
			for propName, propSchema := range schema.Properties {
				mapField := val.MapIndex(reflect.ValueOf(propName))
				err := transformField(ctx, path, propName, &mapField, val, propSchema, fieldTransform)
				if err != nil {
					return nil, err
				}
			}
			return msg, nil
		} else {
			return nil, fmt.Errorf("message of kind %s is not a struct or map", val.Kind())
		}
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
			fieldName = tag
		}
		fieldByNames[fieldName] = &field
	}
	return fieldByNames
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
			val.SetMapIndex(reflect.ValueOf(propName), *newVal)
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

func validateSubschemas(subschemas []*jsonschema2.Schema, msg *reflect.Value) (*jsonschema2.Schema, error) {
	val := deref(msg)
	for _, subschema := range subschemas {
		valid, err := validate(subschema, val)
		if err != nil {
			return nil, err
		}
		if valid {
			return subschema, nil
		}
	}
	return nil, nil
}

func isModernJSONSchema(draft *jsonschema2.Draft) bool {
	u := draft.URL()
	return u == "https://json-schema.org/draft/2020-12/schema" ||
		u == "https://json-schema.org/draft/2019-09/schema"
}

func getType(schema *jsonschema2.Schema) serde.FieldType {
	types := schema.Types
	if len(types) == 0 {
		return serde.TypeNull
	}
	if len(types) > 1 || len(schema.AllOf) > 0 || len(schema.AnyOf) > 0 || len(schema.OneOf) > 0 {
		return serde.TypeCombined
	}
	if len(schema.Constant) > 0 || len(schema.Enum) > 0 {
		return serde.TypeEnum
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
	case "int":
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

func deref(val *reflect.Value) *reflect.Value {
	if val.Kind() == reflect.Pointer || val.Kind() == reflect.Interface {
		v := val.Elem()
		return &v
	}
	return val
}
