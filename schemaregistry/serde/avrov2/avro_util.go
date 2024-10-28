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

package avrov2

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/hamba/avro/v2"
	"github.com/modern-go/reflect2"
	"reflect"
	"strings"
)

func transform(ctx serde.RuleContext, resolver *avro.TypeResolver, schema avro.Schema, msg *reflect.Value,
	fieldTransform serde.FieldTransform) (*reflect.Value, error) {
	if msg == nil || (msg.Kind() == reflect.Pointer && msg.IsNil()) || schema == nil {
		return msg, nil
	}
	fieldCtx := ctx.CurrentField()
	if fieldCtx != nil {
		fieldCtx.Type = getType(schema)
	}
	switch schema.(type) {
	case *avro.UnionSchema:
		val := deref(msg)
		subschema, err := resolveUnion(resolver, schema, val)
		if err != nil {
			return nil, err
		}
		return transform(ctx, resolver, subschema, msg, fieldTransform)
	case *avro.ArraySchema:
		val := deref(msg)
		if val.Kind() != reflect.Slice {
			return msg, nil
		}
		subschema := schema.(*avro.ArraySchema).Items()
		for i := 0; i < val.Len(); i++ {
			item := val.Index(i)
			newVal, err := transform(ctx, resolver, subschema, &item, fieldTransform)
			if err != nil {
				return nil, err
			}
			item.Set(*newVal)
		}
		return msg, nil
	case *avro.MapSchema:
		val := deref(msg)
		if val.Kind() != reflect.Map {
			return msg, nil
		}
		subschema := schema.(*avro.MapSchema).Values()
		iter := val.MapRange()
		for iter.Next() {
			k := iter.Key()
			v := iter.Value()
			newVal, err := transform(ctx, resolver, subschema, &v, fieldTransform)
			if err != nil {
				return nil, err
			}
			val.SetMapIndex(k, *newVal)
		}
		return msg, nil
	case *avro.RecordSchema:
		val := deref(msg)
		recordSchema := schema.(*avro.RecordSchema)
		if val.Kind() == reflect.Struct {
			fieldByNames := fieldByNames(val)
			for _, avroField := range recordSchema.Fields() {
				structField, ok := fieldByNames[avroField.Name()]
				if !ok {
					return nil, fmt.Errorf("avro: missing field %s", avroField.Name())
				}
				err := transformField(ctx, resolver, recordSchema, avroField, structField, val, fieldTransform)
				if err != nil {
					return nil, err
				}
			}
			return msg, nil
		} else if val.Kind() == reflect.Map {
			for _, avroField := range recordSchema.Fields() {
				mapField := val.MapIndex(reflect.ValueOf(avroField.Name()))
				err := transformField(ctx, resolver, recordSchema, avroField, &mapField, val, fieldTransform)
				if err != nil {
					return nil, err
				}
			}
			return msg, nil
		} else {
			return nil, fmt.Errorf("message of kind %s is not a struct or map", val.Kind())
		}
	default:
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
	}
}

func fieldByNames(value *reflect.Value) map[string]*reflect.Value {
	fieldByNames := make(map[string]*reflect.Value, value.NumField())
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		structField := value.Type().Field(i)
		fieldName := structField.Name
		if tag, ok := structField.Tag.Lookup("avro"); ok {
			fieldName = tag
		}
		fieldByNames[fieldName] = &field
	}
	return fieldByNames
}

func transformField(ctx serde.RuleContext, resolver *avro.TypeResolver, recordSchema *avro.RecordSchema, avroField *avro.Field,
	structField *reflect.Value, val *reflect.Value, fieldTransform serde.FieldTransform) error {
	fullName := recordSchema.FullName() + "." + avroField.Name()
	defer ctx.LeaveField()
	ctx.EnterField(val.Interface(), fullName, avroField.Name(), getType(avroField.Type()), getInlineTags(avroField))
	newVal, err := transform(ctx, resolver, avroField.Type(), structField, fieldTransform)
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
		} else {
			val.SetMapIndex(reflect.ValueOf(avroField.Name()), *newVal)
		}
	}
	return nil
}

func getType(schema avro.Schema) serde.FieldType {
	switch schema.Type() {
	case avro.Record:
		return serde.TypeRecord
	case avro.Enum:
		return serde.TypeEnum
	case avro.Array:
		return serde.TypeArray
	case avro.Map:
		return serde.TypeMap
	case avro.Union:
		return serde.TypeCombined
	case avro.Fixed:
		return serde.TypeFixed
	case avro.String:
		return serde.TypeString
	case avro.Bytes:
		return serde.TypeBytes
	case avro.Int:
		return serde.TypeInt
	case avro.Long:
		return serde.TypeLong
	case avro.Float:
		return serde.TypeFloat
	case avro.Double:
		return serde.TypeDouble
	case avro.Boolean:
		return serde.TypeBoolean
	case avro.Null:
		return serde.TypeNull
	default:
		return serde.TypeNull
	}
}

func getInlineTags(field *avro.Field) []string {
	prop := field.Prop("confluent:tags")
	val, ok := prop.([]interface{})
	if ok {
		tags := make([]string, len(val))
		for i, v := range val {
			tags[i] = fmt.Sprint(v)
		}
		return tags
	}
	return []string{}
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

func resolveUnion(resolver *avro.TypeResolver, schema avro.Schema, msg *reflect.Value) (avro.Schema, error) {
	union := schema.(*avro.UnionSchema)
	var names []string
	var err error
	if msg.IsValid() && msg.CanInterface() {
		val := msg.Interface()
		typ := reflect2.TypeOf(val)
		names, err = resolver.Name(typ)
		if err != nil {
			return nil, err
		}
	} else {
		names = []string{"null"}
	}
	for _, name := range names {
		if idx := strings.Index(name, ":"); idx > 0 {
			name = name[:idx]
		}

		schema, _ = union.Types().Get(name)
		if schema != nil {
			return schema, nil
		}
	}
	return nil, fmt.Errorf("avro: unknown union type %s", names[0])
}

func deref(val *reflect.Value) *reflect.Value {
	if val.Kind() == reflect.Pointer {
		v := val.Elem()
		return &v
	}
	return val
}
