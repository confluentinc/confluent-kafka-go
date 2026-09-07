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
	"strings"

	avro "github.com/confluentinc/confluent-avro-go/v2"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/modern-go/reflect2"
)

func transform(ctx serde.RuleContext, resolver *avro.TypeResolver, schema avro.Schema, msg *reflect.Value,
	fieldTransform serde.FieldTransform) (*reflect.Value, error) {
	// Only an absent schema or an absent reflect.Value stops the walk. A **nil pointer** is the
	// null branch of a ["null", T] union and has to reach the rule: the reference binds it as CEL
	// null so a rule can guard with `value == null`, and returning early here skipped the rule
	// entirely - indistinguishable, to the caller, from a rule that ran and passed. resolveUnion
	// already picks the "null" branch for an invalid value, and the record case below guards the
	// one shape that has nothing to walk.
	if msg == nil || schema == nil {
		return msg, nil
	}
	fieldCtx := ctx.CurrentField()
	if fieldCtx != nil {
		fieldCtx.Type = getType(schema)
	}
	switch schema.(type) {
	case *avro.RefSchema:
		// A reference to a named type has to be unwrapped, or the inline tags on the record
		// it points at - and so the fields they mark for encryption - are never seen.
		return transform(ctx, resolver, schema.(*avro.RefSchema).Schema(), msg, fieldTransform)
	case *avro.UnionSchema:
		subschema, submsg, err := resolveUnion(resolver, schema, msg)
		if err != nil {
			return nil, err
		}
		submsg, err = transform(ctx, resolver, subschema, submsg, fieldTransform)
		if err != nil {
			return nil, err
		}
		if msg.IsValid() && msg.CanInterface() {
			val := msg.Interface()
			// Check if the value is a map[string]interface{} with a single entry
			if m, ok := val.(map[string]interface{}); ok && len(m) == 1 {
				for k := range m {
					newMap := map[string]interface{}{k: submsg.Interface()}
					newVal := reflect.ValueOf(newMap)
					return &newVal, nil
				}
			}
		}
		return submsg, nil
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
			// A condition's per-element verdicts are evaluated and then dropped. The
			// reference collects them into an untyped list, which the field-level check
			// never reads as `false`, so a CEL_FIELD condition does not apply to a
			// container field. Writing one back here panics instead: the slice's element
			// type cannot hold a bool.
			if ctx.Rule.Kind != "CONDITION" {
				item.Set(*newVal)
			}
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
			// A verdict is not a replacement for the value: dropped, as on an array.
			if ctx.Rule.Kind != "CONDITION" {
				val.SetMapIndex(k, *newVal)
			}
		}
		return msg, nil
	case *avro.RecordSchema:
		val := deref(msg)
		recordSchema := schema.(*avro.RecordSchema)
		if !val.IsValid() {
			// A null record has no fields to walk - the one place the reference guards a null.
			return msg, nil
		}
		if val.Kind() == reflect.Struct {
			fieldByNames := fieldByNames(val)
			for _, avroField := range recordSchema.Fields() {
				structField, ok := fieldByNames[avroField.Name()]
				if !ok {
					continue
				}
				err := transformField(ctx, resolver, recordSchema, avroField, structField, val, fieldTransform)
				if err != nil {
					return nil, err
				}
			}
			return msg, nil
		} else if val.Kind() == reflect.Map {
			for _, avroField := range recordSchema.Fields() {
				key, ok := serde.MapKeyForName(*val, avroField.Name())
				if !ok {
					continue
				}
				mapField := val.MapIndex(key)
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
				// A null union branch derefs to the zero reflect.Value, and Interface() panics
				// on that. The rule is meant to see the absence, so hand it an untyped nil -
				// which the CEL adapter binds as null.
				var fieldValue interface{}
				if val.IsValid() && val.CanInterface() {
					fieldValue = val.Interface()
				}
				newVal, err := fieldTransform.Transform(ctx, *fieldCtx, fieldValue)
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
	} else if newVal.IsValid() {
		// A transform that produced nothing writes nothing. A null union branch derefs to the
		// zero reflect.Value and comes back as one whenever the walk did not replace it - a
		// rule whose tags do not match this field, for instance - and reflect.Set panics on a
		// zero Value rather than erroring.
		if val.Kind() == reflect.Struct {
			err = setField(structField, newVal)
			if err != nil {
				return err
			}
		} else {
			if key, ok := serde.MapKeyForName(*val, avroField.Name()); ok {
				val.SetMapIndex(key, *newVal)
			}
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

func resolveUnion(resolver *avro.TypeResolver, schema avro.Schema, msg *reflect.Value) (avro.Schema, *reflect.Value, error) {
	union := schema.(*avro.UnionSchema)
	var names []string
	var err error
	// Interface layers come off - a value read out of a map arrives boxed - but pointers do
	// not, because a pointer is part of the type hamba's resolver is asked about below.
	val := derefInterface(msg)
	switch {
	case !val.IsValid() || !val.CanInterface() ||
		(val.Kind() == reflect.Pointer && val.IsNil()):
		// An absent value is the null branch. This has to be tested explicitly: a nil pointer
		// still has a nameable type, so leaving it to the resolver would pick the *value*
		// branch and encode a zero.
		names = []string{"null"}
	default:
		if m, ok := val.Interface().(map[string]interface{}); ok && len(m) == 1 {
			// hamba's own shape for a union value: a single entry keyed by branch name.
			for k, v := range m {
				names = []string{k}
				newMsg := reflect.ValueOf(v)
				msg = &newMsg
			}
		} else {
			names, err = resolveUnionNames(resolver, val)
			if err != nil {
				return nil, msg, err
			}
		}
	}
	for _, name := range names {
		if idx := strings.Index(name, ":"); idx > 0 {
			name = name[:idx]
		}

		schema, _ = union.Types().Get(name)
		if schema != nil {
			return schema, msg, nil
		}
	}
	return nil, nil, fmt.Errorf("avro: unknown union type %s", names[0])
}

// derefInterface unwraps interface layers only, leaving pointers intact.
func derefInterface(val *reflect.Value) *reflect.Value {
	v := *val
	for v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return &v
}

// resolveUnionNames asks hamba to name the value's type, trying it both as held and
// dereferenced.
//
// The resolver's registrations are not consistent about pointers, and neither shape alone
// works: a nullable decimal is known as *big.Rat and not as big.Rat, while a nullable string is
// known as string and not as *string. Resolving only the dereferenced form failed a present
// decimal with "avro: unable to resolve type big.Rat"; only the pointer form fails every
// nullable primitive instead.
func resolveUnionNames(resolver *avro.TypeResolver, val *reflect.Value) ([]string, error) {
	names, err := resolver.Name(reflect2.TypeOf(val.Interface()))
	if err == nil {
		return names, nil
	}
	if val.Kind() == reflect.Pointer {
		inner := val.Elem()
		if inner.IsValid() && inner.CanInterface() {
			if names, innerErr := resolver.Name(reflect2.TypeOf(inner.Interface())); innerErr == nil {
				return names, nil
			}
		}
	}
	return nil, err
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
