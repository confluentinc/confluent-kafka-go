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

package serde

import (
	"encoding"
	"reflect"
	"strconv"
)

var textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()

// MapKeyForName converts a schema property or field name into a key of m's own key type,
// reporting false when the name cannot be one.
//
// The walkers address a record held as a map by the name the schema gives it, but the key
// type is whatever the caller chose. reflect.Value.MapIndex and SetMapIndex require a key
// assignable to that exact type and panic otherwise, so handing them a plain string
// crashes on any map whose keys are not plain strings - including shapes encoding/json
// marshals happily, such as map[MyName]any or map[int]any.
//
// The accepted key types are the ones encoding/json accepts, so any message the caller can
// serialize can also be walked:
//
//   - a string, or any type whose underlying type is a string
//   - any integer type, matching the decimal form encoding/json gives such a key
//   - a type whose keys marshal to text, matched by comparing that text
//
// Anything else reports false, and the caller skips the property rather than panicking.
func MapKeyForName(m reflect.Value, name string) (reflect.Value, bool) {
	keyType := m.Type().Key()

	// map[string]T, and map[any]T, need no conversion.
	if reflect.TypeOf(name).AssignableTo(keyType) {
		return reflect.ValueOf(name), true
	}
	// A named string type: type MyName string; map[MyName]T.
	if keyType.Kind() == reflect.String {
		return reflect.ValueOf(name).Convert(keyType), true
	}

	switch keyType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(name, 10, 64)
		if err != nil || reflect.Zero(keyType).OverflowInt(n) {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(n).Convert(keyType), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n, err := strconv.ParseUint(name, 10, 64)
		if err != nil || reflect.Zero(keyType).OverflowUint(n) {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(n).Convert(keyType), true
	}

	// A key that marshals to text is matched against the text itself, which is what the
	// serialized form holds and therefore what the schema names. Going the other way -
	// unmarshaling the name into a key - would assume the type round-trips and that it
	// implements TextUnmarshaler at all, which many such types do not.
	if keyType.Implements(textMarshalerType) {
		iter := m.MapRange()
		for iter.Next() {
			text, err := iter.Key().Interface().(encoding.TextMarshaler).MarshalText()
			if err == nil && string(text) == name {
				return iter.Key(), true
			}
		}
	}
	return reflect.Value{}, false
}
