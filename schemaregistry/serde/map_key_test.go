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
	"reflect"
	"testing"
)

type namedKey string

// textKey marshals to text but deliberately does not implement TextUnmarshaler, which is
// the common case and the reason keys are matched by their marshaled form.
type textKey struct{ Name string }

func (k textKey) MarshalText() ([]byte, error) { return []byte(k.Name), nil }

type opaqueKey struct{ N int }

// Every key type here is one encoding/json can marshal, so a caller can hand the
// serializer such a map and the walkers have to cope with it.
func TestMapKeyForNameFindsTheValueUnderEveryKeyType(t *testing.T) {
	cases := []struct {
		name string
		m    interface{}
		key  string
		want interface{}
	}{
		{"string", map[string]interface{}{"code": "v"}, "code", "v"},
		{"named string", map[namedKey]interface{}{"code": "v"}, "code", "v"},
		{"any", map[interface{}]interface{}{"code": "v"}, "code", "v"},
		{"int", map[int]interface{}{7: "v"}, "7", "v"},
		{"int8", map[int8]interface{}{7: "v"}, "7", "v"},
		{"uint64", map[uint64]interface{}{7: "v"}, "7", "v"},
		{"text marshaler", map[textKey]interface{}{{Name: "code"}: "v"}, "code", "v"},
	}
	for _, c := range cases {
		m := reflect.ValueOf(c.m)
		key, ok := MapKeyForName(m, c.key)
		if !ok {
			t.Errorf("%s: no key for %q", c.name, c.key)
			continue
		}
		got := m.MapIndex(key)
		if !got.IsValid() {
			t.Errorf("%s: key %q did not find an entry", c.name, c.key)
			continue
		}
		if got.Interface() != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got.Interface(), c.want)
		}
	}
}

func TestMapKeyForNameReportsUnusableNames(t *testing.T) {
	cases := []struct {
		name string
		m    interface{}
		key  string
	}{
		// A property name that is not a number cannot address a numeric key.
		{"int key, non-numeric name", map[int]interface{}{7: "v"}, "code"},
		// 300 does not fit in an int8, so it is not a key of this map.
		{"int8 key, out of range", map[int8]interface{}{7: "v"}, "300"},
		{"uint key, negative name", map[uint]interface{}{7: "v"}, "-1"},
		// No entry marshals to this text.
		{"text marshaler, no match", map[textKey]interface{}{{Name: "other"}: "v"}, "code"},
		// Not marshalable as a key at all; encoding/json rejects such a map too.
		{"opaque struct key", map[opaqueKey]interface{}{{N: 1}: "v"}, "code"},
	}
	for _, c := range cases {
		if _, ok := MapKeyForName(reflect.ValueOf(c.m), c.key); ok {
			t.Errorf("%s: expected %q to be reported unusable", c.name, c.key)
		}
	}
}

// The point of the helper: reflect panics rather than returning an error when the key type
// does not match, so a name that cannot be converted must never reach MapIndex.
func TestMapKeyForNameNeverPanics(t *testing.T) {
	for _, m := range []interface{}{
		map[string]interface{}{},
		map[namedKey]interface{}{},
		map[int]interface{}{},
		map[uint8]interface{}{},
		map[textKey]interface{}{},
		map[opaqueKey]interface{}{},
		map[float64]interface{}{},
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%T: panicked: %v", m, r)
				}
			}()
			MapKeyForName(reflect.ValueOf(m), "code")
		}()
	}
}
