package schemaregistry

/**
 * Copyright 2022 Confluent Inc.
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

import (
	"fmt"
	"reflect"
)

// ConfigValue supports the following types:
//  bool, int, string, any type with the standard String() interface
type ConfigValue interface{}

// ConfigMap is a map containing ConfigValues by key
type ConfigMap map[string]ConfigValue

// SetKey sets configuration property key to value.
func (m ConfigMap) SetKey(key string, value ConfigValue) error {
	m[key] = value
	return nil
}

// get finds key in the configmap and returns its value.
// If the key is not found defval is returned.
// If the key is found but the type is mismatched an error is returned.
func (m ConfigMap) get(key string, defval ConfigValue) (ConfigValue, error) {
	v, ok := m[key]
	if !ok {
		return defval, nil
	}

	if defval != nil && reflect.TypeOf(defval) != reflect.TypeOf(v) {
		return nil, fmt.Errorf("%s expects type %T, not %T", key, defval, v)
	}

	return v, nil
}

func (m ConfigMap) clone() ConfigMap {
	m2 := make(ConfigMap)
	for k, v := range m {
		m2[k] = v
	}
	return m2
}

// Get finds the given key in the ConfigMap and returns its value.
// If the key is not found `defval` is returned.
// If the key is found but the type does not match that of `defval` (unless nil)
// an error is returned.
func (m ConfigMap) Get(key string, defval ConfigValue) (ConfigValue, error) {
	return m.get(key, defval)
}
