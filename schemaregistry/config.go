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
	"strconv"
)

// ConfigMap is a map of string key-values
type ConfigMap map[string]string

// Config is type-safe interface for specifying config key-value pairs
type Config interface {
	GetBool(key string, defval bool) (bool, error)
	GetInt(key string, defval int) (int, error)
	GetString(key string, defval string) (string, error)
	SetBool(key string, value bool) error
	SetInt(key string, value int) error
	SetString(key string, value string) error
}

// SetBool sets configuration property key to the bool value.
func (m ConfigMap) SetBool(key string, value bool) error {
	m[key] = strconv.FormatBool(value)
	return nil
}

// SetInt sets configuration property key to the int value.
func (m ConfigMap) SetInt(key string, value int) error {
	m[key] = strconv.FormatInt(int64(value), 10)
	return nil
}

// SetString sets configuration property key to the string value.
func (m ConfigMap) SetString(key string, value string) error {
	m[key] = value
	return nil
}

func (m ConfigMap) clone() ConfigMap {
	m2 := make(ConfigMap)
	for k, v := range m {
		m2[k] = v
	}
	return m2
}

// GetBool finds the given key in the ConfigMap and returns its bool value.
// If the key is not found `defval` is returned.
// If the key is found but the type does not match that of `defval` (unless nil)
// an error is returned.
func (m ConfigMap) GetBool(key string, defval bool) (bool, error) {
	v, ok := m[key]
	if !ok {
		return defval, nil
	}

	ret, err := strconv.ParseBool(v)
	if err != nil {
		return false, err
	}
	return ret, nil
}

// GetInt finds the given key in the ConfigMap and returns its int value.
// If the key is not found `defval` is returned.
// If the key is found but the type does not match that of `defval` (unless nil)
// an error is returned.
func (m ConfigMap) GetInt(key string, defval int) (int, error) {
	v, ok := m[key]
	if !ok {
		return defval, nil
	}

	ret, err := strconv.ParseInt(v, 10, 0)
	if err != nil {
		return 0, err
	}
	return int(ret), nil
}

// GetString finds the given key in the ConfigMap and returns its string value.
// If the key is not found `defval` is returned.
// If the key is found but the type does not match that of `defval` (unless nil)
// an error is returned.
func (m ConfigMap) GetString(key string, defval string) (string, error) {
	v, ok := m[key]
	if !ok {
		return defval, nil
	}

	return v, nil
}
