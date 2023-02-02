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

package cache

// MapCache is a cache backed by a map
type MapCache struct {
	entries map[interface{}]interface{}
}

// NewMapCache creates a new cache backed by a map
func NewMapCache() *MapCache {
	c := new(MapCache)
	c.entries = make(map[interface{}]interface{})
	return c
}

// Get returns the cache value associated with key
//
// Parameters:
//   - `key` - the key to retrieve
//
// Returns the value associated with key and a bool that is `false`
// if the key was not found
func (c *MapCache) Get(key interface{}) (value interface{}, ok bool) {
	value, ok = c.entries[key]
	return
}

// Put puts a value in cache associated with key
//
// Parameters:
//   - `key` - the key to put
//   - `value` - the value to put
func (c *MapCache) Put(key interface{}, value interface{}) {
	c.entries[key] = value
}

// Delete deletes the cache entry associated with key
//
// Parameters:
//   - `key` - the key to delete
func (c *MapCache) Delete(key interface{}) {
	delete(c.entries, key)
}

// ToMap returns the current cache entries copied into a map
func (c *MapCache) ToMap() map[interface{}]interface{} {
	ret := make(map[interface{}]interface{})
	for k, v := range c.entries {
		ret[k] = v
	}
	return ret
}
