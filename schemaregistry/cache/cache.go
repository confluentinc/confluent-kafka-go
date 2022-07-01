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

// Cache represents a key-value storage where to put cached data
type Cache interface {
	// Get returns the cache value associated with key
	//
	// Parameters:
	//  * `key` - the key to retrieve
	//
	// Returns the value associated with key and a bool that is `false`
	// if the key was not found
	Get(key interface{}) (interface{}, bool)
	// Put puts a value in cache associated with key
	//
	// Parameters:
	//  * `key` - the key to put
	//  * `value` - the value to put
	Put(key interface{}, value interface{})
	// Delete deletes the cache entry associated with key
	//
	// Parameters:
	//  * `key` - the key to delete
	Delete(key interface{})
	// ToMap returns the current cache entries copied into a map
	ToMap() map[interface{}]interface{}
}
