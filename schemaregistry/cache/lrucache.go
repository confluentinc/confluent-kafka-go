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

import (
	"container/list"
	"fmt"
	"sync"
)

const maxPreallocateCapacity = 10000

// LRUCache is a Least Recently Used (LRU) Cache with given capacity
type LRUCache struct {
	cacheLock   sync.RWMutex
	capacity    int
	entries     map[interface{}]interface{}
	lruElements map[interface{}]*list.Element
	lruKeys     *list.List
}

// NewLRUCache creates a new Least Recently Used (LRU) Cache
//
// Parameters:
//   - `capacity` - a positive integer indicating the max capacity of this cache
//
// Returns the new allocated LRU Cache and an error
func NewLRUCache(capacity int) (c *LRUCache, err error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be a positive integer")
	}
	c = new(LRUCache)
	c.capacity = capacity
	if capacity <= maxPreallocateCapacity {
		c.entries = make(map[interface{}]interface{}, capacity)
		c.lruElements = make(map[interface{}]*list.Element, capacity)
	} else {
		c.entries = make(map[interface{}]interface{})
		c.lruElements = make(map[interface{}]*list.Element)
	}
	c.lruKeys = list.New()
	return
}

// Get returns the cache value associated with key
//
// Parameters:
//   - `key` - the key to retrieve
//
// Returns the value associated with key and a bool that is `false`
// if the key was not found
func (c *LRUCache) Get(key interface{}) (value interface{}, ok bool) {
	var element *list.Element
	c.cacheLock.RLock()
	value, ok = c.entries[key]
	if ok {
		element, ok = c.lruElements[key]
	}
	c.cacheLock.RUnlock()
	if ok {
		c.cacheLock.Lock()
		c.lruKeys.MoveToFront(element)
		c.cacheLock.Unlock()
	} else {
		value = nil
	}
	return value, ok
}

// Put puts a value in cache associated with key
//
// Parameters:
//   - `key` - the key to put
//   - `value` - the value to put
func (c *LRUCache) Put(key interface{}, value interface{}) {
	c.cacheLock.Lock()
	_, ok := c.entries[key]
	if !ok {
		// delete in advance to avoid increasing map capacity
		if c.lruKeys.Len() == c.capacity {
			back := c.lruKeys.Back()
			if back != nil {
				value := c.lruKeys.Remove(back)
				delete(c.lruElements, back)
				delete(c.entries, value)
			}
		}
		element := c.lruKeys.PushFront(key)
		c.lruElements[key] = element
	} else {
		existingElement, okElement := c.lruElements[key]
		if okElement {
			c.lruKeys.MoveToFront(existingElement)
		}
	}
	c.entries[key] = value
	c.cacheLock.Unlock()
}

// Delete deletes the cache entry associated with key
//
// Parameters:
//   - `key` - the key to delete
func (c *LRUCache) Delete(key interface{}) {
	c.cacheLock.RLock()
	_, ok := c.entries[key]
	c.cacheLock.RUnlock()
	if ok {
		c.cacheLock.Lock()
		element, okElement := c.lruElements[key]
		if okElement {
			delete(c.lruElements, key)
			c.lruKeys.Remove(element)
		}
		delete(c.entries, key)
		c.cacheLock.Unlock()
	}
}

// ToMap returns the current cache entries copied into a map
func (c *LRUCache) ToMap() map[interface{}]interface{} {
	ret := make(map[interface{}]interface{})
	c.cacheLock.RLock()
	for k, v := range c.entries {
		ret[k] = v
	}
	c.cacheLock.RUnlock()
	return ret
}
