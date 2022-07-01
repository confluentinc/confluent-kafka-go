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
	"testing"
)

func TestWrongCapacity(t *testing.T) {
	for _, capacity := range []int{-1, 0} {
		_, err := NewLRUCache(capacity)
		if err == nil {
			t.Fatalf("expected \"capacity must be a positive integer\" error, not nil\n")
		}
	}
}

func TestCRUD(t *testing.T) {
	cache, err := NewLRUCache(2)
	if err != nil {
		t.Fatalf("expected nil error, not \"%s\"\n", err.Error())
	}

	for key, values := range map[int][]string{
		1: {"test", "test2"},
		2: {"tests", "tests2"},
	} {
		firstValue, secondValue := values[0], values[1]
		cache.Put(key, firstValue)
		readValue, ok := cache.Get(key)
		if !ok {
			t.Fatalf("expected to find key \"%v\"\n", key)
		}
		if readValue != firstValue {
			t.Fatalf("expected to find value \"%v\", not \"%v\"\n", firstValue, readValue)
		}
		cache.Put(key, secondValue)
		readValue, ok = cache.Get(key)
		if !ok {
			t.Fatalf("expected to find key \"%v\"\n", key)
		}
		if readValue != secondValue {
			t.Fatalf("expected to find value \"%v\", not \"%v\"\n", secondValue, readValue)
		}
		cache.Delete(key)
		readValue, ok = cache.Get(key)
		if ok {
			t.Fatalf("value for key \"%v\" not expected, found \"%v\"\n", key, readValue)
		}
	}
}

func TestMaxCapacity(t *testing.T) {
	cache, err := NewLRUCache(2)
	if err != nil {
		t.Fatalf("expected nil error, not \"%s\"\n", err.Error())
	}

	cache.Put(1, "test1")
	cache.Put(2, "test2")
	cache.Put(3, "test3")

	_, ok := cache.Get(1)
	if ok {
		t.Fatalf("not expected to find key 1\n")
	}
	_, ok = cache.Get(2)
	if !ok {
		t.Fatalf("expected to find key 2\n")
	}
	_, ok = cache.Get(3)
	if !ok {
		t.Fatalf("expected to find key 3\n")
	}
}

func TestMaxCapacityWithGet(t *testing.T) {
	cache, err := NewLRUCache(2)
	if err != nil {
		t.Fatalf("expected nil error, not \"%s\"\n", err.Error())
	}

	cache.Put(1, "test1")
	cache.Put(2, "test2")
	_, ok := cache.Get(1)
	if !ok {
		t.Fatalf("expected value \"test1\" not found for key 1\n")
	}
	cache.Put(3, "test3")

	if !ok {
		t.Fatalf("expected to find key 1\n")
	}
	_, ok = cache.Get(2)
	if ok {
		t.Fatalf("not expected to find key 2\n")
	}
	_, ok = cache.Get(3)
	if !ok {
		t.Fatalf("expected to find key 3\n")
	}
}
