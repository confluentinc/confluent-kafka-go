/**
 * Copyright 2016 Confluent Inc.
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

package kafka

import (
	"fmt"
	"testing"
)

// A custom type with Stringer interface to be used to test config map APIs
type HostPortType struct {
	Host string
	Port int
}

// implements String() interface
func (hp HostPortType) String() string {
	return fmt.Sprintf("%s:%d", hp.Host, hp.Port)
}

//Test config map APIs
func TestConfigMapAPIs(t *testing.T) {
	config := &ConfigMap{}

	// set a good key via SetKey()
	err := config.SetKey("bootstrap.servers", testconf.Brokers)
	if err != nil {
		t.Errorf("Failed to set key via SetKey(). Error: %s\n", err)
	}

	// test custom Stringer type
	hostPort := HostPortType{Host: "localhost", Port: 9092}
	err = config.SetKey("bootstrap.servers", hostPort)
	if err != nil {
		t.Errorf("Failed to set custom Stringer type via SetKey(). Error: %s\n", err)
	}

	// test boolean type
	err = config.SetKey("{topic}.produce.offset.report", true)
	if err != nil {
		t.Errorf("Failed to set key via SetKey(). Error: %s\n", err)
	}

	// test offset literal string
	err = config.SetKey("{topic}.auto.offset.reset", "earliest")
	if err != nil {
		t.Errorf("Failed to set key via SetKey(). Error: %s\n", err)
	}

	//test offset constant
	err = config.SetKey("{topic}.auto.offset.reset", OffsetBeginning)
	if err != nil {
		t.Errorf("Failed to set key via SetKey(). Error: %s\n", err)
	}

	//test integer offset
	err = config.SetKey("{topic}.message.timeout.ms", 10)
	if err != nil {
		t.Errorf("Failed to set integer value via SetKey(). Error: %s\n", err)
	}

	// set a good key-value pair via Set()
	err = config.Set("group.id=test.id")
	if err != nil {
		t.Errorf("Failed to set key-value pair via Set(). Error: %s\n", err)
	}

	// negative test cases
	// set a bad key-value pair via Set()
	err = config.Set("group.id:test.id2")
	if err == nil {
		t.Errorf("Expected failure when setting invalid key-value pair via Set()\n")
	}

	// get string value
	v, err := config.Get("group.id", nil)
	if err != nil {
		t.Errorf("Expected Get(group.id) to succeed: %s\n", err)
	}
	if v == nil {
		t.Errorf("Expected Get(group.id) to return non-nil value\n")
	}
	if v.(string) != "test.id" {
		t.Errorf("group.id mismatch: %s\n", v)
	}

	// get string value but request int
	dummyInt := 12
	_, err = config.Get("group.id", dummyInt)
	if err == nil {
		t.Errorf("Expected Get(group.id) to fail\n")
	}

	// get integer value
	v, err = config.Get("{topic}.message.timeout.ms", dummyInt)
	if err != nil {
		t.Errorf("Expected Get(message.timeout.ms) to succeed: %s\n", err)
	}
	if v == nil {
		t.Errorf("Expected Get(message.timeout.ms) to return non-nil value\n")
	}
	if v.(int) != 10 {
		t.Errorf("message.timeout.ms mismatch: %d\n", v.(int))
	}

	// get unknown value
	v, err = config.Get("dummy.value.not.found", nil)
	if v != nil {
		t.Errorf("Expected nil for dummy value, got %v\n", v)
	}

}

// Test that plugins will always be configured before their config options
func TestConfigPluginPaths(t *testing.T) {
	config := &ConfigMap{
		"plugin.library.paths": "monitoring-interceptor",
	}
	_, err := config.convert()
	if err != nil {
		t.Skipf("Missing monitoring-interceptor: %s", err)
	}

	config = &ConfigMap{
		"plugin.library.paths":                     "monitoring-interceptor",
		"confluent.monitoring.interceptor.icdebug": true,
	}

	// convert() would fail randomly due to random order of ConfigMap key iteration.
	// running convert() once gave the test case a 50% failure chance,
	// running it 100 times gives ~100%
	for i := 1; i <= 100; i++ {
		_, err := config.convert()
		if err != nil {
			t.Fatalf("Failed to convert. Error: %s\n", err)
		}
	}
}
