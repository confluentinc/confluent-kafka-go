package kafka

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

import (
	"encoding/json"
	"fmt"
	"os"
)

/*
#include <librdkafka/rdkafka.h>
*/
import "C"

type testConf map[string]interface{}

var testconf testConf = make(testConf)

// NewTestConf reads the test suite config file testconf.json which must
// contain at least Brokers and Topic string properties.
// Returns Testconf if the testconf was found and usable,
// error if file can't be read correctly
func testconfRead() (bool) {
	cf, err := os.Open("testconf.json")
	defer cf.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%% testconf.json not found - ignoring test\n")
		return false
	}

	jp := json.NewDecoder(cf)
	err = jp.Decode(&testconf)

	if err != nil {
		panic(fmt.Sprintf("Failed to parse testconf: %s", err))
	}

	resolveEnvs(testconf)

	return true
}

// resolveEnvs resolves environment variables
func resolveEnvs(conf map[string]interface{}) {
	for key, value := range conf {
		switch v := value.(type) {
		case string:
			if v[0] == '$' {
				conf[key] = os.Getenv(v[1:])
			}
		case int:
		case bool:
		case float64:
		default:
			resolveEnvs(v.(map[string]interface{}))
		}
	}
}

// getObject returns a child object of the root testConf
func (tc testConf) getObject(name string) testConf {
	return tc[name].(map[string]interface{})
}

// getString returns a string representation of the value represented by key from the provided namespace
// if the namespace is an empty string the root object will be searched.
func (tc testConf) getString(key string) string {
	val, ok := tc[key]
	if ok {
		return val.(string)
	}
	return ""
}

// getInt returns an integer representation of the value represented by key from the provided namespace
// If the namespace is an empty string the root object will be searched.
func (tc testConf) getInt(key string) int {
	val, ok := tc[key]
	if ok {
		return val.(int)
	}
	return 0
}

// updateFromTestConf populates existing ConfigMap with all common configs
func (cm *ConfigMap) updateFromTestconf(element string) error {
	for key, value := range testconf {
		switch value.(type) {
		case string:
			cm.SetKey(key, value)
		case int:
			cm.SetKey(key, value)
		case bool:
			cm.SetKey(key, value)
		case float64:
			cm.SetKey(key, value)
		default:
			if key == element {
				cm.updateFromTestconf("")
			}
		}
	}

	return nil
}

// Return the number of messages available in all partitions of a topic.
// WARNING: This uses watermark offsets so it will be incorrect for compacted topics.
func getMessageCountInTopic(topic string) (int, error) {
	cm := ConfigMap{}
	cm.updateFromTestconf("consumer")

	// Create consumer
	c, err := NewConsumer(&cm)
	if err != nil {
		return 0, err
	}

	// get metadata for the topic to find out number of partitions

	metadata, err := c.GetMetadata(&topic, false, 5*1000)
	if err != nil {
		return 0, err
	}

	t, ok := metadata.Topics[topic]
	if !ok {
		return 0, newError(C.RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
	}

	cnt := 0
	for _, p := range t.Partitions {
		low, high, err := c.QueryWatermarkOffsets(topic, p.ID, 5*1000)
		if err != nil {
			continue
		}
		cnt += int(high - low)
	}

	return cnt, nil
}
