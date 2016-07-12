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

// kafka client.
// This package implements high-level Apache Kafka producer and consumers
// using bindings on-top of the C librdkafka library.
package kafka

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

/*
#include <stdlib.h>
#include <librdkafka/rdkafka.h>
*/
import "C"

// ConfigValue supports the following types:
//  bool, int, string, any type with the standard String() interface
type ConfigValue interface{}

// ConfigMap is a map contianing standard librdkafka configuration properties as documented in:
// https://github.com/edenhill/librdkafka/tree/master/CONFIGURATION.md
//
// The special property "default.topic.config" (optional) is a ConfigMap containing default topic
// configuration properties.
type ConfigMap map[string]ConfigValue

func (c ConfigMap) String() string {
	return ""
}

// implements flag.Set
func (m ConfigMap) Set(kv string) error {
	i := strings.Index(kv, "=")
	if i == -1 {
		return KafkaError{ERR__INVALID_ARG, "Expected key=value"}
	}

	k := kv[:i]
	v := kv[i+1:]

	// For user convenience convert any property prefixed
	// with {topic}. to a default.topic.config sub-map property.
	if strings.HasPrefix(k, "{topic}.") {
		_, found := m["default.topic.config"]
		if !found {
			m["default.topic.config"] = ConfigMap{}
		}
		m["default.topic.config"].(ConfigMap)[strings.TrimPrefix(k, "{topic}.")] = v
	} else {
		m[k] = v
	}

	return nil
}

type stringable interface {
	String() string
}

func value2string(v ConfigValue) (ret string, errstr string) {

	switch v.(type) {
	case bool:
		if v.(bool) {
			ret = "true"
		} else {
			ret = "false"
		}
	case int:
		ret = fmt.Sprintf("%d", v)
	case string:
		ret = v.(string)
	case stringable:
		ret = v.(stringable).String()
	default:
		return "", fmt.Sprintf("Invalid value type %T", v)
	}

	return ret, ""
}

// rdk_anyconf abstracts rd_kafka_conf_t and rd_kafka_topic_conf_t
// into a common interface.
type rdk_anyconf interface {
	Set(c_key *C.char, c_val *C.char, c_errstr *C.char, errstr_size int) C.rd_kafka_conf_res_t
}

func anyconf_set(anyconf rdk_anyconf, key string, value string) (err error) {
	var c_key *C.char = C.CString(key)
	var c_val *C.char = C.CString(value)
	var c_errstr *C.char = (*C.char)(C.malloc(C.size_t(128)))
	defer C.free(unsafe.Pointer(c_errstr))

	if anyconf.Set(c_key, c_val, c_errstr, 128) != C.RD_KAFKA_CONF_OK {
		C.free(unsafe.Pointer(c_key))
		C.free(unsafe.Pointer(c_val))
		return NewKafkaErrorFromCString(c_errstr)
	}

	return nil
}

func (c_conf *C.rd_kafka_conf_t) Set(c_key *C.char, c_val *C.char, c_errstr *C.char, errstr_size int) C.rd_kafka_conf_res_t {
	return C.rd_kafka_conf_set(c_conf, c_key, c_val, c_errstr, C.size_t(errstr_size))
}

func (c_topic_conf *C.rd_kafka_topic_conf_t) Set(c_key *C.char, c_val *C.char, c_errstr *C.char, errstr_size int) C.rd_kafka_conf_res_t {
	return C.rd_kafka_topic_conf_set(c_topic_conf, c_key, c_val, c_errstr, C.size_t(errstr_size))
}

func config_convert_anyconf(m ConfigMap, anyconf rdk_anyconf) (err error) {

	for k, v := range m {
		switch v.(type) {
		case ConfigMap:
			/* Special sub-ConfigMap, only used for default.topic.config */

			if k != "default.topic.config" {
				return KafkaError{ERR__INVALID_ARG, fmt.Sprintf("Invalid type for key %s", k)}
			}

			var c_topic_conf = C.rd_kafka_topic_conf_new()

			err = config_convert_anyconf(v.(ConfigMap), c_topic_conf)
			if err != nil {
				C.rd_kafka_topic_conf_destroy(c_topic_conf)
				return err
			}

			C.rd_kafka_conf_set_default_topic_conf(anyconf.(*C.rd_kafka_conf_t), c_topic_conf)

		default:
			val, errstr := value2string(v)
			if errstr != "" {
				return KafkaError{ERR__INVALID_ARG, fmt.Sprintf("%s for key %s (expected string,bool,int,ConfigMap)", errstr, k)}
			}

			err = anyconf_set(anyconf, k, val)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Convert ConfigMap to C rd_kafka_conf_t *
func (m ConfigMap) convert() (c_conf *C.rd_kafka_conf_t, err error) {
	c_conf = C.rd_kafka_conf_new()

	err = config_convert_anyconf(m, c_conf)
	if err != nil {
		C.rd_kafka_conf_destroy(c_conf)
		return nil, err
	}
	return c_conf, nil
}

// extract finds key in the configmap, deletes it, and return its value.
// The value type is also checked to match the provided default value's type.
// If the key is not found defval is returned.
// If the key is found but the type is mismatched an error is returned.
func (m ConfigMap) extract(key string, defval ConfigValue) (ConfigValue, error) {
	v, ok := m[key]
	if !ok {
		return defval, nil
	}

	if reflect.TypeOf(defval) != reflect.TypeOf(v) {
		return nil, KafkaError{ERR__INVALID_ARG, fmt.Sprintf("%s expects type %T, not %T", key, defval, v)}
	}

	delete(m, key)

	return v, nil
}
