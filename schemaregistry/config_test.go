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

package schemaregistry

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestConfigWithAuthentication(t *testing.T) {
	maybeFail = initFailFunc(t)

	c := NewConfigWithBasicAuthentication("mock://", "username", "password")

	maybeFail("BasicAuthCredentialsSource", expect(c.BasicAuthCredentialsSource, "USER_INFO"))
	maybeFail("BasicAuthUserInfo", expect(c.BasicAuthUserInfo, "username:password"))
}

func TestConfigWithBearerAuth(t *testing.T) {
	maybeFail = initFailFunc(t)
	c := NewConfigWithBearerAuthentication("mock://", "token", "lsrc-123", "poolID")
	maybeFail("BearerAuthCredentialsSource", expect(c.BearerAuthCredentialsSource, "STATIC_TOKEN"))
	maybeFail("BearerAuthLogicalCluster", expect(c.BearerAuthLogicalCluster, "lsrc-123"))
	maybeFail("BearerAuthIdentityPoolID", expect(c.BearerAuthIdentityPoolID, "poolID"))
}

// TestNewConfigFromKafkaConfigMap verifies that a Schema Registry config is
// derived from a Kafka ConfigMap, and that the returned ConfigMap keeps the
// Kafka properties while being a copy of the original one.
func TestNewConfigFromKafkaConfigMap(t *testing.T) {
	maybeFail = initFailFunc(t)

	kafkaConf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"acks":              "all",
	}

	// Without a Schema Registry config, an empty one is returned.
	srConf, filteredConf, err := NewConfigFromKafkaConfigMap(nil, kafkaConf)
	maybeFail("NewConfigFromKafkaConfigMap", err)
	maybeFail("SchemaRegistryURL", expect(srConf.SchemaRegistryURL, ""))
	maybeFail("filtered length", expect(len(*filteredConf), len(*kafkaConf)))
	for key, value := range *kafkaConf {
		filteredValue, err := filteredConf.Get(key, nil)
		maybeFail("filtered value", err, expect(filteredValue, value))
	}

	// The returned ConfigMap is a copy: modifying it leaves the original
	// ConfigMap untouched.
	err = filteredConf.SetKey("linger.ms", 100)
	maybeFail("SetKey", err)
	if _, ok := (*kafkaConf)["linger.ms"]; ok {
		t.Errorf("Expected the original ConfigMap not to be modified")
	}

	// An existing Schema Registry config is passed through.
	conf := NewConfig("mock://")
	srConf, filteredConf, err = NewConfigFromKafkaConfigMap(conf, kafkaConf)
	maybeFail("NewConfigFromKafkaConfigMap", err,
		expect(srConf, conf), expect(len(*filteredConf), len(*kafkaConf)))
}
