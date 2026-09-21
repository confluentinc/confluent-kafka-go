/**
 * Copyright 2026 Confluent Inc.
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

package serde

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

// newSchemaRegistryClient creates the Schema Registry client a builder needs
// when the application supplied none. It is a variable so that tests can
// observe the client the builder owns.
var newSchemaRegistryClient = schemaregistry.NewClient

// ResolveSchemaRegistryClient returns the Schema Registry client a Kafka serde
// builder is to use, the [kafka.ConfigMap] to carry on with, and whether the
// client was created here.
//
// When the application supplied a client, it is used as-is and conf is passed
// through unchanged. Otherwise the client is created from srConf completed
// with the Schema Registry properties of conf, and conf is returned with those
// properties removed, so that what reaches the Kafka client holds Kafka
// properties only.
//
// A client created here is owned by the serde that is built around it, and is
// closed along with it; a client the application supplied is never closed by
// the serde.
func ResolveSchemaRegistryClient(srConf *schemaregistry.Config, client schemaregistry.Client,
	conf *kafka.ConfigMap) (schemaregistry.Client, *kafka.ConfigMap, bool, error) {

	if client != nil {
		return client, conf, false, nil
	}

	srConf, filteredConf, err := schemaregistry.NewConfigFromKafkaConfigMap(srConf, conf)
	if err != nil {
		return nil, nil, false, err
	}

	client, err = newSchemaRegistryClient(srConf)
	if err != nil {
		return nil, nil, false, err
	}
	return client, filteredConf, true, nil
}

// BuildSerde resolves the Schema Registry client as [ResolveSchemaRegistryClient]
// does, constructs the serde around it, and hands the serde ownership of the
// client when it was created here.
//
// A serde constructor that fails - on an unknown configuration property, say -
// would otherwise leak the client just created, since nothing else references
// it yet.
//
// construct creates the serde from the resolved client, and
// ownSchemaRegistryClient makes it take ownership of that client; the latter
// is invoked only when the client was created here.
func BuildSerde[S any](srConf *schemaregistry.Config, client schemaregistry.Client,
	conf *kafka.ConfigMap, construct func(schemaregistry.Client) (S, error),
	ownSchemaRegistryClient func(S)) (S, *kafka.ConfigMap, error) {

	var zero S

	client, filteredConf, owned, err := ResolveSchemaRegistryClient(srConf, client, conf)
	if err != nil {
		return zero, nil, err
	}

	serde, err := construct(client)
	if err != nil {
		if owned {
			_ = client.Close()
		}
		return zero, nil, err
	}

	if owned {
		ownSchemaRegistryClient(serde)
	}
	return serde, filteredConf, nil
}
