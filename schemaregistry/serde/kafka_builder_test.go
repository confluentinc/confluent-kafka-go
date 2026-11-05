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
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

// countingClient counts how often the Schema Registry client it wraps is
// closed.
type countingClient struct {
	schemaregistry.Client
	closed int
}

func (c *countingClient) Close() error {
	c.closed++
	return c.Client.Close()
}

func newCountingClient(t *testing.T) *countingClient {
	t.Helper()
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig("mock://"))
	if err != nil {
		t.Fatalf("Failed to create the Schema Registry client: %s", err)
	}
	return &countingClient{Client: client}
}

// withCreatedClient makes the builders create created instead of a real
// client, so that a test can observe the client a builder owns.
func withCreatedClient(t *testing.T, created schemaregistry.Client) {
	t.Helper()
	previous := newSchemaRegistryClient
	newSchemaRegistryClient = func(*schemaregistry.Config) (schemaregistry.Client, error) {
		return created, nil
	}
	t.Cleanup(func() { newSchemaRegistryClient = previous })
}

func builderTestConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{"bootstrap.servers": "localhost:9092"}
}

// TestResolveSchemaRegistryClientInjected verifies that a client the
// application supplied is used as-is, is not owned, and leaves the Kafka
// ConfigMap untouched.
func TestResolveSchemaRegistryClientInjected(t *testing.T) {
	injected := newCountingClient(t)
	conf := builderTestConfigMap()

	client, filteredConf, owned, err := ResolveSchemaRegistryClient(nil, injected, conf)
	if err != nil {
		t.Fatalf("ResolveSchemaRegistryClient failed: %s", err)
	}
	if client != schemaregistry.Client(injected) {
		t.Errorf("Expected the injected client to be used")
	}
	if owned {
		t.Errorf("Expected an injected client not to be owned")
	}
	if filteredConf != conf {
		t.Errorf("Expected the Kafka ConfigMap to be passed through")
	}
}

// TestResolveSchemaRegistryClientCreated verifies that, without an injected
// client, one is created and owned, and the Kafka ConfigMap is filtered.
func TestResolveSchemaRegistryClientCreated(t *testing.T) {
	created := newCountingClient(t)
	withCreatedClient(t, created)
	conf := builderTestConfigMap()

	client, filteredConf, owned, err := ResolveSchemaRegistryClient(
		schemaregistry.NewConfig("mock://"), nil, conf)
	if err != nil {
		t.Fatalf("ResolveSchemaRegistryClient failed: %s", err)
	}
	if client != schemaregistry.Client(created) {
		t.Errorf("Expected the created client to be used")
	}
	if !owned {
		t.Errorf("Expected a created client to be owned")
	}
	if filteredConf == conf {
		t.Errorf("Expected the Kafka ConfigMap to be filtered into a new map")
	}
}

// TestBuildSerdeOwnsACreatedClient verifies that a serde built around a client
// the builder created is handed ownership of it.
func TestBuildSerdeOwnsACreatedClient(t *testing.T) {
	created := newCountingClient(t)
	withCreatedClient(t, created)

	owned := 0
	serde, _, err := BuildSerde(schemaregistry.NewConfig("mock://"), nil, builderTestConfigMap(),
		func(client schemaregistry.Client) (*Serde, error) {
			return &Serde{Client: client}, nil
		},
		func(s *Serde) { owned++; s.OwnSchemaRegistryClient() })
	if err != nil {
		t.Fatalf("BuildSerde failed: %s", err)
	}
	if owned != 1 {
		t.Errorf("Expected the serde to be given ownership once, got %d", owned)
	}

	// Closing the serde closes the client it owns, and only once.
	if err = serde.Close(); err != nil {
		t.Errorf("Close failed: %s", err)
	}
	if err = serde.Close(); err != nil {
		t.Errorf("The second Close failed: %s", err)
	}
	if created.closed != 1 {
		t.Errorf("Expected the owned client to be closed exactly once, got %d", created.closed)
	}
}

// TestBuildSerdeLeavesAnInjectedClientAlone verifies that a serde built around
// a client the application supplied is not given ownership of it, so closing
// the serde leaves the client open.
func TestBuildSerdeLeavesAnInjectedClientAlone(t *testing.T) {
	injected := newCountingClient(t)

	serde, _, err := BuildSerde(nil, injected, builderTestConfigMap(),
		func(client schemaregistry.Client) (*Serde, error) {
			return &Serde{Client: client}, nil
		},
		func(s *Serde) { t.Error("Expected an injected client not to be owned") })
	if err != nil {
		t.Fatalf("BuildSerde failed: %s", err)
	}

	if err = serde.Close(); err != nil {
		t.Errorf("Close failed: %s", err)
	}
	if injected.closed != 0 {
		t.Errorf("Expected the injected client to be left open, got %d Close calls", injected.closed)
	}
}

// TestBuildSerdeClosesACreatedClientWhenConstructionFails verifies that a
// serde constructor that fails does not leak the client the builder just
// created: nothing else references it yet.
func TestBuildSerdeClosesACreatedClientWhenConstructionFails(t *testing.T) {
	created := newCountingClient(t)
	withCreatedClient(t, created)

	_, _, err := BuildSerde(schemaregistry.NewConfig("mock://"), nil, builderTestConfigMap(),
		func(client schemaregistry.Client) (*Serde, error) {
			return nil, fmt.Errorf("bad serde config")
		},
		func(s *Serde) { t.Error("Expected no ownership hand-over on failure") })
	if err == nil {
		t.Fatal("Expected the failing serde constructor to fail the build")
	}

	if created.closed != 1 {
		t.Errorf("Expected the created client to be closed once, got %d", created.closed)
	}
}

// TestBuildSerdeLeavesAnInjectedClientAloneWhenConstructionFails verifies the
// converse: a client the application supplied is never closed by the builder.
func TestBuildSerdeLeavesAnInjectedClientAloneWhenConstructionFails(t *testing.T) {
	injected := newCountingClient(t)

	_, _, err := BuildSerde(nil, injected, builderTestConfigMap(),
		func(client schemaregistry.Client) (*Serde, error) {
			return nil, fmt.Errorf("bad serde config")
		},
		func(s *Serde) { t.Error("Expected no ownership hand-over on failure") })
	if err == nil {
		t.Fatal("Expected the failing serde constructor to fail the build")
	}

	if injected.closed != 0 {
		t.Errorf("Expected the injected client to be left open, got %d Close calls", injected.closed)
	}
}

// TestSerdeCloseWithoutAClient verifies that closing a serde that owns nothing
// is a no-op, however often it is called.
func TestSerdeCloseWithoutAClient(t *testing.T) {
	serde := &Serde{}
	if err := serde.Close(); err != nil {
		t.Errorf("Close failed: %s", err)
	}
	if err := serde.Close(); err != nil {
		t.Errorf("The second Close failed: %s", err)
	}

	// A client the serde does not own is left alone.
	injected := newCountingClient(t)
	serde = &Serde{Client: injected}
	if err := serde.Close(); err != nil {
		t.Errorf("Close failed: %s", err)
	}
	if injected.closed != 0 {
		t.Errorf("Expected an unowned client to be left open, got %d Close calls", injected.closed)
	}
}

// TestResolveSchemaRegistryClientRejectsClientAndConfig verifies that
// supplying both a client and a Schema Registry configuration is an error,
// rather than one of them being silently ignored, and that no client is
// created or taken over.
func TestResolveSchemaRegistryClientRejectsClientAndConfig(t *testing.T) {
	created := newCountingClient(t)
	withCreatedClient(t, created)
	injected := newCountingClient(t)

	client, filteredConf, owned, err := ResolveSchemaRegistryClient(
		schemaregistry.NewConfig("mock://"), injected, builderTestConfigMap())
	if err == nil {
		t.Fatal("Expected supplying both a client and a configuration to fail")
	}
	if client != nil || filteredConf != nil || owned {
		t.Errorf("Expected no client, ConfigMap or ownership on failure, got %v, %v, %v",
			client, filteredConf, owned)
	}
	if injected.closed != 0 {
		t.Errorf("Expected the injected client to be left open, got %d Close calls", injected.closed)
	}

	// BuildSerde fails the same way, before constructing anything.
	constructed := false
	_, _, err = BuildSerde(schemaregistry.NewConfig("mock://"), injected, builderTestConfigMap(),
		func(schemaregistry.Client) (*Serde, error) {
			constructed = true
			return &Serde{}, nil
		},
		func(*Serde) {})
	if err == nil {
		t.Fatal("Expected BuildSerde to fail when given both a client and a configuration")
	}
	if constructed {
		t.Errorf("Expected the serde not to be constructed")
	}
	if injected.closed != 0 {
		t.Errorf("Expected the injected client to be left open, got %d Close calls", injected.closed)
	}
}
