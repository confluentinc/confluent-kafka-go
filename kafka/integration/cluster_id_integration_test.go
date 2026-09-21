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

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// clusterIDForTest returns the cluster ID the broker reports.
func clusterIDForTest(t *testing.T) string {
	t.Helper()

	a := createAdminClient(t)
	defer a.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clusterID, err := a.ClusterID(ctx)
	require.NoError(t, err, "failed to retrieve the cluster ID")
	require.NotEmpty(t, clusterID, "the broker did not report a cluster ID")
	return clusterID
}

// clusterIDAwareSerializer records the cluster ID resolver the
// SerializingProducer hands it while it is being built, and resolves the
// cluster ID only when asked to.
type clusterIDAwareSerializer struct {
	resolve      func() (string, error)
	resolverSets int
	closed       bool
}

// resolveClusterID invokes the resolver the producer supplied, the way a serde
// does when it actually needs the ID.
func (s *clusterIDAwareSerializer) resolveClusterID() (string, error) {
	return s.resolve()
}

func (s *clusterIDAwareSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	return nil, nil
}

func (s *clusterIDAwareSerializer) SerializeWithHeaders(topic string, msg interface{}) ([]Header, []byte, error) {
	return nil, nil, nil
}

func (s *clusterIDAwareSerializer) SetClusterIDResolver(resolve func() (string, error)) {
	s.resolve = resolve
	s.resolverSets++
}

func (s *clusterIDAwareSerializer) Close() error { s.closed = true; return nil }

type clusterIDAwareSerializerBuilder struct {
	serializer *clusterIDAwareSerializer
}

func (b *clusterIDAwareSerializerBuilder) Build(conf *ConfigMap, isKey bool) (Serializer, *ConfigMap, error) {
	b.serializer = &clusterIDAwareSerializer{}
	return b.serializer, conf, nil
}

// clusterIDAwareDeserializer is the deserializer counterpart.
type clusterIDAwareDeserializer struct {
	resolve      func() (string, error)
	resolverSets int
	closed       bool
}

// resolveClusterID invokes the resolver the consumer supplied, the way a serde
// does when it actually needs the ID.
func (d *clusterIDAwareDeserializer) resolveClusterID() (string, error) {
	return d.resolve()
}

func (d *clusterIDAwareDeserializer) DeserializeWithHeaders(topic string, headers []Header,
	payload []byte) (interface{}, error) {
	return nil, nil
}

func (d *clusterIDAwareDeserializer) SetClusterIDResolver(resolve func() (string, error)) {
	d.resolve = resolve
	d.resolverSets++
}

func (d *clusterIDAwareDeserializer) Close() error { d.closed = true; return nil }

type clusterIDAwareDeserializerBuilder struct {
	deserializer *clusterIDAwareDeserializer
}

func (b *clusterIDAwareDeserializerBuilder) Build(conf *ConfigMap, isKey bool) (Deserializer, *ConfigMap, error) {
	b.deserializer = &clusterIDAwareDeserializer{}
	return b.deserializer, conf, nil
}

// TestClusterIDConsistency verifies that the cluster ID the admin client reports
// agrees with DescribeCluster and does not change across repeated calls, each of
// which returns a native string that has to be freed.
//
// TestAdminClient_ClusterID already covers the plain non-empty case.
func (its *IntegrationTestSuite) TestClusterIDConsistency() {
	t := its.T()

	a := createAdminClient(t)
	defer a.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clusterID, err := a.ClusterID(ctx)
	require.NoError(t, err, "ClusterID should not fail")

	described, err := a.DescribeCluster(ctx)
	require.NoError(t, err, "DescribeCluster should not fail")
	require.NotNil(t, described.ClusterID, "DescribeCluster did not report a cluster ID")
	assert.Equal(t, clusterID, *described.ClusterID,
		"ClusterID and DescribeCluster should agree")

	for i := 0; i < 50; i++ {
		repeated, err := a.ClusterID(ctx)
		require.NoError(t, err, "repeated ClusterID call should not fail")
		require.Equal(t, clusterID, repeated, "the cluster ID should be stable")
	}
}

// TestClusterIDPropagation verifies that building a SerializingProducer or a
// DeserializingConsumer hands every serde a cluster ID resolver, and that the
// resolver reports the cluster ID of the broker the client is connected to
// when a serde eventually invokes it.
func (its *IntegrationTestSuite) TestClusterIDPropagation() {
	t := its.T()

	expectedClusterID := clusterIDForTest(t)

	t.Run("producer", func(t *testing.T) {
		keyBuilder := &clusterIDAwareSerializerBuilder{}
		valueBuilder := &clusterIDAwareSerializerBuilder{}

		p, err := NewSerializingProducer[string, string](
			&ConfigMap{"bootstrap.servers": testconf.Brokers}, keyBuilder, valueBuilder)
		require.NoError(t, err, "failed to create the serializing producer")
		defer p.Close()

		for name, serializer := range map[string]*clusterIDAwareSerializer{
			"key": keyBuilder.serializer, "value": valueBuilder.serializer} {
			assert.Equal(t, 1, serializer.resolverSets,
				"the %s serializer should have been given a resolver once", name)
			require.NotNil(t, serializer.resolve,
				"the %s serializer should have been given a resolver", name)

			clusterID, err := serializer.resolveClusterID()
			require.NoError(t, err, "the %s serializer's resolver should not fail", name)
			assert.Equal(t, expectedClusterID, clusterID,
				"the %s serializer should resolve the cluster ID of the broker", name)
		}
	})

	t.Run("consumer", func(t *testing.T) {
		keyBuilder := &clusterIDAwareDeserializerBuilder{}
		valueBuilder := &clusterIDAwareDeserializerBuilder{}

		conf := ConfigMap{
			"bootstrap.servers": testconf.Brokers,
			"group.id":          fmt.Sprintf("%s-clusterid-%d", testconf.GroupID, rand.Intn(1000000)),
			"auto.offset.reset": "earliest",
		}
		c, err := NewDeserializingConsumer[string, string](&conf, keyBuilder, valueBuilder)
		require.NoError(t, err, "failed to create the deserializing consumer")

		for name, deserializer := range map[string]*clusterIDAwareDeserializer{
			"key": keyBuilder.deserializer, "value": valueBuilder.deserializer} {
			assert.Equal(t, 1, deserializer.resolverSets,
				"the %s deserializer should have been given a resolver once", name)
			require.NotNil(t, deserializer.resolve,
				"the %s deserializer should have been given a resolver", name)

			clusterID, err := deserializer.resolveClusterID()
			require.NoError(t, err, "the %s deserializer's resolver should not fail", name)
			assert.Equal(t, expectedClusterID, clusterID,
				"the %s deserializer should resolve the cluster ID of the broker", name)
		}

		require.NoError(t, c.Close(), "Close should not fail")
		assert.True(t, keyBuilder.deserializer.closed,
			"Close should close the key deserializer")
		assert.True(t, valueBuilder.deserializer.closed,
			"Close should close the value deserializer")
	})
}
