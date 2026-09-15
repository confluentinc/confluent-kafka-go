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

// clusterIDAwareSerializer records what the SerializingProducer propagates to it
// while it is being built.
type clusterIDAwareSerializer struct {
	needsClusterID    bool
	clusterID         string
	setClusterIDCalls int
	closed            bool
}

func (s *clusterIDAwareSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	return nil, nil
}

func (s *clusterIDAwareSerializer) SerializeWithHeaders(topic string, msg interface{}) ([]Header, []byte, error) {
	return nil, nil, nil
}

func (s *clusterIDAwareSerializer) NeedsClusterID() bool { return s.needsClusterID }

func (s *clusterIDAwareSerializer) SetClusterID(clusterID string) {
	s.clusterID = clusterID
	s.setClusterIDCalls++
}

func (s *clusterIDAwareSerializer) Close() error { s.closed = true; return nil }

type clusterIDAwareSerializerBuilder struct {
	needsClusterID bool
	serializer     *clusterIDAwareSerializer
}

func (b *clusterIDAwareSerializerBuilder) Build(conf *ConfigMap, isKey bool) (Serializer, *ConfigMap, error) {
	b.serializer = &clusterIDAwareSerializer{needsClusterID: b.needsClusterID}
	return b.serializer, conf, nil
}

// clusterIDAwareDeserializer is the deserializer counterpart.
type clusterIDAwareDeserializer struct {
	needsClusterID    bool
	clusterID         string
	setClusterIDCalls int
	closed            bool
}

func (d *clusterIDAwareDeserializer) DeserializeWithHeaders(topic string, headers []Header,
	payload []byte) (interface{}, error) {
	return nil, nil
}

func (d *clusterIDAwareDeserializer) NeedsClusterID() bool { return d.needsClusterID }

func (d *clusterIDAwareDeserializer) SetClusterID(clusterID string) {
	d.clusterID = clusterID
	d.setClusterIDCalls++
}

func (d *clusterIDAwareDeserializer) Close() error { d.closed = true; return nil }

type clusterIDAwareDeserializerBuilder struct {
	needsClusterID bool
	deserializer   *clusterIDAwareDeserializer
}

func (b *clusterIDAwareDeserializerBuilder) Build(conf *ConfigMap, isKey bool) (Deserializer, *ConfigMap, error) {
	b.deserializer = &clusterIDAwareDeserializer{needsClusterID: b.needsClusterID}
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
// DeserializingConsumer resolves the cluster ID from the broker and hands it to
// exactly those serdes that asked for it.
func (its *IntegrationTestSuite) TestClusterIDPropagation() {
	t := its.T()

	expectedClusterID := clusterIDForTest(t)

	t.Run("producer", func(t *testing.T) {
		keyBuilder := &clusterIDAwareSerializerBuilder{needsClusterID: true}
		valueBuilder := &clusterIDAwareSerializerBuilder{needsClusterID: false}

		p, err := NewSerializingProducer[string, string](
			&ConfigMap{"bootstrap.servers": testconf.Brokers}, keyBuilder, valueBuilder)
		require.NoError(t, err, "failed to create the serializing producer")
		defer p.Close()

		assert.Equal(t, expectedClusterID, keyBuilder.serializer.clusterID,
			"the key serializer should have received the cluster ID")
		assert.Equal(t, 1, keyBuilder.serializer.setClusterIDCalls,
			"the cluster ID should be resolved once and set once")
		assert.Empty(t, valueBuilder.serializer.clusterID,
			"a serializer that does not need the cluster ID should not receive it")
		assert.Equal(t, 0, valueBuilder.serializer.setClusterIDCalls,
			"a serializer that does not need the cluster ID should not be called")
	})

	t.Run("consumer", func(t *testing.T) {
		keyBuilder := &clusterIDAwareDeserializerBuilder{needsClusterID: true}
		valueBuilder := &clusterIDAwareDeserializerBuilder{needsClusterID: false}

		conf := ConfigMap{
			"bootstrap.servers": testconf.Brokers,
			"group.id":          fmt.Sprintf("%s-clusterid-%d", testconf.GroupID, rand.Intn(1000000)),
			"auto.offset.reset": "earliest",
		}
		c, err := NewDeserializingConsumer[string, string](&conf, keyBuilder, valueBuilder)
		require.NoError(t, err, "failed to create the deserializing consumer")

		assert.Equal(t, expectedClusterID, keyBuilder.deserializer.clusterID,
			"the key deserializer should have received the cluster ID")
		assert.Equal(t, 1, keyBuilder.deserializer.setClusterIDCalls,
			"the cluster ID should be resolved once and set once")
		assert.Empty(t, valueBuilder.deserializer.clusterID,
			"a deserializer that does not need the cluster ID should not receive it")

		require.NoError(t, c.Close(), "Close should not fail")
		assert.True(t, keyBuilder.deserializer.closed,
			"Close should close the key deserializer")
		assert.True(t, valueBuilder.deserializer.closed,
			"Close should close the value deserializer")
	})
}
