/**
 * Copyright 2018 Confluent Inc.
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
	"context"
	"strings"
	"testing"
	"time"
)

func testAdminAPIs(what string, a *AdminClient, t *testing.T) {
	t.Logf("AdminClient API testing on %s: %s", a, what)

	expDuration, err := time.ParseDuration("1.5s")
	if err != nil {
		t.Fatalf("%s", err)
	}

	confStrings := map[string]string{
		"some.topic.config":  "unchecked",
		"these.are.verified": "on the broker",
		"and.this.is":        "just",
		"a":                  "unit test"}

	// Correct input, fail with timeout
	ctx, cancel := context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err := a.CreateTopics(
		ctx,
		[]TopicSpecification{
			{
				Topic:             "mytopic",
				NumPartitions:     7,
				ReplicationFactor: 3,
			},
			{
				Topic:         "mytopic2",
				NumPartitions: 2,
				ReplicaAssignment: [][]int32{
					[]int32{1, 2, 3},
					[]int32{3, 2, 1},
				},
			},
			{
				Topic:             "mytopic3",
				NumPartitions:     10000,
				ReplicationFactor: 90,
				Config:            StringMapToConfigEntries(confStrings, AlterOperationSet),
			},
		})
	if res != nil || err == nil {
		t.Fatalf("Expected CreateTopics to fail, but got result: %v, err: %v", res, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v, %v", ctx.Err(), err)
	}

	// Incorrect input, fail with ErrInvalidArg
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err = a.CreateTopics(
		ctx,
		[]TopicSpecification{
			{
				// Must not specify both ReplicationFactor and ReplicaAssignment
				Topic:             "mytopic",
				NumPartitions:     2,
				ReplicationFactor: 3,
				ReplicaAssignment: [][]int32{
					[]int32{1, 2, 3},
					[]int32{3, 2, 1},
				},
			},
		})
	if res != nil || err == nil {
		t.Fatalf("Expected CreateTopics to fail, but got result: %v, err: %v", res, err)
	}
	if ctx.Err() != nil {
		t.Fatalf("Did not expect context to fail: %v", ctx.Err())
	}
	if err.(Error).Code() != ErrInvalidArg {
		t.Fatalf("Expected ErrInvalidArg, not %v", err)
	}

	// Incorrect input, fail with ErrInvalidArg
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err = a.CreateTopics(
		ctx,
		[]TopicSpecification{
			{
				// ReplicaAssignment must be same length as Numpartitions
				Topic:         "mytopic",
				NumPartitions: 7,
				ReplicaAssignment: [][]int32{
					[]int32{1, 2, 3},
					[]int32{3, 2, 1},
				},
			},
		})
	if res != nil || err == nil {
		t.Fatalf("Expected CreateTopics to fail, but got result: %v, err: %v", res, err)
	}
	if ctx.Err() != nil {
		t.Fatalf("Did not expect context to fail: %v", ctx.Err())
	}
	if err.(Error).Code() != ErrInvalidArg {
		t.Fatalf("Expected ErrInvalidArg, not %v", err)
	}

	// Correct input, using options
	ctx, cancel = context.WithTimeout(context.Background(), expDuration)
	defer cancel()
	res, err = a.CreateTopics(
		ctx,
		[]TopicSpecification{
			{
				Topic:         "mytopic4",
				NumPartitions: 9,
				ReplicaAssignment: [][]int32{
					[]int32{1},
					[]int32{2},
					[]int32{3},
					[]int32{4},
					[]int32{1},
					[]int32{2},
					[]int32{3},
					[]int32{4},
					[]int32{1},
				},
				Config: StringMapToConfigEntries(map[string]string{
					"some.topic.config":  "unchecked",
					"these.are.verified": "on the broker",
					"and.this.is":        "just",
					"a":                  "unit test",
				}, AlterOperationSet),
			},
		})
	if res != nil || err == nil {
		t.Fatalf("Expected CreateTopics to fail, but got result: %v, err: %v", res, err)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Expected DeadlineExceeded, not %v", ctx.Err())
	}
}

// TestAdminAPIs dry-tests most Admin APIs, no broker is needed.
func TestAdminAPIs(t *testing.T) {

	a, err := NewAdminClient(&ConfigMap{})
	if err != nil {
		t.Fatalf("%s", err)
	}

	testAdminAPIs("Non-derived, no config", a, t)
	a.Close()

	a, err = NewAdminClient(&ConfigMap{"retries": 1234})
	if err != nil {
		t.Fatalf("%s", err)
	}

	testAdminAPIs("Non-derived, config", a, t)
	a.Close()

	// Test derived clients
	c, err := NewConsumer(&ConfigMap{"group.id": "test"})
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer c.Close()

	a, err = NewAdminClientFromConsumer(c)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if !strings.Contains(a.String(), c.String()) {
		t.Fatalf("Expected derived client %s to have similar name to parent %s", a, c)
	}

	testAdminAPIs("Derived from consumer", a, t)
	a.Close()

	a, err = NewAdminClientFromConsumer(c)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if !strings.Contains(a.String(), c.String()) {
		t.Fatalf("Expected derived client %s to have similar name to parent %s", a, c)
	}

	testAdminAPIs("Derived from same consumer", a, t)
	a.Close()

	p, err := NewProducer(&ConfigMap{})
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer p.Close()

	a, err = NewAdminClientFromProducer(p)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if !strings.Contains(a.String(), p.String()) {
		t.Fatalf("Expected derived client %s to have similar name to parent %s", a, p)
	}

	testAdminAPIs("Derived from Producer", a, t)
	a.Close()

	a, err = NewAdminClientFromProducer(p)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if !strings.Contains(a.String(), p.String()) {
		t.Fatalf("Expected derived client %s to have similar name to parent %s", a, p)
	}

	testAdminAPIs("Derived from same Producer", a, t)
	a.Close()
}
