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

package kafka

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubClusterIDLookup replaces the librdkafka cluster ID lookup for the
// duration of a test. A lookup with a zero timeout consults the cache, which
// holds cached once it is set, and never blocks. Any other lookup is counted,
// signals entered, and blocks until release is closed, then returns id and
// err.
type stubClusterIDLookup struct {
	cached  atomic.Value // string
	lookups atomic.Int32
	entered chan struct{}
	release chan struct{}
	id      string
	err     error
}

func withStubClusterIDLookup(t *testing.T, id string, err error) *stubClusterIDLookup {
	t.Helper()
	stub := &stubClusterIDLookup{
		entered: make(chan struct{}, 64),
		release: make(chan struct{}),
		id:      id,
		err:     err,
	}
	stub.cached.Store("")

	previous := lookupClusterID
	lookupClusterID = func(_ *handle, timeoutMs int) (string, error) {
		if timeoutMs == 0 {
			if cached := stub.cached.Load().(string); cached != "" {
				return cached, nil
			}
			return "", errors.New("Failed to retrieve cluster ID")
		}
		stub.lookups.Add(1)
		stub.entered <- struct{}{}
		<-stub.release
		return stub.id, stub.err
	}
	t.Cleanup(func() { lookupClusterID = previous })
	return stub
}

// resolveConcurrently starts n resolutions on h, the first one alone so that
// it is the one starting the lookup, and returns their results once all are
// done. The others are started while that lookup is blocked, and given time
// to join it before it is released.
func resolveConcurrently(t *testing.T, h *handle, stub *stubClusterIDLookup, n int) ([]string, []error) {
	t.Helper()
	ids := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	resolve := func(i int) {
		defer wg.Done()
		ids[i], errs[i] = h.resolveClusterID(clusterIDTimeoutMs)
	}

	wg.Add(n)
	go resolve(0)
	select {
	case <-stub.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected the first resolution to start a lookup")
	}
	for i := 1; i < n; i++ {
		go resolve(i)
	}
	// The followers cannot be observed waiting, so they are given ample time
	// to reach the lookup in flight before it completes.
	time.Sleep(100 * time.Millisecond)
	close(stub.release)
	wg.Wait()
	return ids, errs
}

// TestResolveClusterIDSharesOneLookup verifies that concurrent resolutions on
// a handle share a single lookup, and all get its result.
func TestResolveClusterIDSharesOneLookup(t *testing.T) {
	stub := withStubClusterIDLookup(t, "cluster-1", nil)

	ids, errs := resolveConcurrently(t, &handle{}, stub, 10)

	if n := stub.lookups.Load(); n != 1 {
		t.Errorf("Expected a single lookup for 10 concurrent resolutions, got %d", n)
	}
	for i := range ids {
		if errs[i] != nil || ids[i] != "cluster-1" {
			t.Errorf("Resolution %d: expected cluster-1, got %q, %v", i, ids[i], errs[i])
		}
	}
}

// TestResolveClusterIDSharesAFailure verifies that a failed lookup fails every
// resolution waiting on it, and is not kept: the next resolution starts a
// fresh lookup.
func TestResolveClusterIDSharesAFailure(t *testing.T) {
	lookupErr := errors.New("lookup failed")
	stub := withStubClusterIDLookup(t, "", lookupErr)
	h := &handle{}

	_, errs := resolveConcurrently(t, h, stub, 5)

	if n := stub.lookups.Load(); n != 1 {
		t.Errorf("Expected a single lookup for 5 concurrent resolutions, got %d", n)
	}
	for i, err := range errs {
		if !errors.Is(err, lookupErr) {
			t.Errorf("Resolution %d: expected the lookup error, got %v", i, err)
		}
	}

	// The release channel is closed, so this lookup returns at once.
	if _, err := h.resolveClusterID(clusterIDTimeoutMs); !errors.Is(err, lookupErr) {
		t.Errorf("Expected the lookup error, got %v", err)
	}
	if n := stub.lookups.Load(); n != 2 {
		t.Errorf("Expected a failed lookup to be retried by the next resolution, got %d lookups", n)
	}
}

// TestResolveClusterIDDoesNotReuseACompletedLookup verifies that a lookup is
// shared only while it is in flight: once it has completed, the next
// resolution that misses librdkafka's cache starts its own.
func TestResolveClusterIDDoesNotReuseACompletedLookup(t *testing.T) {
	stub := withStubClusterIDLookup(t, "cluster-1", nil)
	close(stub.release)
	h := &handle{}

	for i := 0; i < 3; i++ {
		if id, err := h.resolveClusterID(clusterIDTimeoutMs); err != nil || id != "cluster-1" {
			t.Fatalf("Resolution %d: expected cluster-1, got %q, %v", i, id, err)
		}
	}
	if n := stub.lookups.Load(); n != 3 {
		t.Errorf("Expected each sequential resolution to start its own lookup, got %d", n)
	}
}

// TestResolveClusterIDAnswersFromTheCache verifies that, once librdkafka has
// the cluster ID, a resolution returns it without starting a lookup.
func TestResolveClusterIDAnswersFromTheCache(t *testing.T) {
	stub := withStubClusterIDLookup(t, "unused", nil)
	stub.cached.Store("cluster-1")

	id, err := (&handle{}).resolveClusterID(clusterIDTimeoutMs)
	if err != nil || id != "cluster-1" {
		t.Fatalf("Expected cluster-1 from the cache, got %q, %v", id, err)
	}
	if n := stub.lookups.Load(); n != 0 {
		t.Errorf("Expected no lookup once the cluster ID is cached, got %d", n)
	}
}

// TestClusterIDResolversShareOneLookup verifies that the resolvers the
// SerializingProducer hands its two serializers go through the shared lookup
// of the producer's handle, and fail once the producer is closed.
func TestClusterIDResolversShareOneLookup(t *testing.T) {
	keySerializer := &mockSerializer{}
	valueSerializer := &mockSerializer{}
	p := newTestSerializingProducer[string, string](t,
		&mockSerializerBuilder{serializer: keySerializer},
		&mockSerializerBuilder{serializer: valueSerializer})

	stub := withStubClusterIDLookup(t, "cluster-1", nil)
	var wg sync.WaitGroup
	results := make([]error, 2)
	for i, resolve := range []func() (string, error){keySerializer.clusterIDResolve, valueSerializer.clusterIDResolve} {
		wg.Add(1)
		go func(i int, resolve func() (string, error)) {
			defer wg.Done()
			_, results[i] = resolve()
		}(i, resolve)
	}
	<-stub.entered
	time.Sleep(100 * time.Millisecond)
	close(stub.release)
	wg.Wait()

	if n := stub.lookups.Load(); n != 1 {
		t.Errorf("Expected the two serializers to share a single lookup, got %d", n)
	}
	for i, err := range results {
		if err != nil {
			t.Errorf("Resolver %d failed: %v", i, err)
		}
	}

	p.Close()
	var kafkaErr Error
	if _, err := keySerializer.clusterIDResolve(); !errors.As(err, &kafkaErr) || kafkaErr.Code() != ErrState {
		t.Errorf("Expected ErrState from the resolver once the producer is closed, got %v", err)
	}
}
