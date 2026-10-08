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
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeBroker is a host name that the system resolver cannot resolve.
const fakeBroker = "broker.invalid:9092"

// resolveTestCluster starts a mock cluster with a topic and returns the
// numeric address of its bootstrap server.
func resolveTestCluster(t *testing.T, topic string) (*MockCluster, string) {
	t.Helper()
	mc, err := NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %s", err)
	}
	t.Cleanup(mc.Close)
	if err := mc.CreateTopic(topic, 1, 1); err != nil {
		t.Fatalf("Failed to create topic: %s", err)
	}

	host, port, err := net.SplitHostPort(mc.BootstrapServers())
	if err != nil {
		t.Fatalf("Unexpected bootstrap servers %q: %s", mc.BootstrapServers(), err)
	}
	if net.ParseIP(host) == nil {
		host = "127.0.0.1"
	}
	return mc, net.JoinHostPort(host, port)
}

// produceConsume produces n messages through conf and consumes them back,
// returning the resolvers of the producer and consumer once both are closed.
func produceConsume(t *testing.T, conf ConfigMap, topic string, n int) (*resolver, *resolver) {
	t.Helper()

	p, err := NewProducer(&conf)
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	deliveryChan := make(chan Event, n)
	for i := 0; i < n; i++ {
		err = p.Produce(&Message{
			TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
			Value:          []byte(fmt.Sprintf("value-%d", i)),
		}, deliveryChan)
		if err != nil {
			t.Fatalf("Failed to produce: %s", err)
		}
	}
	for i := 0; i < n; i++ {
		select {
		case ev := <-deliveryChan:
			if m := ev.(*Message); m.TopicPartition.Error != nil {
				t.Fatalf("Delivery failed: %s", m.TopicPartition.Error)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("%d messages not delivered", n-i)
		}
	}
	pResolver := p.handle.resolver
	p.Close()

	cConf := conf.clone()
	cConf["group.id"] = "resolve-test"
	cConf["auto.offset.reset"] = "earliest"
	c, err := NewConsumer(&cConf)
	if err != nil {
		t.Fatalf("Failed to create consumer: %s", err)
	}
	if err := c.Subscribe(topic, nil); err != nil {
		t.Fatalf("Failed to subscribe: %s", err)
	}
	received := 0
	deadline := time.Now().Add(30 * time.Second)
	for received < n && time.Now().Before(deadline) {
		m, err := c.ReadMessage(time.Second)
		if err != nil {
			if err.(Error).IsTimeout() {
				continue
			}
			t.Fatalf("Failed to consume: %s", err)
		}
		if *m.TopicPartition.Topic == topic {
			received++
		}
	}
	if received != n {
		t.Fatalf("Consumed %d messages, expected %d", received, n)
	}
	cResolver := c.handle.resolver
	if err := c.Close(); err != nil {
		t.Fatalf("Failed to close consumer: %s", err)
	}
	return pResolver, cResolver
}

// checkFreed checks that librdkafka looked up addresses through the resolver
// and freed every one it got.
func checkFreed(t *testing.T, name string, r *resolver) {
	t.Helper()
	if r == nil {
		t.Fatalf("%s: no resolver", name)
	}
	if r.c != nil {
		t.Fatalf("%s: resolver not destroyed by Close", name)
	}
	if r.finalCalls == 0 || r.finalResults == 0 {
		t.Fatalf("%s: resolver not used: %d calls, %d results", name, r.finalCalls, r.finalResults)
	}
	if r.finalFrees != r.finalResults {
		t.Fatalf("%s: %d results but %d freed", name, r.finalResults, r.finalFrees)
	}
}

// TestResolveMap resolves a fake broker with go.resolve.map, falling through
// to the system resolver for the broker's advertised address.
func TestResolveMap(t *testing.T) {
	topic := "resolve-map"
	_, addr := resolveTestCluster(t, topic)
	handles := liveResolveHandles.Load()

	conf := ConfigMap{
		"bootstrap.servers": fakeBroker,
		// Host names are matched regardless of case.
		"go.resolve.map": map[string]string{strings.ToUpper(fakeBroker): addr},
	}
	pResolver, cResolver := produceConsume(t, conf, topic, 100)
	checkFreed(t, "producer", pResolver)
	checkFreed(t, "consumer", cResolver)

	if got := liveResolveHandles.Load(); got != handles {
		t.Fatalf("go.resolve.map holds %d cgo handles, expected none", got-handles)
	}
}

// TestResolveMapUnresolved checks that without the mapping the fake broker
// is unresolvable, so that TestResolveMap does test the mapping.
func TestResolveMapUnresolved(t *testing.T) {
	_, addr := resolveTestCluster(t, "resolve-unmapped")

	conf := ConfigMap{
		"bootstrap.servers": fakeBroker,
		"go.resolve.map":    map[string]string{"other.invalid:9092": addr},
	}
	expectResolveError(t, conf)
}

// TestResolveCallback resolves every broker with go.resolve.cb.
func TestResolveCallback(t *testing.T) {
	topic := "resolve-cb"
	mc, addr := resolveTestCluster(t, topic)
	handles := liveResolveHandles.Load()
	addrHost, addrPort, _ := net.SplitHostPort(addr)

	var lock sync.Mutex
	lookups := map[string]int{}
	cb := func(host, port string) (string, string, error) {
		lock.Lock()
		lookups[net.JoinHostPort(host, port)]++
		lock.Unlock()
		if net.JoinHostPort(host, port) == fakeBroker {
			return addrHost, addrPort, nil
		}
		// The broker's advertised address.
		return "", "", nil
	}

	conf := ConfigMap{
		"bootstrap.servers": fakeBroker,
		"go.resolve.cb":     cb,
	}
	pResolver, cResolver := produceConsume(t, conf, topic, 100)
	checkFreed(t, "producer", pResolver)
	checkFreed(t, "consumer", cResolver)

	lock.Lock()
	defer lock.Unlock()
	if lookups[fakeBroker] < 2 {
		t.Fatalf("Expected the producer and consumer to look %s up, got lookups %v", fakeBroker, lookups)
	}
	if len(lookups) < 2 {
		t.Fatalf("Expected the advertised broker %s to be looked up, got lookups %v", mc.BootstrapServers(), lookups)
	}
	if got := liveResolveHandles.Load(); got != handles {
		t.Fatalf("%d cgo handles leaked", got-handles)
	}
}

// TestResolveCallbackAdminClient resolves an AdminClient's brokers with
// go.resolve.cb, of type ResolveCallback.
func TestResolveCallbackAdminClient(t *testing.T) {
	_, addr := resolveTestCluster(t, "resolve-admin")
	handles := liveResolveHandles.Load()
	addrHost, addrPort, _ := net.SplitHostPort(addr)

	a, err := NewAdminClient(&ConfigMap{
		"bootstrap.servers": fakeBroker,
		"go.resolve.cb": ResolveCallback(func(host, port string) (string, string, error) {
			return addrHost, addrPort, nil
		}),
	})
	if err != nil {
		t.Fatalf("Failed to create admin client: %s", err)
	}
	md, err := a.GetMetadata(nil, true, 10000)
	if err != nil {
		t.Fatalf("Failed to get metadata: %s", err)
	}
	if len(md.Brokers) != 1 {
		t.Fatalf("Expected 1 broker, got %v", md.Brokers)
	}
	r := a.handle.resolver
	a.Close()
	checkFreed(t, "admin client", r)
	if got := liveResolveHandles.Load(); got != handles {
		t.Fatalf("%d cgo handles leaked", got-handles)
	}
}

// expectResolveError expects a producer created with conf to report that it
// failed to resolve fakeBroker.
func expectResolveError(t *testing.T, conf ConfigMap) {
	t.Helper()
	p, err := NewProducer(&conf)
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev := <-p.Events():
			if err, ok := ev.(Error); ok {
				if err.Code() == ErrResolve && strings.Contains(err.Error(), "Failed to resolve '"+fakeBroker+"'") {
					return
				}
				t.Logf("Ignoring error %v (%s)", err.Code(), err)
			}
		case <-deadline:
			t.Fatalf("No failure to resolve %s reported", fakeBroker)
		}
	}
}

// TestResolveCallbackError checks that errors returned by go.resolve.cb are
// reported as failures to resolve the broker.
func TestResolveCallbackError(t *testing.T) {
	for _, cbErr := range []error{
		errors.New("no route to broker"),
		&net.DNSError{Err: "no such host", Name: "broker.invalid", IsNotFound: true},
		&net.DNSError{Err: "timeout", Name: "broker.invalid", IsTimeout: true},
	} {
		t.Run(cbErr.Error(), func(t *testing.T) {
			handles := liveResolveHandles.Load()
			expectResolveError(t, ConfigMap{
				"bootstrap.servers": fakeBroker,
				"go.resolve.cb": func(host, port string) (string, string, error) {
					return "", "", cbErr
				},
			})
			if got := liveResolveHandles.Load(); got != handles {
				t.Fatalf("%d cgo handles leaked", got-handles)
			}
		})
	}
}

// TestResolveCallbackPanic checks that a panic in go.resolve.cb is recovered
// and reported as a failure to resolve the broker, and that the client
// connects once the callback stops panicking.
func TestResolveCallbackPanic(t *testing.T) {
	t.Run("always", func(t *testing.T) {
		handles := liveResolveHandles.Load()
		expectResolveError(t, ConfigMap{
			"bootstrap.servers": fakeBroker,
			"go.resolve.cb": func(host, port string) (string, string, error) {
				panic("resolver bug")
			},
		})
		if got := liveResolveHandles.Load(); got != handles {
			t.Fatalf("%d cgo handles leaked", got-handles)
		}
	})

	t.Run("once", func(t *testing.T) {
		topic := "resolve-cb-panic"
		_, addr := resolveTestCluster(t, topic)
		handles := liveResolveHandles.Load()
		addrHost, addrPort, _ := net.SplitHostPort(addr)

		var panics atomic.Int64
		var panicked atomic.Bool
		cb := func(host, port string) (string, string, error) {
			if net.JoinHostPort(host, port) != fakeBroker {
				return "", "", nil
			}
			// The producer's first lookup.
			if panicked.CompareAndSwap(false, true) {
				panics.Add(1)
				panic("resolver bug")
			}
			return addrHost, addrPort, nil
		}

		pResolver, _ := produceConsume(t, ConfigMap{
			"bootstrap.servers": fakeBroker,
			"go.resolve.cb":     cb,
		}, topic, 10)
		checkFreed(t, "producer", pResolver)
		if panics.Load() != 1 {
			t.Fatalf("Expected the callback to panic once, got %d panics", panics.Load())
		}
		if pResolver.finalCalls < 2 {
			t.Fatalf("Expected the producer to look %s up again after the panic, got %d lookups",
				fakeBroker, pResolver.finalCalls)
		}
		if got := liveResolveHandles.Load(); got != handles {
			t.Fatalf("%d cgo handles leaked", got-handles)
		}
	})
}

// TestResolveCallbackNotNumeric checks that go.resolve.cb must return a
// numeric address, which is not resolved any further.
func TestResolveCallbackNotNumeric(t *testing.T) {
	expectResolveError(t, ConfigMap{
		"bootstrap.servers": fakeBroker,
		"go.resolve.cb": func(host, port string) (string, string, error) {
			return "localhost", port, nil
		},
	})
}

// TestResolveConfigInvalid checks that invalid go.resolve.* properties fail
// creating the client, without leaking cgo handles.
func TestResolveConfigInvalid(t *testing.T) {
	handles := liveResolveHandles.Load()
	cb := func(host, port string) (string, string, error) { return "", "", nil }

	for name, conf := range map[string]ConfigMap{
		"cb type":            {"go.resolve.cb": "not a func"},
		"map type":           {"go.resolve.map": map[string]interface{}{fakeBroker: "127.0.0.1:9092"}},
		"map key no port":    {"go.resolve.map": map[string]string{"broker.invalid": "127.0.0.1:9092"}},
		"map key no host":    {"go.resolve.map": map[string]string{":9092": "127.0.0.1:9092"}},
		"map value name":     {"go.resolve.map": map[string]string{fakeBroker: "localhost:9092"}},
		"map value no port":  {"go.resolve.map": map[string]string{fakeBroker: "127.0.0.1"}},
		"map value bad port": {"go.resolve.map": map[string]string{fakeBroker: "127.0.0.1:99999"}},
		// Fails in librdkafka, once the resolver is created.
		"librdkafka property": {"go.resolve.cb": cb, "no.such.property": true},
	} {
		t.Run(name, func(t *testing.T) {
			conf["bootstrap.servers"] = fakeBroker
			if _, err := NewProducer(&conf); err == nil || err.(Error).Code() != ErrInvalidArg {
				t.Fatalf("Expected ErrInvalidArg creating producer, got %v", err)
			}
			conf["group.id"] = "resolve-test"
			if _, err := NewConsumer(&conf); err == nil || err.(Error).Code() != ErrInvalidArg {
				t.Fatalf("Expected ErrInvalidArg creating consumer, got %v", err)
			}
			delete(conf, "group.id")
			if _, err := NewAdminClient(&conf); err == nil || err.(Error).Code() != ErrInvalidArg {
				t.Fatalf("Expected ErrInvalidArg creating admin client, got %v", err)
			}
		})
	}

	if got := liveResolveHandles.Load(); got != handles {
		t.Fatalf("%d cgo handles leaked", got-handles)
	}
}

// TestResolveCallbackConcurrentClients creates and closes clients sharing a
// callback concurrently, checking that every cgo handle is released.
func TestResolveCallbackConcurrentClients(t *testing.T) {
	_, addr := resolveTestCluster(t, "resolve-concurrent")
	handles := liveResolveHandles.Load()
	addrHost, addrPort, _ := net.SplitHostPort(addr)
	cb := func(host, port string) (string, string, error) {
		return addrHost, addrPort, nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := NewProducer(&ConfigMap{"bootstrap.servers": fakeBroker, "go.resolve.cb": cb})
			if err != nil {
				t.Errorf("Failed to create producer: %s", err)
				return
			}
			if _, err := p.GetMetadata(nil, false, 10000); err != nil {
				t.Errorf("Failed to get metadata: %s", err)
			}
			p.Close()
		}()
	}
	wg.Wait()

	if got := liveResolveHandles.Load(); got != handles {
		t.Fatalf("%d cgo handles leaked", got-handles)
	}
}
