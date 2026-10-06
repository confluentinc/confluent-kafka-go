package kafka

import (
	"testing"
	"time"
)

// TestRaceClose is a regression test for a use-after-free crash (SIGSEGV in
// cgo) that occurred when a Consumer was closed while another goroutine was
// still calling ReadMessage/Poll on it.
//
// The bug:
//
// ReadMessage -> Poll -> handle.eventPoll blocks inside the C call
// rd_kafka_queue_poll(h.rkq, ...) for up to the poll timeout. Meanwhile
// Consumer.Close() would set isClosed and then immediately call
// rd_kafka_queue_destroy(rkq) and rd_kafka_destroy(rk), freeing the C queue
// and handle. The isClosed / verifyClient() guard is a check-then-use
// race: the polling goroutine passes the closed check while isClosed == 0,
// reads the still-valid h.rkq pointer, enters the blocking C poll, and only
// then does Close() free the underlying memory. The poll goroutine is now
// operating on freed memory, corrupting the heap and crashing (often one
// iteration later, e.g. inside the next rd_kafka_new).
//
// librdkafka is thread-safe for concurrent operations on a *live* handle, but
// rd_kafka_destroy() cannot be made safe against a concurrent in-flight call:
// once the memory is freed, any thread still dereferencing the handle is
// undefined behaviour. Establishing that no in-flight call remains
// is the caller's responsibility, and an atomic closed-flag alone cannot
// provide it.
//
// The fix:
//
// handle gained a pollLock sync.RWMutex. Every entry point that touches the C
// handle/queue takes pollLock.RLock() for the duration of its C call (see
// eventPoll and the rlock()/runlock() helpers used across consumer.go,
// producer.go and adminapi.go), and Close() takes pollLock.Lock() before
// destroying the queue/handle and nils out rkq/rk. The write lock cannot be
// acquired until all in-flight readers have released, so Close() drains any
// running Poll/ReadMessage and blocks new ones before freeing anything --
// closing the race regardless of goroutine scheduling.
//
// This test spawns a background reader for each of many short-lived consumers
// and closes each one out from under the reader; before the fix it reliably
// crashed within these iterations.
func TestRaceClose(t *testing.T) {
	// create a new mock cluster
	cluster, err := NewMockCluster(1)
	if err != nil {
		t.Fatalf("create mock cluster: %v", err)
	}

	defer cluster.Close()

	_ = cluster.CreateTopic("test", 16, 1)

	for idx := range 1024 {
		t.Logf("Iteration %d", idx)

		consumer, err := NewConsumer(&ConfigMap{
			"group.id":          "test",
			"bootstrap.servers": cluster.BootstrapServers(),
			"auto.offset.reset": "earliest",
		})
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			if err := consumer.Subscribe("test", nil); err != nil {
				t.Error(err)
			}

			for !consumer.IsClosed() {
				_, _ = consumer.ReadMessage(50 * time.Millisecond)
			}
		}()

		time.Sleep(50 * time.Millisecond)

		_ = consumer.Close()
	}
}
