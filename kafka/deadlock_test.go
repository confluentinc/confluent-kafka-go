package kafka

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The tests in this file are regression tests for the deadlock / hot-spin
// issues in the pollLock (handle.pollLock) locking scheme introduced to fix
// the use-after-free race between Close() and in-flight C calls (see
// race_test.go).
//
// They are all written so that the *bug* manifests as the scenario failing to
// complete within a deadline. Against the buggy code they run into the
// deadline and fail; once the locking scheme is fixed they should complete
// well within it.
//
// Common failure mode being probed: handle.pollLock is a sync.RWMutex whose
// read lock is not reentrant. eventPoll takes RLock for the whole body,
// including the rebalance dispatch, and several entry points reachable from
// inside that dispatch (Assign/Commit/GetRebalanceProtocol) take rlock()
// again. A concurrent Close() takes the write lock, and because Go's RWMutex
// gives writers priority, the pending writer blocks the nested RLock -> both
// goroutines wedge forever.

// runWithDeadline runs fn in a goroutine and fails the test if it does not
// return within d. It returns true if fn completed in time. This is how we
// turn a deadlock into a deterministic test failure instead of hanging the
// whole test binary until the go test timeout.
func runWithDeadline(t *testing.T, d time.Duration, what string, fn func()) bool {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	select {
	case <-done:
		return true
	case <-time.After(d):
		t.Errorf("%s did not complete within %s (likely deadlock)", what, d)
		return false
	}
}

// newMockConsumer creates a mock cluster with a single-partition topic and a
// consumer subscribed to it, plus a producer that has written one message so a
// rebalance (partition assignment) actually happens. The caller is
// responsible for closing the returned consumer; the cluster and producer are
// cleaned up via t.Cleanup.
func newMockConsumer(t *testing.T, rebalanceCb RebalanceCb) (*MockCluster, *Consumer) {
	t.Helper()

	cluster, err := NewMockCluster(1)
	if err != nil {
		t.Fatalf("create mock cluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	const topic = "test"
	if err := cluster.CreateTopic(topic, 1, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer, err := NewProducer(&ConfigMap{
		"bootstrap.servers": cluster.BootstrapServers(),
	})
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	t.Cleanup(producer.Close)

	value := []byte("value")
	tp := topic
	if err := producer.Produce(&Message{
		TopicPartition: TopicPartition{Topic: &tp, Partition: PartitionAny},
		Value:          value,
	}, nil); err != nil {
		t.Fatalf("produce: %v", err)
	}
	producer.Flush(5000)

	consumer, err := NewConsumer(&ConfigMap{
		"bootstrap.servers": cluster.BootstrapServers(),
		"group.id":          "test",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	if err := consumer.SubscribeTopics([]string{topic}, rebalanceCb); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	return cluster, consumer
}

// pollUntilAssigned polls the consumer until it has been assigned partitions
// (i.e. a rebalance has completed) or the deadline passes.
func pollUntilAssigned(t *testing.T, c *Consumer) {
	t.Helper()
	until := time.Now().Add(30 * time.Second)
	for time.Now().Before(until) {
		if assignment, err := c.Assignment(); err == nil && len(assignment) > 0 {
			return
		}
		c.Poll(100)
	}
	t.Fatal("consumer was never assigned partitions")
}

// TestRebalanceCallbackConcurrentCloseNoUseAfterFree specifically exercises
// the callback window where handleRebalanceEvent releases the pollLock read
// lock. While that lock is released, a concurrent Close() can destroy the
// librdkafka handle. The fix extracts everything needed from the rebalance
// event (and destroys the event) before releasing the lock, and rechecks for a
// closed handle after reacquiring, so no handle-owned C memory is touched once
// the callback returns.
//
// The callback blocks until Close() has fully returned (handle destroyed),
// then returns; handleRebalanceEvent must complete without touching freed
// memory. Run under -race to catch regressions.
func TestRebalanceCallbackConcurrentCloseNoUseAfterFree(t *testing.T) {
	var consumer *Consumer

	cbEntered := make(chan struct{})
	closeReturned := make(chan struct{})

	first := true
	cb := func(c *Consumer, ev Event) error {
		if _, ok := ev.(AssignedPartitions); ok && first {
			first = false
			close(cbEntered)
			// Block until Close() has fully torn down the handle, so the
			// post-callback code path in handleRebalanceEvent runs against a
			// destroyed handle.
			<-closeReturned
		}
		return nil
	}

	_, consumer = newMockConsumer(t, cb)

	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		until := time.Now().Add(10 * time.Second)
		for time.Now().Before(until) && !consumer.IsClosed() {
			consumer.Poll(100)
		}
	}()

	// Wait until the poll goroutine is inside the rebalance callback (pollLock
	// released), then close from this goroutine.
	select {
	case <-cbEntered:
	case <-time.After(30 * time.Second):
		t.Fatal("rebalance callback never fired")
	}

	_ = consumer.Close()
	close(closeReturned)

	<-pollDone
}

// TestDeadlockRebalanceCallbackReentrantAssign covers a reentrant-lock
// deadlock in the rebalance path.
//
// eventPoll holds pollLock.RLock() across the rebalance dispatch. The
// application rebalance callback calls Assign(), which takes rlock() again
// (nested, non-reentrant RLock). If a concurrent Close() is waiting on the
// write lock at that moment, the nested RLock blocks behind the pending
// writer and both goroutines deadlock.
func TestDeadlockRebalanceCallbackReentrantAssign(t *testing.T) {
	var closeStarted sync.WaitGroup
	closeStarted.Add(1)

	cb := func(c *Consumer, ev Event) error {
		switch ev.(type) {
		case AssignedPartitions:
			// Ask Close() to start racing for the write lock, then give it a
			// moment to actually reach pollLock.Lock() while we (the poller)
			// still hold the read lock.
			closeStarted.Done()
			time.Sleep(200 * time.Millisecond)
			// Re-entrant rlock() while a writer is (very likely) pending.
			// Either the Assign succeeds, or Close() has already won the race
			// and the handle is closed -- both are fine. What must NOT happen
			// is a deadlock, which is what this test guards against.
			_ = c.Assign(ev.(AssignedPartitions).Partitions)
		}
		return nil
	}

	_, consumer := newMockConsumer(t, cb)

	// Drive the poll loop that dispatches the rebalance from a background
	// goroutine.
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		until := time.Now().Add(10 * time.Second)
		for time.Now().Before(until) && !consumer.IsClosed() {
			consumer.Poll(100)
		}
	}()

	// Once the callback has fired, close concurrently. Close() must not wedge.
	closeStarted.Wait()
	if runWithDeadline(t, 5*time.Second, "Consumer.Close during reentrant Assign", func() {
		_ = consumer.Close()
	}) {
		<-pollDone
	}
}

// TestDeadlockRebalanceGetRebalanceProtocol covers a reentrant-lock deadlock
// via GetRebalanceProtocol(). handleRebalanceEvent calls GetRebalanceProtocol()
// whenever the application did not reassign.
// GetRebalanceProtocol() takes rlock() while eventPoll already holds the read
// lock, so a concurrent Close() waiting on the write lock deadlocks the poll.
//
// We use a rebalance callback that intentionally does NOT reassign (returns
// without calling *Assign), so the internal path that invokes
// GetRebalanceProtocol() runs. The callback signals Close() to start and
// sleeps, widening the window so Close()'s pending write lock is guaranteed to
// be waiting by the time the nested GetRebalanceProtocol() RLock is attempted.
func TestDeadlockRebalanceGetRebalanceProtocol(t *testing.T) {
	var closeStarted sync.WaitGroup
	closeStarted.Add(1)

	first := true
	cb := func(c *Consumer, ev Event) error {
		if _, ok := ev.(AssignedPartitions); ok && first {
			first = false
			// Let Close() reach pollLock.Lock() while we (the poller) still
			// hold the outer read lock. We deliberately do NOT reassign, so
			// handleRebalanceEvent falls through to GetRebalanceProtocol().
			closeStarted.Done()
			time.Sleep(200 * time.Millisecond)
		}
		return nil
	}

	_, consumer := newMockConsumer(t, cb)

	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		until := time.Now().Add(10 * time.Second)
		for time.Now().Before(until) && !consumer.IsClosed() {
			consumer.Poll(100)
		}
	}()

	closeStarted.Wait()
	if runWithDeadline(t, 5*time.Second, "Consumer.Close racing GetRebalanceProtocol", func() {
		_ = consumer.Close()
	}) {
		<-pollDone
	}
}

// TestDeadlockRebalanceNilEventsChannel covers a nil-channel send deadlock.
// When the events channel is not enabled, c.events is nil.
// If an assign/unassign inside handleRebalanceEvent returns an error, it does
// `c.events <- ...` -- a send on a nil channel that blocks forever while
// holding the read lock, wedging any concurrent Close().
//
// The error-forwarding branch only runs when the internal
// rd_kafka_(incremental_)assign call actually fails, which a healthy
// mock cluster does not reliably produce -- so this test cannot force the hang
// deterministically. It instead pins down the surrounding safety property:
// with the events channel disabled (c.events == nil), a Close() concurrent
// with an in-flight revoke rebalance must still complete. If the nil-channel
// send is ever hit it will manifest here as the deadline firing.
func TestDeadlockRebalanceNilEventsChannel(t *testing.T) {
	_, consumer := newMockConsumer(t, nil)

	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for !consumer.IsClosed() {
			consumer.Poll(50)
		}
	}()

	pollUntilAssigned(t, consumer)

	// Closing triggers a revoke rebalance which is dispatched under the read
	// lock; if that dispatch ever needs to forward an error to the nil
	// c.events channel it hangs. Even absent an error, Close() must complete.
	if runWithDeadline(t, 5*time.Second, "Consumer.Close with nil events channel", func() {
		_ = consumer.Close()
	}) {
		<-pollDone
	}
}

// TestCommitReleasesLockWhenClosing covers the unbounded-wait problem where a
// blocking Commit() holds the handle read lock across an unbounded queue poll,
// pinning a concurrent Close() that needs the write lock.
//
// commit() polls its result queue in bounded slices and bails out once the
// consumer is tearing down (tearingDown set), so it releases the read lock
// promptly instead of blocking on the broker round-trip. We make the broker
// slow so the commit would otherwise block for a long time, mark the consumer
// as tearing down, and assert the in-flight Commit() returns quickly.
func TestCommitReleasesLockWhenClosing(t *testing.T) {
	cluster, consumer := newMockConsumer(t, nil)
	t.Cleanup(func() { _ = consumer.Close() })

	pollUntilAssigned(t, consumer)

	// Make the broker very slow so an in-flight commit stays blocked waiting
	// for its result, holding the read lock, unless it bails out on close.
	if err := cluster.SetRoundtripDuration(1, 30*time.Second); err != nil {
		t.Fatalf("set rtt: %v", err)
	}

	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		_, _ = consumer.Commit()
	}()

	// Let the commit enter its blocking result poll under the read lock.
	time.Sleep(500 * time.Millisecond)

	// Simulate Close() reaching the point where it marks the consumer as
	// tearing down (the flag Close sets right before acquiring the write
	// lock). The blocking commit must observe it and release the read lock
	// promptly.
	atomic.StoreUint32(&consumer.tearingDown, 1)

	select {
	case <-commitDone:
	case <-time.After(5 * time.Second):
		t.Error("in-flight Commit did not release the handle lock within 5s after close began")
	}

	// Allow the deferred Close() to run its normal path.
	atomic.StoreUint32(&consumer.tearingDown, 0)
}

// TestFlushHotSpinAfterClose covers a hot-spin in Flush() after Close().
// After the producer is closed, rlock() fails so the flush goroutine skips the
// C flush, eventPoll returns instantly, but Len() still counts buffered
// entries in produceChannel/events before its own rlock() check. If those
// buffers are non-empty, Flush() never sees Len() drop to zero and spins hot
// until the full timeout elapses.
//
// We enqueue buffered produce-channel entries, close the producer, then call
// Flush with a long timeout and assert it returns quickly instead of burning
// the whole timeout.
func TestFlushHotSpinAfterClose(t *testing.T) {
	cluster, err := NewMockCluster(1)
	if err != nil {
		t.Fatalf("create mock cluster: %v", err)
	}
	defer cluster.Close()

	producer, err := NewProducer(&ConfigMap{
		"bootstrap.servers": cluster.BootstrapServers(),
	})
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}

	// Leave some entries buffered on the produce channel so Len() stays > 0
	// after the handle is torn down.
	topic := "test"
	for range 100 {
		producer.ProduceChannel() <- &Message{
			TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
			Value:          []byte("value"),
		}
	}

	producer.Close()

	// Flush with a long timeout. Correct behaviour: it cannot make progress on
	// a closed handle and should give up quickly (or the buffered-count path
	// should not be able to spin). Buggy behaviour: hot-spins ~30s.
	runWithDeadline(t, 5*time.Second, "Producer.Flush after Close", func() {
		producer.Flush(30000)
	})
}
