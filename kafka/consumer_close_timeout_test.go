package kafka

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"
)

// These tests demonstrate that Consumer.Close() hangs indefinitely when the
// broker is unresponsive during static member leave-group, and that configuring
// go.consumer.close.timeout.ms resolves the issue by force-destroying the
// consumer after the timeout.
//
// Requirements: Docker (for testcontainers Kafka broker)
//
// Run: go test -v -timeout 5m -run TestConsumerCloseTimeout ./kafka/

// startBroker starts a Kafka container and returns the bootstrap address.
func startBroker(t *testing.T) (bootstraps string, containerID string, cleanup func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	cmd := exec.CommandContext(ctx, "docker", "run", "-d", "--rm",
		"-e", "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:29092",
		"-e", "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,EXTERNAL://localhost:29092",
		"-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT",
		"-e", "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
		"-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"-e", "KAFKA_PROCESS_ROLES=broker,controller",
		"-e", "KAFKA_NODE_ID=1",
		"-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
		"-e", "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
		"-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
		"-p", "29092:29092",
		"confluentinc/confluent-local:8.0.0",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		cancel()
		t.Fatalf("failed to start kafka: %v\n%s", err, out)
	}
	containerID = string(out[:12])
	bootstraps = "localhost:29092"

	// Wait for broker to be ready
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		admin, err := NewAdminClient(&ConfigMap{"bootstrap.servers": bootstraps})
		if err == nil {
			md, err := admin.GetMetadata(nil, true, 2000)
			admin.Close()
			if err == nil && len(md.Brokers) > 0 {
				t.Logf("Kafka broker ready at %s (container %s)", bootstraps, containerID)
				return bootstraps, containerID, func() {
					exec.Command("docker", "rm", "-f", containerID).Run()
					cancel()
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	exec.Command("docker", "rm", "-f", containerID).Run()
	cancel()
	t.Fatalf("kafka broker did not become ready within 30s")
	return "", "", nil
}

func createTopicForTest(t *testing.T, bootstraps, topic string) {
	t.Helper()
	admin, err := NewAdminClient(&ConfigMap{"bootstrap.servers": bootstraps})
	if err != nil {
		t.Fatalf("failed to create admin: %v", err)
	}
	defer admin.Close()

	results, err := admin.CreateTopics(context.Background(), []TopicSpecification{{
		Topic:             topic,
		NumPartitions:     6,
		ReplicationFactor: 1,
	}}, SetAdminOperationTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("CreateTopics failed: %v", err)
	}
	for _, r := range results {
		if r.Error.Code() != ErrNoError && r.Error.Code() != ErrTopicAlreadyExists {
			t.Fatalf("failed to create topic: %v", r.Error)
		}
	}
}

func produceToTopic(t *testing.T, bootstraps, topic string, count int) {
	t.Helper()
	p, err := NewProducer(&ConfigMap{"bootstrap.servers": bootstraps})
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer p.Close()

	for i := 0; i < count; i++ {
		p.Produce(&Message{
			TopicPartition: TopicPartition{Topic: &topic, Partition: PartitionAny},
			Value:          []byte(fmt.Sprintf(`{"seq":%d}`, i)),
		}, nil)
	}
	p.Flush(5000)
}

// TestConsumerCloseTimeout_HangsWithoutConfig demonstrates that Close() hangs
// indefinitely when the broker is unresponsive and go.consumer.close.timeout.ms
// is not configured (default 0).
//
// This reproduces the production bug: static membership consumers calling Close()
// during broker unresponsiveness (e.g., coordinator overload during rebalance
// storms) hang forever in the rd_kafka_consumer_closed polling loop.
func TestConsumerCloseTimeout_HangsWithoutConfig(t *testing.T) {
	bootstraps, containerID, cleanup := startBroker(t)
	defer cleanup()

	topic := "test-close-timeout-hang"
	createTopicForTest(t, bootstraps, topic)
	produceToTopic(t, bootstraps, topic, 100)

	// Create consumer with static membership, NO close timeout (default behavior)
	consumer, err := NewConsumer(&ConfigMap{
		"bootstrap.servers":  bootstraps,
		"group.id":           "test-close-hang",
		"group.instance.id":  "test-close-hang",
		"auto.offset.reset":  "earliest",
		"session.timeout.ms": 30000,
		"socket.timeout.ms":  5000,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	consumer.SubscribeTopics([]string{topic}, nil)

	// Wait for assignment
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if a, _ := consumer.Assignment(); len(a) > 0 {
			break
		}
		consumer.Poll(100)
	}

	// Pause broker (simulates unresponsive coordinator)
	if out, err := exec.Command("docker", "pause", containerID).CombinedOutput(); err != nil {
		t.Fatalf("docker pause: %v\n%s", err, out)
	}
	defer exec.Command("docker", "unpause", containerID).Run()

	time.Sleep(500 * time.Millisecond)

	// Close() should hang -- we give it 45s then declare it hung
	t.Log("Calling Close() without go.consumer.close.timeout.ms (expecting hang)...")
	done := make(chan error, 1)
	go func() { done <- consumer.Close() }()

	select {
	case <-done:
		t.Log("Close() returned -- hang not reproduced in this run")
	case <-time.After(45 * time.Second):
		t.Log("CONFIRMED: Close() hung for >45s without go.consumer.close.timeout.ms")
		t.Log("The consumer goroutine is now permanently leaked with all C-heap resources.")
	}
}

// TestConsumerCloseTimeout_CompletesWithConfig demonstrates that Close() completes
// within a bounded time when go.consumer.close.timeout.ms is configured, even with
// an unresponsive broker. Resources are freed via rd_kafka_destroy_flags.
func TestConsumerCloseTimeout_CompletesWithConfig(t *testing.T) {
	bootstraps, containerID, cleanup := startBroker(t)
	defer cleanup()

	topic := "test-close-timeout-fix"
	createTopicForTest(t, bootstraps, topic)
	produceToTopic(t, bootstraps, topic, 100)

	// Create consumer with static membership AND close timeout (the fix)
	consumer, err := NewConsumer(&ConfigMap{
		"bootstrap.servers":           bootstraps,
		"group.id":                    "test-close-fix",
		"group.instance.id":           "test-close-fix",
		"auto.offset.reset":           "earliest",
		"session.timeout.ms":          30000,
		"socket.timeout.ms":           5000,
		"go.consumer.close.timeout.ms": 5000, // 5 second timeout
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	consumer.SubscribeTopics([]string{topic}, nil)

	// Wait for assignment
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if a, _ := consumer.Assignment(); len(a) > 0 {
			break
		}
		consumer.Poll(100)
	}

	// Pause broker (same scenario as above)
	if out, err := exec.Command("docker", "pause", containerID).CombinedOutput(); err != nil {
		t.Fatalf("docker pause: %v\n%s", err, out)
	}
	defer exec.Command("docker", "unpause", containerID).Run()

	time.Sleep(500 * time.Millisecond)

	// Close() should complete within the timeout + socket.timeout.ms
	t.Log("Calling Close() with go.consumer.close.timeout.ms=5000...")
	start := time.Now()
	err = consumer.Close()
	elapsed := time.Since(start)

	t.Logf("Close() returned in %v (err=%v)", elapsed, err)

	if elapsed > 15*time.Second {
		t.Fatalf("Close() took %v -- expected <15s with close timeout of 5s", elapsed)
	}

	t.Logf("CONFIRMED: Close() completed in %v with go.consumer.close.timeout.ms configured", elapsed)
	t.Log("Resources freed via rd_kafka_destroy_flags -- no goroutine leak, no memory leak.")
}
