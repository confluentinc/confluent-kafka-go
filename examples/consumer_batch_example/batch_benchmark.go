package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"time"
)

const limit = 1000000

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topic := os.Args[3]

	// XXX: Must turn off auto commits for batch consume to work
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        broker,
		"broker.address.family":    "v4",
		"group.id":                 group,
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       "false",
		"enable.auto.offset.store": "false",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	// XXX: Subscribe does not work, must use Assign for batch consume
	err = c.Assign([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: 0,
		Offset:    0,
	}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to assign topic: %s\n", err)
		os.Exit(1)
	}

	consumed := 0
	for consumed < limit {
		messages, err := c.ConsumeBatch(1000, 100 * time.Millisecond)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to consume messages: %s\n", err)
			os.Exit(1)
		}
		consumed += len(messages)
	}

	fmt.Printf("Consumed %d messages\n", consumed)
	fmt.Printf("Closing consumer\n")
	c.Close()
}
