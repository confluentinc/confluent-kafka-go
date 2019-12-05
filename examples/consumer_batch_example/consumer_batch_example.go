package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topic := os.Args[3]
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

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
		Offset:    kafka.OffsetStored,
	}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to assign topic: %s\n", err)
		os.Exit(1)
	}

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			messages, err := c.ConsumeBatch(5, time.Second)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to consume messages: %s\n", err)
				os.Exit(1)
			}

			fmt.Printf("Consumed %d messages\n", len(messages))
			for i, m := range messages {
				fmt.Printf("Message %d: %s\n", i, m.Value)
			}

			if len(messages) > 0 {
				fmt.Printf("Committing offsets\n")
				_, err = c.CommitOffsets([]kafka.TopicPartition{messages[len(messages)-1].TopicPartition})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to commit offsets: %s\n", err)
					os.Exit(1)
				}
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
