package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <batchSize> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	batchSize, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Fprintf(os.Stderr, "<batchSize> must be integer: %s\n", err)
		os.Exit(1)
	}
	topics := os.Args[4:]
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":   broker,
		"group.id":            group,
		"session.timeout.ms":  6000,
		"enable.auto.commit":  false,
		"queued.min.messages": min(batchSize*100, 10000000),
		"isolation.level":     "read_committed",
		"auto.offset.reset":   "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(topics, nil)

	run := true
	var controller int
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				if controller != 0 {
					doCommit(c)
					controller = 0
				}
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				controller++
				if controller == batchSize {
					doCommit(c)
					controller = 0
				}
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
	fmt.Printf("Closing consumer\n")
	c.Close()
}
func doCommit(consumer *kafka.Consumer) {
	info, err := consumer.Commit()
	fmt.Printf("Commited Topic %s offset %s \n", *info[len(info)-1].Topic, info[len(info)-1].Offset.String())
	if err != nil {
		fmt.Printf("Error %s", err)
		panic("Error to commit message ")
	}
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
