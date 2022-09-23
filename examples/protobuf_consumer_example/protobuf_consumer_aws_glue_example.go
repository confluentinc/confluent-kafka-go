package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/subhra/confluent-kafka-go/kafka"
	"github.com/subhra/confluent-kafka-go/schemaregistry"
	"github.com/subhra/confluent-kafka-go/schemaregistry/serde"
	"github.com/subhra/confluent-kafka-go/schemaregistry/serde/protobuf"
)

func main() {

	if len(os.Args) < 6 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <schema-registry> <aws-region> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	registryName := os.Args[2]
	awsRegion := os.Args[3]
	group := os.Args[4]
	topics := os.Args[5:]

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           group,
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", consumer)

	client, err := schemaregistry.NewClient(schemaregistry.NewConfigForAwsGlue(registryName, awsRegion))
	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		os.Exit(1)
	}

	deser, err := protobuf.NewDeserializer(client, serde.ValueSerde, protobuf.NewDeserializerConfig())

	if err != nil {
		fmt.Printf("Failed to create deserializer: %s\n", err)
		os.Exit(1)
	}

	deser.SubjectNameStrategy = func(topic string, serdeType serde.Type, schema schemaregistry.SchemaInfo) (string, error) {
		return topic + ".proto", nil
	}
	deser.ProtoRegistry.RegisterMessage((&User{}).ProtoReflect().Type())

	err = consumer.SubscribeTopics(topics, nil)

	if err != nil {
		fmt.Printf("Failed to subcribe to topics: %s\n", err)
		os.Exit(1)
	}

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				value, err := deser.Deserialize(*e.TopicPartition.Topic, e.Value)
				if err != nil {
					fmt.Printf("Failed to deserialize payload: %s\n", err)
				} else {
					fmt.Printf("%% Message on %s:\n%+v\n", e.TopicPartition, value)
				}
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	consumer.Close()
}
