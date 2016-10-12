/**
 * Copyright 2016 Confluent Inc.
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
package main

import (
	"bufio"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	verbosity     = 1
	exit_eof      = false
	eof_cnt       = 0
	partition_cnt = 0
	key_delim     = ""
	sigs          chan os.Signal
)

func run_producer(config *kafka.ConfigMap, topic string, partition int32) {
	p, err := kafka.NewProducer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Created Producer %v, topic %s [%d]\n", p, topic, partition)

	tp := kafka.TopicPartition{Topic: &topic, Partition: partition}

	go func(drs chan kafka.Event) {
		for ev := range drs {
			m, ok := ev.(*kafka.Message)
			if !ok {
				continue
			}
			if m.TopicPartition.Error != nil {
				fmt.Fprintf(os.Stderr, "%% Delivery error: %v\n", m.TopicPartition)
			} else if verbosity >= 2 {
				fmt.Fprintf(os.Stderr, "%% Delivered %v\n", m)
			}
		}
	}(p.Events)

	reader := bufio.NewReader(os.Stdin)
	stdin_chan := make(chan string)

	go func() {
		for true {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}

			line = strings.TrimSuffix(line, "\n")
			if len(line) == 0 {
				continue
			}

			stdin_chan <- line
		}
		close(stdin_chan)
	}()

	run := true

	for run == true {
		select {
		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			run = false

		case line, ok := <-stdin_chan:
			if !ok {
				run = false
				break
			}

			msg := kafka.Message{TopicPartition: tp}

			if key_delim != "" {
				vec := strings.SplitN(line, key_delim, 2)
				if len(vec[0]) > 0 {
					msg.Key = ([]byte)(vec[0])
				}
				if len(vec) == 2 && len(vec[1]) > 0 {
					msg.Value = ([]byte)(vec[1])
				}
			} else {
				msg.Value = ([]byte)(line)
			}

			p.ProduceChannel <- &msg
		}
	}

	fmt.Fprintf(os.Stderr, "%% Flushing %d message(s)\n", p.Len())
	p.Flush(10000)
	fmt.Fprintf(os.Stderr, "%% Closing\n")
	p.Close()
}

func run_consumer(config *kafka.ConfigMap, topics []string) {
	c, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "%% Created Consumer %v\n", c)

	c.SubscribeTopics(topics, nil)

	run := true

	for run == true {
		select {

		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			run = false

		case ev := <-c.Events:
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)
				partition_cnt = len(e.Partitions)
				eof_cnt = 0
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()
				partition_cnt = 0
				eof_cnt = 0
			case *kafka.Message:
				if verbosity >= 2 {
					fmt.Fprintf(os.Stderr, "%% %v:\n", e.TopicPartition)
				}
				if key_delim != "" {
					if e.Key != nil {
						fmt.Printf("%s%s", string(e.Key), key_delim)
					} else {
						fmt.Printf("%s", key_delim)
					}
				}
				fmt.Println(string(e.Value))
			case kafka.PartitionEof:
				fmt.Fprintf(os.Stderr, "%% Reached %v\n", e)
				eof_cnt += 1
				if exit_eof && eof_cnt >= partition_cnt {
					run = false
				}
			case kafka.KafkaError:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			case kafka.OffsetsCommitted:
				if verbosity >= 2 {
					fmt.Fprintf(os.Stderr, "%% %v\n", e)
				}
			default:
				fmt.Fprintf(os.Stderr, "%% Unhandled event %T ignored: %v\n", e, e)
			}
		}
	}

	fmt.Fprintf(os.Stderr, "%% Closing consumer\n")
	c.Close()
}

type ConfigArgs struct {
	conf kafka.ConfigMap
}

func (c *ConfigArgs) String() string {
	return "FIXME"
}

func (c *ConfigArgs) Set(value string) error {
	return c.conf.Set(value)
}

func (c *ConfigArgs) IsCumulative() bool {
	return true
}

func main() {
	sigs = make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	_, libver := kafka.LibraryVersion()
	kingpin.Version(fmt.Sprintf("confluent-kafka-go (librdkafka v%s)", libver))

	// Default config
	var confargs ConfigArgs
	confargs.conf = kafka.ConfigMap{"session.timeout.ms": 6000}

	/* General options */
	brokers := kingpin.Flag("broker", "Bootstrap broker(s)").Required().String()
	kingpin.Flag("config", "Configuration property (prop=val)").Short('X').PlaceHolder("PROP=VAL").SetValue(&confargs)
	key_delim_arg := kingpin.Flag("key-delim", "Key and value delimiter (empty string=dont print/parse key)").Default("").String()
	verbosity_arg := kingpin.Flag("verbosity", "Output verbosity level").Short('v').Default("1").Int()

	/* Producer mode options */
	mode_P := kingpin.Command("produce", "Produce messages")
	topic := mode_P.Flag("topic", "Topic to produce to").Required().String()
	partition := mode_P.Flag("partition", "Partition to produce to").Default("-1").Int()

	/* Consumer mode options */
	mode_C := kingpin.Command("consume", "Consume messages").Default()
	group := mode_C.Flag("group", "Consumer group").Required().String()
	topics := mode_C.Arg("topic", "Topic(s) to subscribe to").Required().Strings()
	var initial_offset kafka.Offset = kafka.KAFKA_OFFSET_BEGINNING
	mode_C.Flag("offset", "Initial offset").Short('o').SetValue(&initial_offset)
	exit_eof_arg := mode_C.Flag("eof", "Exit when EOF is reached for all partitions").Bool()

	mode := kingpin.Parse()

	verbosity = *verbosity_arg
	key_delim = *key_delim_arg
	exit_eof = *exit_eof_arg
	confargs.conf["bootstrap.servers"] = *brokers

	switch mode {
	case "produce":
		confargs.conf["default.topic.config"] = kafka.ConfigMap{"produce.offset.report": true}
		run_producer((*kafka.ConfigMap)(&confargs.conf), *topic, int32(*partition))

	case "consume":
		confargs.conf["group.id"] = *group
		confargs.conf["go.events.channel.enable"] = true
		confargs.conf["go.application.rebalance.enable"] = true
		confargs.conf["default.topic.config"] = kafka.ConfigMap{"auto.offset.reset": initial_offset}
		run_consumer((*kafka.ConfigMap)(&confargs.conf), *topics)
	}

}
