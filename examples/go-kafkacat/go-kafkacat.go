// Example kafkacat clone written in Golang
package main

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

import (
	"bufio"
	"fmt"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	verbosity    = 1
	exitEOF      = false
	eofCnt       = 0
	partitionCnt = 0
	keyDelim     = ""
	sigs         chan os.Signal
)

func runProducer(config *kafka.ConfigMap, topic string, partition int32) {
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
	}(p.Events())

	reader := bufio.NewReader(os.Stdin)
	stdinChan := make(chan string)

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

			stdinChan <- line
		}
		close(stdinChan)
	}()

	run := true

	for run == true {
		select {
		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			run = false

		case line, ok := <-stdinChan:
			if !ok {
				run = false
				break
			}

			msg := kafka.Message{TopicPartition: tp}

			if keyDelim != "" {
				vec := strings.SplitN(line, keyDelim, 2)
				if len(vec[0]) > 0 {
					msg.Key = ([]byte)(vec[0])
				}
				if len(vec) == 2 && len(vec[1]) > 0 {
					msg.Value = ([]byte)(vec[1])
				}
			} else {
				msg.Value = ([]byte)(line)
			}

			p.ProduceChannel() <- &msg
		}
	}

	fmt.Fprintf(os.Stderr, "%% Flushing %d message(s)\n", p.Len())
	p.Flush(10000)
	fmt.Fprintf(os.Stderr, "%% Closing\n")
	p.Close()
}

func runConsumer(config *kafka.ConfigMap, topics []string) {
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

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)
				partitionCnt = len(e.Partitions)
				eofCnt = 0
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()
				partitionCnt = 0
				eofCnt = 0
			case *kafka.Message:
				if verbosity >= 2 {
					fmt.Fprintf(os.Stderr, "%% %v:\n", e.TopicPartition)
				}
				if keyDelim != "" {
					if e.Key != nil {
						fmt.Printf("%s%s", string(e.Key), keyDelim)
					} else {
						fmt.Printf("%s", keyDelim)
					}
				}
				fmt.Println(string(e.Value))
			case kafka.PartitionEOF:
				fmt.Fprintf(os.Stderr, "%% Reached %v\n", e)
				eofCnt++
				if exitEOF && eofCnt >= partitionCnt {
					run = false
				}
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
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

type configArgs struct {
	conf kafka.ConfigMap
}

func (c *configArgs) String() string {
	return "FIXME"
}

func (c *configArgs) Set(value string) error {
	return c.conf.Set(value)
}

func (c *configArgs) IsCumulative() bool {
	return true
}

func main() {
	sigs = make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	_, libver := kafka.LibraryVersion()
	kingpin.Version(fmt.Sprintf("confluent-kafka-go (librdkafka v%s)", libver))

	// Default config
	var confargs configArgs
	confargs.conf = kafka.ConfigMap{"session.timeout.ms": 6000}

	/* General options */
	brokers := kingpin.Flag("broker", "Bootstrap broker(s)").Required().String()
	kingpin.Flag("config", "Configuration property (prop=val)").Short('X').PlaceHolder("PROP=VAL").SetValue(&confargs)
	keyDelimArg := kingpin.Flag("key-delim", "Key and value delimiter (empty string=dont print/parse key)").Default("").String()
	verbosityArg := kingpin.Flag("verbosity", "Output verbosity level").Short('v').Default("1").Int()

	/* Producer mode options */
	modeP := kingpin.Command("produce", "Produce messages")
	topic := modeP.Flag("topic", "Topic to produce to").Required().String()
	partition := modeP.Flag("partition", "Partition to produce to").Default("-1").Int()

	/* Consumer mode options */
	modeC := kingpin.Command("consume", "Consume messages").Default()
	group := modeC.Flag("group", "Consumer group").Required().String()
	topics := modeC.Arg("topic", "Topic(s) to subscribe to").Required().Strings()
	initialOffset := modeC.Flag("offset", "Initial offset").Short('o').Default(kafka.OffsetBeginning.String()).String()
	exitEOFArg := modeC.Flag("eof", "Exit when EOF is reached for all partitions").Bool()

	mode := kingpin.Parse()

	verbosity = *verbosityArg
	keyDelim = *keyDelimArg
	exitEOF = *exitEOFArg
	confargs.conf["bootstrap.servers"] = *brokers

	switch mode {
	case "produce":
		confargs.conf["produce.offset.report"] = true
		runProducer((*kafka.ConfigMap)(&confargs.conf), *topic, int32(*partition))

	case "consume":
		confargs.conf["group.id"] = *group
		confargs.conf["go.events.channel.enable"] = true
		confargs.conf["go.application.rebalance.enable"] = true
		confargs.conf["auto.offset.reset"] = *initialOffset
		// Enable generation of PartitionEOF events to track
		// when end of partition is reached.
		confargs.conf["enable.partition.eof"] = exitEOF
		runConsumer((*kafka.ConfigMap)(&confargs.conf), *topics)
	}

}
