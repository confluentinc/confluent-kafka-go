package main

/**
 * Copyright 2021 Confluent Inc.
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
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"soaktest"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const endToEnd = "end.to.end."
const rate = 2
const producerType = "InputProducer"
const consumerType = "OutputConsumer"

func main() {

	broker := flag.String("broker", "", "bootstrap servers")
	inputTopic := flag.String("inputTopic", "inputTopic",
		"producer will deliver messages to this topic")
	outTopic := flag.String("outTopic", "outTopic",
		"consumer will consume messages from this topic")
	groupID := flag.String("groupID", "groupID",
		"the group consumer will join")
	inputTopicPartitionsNum := flag.Int("inputTopicPartitionsNum", 4,
		"inputTopic partition number")
	outTopicPartitionsNum := flag.Int("outTopicPartitionsNum", 4,
		"outTopic partition number")
	replicationFactor := flag.Int("replicationFactor", 1, "topic replication")
	ccloudAPIKey := flag.String("ccloudAPIKey", "", "sasl username")
	ccloudAPISecret := flag.String("ccloudAPISecret", "", "sasl password")

	flag.Parse()

	soaktest.InitLogFiles(fmt.Sprintf("../log/soakclient_%s.log",
		time.Now().Format("2006-01-02")))
	soaktest.InfoLogger.Printf("Starting the application...\n")

	num, version := kafka.LibraryVersion()
	soaktest.InfoLogger.Printf("num = %d, librdkafka %q\n", num, version)

	var wg sync.WaitGroup
	doneChan := make(chan bool, 1)
	errorChan := make(chan error, 1)

	wg.Add(1)
	go soaktest.GetRusageMetrics(endToEnd, &wg, doneChan, errorChan)

	maxDuration, err := time.ParseDuration("30s")
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to create maxDuration with "+
			"err %s\n", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	newTopics := []kafka.TopicSpecification{
		{Topic: *inputTopic,
			NumPartitions:     *inputTopicPartitionsNum,
			ReplicationFactor: *replicationFactor},
		{Topic: *outTopic,
			NumPartitions:     *outTopicPartitionsNum,
			ReplicationFactor: *replicationFactor}}

	err = CreateTopic(ctx, newTopics, *broker, *ccloudAPIKey, *ccloudAPISecret)
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to create topics: %s\n", err)
		close(doneChan)
		os.Exit(1)
	}

	wg.Add(1)
	go producer(*inputTopic, *broker, *ccloudAPIKey,
		*ccloudAPISecret, &wg, doneChan, errorChan,
		uint64(*inputTopicPartitionsNum))

	wg.Add(1)
	go consumer(*outTopic, *broker, *groupID, *ccloudAPIKey,
		*ccloudAPISecret, &wg, doneChan, errorChan)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		soaktest.InfoLogger.Printf("Signal caught: %v\n", sig)
	case err := <-errorChan:
		soaktest.ErrorLogger.Printf("Error caught: %s\n", err)
	}

	close(doneChan)
	close(errorChan)
	for err = range errorChan {
		soaktest.ErrorLogger.Printf("Error caught: %s\n", err)
	}
	wg.Wait()
	soaktest.PrintConsumerStatus(consumerType)
	soaktest.PrintProducerStatus(producerType)
}

// CreateTopic creates the topics if it doesn't exist
func CreateTopic(ctx context.Context,
	topics []kafka.TopicSpecification,
	broker, ccloudAPIKey, ccloudAPISecret string) error {
	conf := kafka.ConfigMap{
		"bootstrap.servers": broker,
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     ccloudAPIKey,
		"sasl.password":     ccloudAPISecret}
	AdminClient, err := kafka.NewAdminClient(&conf)
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to create AdminClient: %s\n", err)
		return err
	}

	result, err := AdminClient.CreateTopics(ctx, topics)
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to create new topics: %s\n", err)
		return err
	}

	for _, res := range result {
		switch res.Error.Code() {
		case kafka.ErrTopicAlreadyExists:
			soaktest.ErrorLogger.Printf("Failed to create topic %s: %s\n",
				res.Topic, res.Error)

		case kafka.ErrNoError:
			soaktest.ErrorLogger.Printf("Succeed to create topic %s\n",
				res.Topic)

		default:
			err = fmt.Errorf("failed to create topic %s: %s",
				res.Topic, res.Error)
			return err
		}
	}
	return nil
}

// terminateProducer waits for all messages in the Producer queue to
// be delivered, closes the producer and pass error to the error channel
func terminateProducer(p *kafka.Producer, err error, errorChan chan error) {
	remaining := p.Flush(30)
	soaktest.InfoLogger.Printf("producer: %d message(s) remaining in queue "+
		"after flush()\n", remaining)
	p.Close()
	errorChan <- err
}

// terminateConsumer closes consumer and pass error to the error channel
func terminateConsumer(c *kafka.Consumer, err error, errorChan chan error) {
	c.Close()
	errorChan <- err
}

// producer produces messages to input topic
func producer(inputTopic, broker, ccloudAPIKey, ccloudAPISecret string,
	wg *sync.WaitGroup, termChan chan bool, errorChan chan error,
	partitionNum uint64) {
	defer wg.Done()
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":      broker,
		"statistics.interval.ms": 5000,
		"sasl.mechanisms":        "PLAIN",
		"security.protocol":      "SASL_SSL",
		"sasl.username":          ccloudAPIKey,
		"sasl.password":          ccloudAPISecret})
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to create producer %s\n", err)
		errorChan <- err
		return
	}

	run := true
	doneChan := make(chan bool)
	tags := []string{fmt.Sprintf("topic:%s", inputTopic)}
	partitionToMsgIDMap := make(map[uint64]uint64)

	go func() {
		doTerm := false
		for !doTerm {
			select {
			case e := <-p.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					// Message delivery report
					soaktest.ProducerDeliveryCheck(ev)

				case *kafka.Stats:
					err = soaktest.HandleStatsEvent(ev, inputTopic)
					if err != nil {
						soaktest.ErrorLogger.Printf("Failed to "+
							"HandleStatsEvent: %s\n", err)
						errorChan <- err
					}

				case kafka.Error:
					// kafka.Errors should generally be
					// considered informational, the client
					// will try to automatically recover.
					soaktest.ProducerErrCnt++
					soaktest.DatadogIncrement(soaktest.ProducerError, 1, tags)
					soaktest.ErrorLogger.Printf("kafka.Error: %v: %v for "+
						"producer %v\n", ev.Code(), ev, p)

				default:
					soaktest.WarningLogger.Printf("Ignored event: %v for"+
						"producer %v\n", ev, p)
				}
			case <-doneChan:
				doTerm = true
			}
		}
		close(doneChan)
	}()

	value := "Hello Go!"

	sleepIntvl := 1.0 / rate * 1000
	ticker := time.NewTicker(time.Millisecond * time.Duration(sleepIntvl))

	for run {
		select {
		case <-termChan:
			doneChan <- true
			run = false
		case <-ticker.C:
			encodingTime, err := time.Now().GobEncode()
			if err != nil {
				soaktest.ErrorLogger.Printf("Failed to get MarshalText "+
					"%s\n", err)
				doneChan <- true
				terminateProducer(p, err, errorChan)
				return
			}
			key := soaktest.ProduceMsgCnt % partitionNum
			msgid := partitionToMsgIDMap[key] + 1
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &inputTopic,
					Partition: kafka.PartitionAny},
				Key:   soaktest.ConvertUint64ToByteArray(key),
				Value: []byte(value),
				Headers: []kafka.Header{{Key: "time",
					Value: encodingTime},
					{Key: "msgid",
						Value: soaktest.ConvertUint64ToByteArray(msgid)}},
			}, nil)
			if err != nil {
				soaktest.ErrorLogger.Printf("Failed to Produce message to "+
					"inputTopic %s, %s\n", inputTopic, err)
				soaktest.DatadogIncrement(soaktest.FailedToProduceMsg, 1, tags)
			} else {
				partitionToMsgIDMap[key] = msgid
				soaktest.ProduceMsgCnt++
			}
		}
	}
	remaining := p.Flush(30)
	p.Close()
	soaktest.InfoLogger.Printf("producer: %d message(s) remaining in queue "+
		"after flush()\n", remaining)
}

// consumer receives and verifies messages from output topic
func consumer(topic, broker, groupID, ccloudAPIKey, ccloudAPISecret string,
	wg *sync.WaitGroup, termChan chan bool, errorChan chan error) {
	wg.Done()
	hwmarks := make(map[string]uint64)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":      broker,
		"group.id":               groupID,
		"auto.offset.reset":      "earliest",
		"statistics.interval.ms": 5000,
		"sasl.mechanisms":        "PLAIN",
		"security.protocol":      "SASL_SSL",
		"sasl.username":          ccloudAPIKey,
		"sasl.password":          ccloudAPISecret,
	})
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to create consumer: %s\n", err)
		errorChan <- err
		return
	}

	err = c.Subscribe(topic, nil)

	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to Subscribe to topic: %s with "+
			"error %s\n", topic, err)
		terminateConsumer(c, err, errorChan)
		return
	}

	run := true
	tags := []string{fmt.Sprintf("topic:%s", topic)}

	for run {
		select {
		case <-termChan:
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				soaktest.HandleMessage(e, hwmarks)

			case *kafka.Stats:
				err = soaktest.HandleStatsEvent(e, topic)
				if err != nil {
					soaktest.ErrorLogger.Printf("Failed to "+
						"HandleStatsEvent: %s\n", err)
					terminateConsumer(c, err, errorChan)
					return
				}

			case kafka.Error:
				// kafka.Errors should generally be
				// considered informational, the client
				// will try to automatically recover.
				soaktest.ConsumerErrCnt++
				soaktest.DatadogIncrement(soaktest.ConsumerError, 1, tags)
				soaktest.ErrorLogger.Printf("kafka.Error: %v: %v for "+
					"consumer %v\n", e.Code(), e, c)

			default:
				soaktest.WarningLogger.Printf("Ignored %v for consumer %v\n",
					e, c)
			}

		}
	}
	c.Close()
	soaktest.ErrorLogger.Printf("Consumer closed\n")
}
