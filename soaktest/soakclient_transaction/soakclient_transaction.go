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

const producerTransactionCommitSucceed = "producer.transaction.commit.succeed"
const producerTransactionCommitFailed = "producer.transaction.commit.failed"
const producerTransactionAbortSucceed = "producer.transaction.abort.succeed"
const producerTransactionAbortFailed = "producer.transaction.abort.failed"
const transaction = "transaction."

const producerType = "TxnProducer"
const consumerType = "TxnConsumer"

var msgMissCntForInput uint64
var msgDupCntForInput uint64
var retryNum = 3

func main() {

	broker := flag.String("broker", "", "bootstrap servers")
	inputTopic := flag.String("inputTopic", "inputTopic",
		"producer will deliver messages to this topic")
	outTopic := flag.String("outTopic", "outTopic",
		"consumer will consume messages from this topic")
	outTopicPartitionsNum := flag.Int("outTopicPartitionsNum", 1,
		"outTopic partition number")
	groupID := flag.String("groupID", "groupID",
		"the group consumer will join")
	transactionID := flag.String("transactionID", "transactionID",
		"transaction id")
	ccloudAPIKey := flag.String("ccloudAPIKey", "", "sasl username")
	ccloudAPISecret := flag.String("ccloudAPISecret", "", "sasl password")

	flag.Parse()

	soaktest.InitLogFiles(fmt.Sprintf("../log/soakclient_transaction_%s.log",
		time.Now().Format("2006-01-02")))
	soaktest.InfoLogger.Printf("Starting the application...")

	num, version := kafka.LibraryVersion()
	soaktest.InfoLogger.Printf("num = %d, librdkafka %q\n", num, version)

	var wg sync.WaitGroup
	doneChan := make(chan bool, 1)
	errorChan := make(chan error, 1)

	wg.Add(1)
	go soaktest.GetRusageMetrics(transaction, &wg, doneChan, errorChan)

	wg.Add(1)
	go verifyProducerTransaction(*broker, *inputTopic, *outTopic, *groupID,
		*transactionID, *ccloudAPIKey, *ccloudAPISecret, &wg, doneChan,
		errorChan, uint64(*outTopicPartitionsNum))

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		soaktest.InfoLogger.Printf("Signal caught: %v", sig)
	case err := <-errorChan:
		soaktest.ErrorLogger.Printf("Error caught: %v", err)
	}

	close(doneChan)
	close(errorChan)
	for err := range errorChan {
		soaktest.ErrorLogger.Printf("Error caught: %s\n", err)
	}
	wg.Wait()
	soaktest.PrintConsumerStatus(consumerType)
	soaktest.PrintProducerStatus(producerType)
}

// getConsumerPosition gets consumer position according to partition id
func getConsumerPosition(consumer *kafka.Consumer, partition int32,
	topic string) (kafka.TopicPartition, error) {
	position, err := consumer.Position([]kafka.TopicPartition{{
		Topic: &topic, Partition: partition}})
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to get position %s\n", err)
		return kafka.TopicPartition{}, err
	} else if len(position) == 0 {
		err = fmt.Errorf("the position doesn't contain any element")
		return kafka.TopicPartition{}, err
	}
	return position[0], nil
}

// getAllPartitionPositions get offsets for all partitions
func getAllPartitionPositions(positionMap map[int32]kafka.TopicPartition) []kafka.TopicPartition {
	partitionPosition := make([]kafka.TopicPartition, 0, len(positionMap))

	for _, v := range positionMap {
		partitionPosition = append(partitionPosition, v)
	}
	return partitionPosition
}

// terminate closes producer and consumer, and also pass error to the
// error channel
func terminate(c *kafka.Consumer, p *kafka.Producer, err error,
	errorChan chan error) {
	c.Close()
	p.Close()
	errorChan <- err
}

// verifyProducerTransaction receivev messages from input topic,
// then produce it the output topic with transaction producer.
// Commit transaction every 100 messages or every 1 second which every
// comes first
func verifyProducerTransaction(broker, inputTopic, outTopic, groupID,
	transactionID, ccloudAPIKey, ccloudAPISecret string, wg *sync.WaitGroup,
	termChan chan bool, errorChan chan error, outTopicPartitionsNum uint64) {
	defer wg.Done()
	transactionCommitNum := 1
	hwmarks := make(map[string]uint64)
	hwmarksLastCommitted := make(map[string]uint64)
	partitionPositionMap := make(map[int32]kafka.TopicPartition)
	partitionToMsgIDMap := make(map[uint64]uint64)
	partitionToMsgIDMapLastCommitted := make(map[uint64]uint64)
	ticker := time.NewTicker(1000 * time.Millisecond)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        broker,
		"group.id":                 groupID,
		"go.events.channel.enable": true,
		"statistics.interval.ms":   5000,
		"sasl.mechanisms":          "PLAIN",
		"security.protocol":        "SASL_SSL",
		"sasl.username":            ccloudAPIKey,
		"sasl.password":            ccloudAPISecret})

	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to create consumer: %s\n", err)
		errorChan <- err
		return
	}

	err = c.Subscribe(inputTopic, nil)
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to subscribe to topic: %s with "+
			"error %s\n", inputTopic, err)
		c.Close()
		errorChan <- err
		return
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":      broker,
		"transactional.id":       transactionID,
		"statistics.interval.ms": 5000,
		"sasl.mechanisms":        "PLAIN",
		"security.protocol":      "SASL_SSL",
		"sasl.username":          ccloudAPIKey,
		"sasl.password":          ccloudAPISecret,
	})
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to create transactional "+
			"producer: %s\n", err)
		c.Close()
		errorChan <- err
		return
	}

	maxDuration, err := time.ParseDuration("15s")
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to parse Duration %v\n", err)
		terminate(c, p, err, errorChan)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	// If InitTransactions succeed, will continue to BeginTransaction
	// If InitTransactions failed with err.(kafka.Error).IsRetriable(),
	// sleep 3 seconds then retry
	// If failed with other errors, return the fatal error
	for i := 0; i < retryNum; i++ {
		err = p.InitTransactions(ctx)
		if err != nil {
			soaktest.ErrorLogger.Printf("InitTransactions() failed: %s\n", err)
			if err.(kafka.Error).IsRetriable() {
				if i == retryNum-1 {
					soaktest.ErrorLogger.Printf("InitTransactions() failed "+
						"after %d times retries: %s\n", retryNum, err)
					terminate(c, p, err, errorChan)
					return
				}
				time.Sleep(3 * time.Second)
				continue
			} else {
				soaktest.ErrorLogger.Printf("InitTransactions() failed: %s\n",
					err)
				terminate(c, p, err, errorChan)
				return
			}
		} else {
			break
		}
	}

	//Start producer transaction.
	err = p.BeginTransaction()
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to begin transaction %s\n", err)
		terminate(c, p, err, errorChan)
		return
	}

	producerTags := []string{fmt.Sprintf("topic:%s", outTopic)}
	consumerTags := []string{fmt.Sprintf("topic:%s", inputTopic)}

	run := true
	committed := false
	for run == true {
		select {
		case <-termChan:
			run = false
			if soaktest.ProduceMsgCnt != 0 && !committed {
				transactionCommitNum, err = commitTransaction(
					getAllPartitionPositions(partitionPositionMap),
					p,
					c,
					transactionCommitNum,
					hwmarks,
					hwmarksLastCommitted,
					partitionToMsgIDMap,
					partitionToMsgIDMapLastCommitted)
			}
		case <-ticker.C:
			if soaktest.ProduceMsgCnt != 0 && !committed {
				transactionCommitNum, err = commitTransaction(
					getAllPartitionPositions(partitionPositionMap),
					p,
					c,
					transactionCommitNum,
					hwmarks,
					hwmarksLastCommitted,
					partitionToMsgIDMap,
					partitionToMsgIDMapLastCommitted)
				if err != nil {
					soaktest.ErrorLogger.Printf("Failed to commit "+
						"transaction: %v\n", err)
					terminate(c, p, err, errorChan)
					return
				}
				committed = true
				p.BeginTransaction()
			}
		case ev := <-p.Events():
			// Producer delivery report
			switch e := ev.(type) {
			case *kafka.Message:
				soaktest.ProducerDeliveryCheck(e)

			case *kafka.Stats:
				err = soaktest.HandleStatsEvent(e, outTopic)
				if err != nil {
					soaktest.ErrorLogger.Printf("Failed to handle stats "+
						"event for producer %v\n", err)
					terminate(c, p, err, errorChan)
					return
				}

			case kafka.Error:
				// kafka.Errors should generally be
				// considered informational, the client
				// will try to automatically recover.
				soaktest.ProducerErrCnt++
				soaktest.DatadogIncrement(soaktest.ProducerError,
					1, producerTags)
				soaktest.ErrorLogger.Printf("kafka.Error: %v: %v for "+
					"producer %v\n", e.Code(), e, p)

			default:
				soaktest.WarningLogger.Printf("Ignored event: %s for "+
					"producer %v\n", ev, p)
			}

		default:
			var ev = c.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				if !soaktest.HandleMessage(e, hwmarks) {
					soaktest.ErrorLogger.Printf("Message receive failed, " +
						"skip producing message\n")
					continue
				}
				key := soaktest.ProduceMsgCnt % outTopicPartitionsNum
				msgid := partitionToMsgIDMap[key] + 1
				err = produceMessage(e, p, msgid, key, outTopic)
				if err != nil {
					soaktest.ErrorLogger.Printf("Failed to Produce message "+
						"to outTopic %s, %s\n", outTopic, err)
					continue
				}
				partitionToMsgIDMap[key] = msgid
				soaktest.ProduceMsgCnt++
				position, err := getConsumerPosition(
					c,
					e.TopicPartition.Partition,
					inputTopic)
				if err != nil {
					soaktest.ErrorLogger.Printf("Failed to get consumer "+
						"position: %v\n", err)
					terminate(c, p, err, errorChan)
					return
				}
				partitionPositionMap[e.TopicPartition.Partition] = position
				if soaktest.ProduceMsgCnt%100 == 0 && !committed {
					transactionCommitNum, err = commitTransaction(
						getAllPartitionPositions(partitionPositionMap),
						p,
						c,
						transactionCommitNum,
						hwmarks,
						hwmarksLastCommitted,
						partitionToMsgIDMap,
						partitionToMsgIDMapLastCommitted)
					if err != nil {
						soaktest.ErrorLogger.Printf("Failed to commit "+
							"transaction %s\n", err)
						terminate(c, p, err, errorChan)
						return
					}
					committed = true
					p.BeginTransaction()
				} else {
					committed = false
				}

			case *kafka.Stats:
				err = soaktest.HandleStatsEvent(e, inputTopic)
				if err != nil {
					soaktest.ErrorLogger.Printf("Failed to handle stats "+
						"event for consumer %v\n", err)
					terminate(c, p, err, errorChan)
					return
				}

			case kafka.Error:
				// kafka.Errors should generally be
				// considered informational, the client
				// will try to automatically recover.
				soaktest.ConsumerErrCnt++
				soaktest.DatadogIncrement(soaktest.ConsumerError,
					1, consumerTags)
				soaktest.ErrorLogger.Printf("kafka.Error: %v: %v for "+
					"consumer %v\n", e.Code(), e, c)

			default:
				soaktest.WarningLogger.Printf("Ignored %v for consumer %v\n",
					e, c)
			}
		}
	}
	p.Close()
	c.Close()
}

// produceMessage produces messages to output topic,
// the producer is transaction producer
func produceMessage(e *kafka.Message,
	p *kafka.Producer,
	msgid, key uint64,
	outTopic string) error {
	encodingTime, err := time.Now().GobEncode()
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to encode current time %v\n", err)
		return err
	}

	tags := []string{fmt.Sprintf("topic:%s", outTopic)}
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &outTopic,
			Partition: kafka.PartitionAny},
		Value: []byte(string(e.Value)),
		Key:   soaktest.ConvertUint64ToByteArray(key),
		Headers: []kafka.Header{{Key: "time",
			Value: encodingTime},
			{Key: "msgid",
				Value: soaktest.ConvertUint64ToByteArray(msgid)}},
	}, nil)
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to Produce message to "+
			"outTopic %s, %s\n", outTopic, err)
		soaktest.DatadogIncrement(soaktest.FailedToProduceMsg, 1, tags)
	}
	return err
}

// commitTransaction commits transaction to output topic.
// Abort transaction once every 100 commit transactions
func commitTransaction(partitionPosition []kafka.TopicPartition,
	p *kafka.Producer,
	c *kafka.Consumer,
	transactionCommitNum int,
	hwmarks, hwmarksLastCommitted map[string]uint64,
	partitionToMsgIDMap, partitionToMsgIDMapLastCommitted map[uint64]uint64) (commitNum int, err error) {
	soaktest.InfoLogger.Printf("=== Committing transaction===\n")

	cgmd, err := c.GetConsumerGroupMetadata()
	if err != nil {
		soaktest.ErrorLogger.Printf("Failed to get Consumer Group "+
			"Metadata %v\n", err)
		return transactionCommitNum, err
	}

	// If SendOffsetsToTransaction succeed, will continue to commit
	// or abort transaction
	// If SendOffsetsToTransaction failed with err.(kafka.Error).IsRetriable(),
	// sleep 3 seconds then retry
	// If SendOffsetsToTransaction failed with err.(kafka.Error).TxnRequiresAbort(),
	// AbortTransaction and return (transactionCommitNum, err)
	// If failed with other errors, return transactionCommitNum and the
	// fatal error
	for i := 0; i < retryNum; i++ {
		err = p.SendOffsetsToTransaction(nil, partitionPosition, cgmd)
		if err != nil {
			soaktest.ErrorLogger.Printf("SendOffsetsToTransaction() "+
				"failed: %s\n", err)
			if err.(kafka.Error).IsRetriable() {
				if i == retryNum-1 {
					soaktest.ErrorLogger.Printf("SendOffsetsToTransaction() "+
						"failed with max retry %d times: %s\n", retryNum, err)
					return transactionCommitNum, err
				}
				time.Sleep(3 * time.Second)
				continue
			} else if err.(kafka.Error).TxnRequiresAbort() {
				err = p.AbortTransaction(nil)
				if err != nil {
					soaktest.ErrorLogger.Printf("AbortTransaction() "+
						"failed: %s\n", err)
					return transactionCommitNum, err
				}
				rewindConsumerPosition(c)
				return transactionCommitNum, nil
			} else {
				return transactionCommitNum, err
			}
		} else {
			break
		}
	}

	if transactionCommitNum%100 != 0 {
		// If CommitTransaction succeed, transactionCommitNum + 1 and return
		// If CommitTransaction failed with err.(kafka.Error).IsRetriable(),
		// sleep 3 seconds then retry
		// If CommitTransaction failed with
		// err.(kafka.Error).TxnRequiresAbort(),
		// AbortTransaction and return (transactionCommitNum, err)
		// If failed with other errors, return transactionCommitNum and the
		// fatal error
		for i := 0; i < retryNum; i++ {
			err = p.CommitTransaction(nil)
			if err != nil {
				if i == 0 {
					soaktest.DatadogIncrement(
						producerTransactionCommitFailed, 1, nil)
				}
				soaktest.ErrorLogger.Printf("CommitTransaction() failed: %s\n",
					err)
				if err.(kafka.Error).IsRetriable() {
					if i == retryNum-1 {
						soaktest.ErrorLogger.Printf("CommitTransaction() "+
							"failed with max retry %d times: %s\n",
							retryNum, err)
						return transactionCommitNum, err
					}
					time.Sleep(3 * time.Second)
					continue
				} else if err.(kafka.Error).TxnRequiresAbort() {
					err = p.AbortTransaction(nil)
					if err != nil {
						soaktest.ErrorLogger.Printf("AbortTransaction() "+
							"failed: %s\n", err)
						return transactionCommitNum, err
					}
					err = rewindConsumerPosition(c)
					if err != nil {
						soaktest.ErrorLogger.Printf("rewindConsumerPosition()"+
							" failed: %s\n", err)
					}
					return transactionCommitNum, nil
				} else {
					return transactionCommitNum, err
				}
			} else {
				for k, v := range hwmarks {
					hwmarksLastCommitted[k] = v
				}
				for k, v := range partitionToMsgIDMap {
					partitionToMsgIDMapLastCommitted[k] = v
				}
				soaktest.DatadogIncrement(
					producerTransactionCommitSucceed, 1, nil)
				transactionCommitNum++
				return transactionCommitNum, nil
			}
		}
	} else {
		// If AbortTransaction succeed, transactionCommitNum = 1 and return
		// If AbortTransaction failed with err.(kafka.Error).IsRetriable(),
		// sleep 3 seconds then retry
		// If failed with other errors, return transactionCommitNum and the
		// fatal error
		for i := 0; i < retryNum; i++ {
			err = p.AbortTransaction(nil)
			if err != nil {
				if i == 0 {
					soaktest.DatadogIncrement(
						producerTransactionAbortFailed, 1, nil)
				}
				soaktest.ErrorLogger.Printf("AbortTransaction() failed: %s\n",
					err)
				if err.(kafka.Error).IsRetriable() {
					if i == retryNum-1 {
						soaktest.ErrorLogger.Printf("AbortTransaction() "+
							"failed with max retry %d times: %s\n", retryNum, err)
						return transactionCommitNum, err
					}
					time.Sleep(3 * time.Second)
					continue
				} else {
					soaktest.ErrorLogger.Printf("AbortTransaction() "+
						"failed: %s\n", err)
					return transactionCommitNum, err
				}
			} else {
				soaktest.DatadogIncrement(
					producerTransactionAbortSucceed,
					1,
					nil)
				transactionCommitNum = 1
				err = rewindConsumerPosition(c)
				if err != nil {
					soaktest.ErrorLogger.Printf("rewindConsumerPosition() "+
						"failed: %s\n", err)
				}
				// If AbortTransaction() happens, rewind the msgid to the
				// last committed msid per partition
				for k, v := range hwmarksLastCommitted {
					hwmarks[k] = v
				}
				for k, v := range partitionToMsgIDMapLastCommitted {
					partitionToMsgIDMap[k] = v
				}
				return transactionCommitNum, err
			}
		}
	}

	return transactionCommitNum, err
}

// rewindConsumerPosition Rewinds consumer's position to the
// pre-transaction offset
func rewindConsumerPosition(c *kafka.Consumer) error {
	assignment, err := c.Assignment()
	if err != nil {
		soaktest.ErrorLogger.Printf("Assignment() failed: %s\n", err)
		return err
	}

	committed, err := c.Committed(assignment, 30*1000 /* 30s */)
	if err != nil {
		soaktest.ErrorLogger.Printf("Committed() failed: %s\n", err)
		return err
	}

	for _, tp := range committed {
		if tp.Offset < 0 {
			tp.Offset = kafka.OffsetBeginning
		}
		err := c.Seek(tp, 1)
		if err != nil {
			soaktest.ErrorLogger.Printf("Seek() failed: %s\n", err)
			return err
		}
	}
	return nil
}
