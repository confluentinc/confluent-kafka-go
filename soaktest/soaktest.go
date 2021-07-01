package soaktest

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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var drErrCnt uint64
var drCnt uint64
var producerErrorCbCnt uint64
var msgDupCntForOutput uint64
var msgMissCntForOutput uint64
var consumerMsgCnt uint64
var consumerMsgErrs uint64

// ConsumerErrCnt counts the number of consumer client-level errors
var ConsumerErrCnt uint64

// ProducerErrCnt counts the number of producer client-level errors
var ProducerErrCnt uint64

// ProduceMsgCnt is msg counter for producer
var ProduceMsgCnt uint64

// WarningLogger logs warning level logs to the log file
var WarningLogger *log.Logger

// InfoLogger logs info level logs to the log file
var InfoLogger *log.Logger

// ErrorLogger logs error level logs to the log file
var ErrorLogger *log.Logger

// ConsumerError combines the metrics name for consumer client-level errors
const ConsumerError = "consumer.err"

// ProducerError combines the metrics name for producer client-level errors
const ProducerError = "producer.err"

// FailedToProduceMsg combines the metrics name if failed to produce messages
const FailedToProduceMsg = "failed.to.produce.message"

var producerDr = "producer.dr"
var producerDrErr = "producer.dr.err"

const consumerConsumeMsg = "consumer.consume.msg"
const consumerReceiveError = "consumer.receive.err"
const latency = "latency"
const consumerConsumeDupMSG = "consumer.consume.dup.msg"
const consumerConsumeMissMSG = "consumer.consume.miss.msg"
const brokerRttP99 = "broker.rtt.p99"
const brokerRttAvg = "broker.rtt.avg"

// InitLogFiles initials the log folder and file, if the folder or file
// doesn't exist, create one, otherwise open it directly
func InitLogFiles(filename string) {
	path := filepath.Dir(filename)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, os.ModePerm)
	}
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644)
	if err != nil {
		log.Fatal(err)
	}

	InfoLogger = log.New(file, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	WarningLogger = log.New(file, "WARNING: ",
		log.Ldate|log.Ltime|log.Lshortfile)
	ErrorLogger = log.New(file, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}

// rttStats extracts broker rtt statistics from the raw map,
// monitors broker avg and p99 using datadog
func rttStats(raw map[string]interface{}, topic string) error {
	for _, broker := range raw["brokers"].(map[string]interface{}) {
		var avg float64
		var p99 float64
		var nodeid int

		topparsValue := broker.(map[string]interface{})["toppars"]
		if topparsValue == nil {
			continue
		}

		rttValue := broker.(map[string]interface{})["rtt"]
		avg, ok := rttValue.(map[string]interface{})["avg"].(float64)
		if !ok {
			err := fmt.Errorf("failed to convert avg to float64 %v", ok)
			return err
		}

		p99, ok = rttValue.(map[string]interface{})["p99"].(float64)
		if !ok {
			err := fmt.Errorf("failed to convert p99 to float64 %v", ok)
			return err
		}

		nid, ok := broker.(map[string]interface{})["nodeid"].(float64)
		if !ok {
			err := fmt.Errorf("failed to convert nodeid to float64 %v", ok)
			return err
		}
		nodeid = int(nid)

		tags := []string{fmt.Sprintf("broker:%d", nodeid),
			fmt.Sprintf("type:%s", raw["type"])}

		DatadogGauge(brokerRttP99, p99/1000000.0, tags)
		DatadogGauge(brokerRttAvg, avg/1000000.0, tags)
	}
	return nil
}

// HandleStatsEvent converts Stats events to map
func HandleStatsEvent(e *kafka.Stats, topic string) error {
	var raw map[string]interface{}
	err := json.Unmarshal([]byte(e.String()), &raw)
	if err != nil {
		ErrorLogger.Printf("Json unmarshall error: %s\n", err)
		return err
	}

	err = rttStats(raw, topic)
	if err != nil {
		ErrorLogger.Printf("Failed to calculate broker rtt statistics: %s",
			err)
		return err
	}
	return nil
}

// ProducerDeliveryCheck handles delivery report for producer
func ProducerDeliveryCheck(e *kafka.Message) {
	tags := []string{fmt.Sprintf("topic:%s, partition:%d",
		*e.TopicPartition.Topic, e.TopicPartition.Partition)}
	if e.TopicPartition.Error != nil {
		drErrCnt++
		DatadogIncrement(producerDrErr, 1, tags)
		ErrorLogger.Printf("Delivery failed: %v\n", e.TopicPartition)
	} else {
		drCnt++
		DatadogIncrement(producerDr, 1, tags)
		if drCnt%1000 == 0 {
			InfoLogger.Printf("Delivered message to topic "+
				"%s [%d] at offset %d\n", *e.TopicPartition.Topic,
				e.TopicPartition.Partition, e.TopicPartition.Offset)
		}
	}
}

// PrintConsumerStatus prints the information for the consumer
func PrintConsumerStatus(consumer string) {
	InfoLogger.Printf("%s: %d messages consumed, %d duplicates, "+
		"%d missed, %d message errors, %d consumer client-level errors\n",
		consumer, consumerMsgCnt, msgDupCntForOutput, msgMissCntForOutput,
		consumerMsgErrs, ConsumerErrCnt)
}

// PrintProducerStatus prints the information for the producer
func PrintProducerStatus(producer string) {
	InfoLogger.Printf("%s: %d messages produced, %d delivered, %d "+
		"failed to deliver, %d producer client-level errors\n",
		producer, ProduceMsgCnt, drCnt, drErrCnt, ProducerErrCnt)
}

// HandleMessage handles received messages, monitors latency and
// verifies messages
func HandleMessage(e *kafka.Message,
	hwmarks map[string]uint64) bool {
	tags := []string{fmt.Sprintf("topic:%s", *e.TopicPartition.Topic),
		fmt.Sprintf("partition:%s", *e.TopicPartition.Topic+"_"+
			strconv.FormatInt(int64(e.TopicPartition.Partition), 10))}
	if e.TopicPartition.Error != nil {
		consumerMsgErrs++
		DatadogIncrement(consumerReceiveError, 1, tags)
		ErrorLogger.Printf("Consumer received message on TopicPartition: %s "+
			"failed with error: %s\n", e.TopicPartition,
			e.TopicPartition.Error)
		return false
	}
	consumerMsgCnt++
	DatadogIncrement(consumerConsumeMsg, 1, tags)
	if consumerMsgCnt%1000 == 0 {
		InfoLogger.Printf("Consumer received message on TopicPartition: "+
			"%s, Headers: %s, values: %s\n", e.TopicPartition,
			e.Headers, string(e.Value))
	}

	if e.Headers != nil {
		for _, hdr := range e.Headers {
			if hdr.Key == "time" {
				var timestamp time.Time
				timestamp.GobDecode(hdr.Value)
				DatadogGauge(latency,
					time.Now().Sub(timestamp).Seconds(),
					tags)
			} else if hdr.Key == "msgid" {
				msgid := binary.LittleEndian.Uint64(hdr.Value)
				verifyMessage(e, hwmarks, msgid)
			}
		}
	}
	return true
}

// verifyMessage verifies if there isn't any duplicate or
// lost messages from consumer side
func verifyMessage(e *kafka.Message,
	hwmarks map[string]uint64, msgid uint64) {
	tags := []string{fmt.Sprintf("topic:%s", *e.TopicPartition.Topic),
		fmt.Sprintf("partition:%d", e.TopicPartition.Partition)}
	hwkey := fmt.Sprintf("%s--%d",
		*e.TopicPartition.Topic,
		e.TopicPartition.Partition)
	hw := hwmarks[hwkey]
	if hw > 0 {
		if msgid <= hw {
			ErrorLogger.Printf("Consumer: Old or duplicate message %s [%d] "+
				"at offset %d with msgid %d (headers %s): wanted msgid > %d\n",
				*e.TopicPartition.Topic, e.TopicPartition.Partition,
				e.TopicPartition.Offset, msgid, e.Headers, hw)
			msgDupCntForOutput += (hw + 1) - msgid
			DatadogIncrement(consumerConsumeDupMSG, 1, tags)
		} else if msgid > hw+1 {
			ErrorLogger.Printf("Consumer: Lost messages, now at %s [%d] at "+
				"offset %d with msgid %d (headers %s): expected msgid %d+1\n",
				*e.TopicPartition.Topic, e.TopicPartition.Partition,
				e.TopicPartition.Offset, msgid, e.Headers, hw)
			msgMissCntForOutput += msgid - (hw + 1)
			DatadogIncrement(consumerConsumeMissMSG, 1, tags)
		}
	}
	hwmarks[hwkey] = msgid
}

// ConvertUint64ToByteArray converts the unit64 type to []byte
func ConvertUint64ToByteArray(num uint64) []byte {
	byteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteArray, num)
	return byteArray
}
