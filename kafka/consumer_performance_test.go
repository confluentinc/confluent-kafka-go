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

package kafka

import (
	"fmt"
	"math/rand"
	"testing"
)

// consumerPerfTest measures the consumer performance using a pre-primed (produced to) topic
func consumerPerfTest(b *testing.B, testname string, msgcnt int, useChannel bool, consumeFunc func(c *Consumer, rd *ratedisp, expCnt int), rebalanceCb func(c *Consumer, event Event) error, extraConfig ConfigMap) {

	r := testconsumerInit(b)
	if r == -1 {
		b.Skipf("Missing testconf.json")
		return
	}
	if msgcnt == 0 {
		msgcnt = r
	}

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"go.events.channel.enable": useChannel,
		"group.id":                 fmt.Sprintf("go_cperf_%d", rand.Intn(1000000)),
		"session.timeout.ms":       6000,
		"enable.auto.commit":       false,
		"debug":                    ",",
		"auto.offset.reset":        "earliest",

		// For benchmarks we want to avoid long fetch stalls when go can't keep
		// up with librdkafka filling the fetch queue and then backing off by
		// the default one second. When it's possible to consume all 2M messages
		// within a second, losing the race with librdkafka and backing off for
		// a whole second really slows us down and skews the overall timings,
		// with consecutive runs varying by a whole second due to one
		// additional backoff. So go lower than the default, but not too low so
		// we avoid spinning.
		"fetch.queue.backoff.ms": 10,
	}

	conf.updateFromTestconf()

	for k, v := range extraConfig {
		conf[k] = v
	}

	c, err := NewConsumer(&conf)

	if err != nil {
		panic(err)
	}

	expCnt := msgcnt
	b.Logf("%s, expecting %d messages", testname, expCnt)

	c.Subscribe(testconf.TopicName, rebalanceCb)

	rd := ratedispStart(b, testname, 10)

	consumeFunc(c, &rd, expCnt)

	rd.print("TOTAL: ")

	c.Close()

	b.SetBytes(rd.size)

}

// handleEvent returns false if processing should stop, else true
func handleEvent(c *Consumer, rd *ratedisp, expCnt int, ev Event) bool {
	switch e := ev.(type) {
	case *Message:
		if e.TopicPartition.Error != nil {
			rd.b.Logf("Error: %v", e.TopicPartition)
		}

		if rd.cnt == 0 {
			// start measuring time from first message to avoid
			// including rebalancing time.
			rd.b.ResetTimer()
			rd.reset()
		}

		rd.tick(1, int64(len(e.Value)))

		if rd.cnt >= int64(expCnt) {
			return false
		}
	case PartitionEOF:
		break // silence
	default:
		rd.b.Fatalf("Consumer error: %v", e)
	}
	return true

}

// consume messages through the Events channel
func eventChannelConsumer(c *Consumer, rd *ratedisp, expCnt int) {
	for ev := range c.Events() {
		if !handleEvent(c, rd, expCnt, ev) {
			break
		}
	}
}

// consume messages through the Poll() interface
func eventPollConsumer(c *Consumer, rd *ratedisp, expCnt int) {
	for true {
		ev := c.Poll(100)
		if ev == nil {
			// timeout
			continue
		}
		if !handleEvent(c, rd, expCnt, ev) {
			break
		}
	}
}

var testconsumerInited = false

// Produce messages to consume (if needed)
// Query watermarks of topic to see if we need to prime it at all.
// NOTE: This wont work for compacted topics..
// returns the number of messages to consume
func testconsumerInit(b *testing.B) int {
	if testconsumerInited {
		return testconf.PerfMsgCount
	}

	if !testconfRead() {
		return -1
	}

	msgcnt := testconf.PerfMsgCount

	currcnt, err := getMessageCountInTopic(testconf.TopicName)
	if err == nil {
		b.Logf("Topic %s has %d messages, need %d", testconf.TopicName, currcnt, msgcnt)
	}
	if currcnt < msgcnt {
		producerPerfTest(b, "Priming producer", msgcnt, false, false,
			true,
			func(p *Producer, m *Message, drChan chan Event) {
				p.ProduceChannel() <- m
			})
	}

	testconsumerInited = true
	b.ResetTimer()
	return msgcnt
}

func BenchmarkConsumerChannelPerformance(b *testing.B) {
	consumerPerfTest(b, "Channel Consumer",
		0, true, eventChannelConsumer, nil, nil)
}

func BenchmarkConsumerPollPerformance(b *testing.B) {
	consumerPerfTest(b, "Poll Consumer",
		0, false, eventPollConsumer, nil, nil)
}

func BenchmarkConsumerPollRebalancePerformance(b *testing.B) {
	consumerPerfTest(b, "Poll Consumer (rebalance callback)",
		0, false, eventPollConsumer,
		func(c *Consumer, event Event) error {
			b.Logf("Rebalanced: %s", event)
			return nil
		}, nil)
}

func BenchmarkConsumerMessageFieldsConfig(b *testing.B) {
	for _, variant := range []string{"all", "key", "value", "headers", "none"} {
		b.Run(fmt.Sprintf("variant=%s", variant), func(b *testing.B) {
			consumerPerfTest(b, "Poll Consumer", 0, false, eventPollConsumer,
				nil, ConfigMap{"go.consumer.message.fields": variant})
		})
	}
}
