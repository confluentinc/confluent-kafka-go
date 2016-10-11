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
	"time"
)

// consumer_perf_test measures the consumer performance using a pre-primed (produced to) topic
func consumer_perf_test(b *testing.B, testname string, msgcnt int, use_channel bool, consume_func func(c *Consumer, rd *ratedisp, exp_cnt int), rebalance_cb func(c *Consumer, event Event) error) {

	r := testconsumer_init(b)
	if r == -1 {
		b.Skipf("Missing testconf.json")
		return
	}
	if msgcnt == 0 {
		msgcnt = r
	}

	rand.Seed(int64(time.Now().Unix()))

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"go.events.channel.enable": use_channel,
		"group.id":                 fmt.Sprintf("go_cperf_%d", rand.Intn(1000000)),
		"session.timeout.ms":       6000,
		"api.version.request":      "true",
		"enable.auto.commit":       false,
		"debug":                    ",",
		"default.topic.config":     ConfigMap{"auto.offset.reset": "earliest"}}

	conf.update_from_testconf()

	c, err := NewConsumer(&conf)

	if err != nil {
		panic(err)
	}

	exp_cnt := msgcnt
	b.Logf("%s, expecting %d messages", testname, exp_cnt)

	c.Subscribe(testconf.Topic, rebalance_cb)

	rd := ratedisp_start(b, testname, 10)

	consume_func(c, &rd, exp_cnt)

	rd.print("TOTAL: ")

	c.Close()

	b.SetBytes(rd.size)

}

// handle_event returns false if processing should stop, else true
func handle_event(c *Consumer, rd *ratedisp, exp_cnt int, ev Event) bool {
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

		if rd.cnt >= int64(exp_cnt) {
			return false
		}
	case PartitionEof:
		break // silence
	default:
		rd.b.Fatalf("Consumer error: %v", e)
	}
	return true

}

// consume messages through the Events channel
func event_channel_consumer(c *Consumer, rd *ratedisp, exp_cnt int) {
	for ev := range c.Events {
		if !handle_event(c, rd, exp_cnt, ev) {
			break
		}
	}
}

// consume messages through the Poll() interface
func event_poll_consumer(c *Consumer, rd *ratedisp, exp_cnt int) {
	for true {
		ev := c.Poll(100)
		if ev == nil {
			// timeout
			continue
		}
		if !handle_event(c, rd, exp_cnt, ev) {
			break
		}
	}
}

var testconsumer_inited bool = false

// Produce messages to consume (if needed)
// Query watermarks of topic to see if we need to prime it at all.
// NOTE: This wont work for compacted topics..
// returns the number of messages to consume
func testconsumer_init(b *testing.B) int {
	if testconsumer_inited {
		return testconf.PerfMsgCount
	}

	if !testconf_read() {
		return -1
	}

	msgcnt := testconf.PerfMsgCount

	currcnt, err := get_message_count_in_topic(testconf.Topic)
	if err == nil {
		b.Logf("Topic %s has %d messages, need %d", testconf.Topic, currcnt, msgcnt)
	}
	if currcnt < msgcnt {
		producer_perf_test(b, "Priming producer", msgcnt, false, false,
			true,
			func(p *Producer, m *Message, dr_chan chan Event) {
				p.ProduceChannel <- m
			})
	}

	testconsumer_inited = true
	b.ResetTimer()
	return msgcnt
}

func BenchmarkConsumerChannelPerformance(b *testing.B) {
	consumer_perf_test(b, "Channel Consumer",
		0, true, event_channel_consumer, nil)
}

func BenchmarkConsumerPollPerformance(b *testing.B) {
	consumer_perf_test(b, "Poll Consumer",
		0, false, event_poll_consumer, nil)
}

func BenchmarkConsumerPollRebalancePerformance(b *testing.B) {
	consumer_perf_test(b, "Poll Consumer (rebalance callback)",
		0, false, event_poll_consumer,
		func(c *Consumer, event Event) error {
			b.Logf("Rebalanced: %s", event)
			return nil
		})
}
