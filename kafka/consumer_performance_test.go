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
func consumer_perf_test(testname string, msgcnt int, with_timestamps bool, use_channel bool, consume_func func(c *Consumer, rd *ratedisp, exp_cnt int), rebalance_cb func(c *Consumer, event Event) error) {

	rand.Seed(int64(time.Now().Unix()))

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"go.message.timestamp.enable": with_timestamps,
		"go.events.channel.enable":    use_channel,
		"group.id":                    fmt.Sprintf("go_cperf_%d", rand.Intn(1000000)),
		"session.timeout.ms":          6000,
		"api.version.request":         "true",
		"enable.auto.commit":          false,
		"debug":                       ",",
		"default.topic.config":        ConfigMap{"auto.offset.reset": "earliest"}}
	c, err := NewConsumer(&conf)

	if err != nil {
		panic(err)
	}

	exp_cnt := msgcnt
	fmt.Printf("%s, expecting %d messages\n", testname, exp_cnt)

	c.Subscribe(testconf.Topic, rebalance_cb)

	rd := ratedisp_start(testname)

	consume_func(c, &rd, exp_cnt)

	rd.print("TOTAL: ")

	c.Close()

}

// consume messages through the Events channel
func event_channel_consumer(c *Consumer, rd *ratedisp, exp_cnt int) {
	for ev := range c.Events {
		m, ok := ev.(*Message)
		if !ok {
			fmt.Printf("Ignoring %v\n", ev)
			continue
		}
		if m.TopicPartition.Err != nil {
			fmt.Printf("Error: %v\n", m.TopicPartition.Err)
			continue
		}

		if rd.cnt == 0 {
			// start measuring time from first message to avoid including
			// rebalancing time.
			rd.reset()
		}

		rd.tick(1, int64(len(m.Value)))

		if rd.cnt >= int64(exp_cnt) {
			break
		}
	}
}

// consume messages through the Poll() interface
func event_poll_consumer(c *Consumer, rd *ratedisp, exp_cnt int) {
	for rd.cnt < int64(exp_cnt) {
		ev := c.Poll(100)
		if ev == nil {
			// timeout
			continue
		}

		switch e := ev.(type) {
		case *Message:
			rd.tick(1, int64(len(e.Value)))
		case PartitionEof:
			fmt.Printf("Reached %s\n", e)
		default:
			panic(fmt.Sprintf("Consumer error: %v", e))
		}
	}
}

func TestConsumerPerformance(t *testing.T) {

	if !testconf_read() {
		return
	}

	msgcnt := 3000000

	// Produce messages to consume (if needed)
	// Query watermarks of topic to see if we need to prime it at all.
	currcnt, err := get_message_count_in_topic(testconf.Topic)
	if err == nil {
		fmt.Printf("Topic %s has %d messages\n", testconf.Topic, currcnt)
	}
	if currcnt < msgcnt {
		producer_perf_test("Priming producer", msgcnt, false, false,
			func(p *Producer, m *Message, dr_chan chan Event) {
				p.ProduceChannel <- m
			})
	}

	consumer_perf_test("Consumer (channel, go.message.timestamp.enable=true)",
		msgcnt, true, true, event_channel_consumer, nil)
	consumer_perf_test("Consumer (channel, go.message.timestamp.enable=false)",
		msgcnt, false, true, event_channel_consumer, nil)

	consumer_perf_test("Consumer (poll, go.message.timestamp.enable=true)",
		msgcnt, true, false, event_poll_consumer, nil)
	consumer_perf_test("Consumer (poll, go.message.timestamp.enable=false)",
		msgcnt, false, false, event_poll_consumer,
		func(c *Consumer, event Event) error {
			fmt.Printf("Rebalanced: %s\n", event)
			return nil
		})
}
