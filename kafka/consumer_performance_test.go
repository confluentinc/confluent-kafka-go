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
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// consumerPerfTest measures the consumer performance using a pre-primed (produced to) topic
func consumerPerfTest(b *testing.B, testname string, msgcnt int, useChannel bool, readFromPartitionQueue bool,
	consumeFunc func(c *Consumer, rd *ratedisp, expCnt int), rebalanceCb func(c *Consumer, event Event) error) {

	r := testconsumerInit(b)
	if r == -1 {
		b.Skipf("Missing testconf.json")
		return
	}
	if msgcnt == 0 {
		msgcnt = r
	}

	rand.Seed(int64(time.Now().Unix()))

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"go.events.channel.enable":             useChannel,
		"go.enable.read.from.partition.queues": readFromPartitionQueue,
		"group.id":                             fmt.Sprintf("go_cperf_%d", rand.Intn(1000000)),
		"session.timeout.ms":                   6000,
		"api.version.request":                  "true",
		"enable.auto.commit":                   false,
		"debug":                                ",",
		"auto.offset.reset":                    "earliest"}

	conf.updateFromTestconf()

	c, err := NewConsumer(&conf)

	if err != nil {
		panic(err)
	}

	expCnt := msgcnt
	b.Logf("%s, expecting %d messages", testname, expCnt)

	c.Subscribe(testconf.Topic, rebalanceCb)

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

func handleConcurrentEvent(ctx context.Context, ticker chan int64, rd *ratedisp, ev Event) {
	switch e := ev.(type) {
	case *Message:
		if e.TopicPartition.Error != nil {
			rd.b.Logf("Error: %v", e.TopicPartition)
		}
		select {
		case <-ctx.Done():
		default:
			ticker <- int64(len(e.Value))
		}
	case PartitionEOF:
		break // silence
	default:
		rd.b.Fatalf("Consumer error: %v", e)
	}
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

// consume messages through the ReadFromPartition() interface
func eventReadFromPartition(hasAssigned <-chan bool) func(c *Consumer, rd *ratedisp, expCnt int) {
	return func(c *Consumer, rd *ratedisp, expCnt int) {

		pollForEvents := func(ctx context.Context, wg *sync.WaitGroup, cancel func(), c *Consumer) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					evt := c.Poll(100)
					if evt == nil {
						// timeout
						continue
					}
					switch evt.(type) {
					case *Message:
						cancel()
					case PartitionEOF:
						break // silence
					default:
						continue
					}
				}
			}
		}

		readMessages := func(ctx context.Context, wg *sync.WaitGroup, cancel func(), key topicPartitionKey, events chan int64) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					ev, _ := c.ReadFromPartition(TopicPartition{Topic: &key.Topic, Partition: key.Partition}, 100*time.Millisecond)
					if ev == nil || ev.TopicPartition.Error != nil {
						// timeout
						continue
					}
					handleConcurrentEvent(ctx, events, rd, ev)
				}
			}
		}

		tickr := func(ctx context.Context, cancel func(), wg *sync.WaitGroup, events chan int64) {
			defer wg.Done()
			tick := <-events
			// start measuring time from first message to avoid
			// including rebalancing time.
			rd.b.ResetTimer()
			rd.reset()
			rd.tick(1, tick)

			for tick := range events {
				if rd.cnt >= int64(expCnt) {
					break
				}
				rd.tick(1, tick)
			}
			cancel()
		}

		events := make(chan int64, 1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
		var wg = &sync.WaitGroup{}

		wg.Add(1)
		go pollForEvents(ctx, wg, cancel, c)

		var tickerWg = &sync.WaitGroup{}
		tickerWg.Add(1)
		go tickr(ctx, cancel, tickerWg, events)

		<-hasAssigned
		for key := range c.openTopParQueues {
			wg.Add(1)
			go readMessages(ctx, wg, cancel, key, events)
		}
		wg.Wait()
		close(events)
		tickerWg.Wait()
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

	currcnt, err := getMessageCountInTopic(testconf.Topic)
	if err == nil {
		b.Logf("Topic %s has %d messages, need %d", testconf.Topic, currcnt, msgcnt)
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
		0, true, false, eventChannelConsumer, nil)
}

func BenchmarkConsumerPollPerformance(b *testing.B) {
	consumerPerfTest(b, "Poll Consumer",
		0, false, false, eventPollConsumer, nil)
}

func BenchmarkConsumerPollRebalancePerformance(b *testing.B) {
	consumerPerfTest(b, "Poll Consumer (rebalance callback)",
		0, false, false, eventPollConsumer,
		func(c *Consumer, event Event) error {
			b.Logf("Rebalanced: %s", event)
			return nil
		})
}

func BenchmarkConsumerReadFromPartitionPerformance(b *testing.B) {
	hasAssigned := make(chan bool, 1)
	consumerPerfTest(b, "Poll Consumer (ReadFromPartition)",
		0, false, true, eventReadFromPartition(hasAssigned),
		func(c *Consumer, event Event) error {
			b.Logf("Rebalanced: %s", event)
			if _, ok := event.(RevokedPartitions); ok {
				if err := c.Unassign(); err != nil {
					b.Errorf("Failed to Unassign: %s\n", err)
				}
			}
			if ap, ok := event.(AssignedPartitions); ok {
				if err := c.Assign(ap.Partitions); err != nil {
					b.Errorf("Failed to Assign: %s\n", err)
				}
				hasAssigned <- true
			}
			return nil
		})
}
