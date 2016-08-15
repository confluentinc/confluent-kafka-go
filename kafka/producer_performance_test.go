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
	"strings"
	"testing"
)

func delivery_handler(exp_cnt int64, delivery_chan chan Event, done_chan chan int64) {

	var cnt, size int64

	for ev := range delivery_chan {
		m, ok := ev.(*Message)
		if !ok {
			continue
		}

		if m.TopicPartition.Error != nil {
			b.Errorf("Message delivery error: %v", m.TopicPartition)
			break
		}

		//fmt.Printf("Delivered to %s [%d] @ %d\n", *m.Topic, m.Partition, m.Offset)
		cnt += 1
		if m.Value != nil {
			size += int64(len(m.Value))
		}
		if cnt >= exp_cnt {
			break
		}

	}

	done_chan <- cnt
	done_chan <- size
	close(done_chan)
}

func producer_perf_test(testname string, msgcnt int, with_dr bool, batch_producer bool, produce_func func(p *Producer, m *Message, dr_chan chan Event)) {

	if !testconf_read() {
		return
	}

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"go.batch.producer":            batch_producer,
		"go.delivery.reports":          with_dr,
		"queue.buffering.max.messages": msgcnt,
		"api.version.request":          "true",
		"broker.version.fallback":      "0.9.0.1",
		"default.topic.config":         ConfigMap{"acks": 1}}
	p, err := NewProducer(&conf)
	if err != nil {
		panic(err)
	}

	topic := testconf.Topic
	partition := int32(-1)
	size := 100
	pattern := "Hello"
	buf := []byte(strings.Repeat(pattern, size/len(pattern)))

	var done_chan chan int64
	var dr_chan chan Event = nil

	if with_dr {
		done_chan = make(chan int64)
		dr_chan = p.Events
		go delivery_handler(int64(msgcnt), p.Events, done_chan)
	}

	fmt.Printf("%s: produce %d messages\n", testname, msgcnt)

	rd := ratedisp_start(fmt.Sprintf("%s: produce", testname))
	rd_delivery := ratedisp_start(fmt.Sprintf("%s: delivery", testname))

	for i := 0; i < msgcnt; i += 1 {
		m := Message{TopicPartition: TopicPartition{Topic: &topic, Partition: partition}, Value: buf}

		produce_func(p, &m, dr_chan)

		rd.tick(1, int64(size))
	}

	rd.print("produce done: ")

	// Wait for messages in-flight and in-queue to get delivered.
	fmt.Printf("%s: %d messages in queue\n", testname, p.Len())
	r := p.Flush(10000)
	fmt.Printf("%s: %d messages remains in queue after Flush()\n", testname, r)

	// Close producer
	p.Close()

	var delivery_cnt, delivery_size int64

	if with_dr {
		delivery_cnt = <-done_chan
		delivery_size = <-done_chan
	} else {
		delivery_cnt = int64(msgcnt)
		delivery_size = delivery_cnt * int64(size)
	}
	rd_delivery.tick(delivery_cnt, delivery_size)

	rd.print("TOTAL: ")
}

func TestProducerGoPerformance(t *testing.T) {
	msgcnt := 2000000

	producer_perf_test("Function producer (without DR)", msgcnt, false, false,
		func(p *Producer, m *Message, dr_chan chan Event) {
			err := p.Produce(m, dr_chan, nil)
			if err != nil {
				fmt.Printf("%% Produce() failed: %v\n", err)
			}
		})

	producer_perf_test("Function producer (with DR)", msgcnt, true, false,
		func(p *Producer, m *Message, dr_chan chan Event) {
			err := p.Produce(m, dr_chan, nil)
			if err != nil {
				fmt.Printf("%% Produce() failed: %v\n", err)
			}
		})

}

func TestProducerChannelGoPerformance(t *testing.T) {
	msgcnt := 2000000
	producer_perf_test("Channel producer (without DR)", msgcnt, false, false,
		func(p *Producer, m *Message, dr_chan chan Event) {
			p.ProduceChannel <- m
		})

	producer_perf_test("Channel producer (DR, non-batch channel)", msgcnt, true, false,
		func(p *Producer, m *Message, dr_chan chan Event) {
			p.ProduceChannel <- m
		})

	producer_perf_test("Channel producer (DR, batch channel)", msgcnt, true, true,
		func(p *Producer, m *Message, dr_chan chan Event) {
			p.ProduceChannel <- m
		})
}
