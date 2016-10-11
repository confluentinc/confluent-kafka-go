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

func delivery_handler(b *testing.B, exp_cnt int64, delivery_chan chan Event, done_chan chan int64) {

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

		cnt += 1
		// b.Logf("Delivered %d/%d to %s", cnt, exp_cnt, m.TopicPartition)

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

func producer_perf_test(b *testing.B, testname string, msgcnt int, with_dr bool, batch_producer bool, silent bool, produce_func func(p *Producer, m *Message, dr_chan chan Event)) {

	if !testconf_read() {
		b.Skipf("Missing testconf.json")
	}

	if msgcnt == 0 {
		msgcnt = testconf.PerfMsgCount
	}

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"go.batch.producer":            batch_producer,
		"go.delivery.reports":          with_dr,
		"queue.buffering.max.messages": msgcnt,
		"api.version.request":          "true",
		"broker.version.fallback":      "0.9.0.1",
		"default.topic.config":         ConfigMap{"acks": 1}}

	conf.update_from_testconf()

	p, err := NewProducer(&conf)
	if err != nil {
		panic(err)
	}

	topic := testconf.Topic
	partition := int32(-1)
	size := testconf.PerfMsgSize
	pattern := "Hello"
	buf := []byte(strings.Repeat(pattern, size/len(pattern)))

	var done_chan chan int64
	var dr_chan chan Event = nil

	if with_dr {
		done_chan = make(chan int64)
		dr_chan = p.Events
		go delivery_handler(b, int64(msgcnt), p.Events, done_chan)
	}

	if !silent {
		b.Logf("%s: produce %d messages", testname, msgcnt)
	}

	display_interval := 5.0
	if !silent {
		display_interval = 1000.0
	}
	rd := ratedisp_start(b, fmt.Sprintf("%s: produce", testname), display_interval)
	rd_delivery := ratedisp_start(b, fmt.Sprintf("%s: delivery", testname), display_interval)

	for i := 0; i < msgcnt; i += 1 {
		m := Message{TopicPartition: TopicPartition{Topic: &topic, Partition: partition}, Value: buf}

		produce_func(p, &m, dr_chan)

		rd.tick(1, int64(size))
	}

	if !silent {
		rd.print("produce done: ")
	}

	// Wait for messages in-flight and in-queue to get delivered.
	if !silent {
		b.Logf("%s: %d messages in queue", testname, p.Len())
	}
	r := p.Flush(10000)
	if r > 0 {
		b.Errorf("%s: %d messages remains in queue after Flush()", testname, r)
	}

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

	b.SetBytes(delivery_size)
}

func BenchmarkProducerFunc(b *testing.B) {
	producer_perf_test(b, "Function producer (without DR)",
		0, false, false, false,
		func(p *Producer, m *Message, dr_chan chan Event) {
			err := p.Produce(m, dr_chan, nil)
			if err != nil {
				b.Errorf("Produce() failed: %v", err)
			}
		})
}

func BenchmarkProducerFuncDR(b *testing.B) {
	producer_perf_test(b, "Function producer (with DR)",
		0, true, false, false,
		func(p *Producer, m *Message, dr_chan chan Event) {
			err := p.Produce(m, dr_chan, nil)
			if err != nil {
				b.Errorf("Produce() failed: %v", err)
			}
		})
}

func BenchmarkProducerChannel(b *testing.B) {
	producer_perf_test(b, "Channel producer (without DR)",
		0, false, false, false,
		func(p *Producer, m *Message, dr_chan chan Event) {
			p.ProduceChannel <- m
		})
}

func BenchmarkProducerChannelDR(b *testing.B) {
	producer_perf_test(b, "Channel producer (with DR)",
		testconf.PerfMsgCount, true, false, false,
		func(p *Producer, m *Message, dr_chan chan Event) {
			p.ProduceChannel <- m
		})

}

func BenchmarkProducerBatchChannel(b *testing.B) {
	producer_perf_test(b, "Channel producer (without DR, batch channel)",
		0, false, true, false,
		func(p *Producer, m *Message, dr_chan chan Event) {
			p.ProduceChannel <- m
		})
}

func BenchmarkProducerBatchChannelDR(b *testing.B) {
	producer_perf_test(b, "Channel producer (DR, batch channel)",
		0, true, true, false,
		func(p *Producer, m *Message, dr_chan chan Event) {
			p.ProduceChannel <- m
		})
}

func BenchmarkProducerInternal_MessageInstantiation(b *testing.B) {
	topic := "test"
	buf := []byte(strings.Repeat("Ten bytes!", 10))
	v := 0
	for i := 0; i < b.N; i += 1 {
		msg := Message{TopicPartition: TopicPartition{Topic: &topic, Partition: 0}, Value: buf}
		v += int(msg.TopicPartition.Partition) // avoid msg unused error
	}
}

func BenchmarkProducerInternal_message_to_c(b *testing.B) {
	p, err := NewProducer(&ConfigMap{})
	if err != nil {
		b.Fatalf("NewProducer failed: %s", err)
	}
	b.ResetTimer()
	topic := "test"
	buf := []byte(strings.Repeat("Ten bytes!", 10))
	for i := 0; i < b.N; i += 1 {
		msg := Message{TopicPartition: TopicPartition{Topic: &topic, Partition: 0}, Value: buf}
		p.handle.message_to_c_dummy(&msg)
	}
}
