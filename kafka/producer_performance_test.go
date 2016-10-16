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

func deliveryHandler(b *testing.B, expCnt int64, deliveryChan chan Event, doneChan chan int64) {

	var cnt, size int64

	for ev := range deliveryChan {
		m, ok := ev.(*Message)
		if !ok {
			continue
		}

		if m.TopicPartition.Error != nil {
			b.Errorf("Message delivery error: %v", m.TopicPartition)
			break
		}

		cnt++
		// b.Logf("Delivered %d/%d to %s", cnt, expCnt, m.TopicPartition)

		if m.Value != nil {
			size += int64(len(m.Value))
		}
		if cnt >= expCnt {
			break
		}

	}

	doneChan <- cnt
	doneChan <- size
	close(doneChan)
}

func producerPerfTest(b *testing.B, testname string, msgcnt int, withDr bool, batchProducer bool, silent bool, produceFunc func(p *Producer, m *Message, drChan chan Event)) {

	if !testconfRead() {
		b.Skipf("Missing testconf.json")
	}

	if msgcnt == 0 {
		msgcnt = testconf.PerfMsgCount
	}

	conf := ConfigMap{"bootstrap.servers": testconf.Brokers,
		"go.batch.producer":            batchProducer,
		"go.delivery.reports":          withDr,
		"queue.buffering.max.messages": msgcnt,
		"api.version.request":          "true",
		"broker.version.fallback":      "0.9.0.1",
		"default.topic.config":         ConfigMap{"acks": 1}}

	conf.updateFromTestconf()

	p, err := NewProducer(&conf)
	if err != nil {
		panic(err)
	}

	topic := testconf.Topic
	partition := int32(-1)
	size := testconf.PerfMsgSize
	pattern := "Hello"
	buf := []byte(strings.Repeat(pattern, size/len(pattern)))

	var doneChan chan int64
	var drChan chan Event

	if withDr {
		doneChan = make(chan int64)
		drChan = p.Events()
		go deliveryHandler(b, int64(msgcnt), p.Events(), doneChan)
	}

	if !silent {
		b.Logf("%s: produce %d messages", testname, msgcnt)
	}

	displayInterval := 5.0
	if !silent {
		displayInterval = 1000.0
	}
	rd := ratedispStart(b, fmt.Sprintf("%s: produce", testname), displayInterval)
	rdDelivery := ratedispStart(b, fmt.Sprintf("%s: delivery", testname), displayInterval)

	for i := 0; i < msgcnt; i++ {
		m := Message{TopicPartition: TopicPartition{Topic: &topic, Partition: partition}, Value: buf}

		produceFunc(p, &m, drChan)

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

	var deliveryCnt, deliverySize int64

	if withDr {
		deliveryCnt = <-doneChan
		deliverySize = <-doneChan
	} else {
		deliveryCnt = int64(msgcnt)
		deliverySize = deliveryCnt * int64(size)
	}
	rdDelivery.tick(deliveryCnt, deliverySize)

	rd.print("TOTAL: ")

	b.SetBytes(deliverySize)
}

func BenchmarkProducerFunc(b *testing.B) {
	producerPerfTest(b, "Function producer (without DR)",
		0, false, false, false,
		func(p *Producer, m *Message, drChan chan Event) {
			err := p.Produce(m, drChan)
			if err != nil {
				b.Errorf("Produce() failed: %v", err)
			}
		})
}

func BenchmarkProducerFuncDR(b *testing.B) {
	producerPerfTest(b, "Function producer (with DR)",
		0, true, false, false,
		func(p *Producer, m *Message, drChan chan Event) {
			err := p.Produce(m, drChan)
			if err != nil {
				b.Errorf("Produce() failed: %v", err)
			}
		})
}

func BenchmarkProducerChannel(b *testing.B) {
	producerPerfTest(b, "Channel producer (without DR)",
		0, false, false, false,
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})
}

func BenchmarkProducerChannelDR(b *testing.B) {
	producerPerfTest(b, "Channel producer (with DR)",
		testconf.PerfMsgCount, true, false, false,
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})

}

func BenchmarkProducerBatchChannel(b *testing.B) {
	producerPerfTest(b, "Channel producer (without DR, batch channel)",
		0, false, true, false,
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})
}

func BenchmarkProducerBatchChannelDR(b *testing.B) {
	producerPerfTest(b, "Channel producer (DR, batch channel)",
		0, true, true, false,
		func(p *Producer, m *Message, drChan chan Event) {
			p.ProduceChannel() <- m
		})
}

func BenchmarkProducerInternalMessageInstantiation(b *testing.B) {
	topic := "test"
	buf := []byte(strings.Repeat("Ten bytes!", 10))
	v := 0
	for i := 0; i < b.N; i++ {
		msg := Message{TopicPartition: TopicPartition{Topic: &topic, Partition: 0}, Value: buf}
		v += int(msg.TopicPartition.Partition) // avoid msg unused error
	}
}

func BenchmarkProducerInternalMessageToC(b *testing.B) {
	p, err := NewProducer(&ConfigMap{})
	if err != nil {
		b.Fatalf("NewProducer failed: %s", err)
	}
	b.ResetTimer()
	topic := "test"
	buf := []byte(strings.Repeat("Ten bytes!", 10))
	for i := 0; i < b.N; i++ {
		msg := Message{TopicPartition: TopicPartition{Topic: &topic, Partition: 0}, Value: buf}
		p.handle.messageToCDummy(&msg)
	}
}
