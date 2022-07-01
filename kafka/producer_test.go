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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestProducerAPIs dry-tests all Producer APIs, no broker is needed.
func TestProducerAPIs(t *testing.T) {

	// expected message dr count on events channel
	expMsgCnt := 0
	p, err := NewProducer(&ConfigMap{
		"socket.timeout.ms":         10,
		"message.timeout.ms":        10,
		"go.delivery.report.fields": "key,value,headers"})
	if err != nil {
		t.Fatalf("%s", err)
	}

	t.Logf("Producer %s", p)

	drChan := make(chan Event, 10)

	topic1 := "gotest"
	topic2 := "gotest2"

	// Produce with function, DR on passed drChan
	err = p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic1, Partition: 0},
		Value: []byte("Own drChan"), Key: []byte("This is my key")},
		drChan)
	if err != nil {
		t.Errorf("Produce failed: %s", err)
	}

	// Produce with function, use default DR channel (Events)
	err = p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0},
		Value: []byte("Events DR"), Key: []byte("This is my key")},
		nil)
	if err != nil {
		t.Errorf("Produce failed: %s", err)
	}
	expMsgCnt++

	// Produce with function and timestamp,
	// success depends on librdkafka version
	err = p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0}, Timestamp: time.Now()}, nil)
	numver, strver := LibraryVersion()
	t.Logf("Produce with timestamp on %s returned: %s", strver, err)
	if numver < 0x00090400 {
		if err == nil || err.(Error).Code() != ErrNotImplemented {
			t.Errorf("Expected Produce with timestamp to fail with ErrNotImplemented on %s (0x%x), got: %s", strver, numver, err)
		}
	} else {
		if err != nil {
			t.Errorf("Produce with timestamp failed on %s: %s", strver, err)
		}
	}
	if err == nil {
		expMsgCnt++
	}

	// Produce through ProducerChannel, uses default DR channel (Events),
	// pass Opaque object.
	myOpq := "My opaque"
	p.ProduceChannel() <- &Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0},
		Opaque: &myOpq,
		Value:  []byte("ProducerChannel"), Key: []byte("This is my key")}
	expMsgCnt++

	// Len() will not report messages on private delivery report chans (our drChan for example),
	// so expect at least 2 messages, not 3.
	// And completely ignore the timestamp message.
	if p.Len() < 2 {
		t.Errorf("Expected at least 2 messages (+requests) in queue, only %d reported", p.Len())
	}

	// Message Headers
	varIntHeader := make([]byte, binary.MaxVarintLen64)
	varIntLen := binary.PutVarint(varIntHeader, 123456789)

	myHeaders := []Header{
		{"thisHdrIsNullOrNil", nil},
		{"empty", []byte("")},
		{"MyVarIntHeader", varIntHeader[:varIntLen]},
		{"mystring", []byte("This is a simple string")},
	}

	p.ProduceChannel() <- &Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0},
		Value:   []byte("Headers"),
		Headers: myHeaders}
	expMsgCnt++

	//
	// Now wait for messages to time out so that delivery reports are triggered
	//

	// drChan (1 message)
	ev := <-drChan
	m := ev.(*Message)
	if string(m.Value) != "Own drChan" {
		t.Errorf("DR for wrong message (wanted 'Own drChan'), got %s",
			string(m.Value))
	} else if m.TopicPartition.Error == nil {
		t.Errorf("Expected error for message")
	} else {
		t.Logf("Message %s", m.TopicPartition)
	}
	close(drChan)

	// Events chan (3 messages and possibly events)
	for msgCnt := 0; msgCnt < expMsgCnt; {
		ev = <-p.Events()
		switch e := ev.(type) {
		case *Message:
			msgCnt++
			if (string)(e.Value) == "ProducerChannel" {
				s := e.Opaque.(*string)
				if s != &myOpq {
					t.Errorf("Opaque should point to %v, not %v", &myOpq, s)
				}
				if *s != myOpq {
					t.Errorf("Opaque should be \"%s\", not \"%v\"",
						myOpq, *s)
				}
				t.Logf("Message \"%s\" with opaque \"%s\"\n",
					(string)(e.Value), *s)

			} else if (string)(e.Value) == "Headers" {
				if e.Opaque != nil {
					t.Errorf("Message opaque should be nil, not %v", e.Opaque)
				}
				if !reflect.DeepEqual(e.Headers, myHeaders) {
					t.Errorf("Message headers should be %v, not %v", myHeaders, e.Headers)
				}
			} else {
				if e.Opaque != nil {
					t.Errorf("Message opaque should be nil, not %v", e.Opaque)
				}
			}
		default:
			t.Logf("Ignored event %s", e)
		}
	}

	r := p.Flush(2000)
	if r > 0 {
		t.Errorf("Expected empty queue after Flush, still has %d", r)
	}

	// OffsetsForTimes
	offsets, err := p.OffsetsForTimes([]TopicPartition{{Topic: &topic2, Offset: 12345}}, 100)
	t.Logf("OffsetsForTimes() returned Offsets %s and error %s\n", offsets, err)
	if err == nil {
		t.Errorf("OffsetsForTimes() should have failed\n")
	}
	if offsets != nil {
		t.Errorf("OffsetsForTimes() failed but returned non-nil Offsets: %s\n", offsets)
	}
}

// TestPurgeAPI test if messages are purged successfully
func TestPurgeAPI(t *testing.T) {
	topic := "sometopic"
	unreachableProducer, _ := NewProducer(&ConfigMap{
		"bootstrap.servers": "127.0.0.1:65533",
	})
	purgeDrChan := make(chan Event)
	err := unreachableProducer.Produce(&Message{
		TopicPartition: TopicPartition{
			Topic:     &topic,
			Partition: 0,
		},
		Value: []byte("somevalue"),
	}, purgeDrChan)
	if err != nil {
		t.Errorf("Produce failed: %s", err)
	}

	err = unreachableProducer.Purge(PurgeInFlight | PurgeQueue)
	if err != nil {
		t.Errorf("Failed to purge message: %s", err)
	}

	select {
	case e := <-purgeDrChan:
		purgedMessage := e.(*Message)
		err = purgedMessage.TopicPartition.Error
		if err != nil {
			if err.(Error).Code() != ErrPurgeQueue {
				t.Errorf("Unexpected error after purge api is called: %s", e)
			}
		} else {
			t.Errorf("Purge should have triggered error report")
		}
	case <-time.After(1 * time.Second):
		t.Errorf("No delivery report after purge api is called")
	}

	close(purgeDrChan)
}

// TestProducerBufferSafety verifies issue #24, passing any type of memory backed buffer
// (JSON in this case) to Produce()
func TestProducerBufferSafety(t *testing.T) {

	p, err := NewProducer(&ConfigMap{
		"socket.timeout.ms": 10,
		// Use deprecated default.topic.config here to verify
		// it still works.
		"default.topic.config": ConfigMap{"message.timeout.ms": 10}})
	if err != nil {
		t.Fatalf("%s", err)
	}

	topic := "gotest"
	value, _ := json.Marshal(struct{ M string }{M: "Hello Go!"})
	empty := []byte("")

	// Try combinations of Value and Key: json value, empty, nil
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: value, Key: nil}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: value, Key: value}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: nil, Key: value}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: nil, Key: nil}, nil)

	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: empty, Key: nil}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: empty, Key: empty}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: nil, Key: empty}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: value, Key: empty}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: value, Key: value}, nil)
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: empty, Key: value}, nil)

	// And Headers
	p.Produce(&Message{TopicPartition: TopicPartition{Topic: &topic}, Value: empty, Key: value,
		Headers: []Header{{"hdr", value}, {"hdr2", empty}, {"hdr3", nil}}}, nil)

	p.Flush(100)

	p.Close()
}

// TestProducerInvalidConfig verifies that invalid configuration is handled correctly.
func TestProducerInvalidConfig(t *testing.T) {

	_, err := NewProducer(&ConfigMap{
		"delivery.report.only.error": true,
	})
	if err == nil {
		t.Fatalf("Expected NewProducer() to fail with delivery.report.only.error set")
	}
}

func TestProducerOAuthBearerConfig(t *testing.T) {
	myOAuthConfig := "scope=myscope principal=gotest"

	p, err := NewProducer(&ConfigMap{
		"security.protocol":       "SASL_PLAINTEXT",
		"sasl.mechanisms":         "OAUTHBEARER",
		"sasl.oauthbearer.config": myOAuthConfig,
	})
	if err != nil {
		t.Fatalf("NewProducer failed: %s", err)
	}

	// Wait for initial OAuthBearerTokenRefresh and check
	// that its SerializerConfig string is identical to myOAuthConfig
	for {
		ev := <-p.Events()
		oatr, ok := ev.(OAuthBearerTokenRefresh)
		if !ok {
			continue
		}

		t.Logf("Got %s with SerializerConfig \"%s\"", oatr, oatr.Config)

		if oatr.Config != myOAuthConfig {
			t.Fatalf("%s: Expected .SerializerConfig to be %s, not %s",
				oatr, myOAuthConfig, oatr.Config)
		}

		// Verify that we can set a token
		err = p.SetOAuthBearerToken(OAuthBearerToken{
			TokenValue: "aaaa",
			Expiration: time.Now().Add(time.Second * time.Duration(60)),
			Principal:  "gotest",
		})
		if err != nil {
			t.Fatalf("Failed to set token: %s", err)
		}

		// Verify that we can set a token refresh failure
		err = p.SetOAuthBearerTokenFailure("A token failure test")
		if err != nil {
			t.Fatalf("Failed to set token failure: %s", err)
		}

		break
	}

	p.Close()
}

func TestProducerLog(t *testing.T) {
	p, err := NewProducer(&ConfigMap{
		"debug":                  "all",
		"go.logs.channel.enable": true,
		"socket.timeout.ms":      10,
		"default.topic.config":   ConfigMap{"message.timeout.ms": 10}})
	if err != nil {
		t.Fatalf("%s", err)
	}

	expectedLogs := map[struct {
		tag     string
		message string
	}]bool{
		{"INIT", "librdkafka"}: false,
	}

	go func() {
		for {
			select {
			case log, ok := <-p.Logs():
				if !ok {
					return
				}

				t.Log(log.String())

				for expectedLog, found := range expectedLogs {
					if found {
						continue
					}
					if log.Tag != expectedLog.tag {
						continue
					}
					if strings.Contains(log.Message, expectedLog.message) {
						expectedLogs[expectedLog] = true
					}
				}
			}
		}
	}()

	<-time.After(time.Second * 5)
	p.Close()

	for expectedLog, found := range expectedLogs {
		if !found {
			t.Errorf(
				"Expected to find log with tag `%s' and message containing `%s',"+
					" but didn't find any.",
				expectedLog.tag,
				expectedLog.message)
		}
	}
}

// TestTransactionalAPI test the transactional producer API
func TestTransactionalAPI(t *testing.T) {
	p, err := NewProducer(&ConfigMap{
		"bootstrap.servers":      "127.0.0.1:65533",
		"transactional.id":       "test",
		"transaction.timeout.ms": "4000",
	})
	if err != nil {
		t.Fatalf("Failed to create transactional producer: %v", err)
	}

	//
	// Call InitTransactions() with explicit timeout and check that
	// it times out accordingly.
	//
	maxDuration, err := time.ParseDuration("2s")
	if err != nil {
		t.Fatalf("%s", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	start := time.Now()
	err = p.InitTransactions(ctx)
	duration := time.Now().Sub(start).Seconds()

	t.Logf("InitTransactions(%v) returned '%v' in %.2fs",
		maxDuration, err, duration)
	if err.(Error).Code() != ErrTimedOut {
		t.Errorf("Expected ErrTimedOut, not %v", err)
	} else if duration < maxDuration.Seconds()*0.8 ||
		duration > maxDuration.Seconds()*1.2 {
		t.Errorf("InitTransactions() should have finished within "+
			"%.2f +-20%%, not %.2f",
			maxDuration.Seconds(), duration)
	}

	//
	// Call InitTransactions() without timeout, which makes it
	// default to the transaction.timeout.ms.
	// NOTE: cancelling the context currently does not work.
	//
	maxDuration, err = time.ParseDuration("4s") // transaction.tiemout.ms
	if err != nil {
		t.Fatalf("%s", err)
	}
	ctx = context.TODO()

	start = time.Now()
	err = p.InitTransactions(ctx)
	duration = time.Now().Sub(start).Seconds()

	t.Logf("InitTransactions() returned '%v' in %.2fs", err, duration)
	if err.(Error).Code() != ErrTimedOut {
		t.Errorf("Expected ErrTimedOut, not %v", err)
	} else if duration < maxDuration.Seconds()*0.8 ||
		duration > maxDuration.Seconds()*1.2 {
		t.Errorf("InitTransactions() should have finished within "+
			"%.2f +-20%%, not %.2f",
			maxDuration.Seconds(), duration)
	}

	// And again with a nil context
	start = time.Now()
	err = p.InitTransactions(nil)
	duration = time.Now().Sub(start).Seconds()

	t.Logf("InitTransactions() returned '%v' in %.2fs", err, duration)
	if err.(Error).Code() != ErrTimedOut {
		t.Errorf("Expected ErrTimedOut, not %v", err)
	} else if duration < maxDuration.Seconds()*0.8 ||
		duration > maxDuration.Seconds()*1.2 {
		t.Errorf("InitTransactions() should have finished within "+
			"%.2f +-20%%, not %.2f",
			maxDuration.Seconds(), duration)
	}

	//
	// All sub-sequent APIs should fail (unless otherwise noted)
	// since InitTransactions() has not succeeded.
	//
	maxDuration, err = time.ParseDuration("1s") // Should fail quickly, not by timeout
	if err != nil {
		t.Fatalf("%s", err)
	}
	ctx = context.TODO()

	// Perform multiple iterations to make sure API behaviour is consistent.
	for iter := 0; iter < 5; iter++ {

		if iter == 4 {
			// Last iteration, pass context as nil
			ctx = nil
		}

		// BeginTransaction
		what := "BeginTransaction"
		start = time.Now()
		err = p.BeginTransaction()
		duration = time.Now().Sub(start).Seconds()

		t.Logf("%s() returned '%v' in %.2fs", what, err, duration)
		if err == nil || err.(Error).Code() == ErrTimedOut {
			t.Errorf("Expected %s() to fail due to state, not %v", what, err)
		}

		// SendOffsetsToTransaction
		what = "SendOffsetsToTransaction"
		topic := "myTopic"
		start = time.Now()
		cgmd, err := NewTestConsumerGroupMetadata("myConsumerGroup")
		if err != nil {
			t.Fatalf("Failed to create group metadata: %v", err)
		}
		err = p.SendOffsetsToTransaction(ctx,
			[]TopicPartition{
				{Topic: &topic, Partition: 1, Offset: 123},
				{Topic: &topic, Partition: 0, Offset: 4567890},
			},
			cgmd)
		duration = time.Now().Sub(start).Seconds()

		t.Logf("%s() returned '%v' in %.2fs", what, err, duration)
		if err == nil || err.(Error).Code() == ErrTimedOut {
			t.Errorf("Expected %s() to fail due to state, not %v", what, err)
		}

		what = "SendOffsetsToTransaction(nil offsets)"
		start = time.Now()
		err = p.SendOffsetsToTransaction(ctx, nil, cgmd)
		duration = time.Now().Sub(start).Seconds()

		t.Logf("%s() returned '%v' in %.2fs", what, err, duration)
		if err == nil || err.(Error).Code() != ErrInvalidArg {
			t.Errorf("Expected %s() to fail due to bad args, not %v", what, err)
		}

		what = "SendOffsetsToTransaction(empty offsets, empty group)"
		cgmdEmpty, err := NewTestConsumerGroupMetadata("")
		if err != nil {
			t.Fatalf("Failed to create group metadata: %v", err)
		}
		start = time.Now()
		err = p.SendOffsetsToTransaction(ctx, []TopicPartition{}, cgmdEmpty)
		duration = time.Now().Sub(start).Seconds()

		t.Logf("%s() returned '%v' in %.2fs", what, err, duration)
		if err != nil {
			t.Errorf("Expected %s() to succeed as a no-op, but got %v", what, err)
		}

		what = "SendOffsetsToTransaction(empty offsets)"
		start = time.Now()
		err = p.SendOffsetsToTransaction(ctx, []TopicPartition{}, cgmd)
		duration = time.Now().Sub(start).Seconds()

		t.Logf("%s() returned '%v' in %.2fs", what, err, duration)
		if err != nil {
			t.Errorf("Expected %s() to succeed as a no-op, but got %v", what, err)
		}

		// AbortTransaction
		what = "AbortTransaction"
		start = time.Now()
		err = p.AbortTransaction(ctx)
		duration = time.Now().Sub(start).Seconds()

		t.Logf("%s() returned '%v' in %.2fs", what, err, duration)
		if err == nil || err.(Error).Code() == ErrTimedOut {
			t.Errorf("Expected %s() to fail due to state, not %v", what, err)
		}

		// CommitTransaction
		what = "CommitTransaction"
		start = time.Now()
		err = p.CommitTransaction(ctx)
		duration = time.Now().Sub(start).Seconds()

		t.Logf("%s() returned '%v' in %.2fs", what, err, duration)
		if err == nil || err.(Error).Code() == ErrTimedOut {
			t.Errorf("Expected %s() to fail due to state, not %v", what, err)
		}
	}

	p.Close()
}

// TestProducerDeliveryReportFields tests the `go.delivery.report.fields` config setting
func TestProducerDeliveryReportFields(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		runProducerDeliveryReportFieldTest(t, &ConfigMap{
			"socket.timeout.ms":         10,
			"message.timeout.ms":        10,
			"go.delivery.report.fields": "",
		}, func(expected, actual *Message) {
			if len(actual.Key) > 0 {
				t.Errorf("key should not be set")
			}
			if len(actual.Value) > 0 {
				t.Errorf("value should not be set")
			}
			if s, ok := actual.Opaque.(*string); ok {
				if *s != *(expected.Opaque.(*string)) {
					t.Errorf("Opaque should be \"%v\", not \"%v\"", expected.Opaque, actual.Opaque)
				}
			} else {
				t.Errorf("opaque value should be a string, not \"%v\"", actual.Opaque)
			}
		})
	})
	t.Run("single", func(t *testing.T) {
		runProducerDeliveryReportFieldTest(t, &ConfigMap{
			"socket.timeout.ms":         10,
			"message.timeout.ms":        10,
			"go.delivery.report.fields": "key",
		}, func(expected, actual *Message) {
			if !bytes.Equal(expected.Key, actual.Key) {
				t.Errorf("key should be \"%s\", not \"%s\"", expected.Key, actual.Key)
			}
			if len(actual.Value) > 0 {
				t.Errorf("value should not be set")
			}
			if s, ok := actual.Opaque.(*string); ok {
				if *s != *(expected.Opaque.(*string)) {
					t.Errorf("Opaque should be \"%v\", not \"%v\"", expected.Opaque, actual.Opaque)
				}
			} else {
				t.Errorf("opaque value should be a string, not \"%v\"", actual.Opaque)
			}
		})
	})
	t.Run("multiple", func(t *testing.T) {
		runProducerDeliveryReportFieldTest(t, &ConfigMap{
			"socket.timeout.ms":         10,
			"message.timeout.ms":        10,
			"go.delivery.report.fields": "key,value",
		}, func(expected, actual *Message) {
			if !bytes.Equal(expected.Key, actual.Key) {
				t.Errorf("key should be \"%s\", not \"%s\"", expected.Key, actual.Key)
			}
			if !bytes.Equal(expected.Value, actual.Value) {
				t.Errorf("value should be \"%s\", not \"%s\"", expected.Value, actual.Value)
			}
			if s, ok := actual.Opaque.(*string); ok {
				if *s != *(expected.Opaque.(*string)) {
					t.Errorf("Opaque should be \"%v\", not \"%v\"", expected.Opaque, actual.Opaque)
				}
			} else {
				t.Errorf("opaque value should be a string, not \"%v\"", actual.Opaque)
			}
		})
	})
	t.Run("default", func(t *testing.T) {
		runProducerDeliveryReportFieldTest(t, &ConfigMap{
			"socket.timeout.ms":  10,
			"message.timeout.ms": 10,
		}, func(expected, actual *Message) {
			if !bytes.Equal(expected.Key, actual.Key) {
				t.Errorf("key should be \"%s\", not \"%s\"", expected.Key, actual.Key)
			}
			if !bytes.Equal(expected.Value, actual.Value) {
				t.Errorf("value should be \"%s\", not \"%s\"", expected.Value, actual.Value)
			}
			if actual.Headers != nil {
				t.Errorf("Did not expect Headers")
			}
			if s, ok := actual.Opaque.(*string); ok {
				if *s != *(expected.Opaque.(*string)) {
					t.Errorf("Opaque should be \"%v\", not \"%v\"", expected.Opaque, actual.Opaque)
				}
			} else {
				t.Errorf("opaque value should be a string, not \"%v\"", actual.Opaque)
			}
		})
	})
	t.Run("all", func(t *testing.T) {
		runProducerDeliveryReportFieldTest(t, &ConfigMap{
			"socket.timeout.ms":         10,
			"message.timeout.ms":        10,
			"go.delivery.report.fields": "all",
		}, func(expected, actual *Message) {
			if !bytes.Equal(expected.Key, actual.Key) {
				t.Errorf("key should be \"%s\", not \"%s\"", expected.Key, actual.Key)
			}
			if !bytes.Equal(expected.Value, actual.Value) {
				t.Errorf("value should be \"%s\", not \"%s\"", expected.Value, actual.Value)
			}
			if actual.Headers == nil {
				t.Errorf("Expected headers")
			}
			if !reflect.DeepEqual(expected.Headers, actual.Headers) {
				t.Errorf("Headers mismatch: Expected %v, got %v",
					expected.Headers, actual.Headers)
			}
			if s, ok := actual.Opaque.(*string); ok {
				if *s != *(expected.Opaque.(*string)) {
					t.Errorf("Opaque should be \"%v\", not \"%v\"", expected.Opaque, actual.Opaque)
				}
			} else {
				t.Errorf("opaque value should be a string, not \"%v\"", actual.Opaque)
			}
		})
	})
}

func runProducerDeliveryReportFieldTest(t *testing.T, config *ConfigMap, fn func(expected, actual *Message)) {
	p, err := NewProducer(config)
	if err != nil {
		t.Fatalf("%s", err)
	}

	topic1 := "gotest"

	myOpq := "My opaque"
	expected := &Message{
		TopicPartition: TopicPartition{Topic: &topic1, Partition: 0},
		Opaque:         &myOpq,
		Value:          []byte("ProducerChannel"),
		Key:            []byte("This is my key"),
		Headers: []Header{
			{"hdr1", []byte("value1")},
			{"hdr2", []byte("value2")},
			{"hdr2", []byte("headers are not unique")}},
	}
	p.ProduceChannel() <- expected

	// We should expect a single message and possibly events
	for msgCnt := 0; msgCnt < 1; {
		ev := <-p.Events()
		switch e := ev.(type) {
		case *Message:
			msgCnt++
			fn(expected, e)
		default:
			t.Logf("Ignored event %s", e)
		}
	}

	r := p.Flush(2000)
	if r > 0 {
		t.Errorf("Expected empty queue after Flush, still has %d", r)
	}
}
