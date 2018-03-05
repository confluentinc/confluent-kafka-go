Confluent's Golang Client for Apache Kafka<sup>TM</sup>
=====================================================

**confluent-kafka-go** is Confluent's Golang client for [Apache Kafka](http://kafka.apache.org/) and the
[Confluent Platform](https://www.confluent.io/product/compare/).

Features:

- **High performance** - confluent-kafka-go is a lightweight wrapper around
[librdkafka](https://github.com/edenhill/librdkafka), a finely tuned C
client.

- **Reliability** - There are a lot of details to get right when writing an Apache Kafka
client. We get them right in one place (librdkafka) and leverage this work
across all of our clients (also [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python)
and [confluent-kafka-dotnet](https://github.com/confluentinc/confluent-kafka-dotnet)).

- **Supported** - Commercial support is offered by
[Confluent](https://confluent.io/).

- **Future proof** - Confluent, founded by the
creators of Kafka, is building a [streaming platform](https://www.confluent.io/product/compare/)
with Apache Kafka at its core. It's high priority for us that client features keep
pace with core Apache Kafka and components of the [Confluent Platform](https://www.confluent.io/product/compare/).


The Golang bindings provides a high-level Producer and Consumer with support
for the balanced consumer groups of Apache Kafka 0.9 and above.

See the [API documentation](http://docs.confluent.io/current/clients/confluent-kafka-go/index.html) for more information.

**License**: [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)


Examples
========

High-level balanced consumer

```golang
import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"myTopic", "^aRegex.*[Tt]opic"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}

	c.Close()
}
```

Producer

```golang
import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "myTopic"
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries
	p.Flush(15 * 1000)
}
```

More elaborate examples are available in the [examples](examples) directory.


Getting Started
===============

Installing librdkafka
---------------------

This client for Go depends on librdkafka v0.11.4 or later, so you either need to install librdkafka through your OS/distributions package manager,
or download and build it from source.

- For Debian and Ubuntu based distros, install `librdkafka-dev` from the standard
repositories or using [Confluent's Deb repository](http://docs.confluent.io/current/installation.html#installation-apt).
- For Redhat based distros, install `librdkafka-devel` using [Confluent's YUM repository](http://docs.confluent.io/current/installation.html#rpm-packages-via-yum).
- For MacOS X, install `librdkafka` from Homebrew.  You may also need to brew install pkg-config if you don't already have it.
- For Windows, see the `librdkafka.redist` NuGet package.

Build from source:

    git clone https://github.com/edenhill/librdkafka.git
    cd librdkafka
    ./configure --prefix /usr
    make
    sudo make install


Install the client
-------------------

```
go get -u github.com/confluentinc/confluent-kafka-go/kafka
```

See the [examples](examples) for usage details.

Note that the development of librdkafka and the Go client are kept in synch. So
if you use HEAD on master of the Go client, then you need to use HEAD on master of
librdkafka. See this [issue](https://github.com/confluentinc/confluent-kafka-go/issues/61#issuecomment-303746159) for more details.

API Strands
===========

There are two main API strands: channel based or function based.

Channel Based Consumer
----------------------

Messages, errors and events are posted on the consumer.Events channel
for the application to read.

Pros:

 * Possibly more Golang:ish
 * Makes reading from multiple channels easy
 * Fast

Cons:

 * Outdated events and messages may be consumed due to the buffering nature
   of channels. The extent is limited, but not remedied, by the Events channel
   buffer size (`go.events.channel.size`).

See [examples/consumer_channel_example](examples/consumer_channel_example)



Function Based Consumer
-----------------------

Messages, errors and events are polled through the consumer.Poll() function.

Pros:

 * More direct mapping to underlying librdkafka functionality.

Cons:

 * Makes it harder to read from multiple channels, but a go-routine easily
   solves that (see Cons in channel based consumer above about outdated events).
 * Slower than the channel consumer.

See [examples/consumer_example](examples/consumer_example)



Channel Based Producer
----------------------

Application writes messages to the producer.ProducerChannel.
Delivery reports are emitted on the producer.Events or specified private channel.

Pros:

 * Go:ish
 * Proper channel backpressure if librdkafka internal queue is full.

Cons:

 * Double queueing: messages are first queued in the channel (size is configurable)
   and then inside librdkafka.

See [examples/producer_channel_example](examples/producer_channel_example)


Function Based Producer
-----------------------

Application calls producer.Produce() to produce messages.
Delivery reports are emitted on the producer.Events or specified private channel.

Pros:

 * Go:ish

Cons:

 * Produce() is a non-blocking call, if the internal librdkafka queue is full
   the call will fail.
 * Somewhat slower than the channel producer.

See [examples/producer_example](examples/producer_example)


Static Builds
=============

**NOTE**: Requires pkg-config

To link your application statically with librdkafka append `-tags static` to
your application's `go build` command, e.g.:

    $ cd kafkatest/go_verifiable_consumer
    $ go build -tags static

This will create a binary with librdkafka statically linked, do note however
that any librdkafka dependencies (such as ssl, sasl2, lz4, etc, depending
on librdkafka build configuration) will be linked dynamically and thus required
on the target system.

To create a completely static binary append `-tags static_all` instead.
This requires all dependencies to be available as static libraries
(e.g., libsasl2.a). Static libraries are typically not installed
by default but are available in the corresponding `..-dev` or `..-devel`
packages (e.g., libsasl2-dev).

After a succesful static build verify the dependencies by running
`ldd ./your_program` (or `otool -L ./your_program` on OSX), librdkafka should not be listed.


Tests
=====

See [kafka/README](kafka/README.md)

Contributing
------------
Contributions to the code, examples, documentation, et.al, are very much appreciated.

Make your changes, run gofmt, tests, etc, push your branch, create a PR, and [sign the CLA](http://clabot.confluent.io/cla).
