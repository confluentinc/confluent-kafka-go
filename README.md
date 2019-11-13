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
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
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
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}
```

Producer

```golang
import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

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

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
```

More elaborate examples are available in the [examples](examples) directory,
including [how to configure](examples/confluent_cloud_example) the Go client
for use with [Confluent Cloud](https://www.confluent.io/confluent-cloud/).


Getting Started
===============

Installing librdkafka
---------------------

This client for Go depends on librdkafka v1.1.0 or later, so you either need to install librdkafka
through your OS/distributions package manager, or download and build it from source.

- For Debian and Ubuntu based distros, install `librdkafka-dev` from the standard
repositories or using [Confluent's Deb repository](http://docs.confluent.io/current/installation.html#installation-apt).
- For Redhat based distros, install `librdkafka-devel` using [Confluent's YUM repository](http://docs.confluent.io/current/installation.html#rpm-packages-via-yum).
- For MacOS X, install `librdkafka` from Homebrew. You may also need to brew install pkg-config if you don't already have it. `brew install librdkafka pkg-config`.
- For Alpine: `apk add librdkafka-dev pkgconf`
- confluent-kafka-go is not supported on Windows.

Build from source:

    git clone https://github.com/edenhill/librdkafka.git
    cd librdkafka
    ./configure --prefix /usr
    make
    sudo make install


Install the client
-------------------

We recommend that you version pin the confluent-kafka-go import to v1:

Manual install:
```bash
go get -u gopkg.in/confluentinc/confluent-kafka-go.v1/kafka
```

Golang import:
```golang
import "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
```

**Note:** that the development of librdkafka and the Go client are kept in sync.
If you use the master branch of the Go client, then you need to use the master branch of
librdkafka.

See the [examples](examples) for usage details.

Using Go 1.13+ Modules
----------------------

Starting with Go 1.13, you can use [Go Modules](https://blog.golang.org/using-go-modules) to install
confluent-kafka-go.

Import the `kafka` package from GitHub in your code:

```golang
import "github.com/confluentinc/confluent-kafka-go/kafka"
```

Build your project:

```bash
go build ./...
```

A dependency to the latest stable version of confluent-kafka-go should be automatically added to
your `go.mod` file.

API Strands
===========

There are two main API strands: function and channel based.

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


Channel Based Consumer (deprecated)
-----------------------------------

*Deprecated*: The channel based consumer is deprecated due to the channel issues
              mentioned below. Use the function based consumer.

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
