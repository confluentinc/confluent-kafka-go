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

For a step-by-step guide on using the client see [Getting Started with Apache Kafka and Golang](https://developer.confluent.io/get-started/go/).



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
	"github.com/confluentinc/confluent-kafka-go/kafka"
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

Supports Go 1.11+ and librdkafka 1.9.0+.

Using Go Modules
----------------

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

If you are building for Alpine Linux (musl), `-tags musl` must be specified.

```bash
go build -tags musl ./...
```

A dependency to the latest stable version of confluent-kafka-go should be automatically added to
your `go.mod` file.

Install the client
------------------

Manual install:
```bash
go get -u github.com/confluentinc/confluent-kafka-go/kafka
```

Golang import:
```golang
import "github.com/confluentinc/confluent-kafka-go/kafka"
```

librdkafka
----------

Prebuilt librdkafka binaries are included with the Go client and librdkafka
does not need to be installed separately on the build or target system.
The following platforms are supported by the prebuilt librdkafka binaries:

 * Mac OSX x64
 * glibc-based Linux x64 (e.g., RedHat, Debian, CentOS, Ubuntu, etc) - without GSSAPI/Kerberos support
 - musl-based Linux 64 (Alpine) - without GSSAPI/Kerberos support

When building your application for Alpine Linux (musl libc) you must pass
`-tags musl` to `go get`, `go build`, etc.

`CGO_ENABLED` must NOT be set to `0` since the Go client is based on the
C library librdkafka.

If GSSAPI/Kerberos authentication support is required you will need
to install librdkafka separately, see the **Installing librdkafka** chapter
below, and then build your Go application with `-tags dynamic`.

Installing librdkafka
---------------------

If the bundled librdkafka build is not supported on your platform, or you
need a librdkafka with GSSAPI/Kerberos support, you must install librdkafka
manually on the build and target system using one of the following alternatives:

- For Debian and Ubuntu based distros, install `librdkafka-dev` from the standard
repositories or using [Confluent's Deb repository](http://docs.confluent.io/current/installation.html#installation-apt).
- For Redhat based distros, install `librdkafka-devel` using [Confluent's YUM repository](http://docs.confluent.io/current/installation.html#rpm-packages-via-yum).
- For MacOS X, install `librdkafka` from Homebrew. You may also need to brew install pkg-config if you don't already have it: `brew install librdkafka pkg-config`.
- For Alpine: `apk add librdkafka-dev pkgconf`
- confluent-kafka-go is not supported on Windows.
- For source builds, see instructions below.

Build from source:

    git clone https://github.com/edenhill/librdkafka.git
    cd librdkafka
    ./configure
    make
    sudo make install

After installing librdkafka you will need to build your Go application
with `-tags dynamic`.

**Note:** If you use the `master` branch of the Go client, then you need to use
          the `master` branch of librdkafka.

**confluent-kafka-go requires librdkafka v1.9.0 or later.**


API Strands
===========

The recommended API strand is the Function-Based one,
the Channel-Based one is documented in [examples/legacy](examples/legacy).

Function-Based Consumer
-----------------------

Messages, errors and events are polled through the `consumer.Poll()` function.

It has direct mapping to underlying librdkafka functionality.

See [examples/consumer_example](examples/consumer_example)

Function-Based Producer
-----------------------

Application calls `producer.Produce()` to produce messages.
Delivery reports are emitted on the `producer.Events()` or specified private channel.

_Warnings_

 * `Produce()` is a non-blocking call, if the internal librdkafka queue is full
   the call will fail and can be retried.

See [examples/producer_example](examples/producer_example)

License
=======

[Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)

KAFKA is a registered trademark of The Apache Software Foundation and has been licensed for use
by confluent-kafka-go. confluent-kafka-go has no affiliation with and is not endorsed by The Apache
Software Foundation.

Developer Notes
===============

See [kafka/README](kafka/README.md)

Contributions to the code, examples, documentation, et.al, are very much appreciated.

Make your changes, run `gofmt`, tests, etc, push your branch, create a PR, and [sign the CLA](http://clabot.confluent.io/cla).

Confluent Cloud
===============

For a step-by-step guide on using the Golang client with Confluent Cloud see [Getting Started with Apache Kafka and Golang](https://developer.confluent.io/get-started/go/) on [Confluent Developer](https://developer.confluent.io/). 
