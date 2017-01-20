Confluent's Apache Kafka client for Golang
==========================================

Confluent's Kafka client for Golang wraps the librdkafka C library, providing
full Kafka protocol support with great performance and reliability.

The Golang bindings provides a high-level Producer and Consumer with support
for the balanced consumer groups of Apache Kafka 0.9 and above.

**License**: [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)


Beta information
================
The Go client is currently in beta and APIs are subject to (minor) change.

API strands
===========
There are two main API strands: channel based or function based.

Channel based consumer
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



Function based consumer
-----------------------

Messages, errors and events are polled through the consumer.Poll() function.

Pros:

 * More direct mapping to underlying librdkafka functionality.

Cons:

 * Makes it harder to read from multiple channels, but a go-routine easily
   solves that (see Cons in channel based consumer above about outdated events).
 * Slower than the channel consumer.

See [examples/consumer_example](examples/consumer_example)



Channel based producer
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


Function based producer
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







Usage
=====

See [examples](examples)



Prerequisites
=============

 * [librdkafka](https://github.com/edenhill/librdkafka) >= 0.9.2 (or `master` branch => 2016-08-16)



Build
=====

    $ cd kafka
    $ go install


Static builds
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
`ldd ./your_program`, librdkafka should not be listed.



Tests
=====

See [kafka/README](kafka/README.md)



Getting started
===============

Installing librdkafka
---------------------

    git clone https://github.com/edenhill/librdkafka.git
    cd librdkafka
    ./configure --prefix /usr
    make
    sudo make install


Build the Go client
-------------------

From the confluent-kafka-go directory which should reside
in `$GOPATH/src/github.com/confluentinc/confluent-kafka-go`:

    cd kafka
    go install


Contributing
------------
Contributions to the code, examples, documentation, et.al, are very much appreciated.

Make your changes, run gofmt, tests, etc, push your branch, create a PR, and [sign the CLA](http://clabot.confluent.io/cla).
