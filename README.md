Confluent's Apache Kafka client for Golang
==========================================

**WARNING: This client is in initial development, NOT FOR PRODUCTION USE**

Confluent's Kafka client for Golang wraps the librdkafka C library, providing
full Kafka protocol support with great performance and reliability.

The Golang bindings provides a high-level Producer and Consumer with support
for the balanced consumer groups of Apache Kafka 0.9.

See the [API documentation: FIXME]()

**License**: [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)


Early preview information
=========================

The Go client is currently under heavy initial development and is not
ready for production use. APIs are not be considered stable.

As an excercise for early birds the Go client currently provides
a number of possibly competing interfaces to various functionality.
Your feedback is highly valuable to us on which APIs that should go into
the final client.

There are two main strands: channel based or function based.

Channel based consumer
----------------------

Messages, errors and events are posted on the consumer.Events channel
for the application to read.

Pros:

 * Possibly more Golang:ish
 * Makes reading from multiple channels easy

Cons:

 * ?

See [examples/consumer_channel_example](examples/consumer_channel_example)



Function based consumer
-----------------------

Messages, errors and events are polled through the consumer.Poll() function.

Pros:

 * More direct mapping to underlying librdkafka functionality.

Cons:

 * Makes it harder to read from multiple channels, but a go-routine easily solves that.

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

See [examples/producer_example](examples/producer_example)







Usage
=====

See [examples](examples)



Prerequisites
=============

 * [librdkafka](https://github.com/edenhill/librdkafka) >= 0.9.2 (or `events_api` branch)



Build
=====

    $ cd kafka
    $ go install



Tests
=====

See [kafka/README.test](kafka/README.test)
