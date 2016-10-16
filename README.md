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

See [kafka/README](kafka/README)



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


High-level consumer
-------------------

 * Decide if you want to read messages and events from the `.Events()` channel
   (set `"go.events.channel.enable": true`) or by calling `.Poll()`.

 * Create a Consumer with `kafka.NewConsumer()` providing at
   least the `bootstrap.servers` and `group.id` configuration properties.

 * Call `.Subscribe()` or (`.SubscribeTopics()` to subscribe to multiple topics)
   to join the group with the specified subscription set.
   Subscriptions are atomic, calling `.Subscribe*()` again will leave
   the group and rejoin with the new set of topics.

 * Start reading events and messages from either the `.Events` channel
   or by calling `.Poll()`.

 * When the group has rebalanced each client member is assigned a
   (sub-)set of topic+partitions.
   By default the consumer will start fetching messages for its assigned
   partitions at this point, but your application may enable rebalance
   events to get an insight into what the assigned partitions where
   as well as set the initial offsets. To do this you need to pass
   `"go.application.rebalance.enable": true` to the `NewConsumer()` call
   mentioned above. You will (eventually) see a `kafka.AssignedPartitions` event
   with the assigned partition set. You can optionally modify the initial
   offsets (they'll default to stored offsets and if there are no previously stored
   offsets it will fall back to `"default.topic.config": {"auto.offset.reset": ..}`
   which defaults to the `latest` message) and then call `.Assign(partitions)`
   to start consuming. If you don't need to modify the initial offsets you will
   not need to call `.Assign()`, the client will do so automatically for you if
   you dont.

 * As messages are fetched they will be made available on either the
   `.Events` channel or by calling `.Poll()`, look for event type `*kafka.Message`.

 * Handle messages, events and errors to your liking.

 * When you are done consuming call `.Close()` to commit final offsets
   and leave the consumer group.



Producer
--------

 * Create a Producer with `kafka.NewProducer()` providing at least
   the `bootstrap.servers` configuration properties.

 * Messages may now be produced either by sending a `*kafka.Message`
   on the `.ProduceChannel` or by calling `.Produce()`.

 * Producing is an asynchronous operation so the client notifies the application
   of per-message produce success or failure through something called delivery reports.
   Delivery reports are by default emitted on the `.Events` channel as `*kafka.Message`
   and you should check `msg.TopicPartition.Error` for `nil` to find out if the message
   was succesfully delivered or not.
   It is also possible to direct delivery reports to alternate channels
   by providing a non-nil `chan Event` channel to `.Produce()`.
   If no delivery reports are wanted they can be completely disabled by
   setting configuration property `"go.delivery.reports": false`.

 * When you are done producing messages you will need to make sure all messages
   are indeed delivered to the broker (or failed), remember that this is
   an asynchronous client so some of your messages may be lingering in internal
   channels or tranmission queues.
   To do this you can either keep track of the messages you've produced
   and wait for their corresponding delivery reports, or call the convenience
   function `.Flush()` that will block until all message deliveries are done
   or the provided timeout elapses.

 * Finally call `.Close()` to decommission the producer.


Events
------

Apart from emitting messages and delivery reports the client also communicates
with the application through a number of different event types.
An application may choose to handle or ignore these events.

**Consumer events**:
 * `*kafka.Message` - a fetched message.
 * `AssignedPartitions` - The assigned partition set for this client following a rebalance.
                          Requires `go.application.rebalance.enable`
 * `RevokedPartitions` - The counter part to `AssignedPartitions` following a rebalance.
                         `AssignedPartitions` and `RevokedPartitions` are symetrical.
                         Requires `go.application.rebalance.enable`
 * `PartitionEof` - Consumer has reached the end of a partition.
                    NOTE: The consumer keeps trying to fetch new messages for the partition.

**Producer events**:
 * `*kafka.Message` - delivery report for produced message.
                      Check `.TopicPartition.Error` for delivery result.

**Generic events** for both Consumer and Producer:
 * `KafkaError` - client (error codes are prefixed with _) or broker error.
                  These errors are normally just informational since the
                  client will try its best to automatically recover (eventually).

See the [examples](examples) directory for example implementations of the above.
