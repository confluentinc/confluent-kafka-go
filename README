Confluent's Apache Kafka client for Golang
==========================================

Confluent's Kafka client for Golang wraps the librdkafka C library, providing
full Kafka protocol support with great performance and reliability.

The Golang bindings provides a high-level Producer and Consumer with support
for the balanced consumer groups of Apache Kafka 0.9.

See the [API documentation: FIXME]()

**License**: [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0)


Examples
========

See [examples](examples)



Prerequisites
=============

 * [librdkafka](https://github.com/edenhill/librdkafka) >= 0.9.2 (or events_api branch)



Build
=====

    $ cd kafka
    $ go install



Tests
=====


**Run unit tests and integration tests:**

*Note*: Please see [kafka/README.test](kafka/README.test)


    $ cd kafka
    $ go test




Introduction
============

The confluent-kafka-go client is currently under early development and the APIs have
not been settled, in fact the client currently contains multiple alternative APIs to
give early reviewers a chance to decide which API is most convenient to use in practice.


Configuration
-------------
Producer and Consumer instances are configured by passing a kafka.ConfigMap pointer
to the NewProducer/NewConsumer() APIs.
The ConfigMap consists of key-value librdkafka config property pairs.
The full range of supported properties depends on the librdkafka version and the
latest set is available here:
[CONFIGURATION.md](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

The value type is string (unless otherwise noted) but native Go types are allowed
as a convenience, the supported value types are:
 string, bool, int, any type with the standard String() interface

Topic configuration is passed in the special `default.topic.config` key where the value
is a sub ConfigMap containing the default topic configuration.

**Required configuration properties for Producer**:

 * bootstrap.servers=broker(s)

**Required configuration for Consumer**:

 * bootstrap.servers=broker(s)
 * group.id=groupname (if using Subscribe(), not required for simple Assign() usage)


**Special configuration properties:**

There are also a number of special configuration properties that control the
Go client itself, they are extracted during instance setup and not passed to librdkafka.

**Producer:**

 * `go.batch.producer` (bool, false) - Enable batch producer (experimental for increased performance).
                                     These batches do not relate to Kafka message batches in any way.
 * `go.delivery.reports` (bool, true) - Disable forwarding of per-message delivery reports to the
                                      Events channel.
 * `go.produce.channel.size` (int, 1000000) - ProduceChannel buffer size (in number of messages)


**Consumer:**

 * `go.application.rebalance.enable` (bool, false) - Forward rebalancing responsibility to application via the Events channel.
                                        If set to true the app must handle the AssignedPartitions and
                                        RevokedPartitions events and call Assign() and Unassign()
                                        respectively.
 * `go.message.timestamp.enable` (bool, false) - Enable extraction of message timestamps for received messages.
                                        (Experimental for increased performance (false)).
 * `go.events.channel.enable` (bool, false) - Enable the Events channel. Messages and events will be pushed on the Events channel and the Poll() interface will be disabled.



Producer
--------

There are two ways to produce messages:

 * the Produce() method, takes a Message pointer and an optional delivery report channel.
 * the ProduceChannel, where Message pointers may be sent.

Both these APIs are asynchronous (the ProduceChannel is buffered) and the success or
failure to produce a message are called delivery reports.
Delivery reports are emitted as Message events either on producer's Events channel
(if `go.delivery.reports` is true) or the optional channel provided to Produce().

The delivery report event's Message (pointer) contains an Error field that the application
should check to determine if the message was produced succesfully or not.
Failed messages are to be considered permanently failed since the underlying librdkafka
library will already have performed retries on temporary errors.

librdkafka will only allow `queue.buffering.max.messages` messages queued internally
at any given time, this limit not only includes the queue of messages to be produced
but also messages that have been produced and awaiting delivery report to be read
by the application.

Due to the asynchronous nature of the producer an application will have to wait for
messages to be fully delivered before exiting, as an alternative to keeping track
of the number of outstanding Messages in flight the application may call the
Flush() API to wait until all messages are delivered (or failed).

Finally, the application should call Close() to clean up the producer state.



Consumer
--------

The Go client implements the standard Kafka client high-level consumer API as
seen in the official Java client as well as librdkafka, confluent-kafka-python, etc.

To start consuming messages there are two alternatives:

 * Subscribe() or SubscribeTopics() - join the configured consumer group and
   start subscribing to the provided topics.
 * Assign() - start consuming the provided topics+partitions without joining a group.

To stop consuming:

 * Unsubscribe()
 * Unassign()


When subscribing to topics a rebalancing operation will performed across all
consumers in the group and partitions will be assigned depending on the configured
partition.assignment.strategy.
The Go client will automatically Assign() the assigned partition set but if the
application wants to know when and which partitions were assigned, and optionally
set initial offsets (e.g., because of an external offset store), it can do so by
either passing a RebalanceCb to Subscribe*() and later call Poll(), or
by listening to AssignedPartitions or RevokedPartitions events on the Events channel
(after configuring `go.application.rebalance.enable` to true).

In the latter case it is required of the application to call Assign(partitions)
on receiving the AssignedPartitions event, or Unassign() on receiving the RevokedPartitions
event. This is required to synchronize state with the underlying library.
It is not required (but allowed) when using a ReblalanceCb).

There are currently two ways to consume messages and events:

 * call Poll() regularily, it will return Message pointers or other events.
 * read the Events channel, Message pointers and other events will be emitted on it.
   (if `go.events.channel.enable` is true)


When the application is finished consuming messages it should call Close() to
cleanly leave the consumer group as well as to make sure final offsets are committed.



Events
------

Apart from the aforementioned events emitted on the Events channel there are also
a number of generic events emitted on the Events channel that an application may
choose to handle, or ignore.

The Consumer may choose to read the Events channel or fetch these events using Poll().
The Producer must read the Events channel.

Producer events:

 * *Message - Delivery report

Consumer events:

 * *Message - Consumed message
 * AssignedPartitions - (if go.application.rebalance.enable=true) Assigned partitions after group rebalance
 * RevokedPartitions - (if go.application.rebalance.enable=true) Revoked partitions after group rebalance
 * PartitionEof - End of partition reached (may reoccur if more messages are produced)

Generic events:

 * KafkaError - indicates a generic error event
 * Statistics - librdkafka statistics (JSON) (NOT IMPLEMENTED)
 * Throttle - broker throttling event (NOT IMPLEMENTED)
