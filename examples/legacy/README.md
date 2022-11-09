Legacy examples
===============

This directory contains examples for no longer recommended functionality

Channel-Based Consumer (deprecated)
-----------------------------------

*Deprecated*: The channel-based consumer is deprecated due to the channel issues
              mentioned below. Use the function-based consumer.

Messages, errors and events are posted on the `consumer.Events()` channel
for the application to read.

Pros:

 * Possibly more Golang:ish
 * Makes reading from multiple channels easy
 * Fast

Cons:

 * Outdated events and messages may be consumed due to the buffering nature
   of channels. The extent is limited, but not remedied, by the Events channel
   buffer size (`go.events.channel.size`).

See [consumer_channel_example](consumer_channel_example)

Channel-Based Producer
----------------------

Application writes messages to the `producer.ProducerChannel()`.
Delivery reports are emitted on the `producer.Events()` or specified private channel.

Pros:

 * Go:ish
 * Proper channel backpressure if librdkafka internal queue is full.

Cons:

 * Double queueing: messages are first queued in the channel (size is configurable)
   and then inside librdkafka.
 * Flushing issues if using channel based producer due to a known bug in the implementation.

See [producer_channel_example](producer_channel_example)

Usage example
-------------

    $ cd consumer_channel_example
    $ go build   (or 'go install')
    $ ./consumer_channel_example    # see usage
    $ ./consumer_channel_example mybroker mygroup mytopic
