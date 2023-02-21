# Notes for Zendesk Developers
This fork of confluent-kafka-go is necessary to support [zendesk_kafka_go](https://github.com/zendesk/zendesk_kafka_go).

This document describes some of the changes that we've made, our attempts to get them merged upstream, and some internal
maintenance details, such as vendoring librdka for static linking.

- [Notes for Zendesk Developers](#notes-for-zendesk-developers)
  - [Changes](#changes)
    - [PollContext()](#pollcontext)
    - [PollPartitionContext()](#pollpartitioncontext)
    - [Adding errors to messages via the DR channel](#adding-errors-to-messages-via-the-dr-channel)
  - [Vendoring librdkafka for static linking](#vendoring-librdkafka-for-static-linking)

## Changes

### PollContext()
We added a PollContext() family of functions to the consumer. Normally, confluent-kafka-go expects you to use the Poll() 
functions to wait for new messages to arrive on your consumer group. However, there is no way to stop waiting for 
messages, other than a timeout. This means that it does not integrate well with the normal Golang lifecycle expectations 
that we've codified in zendesk_service_go; in particular, when the service's context is cancelled, we want it to 
immediately stop polling and shut down. In our fork, we've used the rd_kafka_queue_yield() family of functions in 
librdkafka to achieve this, and used it in a PollContext function to ensure that polling returns immediately if the 
context is canceled.

PollContext() has been pull-requested for upstream here: 
confluentinc/confluent-kafka-go#626

### PollPartitionContext()
We also added a PollPartitionContext() function to allow polling for messages only in a single partition. By default, 
confluent-kafka-go returns messages from any partitions that have messages ready; librdkafka has an option to create 
separate receive queues per partition, but this is not exposed by confluent-kafka-go. We need this method to have more 
control over offset management so that we can implement some of the parallelism options in zendesk_kafka_go.

PollPartitionContext() is based on an open PR already proposed for inclusion in confluent-kafka-go here: 
confluentinc/confluent-kafka-go#456. 

This has been merged into our forked tree of confluent-kafka-go on top of the PollContext() changes. This functionality 
is enabled by setting the `go.enable.read.from.partition.queues` property on the consumer config.

### Adding errors to messages via the DR channel
Previously, producer errors received via the Delivery Report channel were not propagated back to the caller. We've
adjusted this DR handling to enrich the message provided to the DR channel with an error field.

https://github.com/zendesk/confluent-kafka-go/commit/0efd32b9718dfc098028e4c71212940931b78dd9

This functionality is enabled by setting the `go.message.dr.errors` property on the producer config.

## Vendoring librdkafka for static linking
confluent-kafka-go bundles prebuilt statically linked versions of librdkafka. New versions of librdkafka are regularly 
vendored upstream. 

Before going ahead and updating the librdkafka version, please check the release notes for the relevant version for any relevant or breaking changes:
https://github.com/confluentinc/librdkafka/releases

The following steps can be followed to synchronise the vendored dependencies with the upstream repository:
```sh
# refresh old linking files to ensure latest info is used
$ rm -rf kafka/build_*

# add upstream remote
$ git remote add upstream git@github.com:confluentinc/confluent-kafka-go

# sync vendored librdkafka kafka, linking files, and rdkafka generated errors
$ git checkout upstream/master -- $(git ls-tree --name-only -r upstream/master | egrep 'kafka/build_*') kafka/librdkafka_vendor kafka/generated_errors.go

# test with a build
$ go build ./...
```

After running the above steps, simply commit the result and raise a PR.
