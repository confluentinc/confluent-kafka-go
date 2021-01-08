# Confluent's Golang client for Apache Kafka

## v1.6.1

v1.6.1 is a feature release:

 * KIP-429: Incremental consumer rebalancing - see [cooperative_consumer_example.go](examples/cooperative_consumer_example/cooperative_consumer_example.go)
   for an example how to use the new incremental rebalancing consumer.
 * KIP-480: Sticky producer partitioner - increase throughput and decrease
   latency by sticking to a single random partition for some time.
 * KIP-447: Scalable transactional producer - a single transaction producer can
   now be used for multiple input partitions.

confluent-kafka-go is based on and bundles librdkafka v1.6.1, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.6.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.

### Enhancements

 * `go.delivery.report.fields=all,key,value,none` can now be used to
   avoid copying message key and/or value to the delivery report, improving
   performance in high-throughput applications (by @kevinconaway).


### Fixes

 * Consumer.Close() previously did not trigger the final RevokePartitions
   callback, this is now fixed.



## v1.5.2

v1.5.2 is a maintenance release with the following fixes and enhancements:

 - Bundles librdkafka v1.5.2 - see release notes for all enhancements and fixes.
 - Documentation fixes

confluent-kafka-go is based on librdkafka v1.5.2, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.5.2)
for a complete list of changes, enhancements, fixes and upgrade considerations.

