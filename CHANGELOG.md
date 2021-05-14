# Confluent's Golang client for Apache Kafka

## v1.9.0

This is a feature release:

 * OAUTHBEARER OIDC support
 * Added MockCluster for functional testing of applications without the need
   for a real Kafka cluster (by @SourceFellows and @kkoehler, #729).
   See [examples/mock_cluster](examples/mock_cluster).


### Fixes

 * Fix Rebalance events behavior for static membership (@jliunyu, #757).
 * Fix consumer close taking 10 seconds when there's no rebalance
   needed (@jliunyu, #757).

confluent-kafka-go is based on librdkafka FUTUREFIXME, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.9.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.


## v1.8.2

This is a maintenance release:

 * Bundles librdkafka v1.8.2
 * Check termination channel while reading delivery reports (by @zjj)
 * Added convenience method Consumer.StoreMessage() (@finncolman, #676)


confluent-kafka-go is based on librdkafka v1.8.2, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.8.2)
for a complete list of changes, enhancements, fixes and upgrade considerations.


**Note**: There were no confluent-kafka-go v1.8.0 and v1.8.1 releases.


## v1.7.0

### Enhancements

 * Experimental Windows support (by @neptoess).
 * The produced message headers are now available in the delivery report
   `Message.Headers` if the Producer's `go.delivery.report.fields`
   configuration property is set to include `headers`, e.g.:
   `"go.delivery.report.fields": "key,value,headers"`
   This comes at a performance cost and are thus disabled by default.


### Fixes

* AdminClient.CreateTopics() previously did not accept default value(-1) of
  ReplicationFactor without specifying an explicit ReplicaAssignment, this is
  now fixed.

confluent-kafka-go is based on librdkafka v1.7.0, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.7.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.



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

