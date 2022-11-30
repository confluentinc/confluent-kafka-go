# Confluent's Golang client for Apache Kafka

## vNext

 * Added Consumer `SeekPartitions()` method to seek multiple partitions at
   once and deprecated `Seek()`.

## v2.0.2

This is a feature release:

 * Added SetSaslCredentials. This new method (on the Producer, Consumer, and
   AdminClient) allows modifying the stored SASL PLAIN/SCRAM credentials that
   will be used for subsequent (new) connections to a broker.
 * Channel based producer (Producer `ProduceChannel()`) and channel based
   consumer (Consumer `Events()`) are deprecated.
 * Added `IsTimeout()` on Error type. This is a convenience method that checks
   if the error is due to a timeout.
 * The timeout parameter on `Seek()` is now ignored and an infinite timeout is
   used, the method will block until the fetcher state is updated (typically
   within microseconds).
 * The minimum version of Go supported has been changed from 1.11 to 1.14.
 * [KIP-222](https://cwiki.apache.org/confluence/display/KAFKA/KIP-222+-+Add+Consumer+Group+operations+to+Admin+API)
   Add Consumer Group operations to Admin API.
 * [KIP-518](https://cwiki.apache.org/confluence/display/KAFKA/KIP-518%3A+Allow+listing+consumer+groups+per+state)
   Allow listing consumer groups per state.
 * [KIP-396](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=97551484)
   Partially implemented: support for AlterConsumerGroupOffsets.
 * As result of the above KIPs, added (#923)
   - `ListConsumerGroups` Admin operation. Supports listing by state.
   - `DescribeConsumerGroups` Admin operation. Supports multiple groups.
   - `DeleteConsumerGroups` Admin operation. Supports multiple groups (@vsantwana).
   - `ListConsumerGroupOffsets` Admin operation. Currently, only supports
      1 group with multiple partitions. Supports the `requireStable` option.
   - `AlterConsumerGroupOffsets` Admin operation. Currently, only supports
      1 group with multiple offsets.
  * Added `SetRoundtripDuration` to the mock broker for setting RTT delay for
    a given mock broker (@kkoehler, #892).
  * Built-in support for Linux/ arm64. (#933).

### Fixes

  * The `SpecificDeserializer.Deserialize` method was not returning its result
    result correctly, and was hence unusable. The return has been fixed (#849).
  * The schema ID to use during serialization, specified in `SerializerConfig`,
    was ignored. It is now used as expected (@perdue, #870).
  * Creating a new schema registry client with an SSL CA Certificate led to a
    panic. This was due to a `nil` pointer, fixed with proper initialization
    (@HansK-p, @ju-popov, #878).

### Upgrade considerations

  * OpenSSL 3.0.x upgrade in librdkafka requires a major version bump, as some legacy
    ciphers need to be explicitly configured to continue working, but it is highly
    recommended **not** to use them.
    The rest of the API remains backward compatible, see the librdkafka release notes
    below for details.
  * As required by the Go module system, a suffix with the new major version has been
    added to the module name, and package imports must reflect this change.


confluent-kafka-go is based on librdkafka v2.0.2, see the
[librdkafka v2.0.0 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.0.0)
and later ones for a complete list of changes, enhancements, fixes and upgrade considerations.


**Note**: There were no confluent-kafka-go v2.0.0 or v2.0.1 releases.


## v1.9.2

This is a maintenance release:

 * Bundles librdkafka v1.9.2.
 * [Example](examples/docker_aws_lambda_example) for using go clients with AWS lambda (@jliunyu, #823).
 * OAUTHBEARER unsecured [producer](examples/oauthbearer_producer_example), [consumer](examples/oauthbearer_consumer_example) and [OIDC](examples/oauthbearer_oidc_example) examples.


confluent-kafka-go is based on librdkafka v1.9.2, see the
[librdkafka release notes](https://github.com/confluentinc/librdkafka/releases/tag/v1.9.2)
for a complete list of changes, enhancements, fixes and upgrade considerations.


## v1.9.1

This is a feature release:

 * Schema Registry support for Avro [Generic](examples/avro_generic_producer_example) and [Specific](examples/avro_specific_producer_example), [Protocol Buffers](examples/protobuf_producer_example) and [JSON Schema](examples/json_producer_example). (@rayokota, #776).
 * Built-in support for Mac OSX M1 / arm64. (#818).


confluent-kafka-go is based on librdkafka v1.9.1, see the
[librdkafka release notes](https://github.com/confluentinc/librdkafka/releases/tag/v1.9.1)
for a complete list of changes, enhancements, fixes and upgrade considerations.



## v1.9.0

This is a feature release:

 * OAUTHBEARER OIDC support
 * KIP-140 Admin API ACL support
 * Added MockCluster for functional testing of applications without the need
   for a real Kafka cluster (by @SourceFellows and @kkoehler, #729).
   See [examples/mock_cluster](examples/mock_cluster).


### Fixes

 * Fix Rebalance events behavior for static membership (@jliunyu, #757,
   #798).
 * Fix consumer close taking 10 seconds when there's no rebalance
   needed (@jliunyu, #757).

confluent-kafka-go is based on librdkafka v1.9.0, see the
[librdkafka release notes](https://github.com/confluentinc/librdkafka/releases/tag/v1.9.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.


## v1.8.2

This is a maintenance release:

 * Bundles librdkafka v1.8.2
 * Check termination channel while reading delivery reports (by @zjj)
 * Added convenience method Consumer.StoreMessage() (@finncolman, #676)


confluent-kafka-go is based on librdkafka v1.8.2, see the
[librdkafka release notes](https://github.com/confluentinc/librdkafka/releases/tag/v1.8.2)
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
[librdkafka release notes](https://github.com/confluentinc/librdkafka/releases/tag/v1.7.0)
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
[librdkafka release notes](https://github.com/confluentinc/librdkafka/releases/tag/v1.6.0)
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
[librdkafka release notes](https://github.com/confluentinc/librdkafka/releases/tag/v1.5.2)
for a complete list of changes, enhancements, fixes and upgrade considerations.

