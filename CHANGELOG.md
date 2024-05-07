# Confluent's Golang client for Apache Kafka

# v2.4.0

This is a feature release.

 * [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol): 
   Integration tests running with the new consumer group protocol. The feature is an **Early Access**: not production ready (#1185).

## Fixes

 * The version of Go in go.mod has been changed from 1.17 to 1.21.
   This is necessary to update test dependencies with security vulnerabilities.
   Code using the library will still work with Go 1.17.
   (#1136, @rzeijde).

confluent-kafka-go is based on librdkafka v2.4.0, see the
[librdkafka v2.4.0 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.4.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.


# v2.3.0

This is a feature release.

 * Adds support for AdminAPI `DescribeCluster()` and `DescribeTopics()`
   (#964, @jainruchir).
 * [KIP-430](https://cwiki.apache.org/confluence/display/KAFKA/KIP-430+-+Return+Authorized+Operations+in+Describe+Responses):
   Return authorized operations in Describe Responses.
   (#964, @jainruchir).
 * Adds `Rack` to the `Node` type, so AdminAPI calls can expose racks for brokers
   (currently, all Describe Responses) (#964, @jainruchir).
 * [KIP-396](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=97551484): completed the implementation with
   the addition of ListOffsets (#1029).
 * Adds cache for Schema Registry client's `GetSchemaMetadata` (#1042).
 * MockCluster can now be shutdown and started again to test broker
   availability problems (#998, @kkoehler).
 * Adds `CreateTopic` method to the MockCluster. (#1047, @mimikwang).
 * Honor `HTTPS_PROXY` environment variable, if set, for the Schema Registry
   client (#1065, @finncolman).
 * [KIP-516](https://cwiki.apache.org/confluence/display/KAFKA/KIP-516%3A+Topic+Identifiers):
   Partial support of topic identifiers. Topic identifiers in metadata response
   are available through the new `DescribeTopics` function (#1068).

## Fixes

 * Fixes a bug in the mock schema registry client where the wrong ID was being
   returned for pre-registered schema (#971, @srlk).
 * The minimum version of Go supported has been changed from 1.16 to 1.17
   (#1074).
 * Fixes an issue where `testing` was being imported by a non-test file,
   testhelpers.go. (#1049, @dmlambea).
 * Fixes the optional `Coordinator` field in `ConsumerGroupDescription` in case
   it's not known. It now contains a `Node` with ID -1 in that case.
   Avoids a C segmentation fault.
 * Fixes an issue with `Producer.Flush`. It was waiting for
   `queue.buffering.max.ms` while flushing (#1013).
 * Fixes an issue where consumer methods would not be allowed to run while the
   consumer was closing, and during the final partition revoke (#1073).

confluent-kafka-go is based on librdkafka v2.3.0, see the
[librdkafka v2.3.0 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.3.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.


# v2.2.0

This is a feature release.

 * [KIP-339](https://cwiki.apache.org/confluence/display/KAFKA/KIP-339%3A+Create+a+new+IncrementalAlterConfigs+API)
   IncrementalAlterConfigs API (#945).
 * [KIP-554](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API):
   User SASL/SCRAM credentials alteration and description (#1004).

## Fixes

 * Fixes a nil pointer bug in the protobuf `Serializer.Serialize()`, caused due to
   an unchecked error (#997, @baganokodo2022).
 * Fixes incorrect protofbuf FileDescriptor references (#989, @Mrmann87).
 * Allow fetching all partition offsets for a consumer group by passing a
   `nil` slice in `AdminClient.ListConsumerGroupOffsets`, when earlier it
   was not processing that correctly (#985, @alexandredantas).
 * Deprecate m.LeaderEpoch in favor of m.TopicPartition.LeaderEpoch (#1012).

confluent-kafka-go is based on librdkafka v2.2.0, see the
[librdkafka v2.2.0 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.2.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.


## v2.1.1

This is a maintenance release.

It is strongly recommended to update to v2.1.1 if v2.1.0 is being used, as it
fixes a critical issue in the consumer (#980).

confluent-kafka-go is based on librdkafka v2.1.1, see the
[librdkafka v2.1.1 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.1.1)
for a complete list of changes, enhancements, fixes and upgrade considerations.


## v2.1.0

This is a feature release:

 * Added Consumer `SeekPartitions()` method to seek multiple partitions at
   once and deprecated `Seek()` (#940).
 * [KIP-320](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation):
   add offset leader epoch to the TopicPartition \
   and Message structs (#968).
 * The minimum version of Go supported has been changed from 1.14 to 1.16
   (#973).
 * Add validation on the Producer, the Consumer and the AdminClient to prevent
   panic when they are used after close (#901).
 * Fix bug causing schema-registry URL with existing path to not be parsed
   correctly (#950).
 * Support for Offset types on `Offset.Set()` (#962, @jdockerty).
 * Added example for using [rebalance callback with manual commit](examples/consumer_rebalance_example).


confluent-kafka-go is based on librdkafka v2.1.0, see the
[librdkafka v2.1.0 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.1.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.


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

