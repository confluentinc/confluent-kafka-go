# Confluent's Golang client for Apache Kafka

## v1.6.0

v1.6.0 is a feature release adding support for KIP-429 Incremental rebalancing.

 - Bundles librdkafka v1.6.0 - see release notes for all enhancements and fixes.


confluent-kafka-go is based on librdkafka v1.6.0, see the
[librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.6.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.


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

