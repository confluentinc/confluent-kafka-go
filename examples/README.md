
Examples
--------

  [admin_alter_consumer_group_offsets](admin_alter_consumer_group_offsets) - Alter Consumer Group Offsets

  [admin_create_acls](admin_create_acls) - Create Access Control Lists

  [admin_create_topic](admin_create_topic) - Create a topic

  [admin_delete_acls](admin_delete_acls) - Delete Access Control Lists using different filters

  [admin_delete_topics](admin_delete_topics) - Delete some topics

  [admin_delete_consumer_groups](admin_delete_consumer_groups) - Delete consumer groups

  [admin_delete_topics](admin_delete_topics) - Delete topics

  [admin_describe_acls](admin_describe_acls) - Find Access Control Lists using a filter

  [admin_describe_cluster](admin_describe_cluster) - Describe cluster

  [admin_describe_config](admin_describe_config) - Describe broker, topic or group configs

  [admin_describe_consumer_groups](admin_describe_consumer_groups) - Describe one or more consumer groups

  [admin_describe_topics](admin_describe_topics) - Describe topics

  [admin_list_consumer_group_offsets](admin_list_consumer_group_offsets) - List consumer group offsets

  [admin_list_offsets](admin_list_offsets) - List partition offsets

  [admin_list_consumer_groups](admin_list_consumer_groups) - List consumer groups

  [avro_generic_consumer_example](avro_generic_consumer_example) - consumer with Schema Registry and Avro Generic Deserializer

  [avro_generic_producer_example](avro_generic_producer_example) - producer with Schema Registry and Avro Generic Serializer

  [avro_specific_consumer_example](avro_specific_consumer_example) - consumer with Schema Registry and Avro Specific Deserializer

  [avro_specific_producer_example](avro_specific_producer_example) - producer with Schema Registry and Avro Specific Serializer

  [consumer_example](consumer_example) - Function & callback based consumer

  [consumer_offset_metadata](consumer_offset_metadata) - Commit offset with metadata

  [consumer_rebalance_example](consumer_rebalance_example) - Use of rebalance callback with manual commit

  [cooperative_consumer_example](cooperative_consumer_example) - Using the cooperative incremental rebalancing protocol

  [confluent_cloud_example](confluent_cloud_example) - Usage example with Confluent Cloud

  [go-kafkacat](go-kafkacat) - Channel based kafkacat Go clone

  [idempotent_producer_example](idempotent_producer_example) - Idempotent producer

  [json_consumer_example](json_consumer_example) - consumer with Schema Registry and JSON Schema Deserializer

  [json_producer_example](json_producer_example) - producer with Schema Registry and JSON Schema Serializer

  [legacy](legacy) - Legacy examples

  [library-version](library-version) - Show the library version

  [mockcluster_example](mockcluster_example) - Use a mock cluster for testing

  [mockcluster_failure_example](mockcluster_failure_example) - Use a mock cluster for failure testing

  [oauthbearer_consumer_example](oauthbearer_consumer_example) - Unsecured SASL/OAUTHBEARER consumer example

  [oauthbearer_oidc_example](oauthbearer_oidc_example) - SASL/OAUTHBEARER with OIDC method example

  [oauthbearer_producer_example](oauthbearer_producer_example) - Unsecured SASL/OAUTHBEARER producer example

  [producer_custom_channel_example](producer_custom_channel_example) - Function based producer with a custom delivery channel

  [producer_example](producer_example) - Function based producer

  [protobuf_consumer_example](protobuf_consumer_example) - consumer with Schema Registry and Protocol Buffers Deserializer

  [protobuf_producer_example](protobuf_producer_example) - producer with Schema Registry and Protocol Buffers Serializer

  [stats_example](stats_example) - Receiving stats events

  [transactions_example](transactions_example) - Showcasing a transactional consume-process-produce application

Usage example
-------------

    $ cd consumer_example
    $ go build   (or 'go install')
    $ ./consumer_example    # see usage
    $ ./consumer_example mybroker mygroup mytopic
