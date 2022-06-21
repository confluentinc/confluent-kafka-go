
Examples
--------

  [admin_create_acls](admin_create_acls) - Create Access Control Lists
  
  [admin_create_topic](admin_create_topic) - Create a topic

  [admin_delete_acls](admin_delete_acls) - Delete Access Control Lists using different filters
  
  [admin_delete_topics](admin_delete_topics) - Delete some topics
  
  [admin_describe_acls](admin_describe_acls) - Find Access Control Lists using a filter
  
  [admin_describe_config](admin_describe_config) - Describe broker, topic or group configs
  
  [consumer_example](consumer_example) - Function & callback based consumer
  
  [consumer_offset_metadata](consumer_offset_metadata) - Commit offset with metadata
  
  [cooperative_consumer_example](cooperative_consumer_example) - Using the cooperative incremental rebalancing protocol

  [confluent_cloud_example](confluent_cloud_example) - Usage example with Confluent Cloud

  [go-kafkacat](go-kafkacat) - Channel based kafkacat Go clone

  [idempotent_producer_example](idempotent_producer_example) - Idempotent producer
  
  [legacy](legacy) - Legacy examples
  
  [library-version](library-version) - Show the library version

  [mockcluster_example](mockcluster_example) - Use a mock cluster for testing

  [oauthbearer_example](oauthbearer_example) - Provides unsecured SASL/OAUTHBEARER example

  [producer_custom_channel_example](producer_custom_channel_example) - Function based producer with a custom delivery channel

  [producer_example](producer_example) - Function based producer
  
  [stats_example](stats_example) - Receiving stats events

  [transactions_example](transactions_example) - Showcasing a transactional consume-process-produce application

Usage example
-------------

    $ cd consumer_example
    $ go build   (or 'go install')
    $ ./consumer_example    # see usage
    $ ./consumer_example mybroker mygroup mytopic
