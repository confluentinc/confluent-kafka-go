
Examples:

  admin_create_acls - Create Access Control Lists
  admin_describe_acls - Find Access Control Lists using a filter
  admin_delete_acls - Delete Access Control Lists using different filters
  consumer_channel_example - Channel based consumer
  consumer_example - Function & callback based consumer
  consumer_offset_metadata - Commit offset with metadata

  producer_channel_example - Channel based producer
  producer_example - Function based producer

  transactions_example - Showcasing a transactional consume-process-produce application

  go-kafkacat - Channel based kafkacat Go clone

  oauthbearer_example - Provides unsecured SASL/OAUTHBEARER example


Usage example:

    $ cd consumer_example
    $ go build   (or 'go install')
    $ ./consumer_example    # see usage
    $ ./consumer_example mybroker mygroup mytopic

