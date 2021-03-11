# Information for soak test for go client
The soak test contains 2 applications:
soakclient.go for one application : Create Producer-1 to keep sending messages to the input-topic.
Create Consumer-2 to receive messages from the output-topic.

soakclient_transaction.go for the other application: Create Consumer-1 to receive messages from the input-topic.
Create transactional producer Producer-2 to sending the received messages to the output-topic.
The initTransaction API registers a transactional.id when creating the producer.
The producer sends the messages to the output-topic.
Commit transaction after sending 100 messages each time or 1 second.

# Instruction on configuration and running

# Build soaktest API
    $ cd soaktest
    $ go mod init soaktest
    $ go mod tidy
    $ go install soaktest

# Build and run soakclient.go
    $ cd soaktest/soakclient
    $ go build
    $ ./soakclient -broker <broker> -inputTopic <..> -outTopic <..> -groupID <..> -inputTopicPartitionsNum <..> -outTopicPartitionsNum <..> -replicationFactor <..> -ccloudAPIKey <ccloudAPIKey> -ccloudAPISecret <ccloudAPISecret>

# Build and run soakclient_transaction.go
    $ cd soaktest/soakclient_transaction
    $ go build
    $ ./soakclient_transaction -broker <broker> -inputTopic <..> -outTopic <..> -outTopicPartitionsNum <..> -groupID <..> -transactionID <..> -ccloudAPIKey <ccloudAPIKey> -ccloudAPISecret <ccloudAPISecret>

### The default values if doesn't input from the command lines
    inputTopic: "inputTopic"
    outTopic: "outTopic"
    groupID : "groupID"
    inputTopicPartitionsNum: 1
    outTopicPartitionsNum: 1
    replicationFactor: 1
    transactionID: transactionID

# Recommend instance type and size
At lease 4 CPUs and 8 GB volume size
