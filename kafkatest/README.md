Contains kafkatest compatible clients for plugging in with the official Apache Kafka client tests


Instructions
============

**Build both clients with statically linked librdkafka:**

    $ mkdir ~/src/kafka/tests/go
    
    $ cd go_verifiable_consumer
    $ go build -tags static
    $ cp go_verifiable_producer ~/src/kafka/tests/go

    $ cd go_verifiable_consumer
    $ go build -tags static
    $ $ cp go_verifiable_consumer ~/src/kafka/tests/go


**Install librdkafka's dependencies on kafkatest VMs:**

    $ cd ~/src/kafka  # your Kafka git checkout
    $ for n in $(vagrant status | grep running | awk '{print $1}') ; do \
      vagrant ssh $n -c 'sudo apt-get install -y libssl1.0.0 libsasl2-modules-gssapi-mit liblz4-1 zlib1g' ; done

*Note*: There is also a deploy.sh script in this directory that can be
        used on the VMs to do the same.



**Run kafkatests using Go client:**

    $ cd ~/src/kafka # your Kafka git checkout
    $ source ~/src/venv2.7/bin/activate # your virtualenv containing ducktape
    $ vagrant rsync  # to copy go_verifiable_* clients to worker instances
    $ ducktape --debug tests/kafkatest/tests/client --globals $GOPATH/src/github.com/confluentinc/confluent-kafka-go/kafkatest/globals.json
    # Go do something else for 40 minutes
    # Come back and look at the results
