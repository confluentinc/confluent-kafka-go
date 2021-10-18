# Confluent Kafka Go instrumentation example

A Kafka producer and consumer using [confluentinc/confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) library with instrumentation.

## Prerequisites

These instructions expect you have [docker-compose](https://docs.docker.com/compose/) installed.

Launch `Kafka` and `ZooKeeper` containers in order to run this example:

```sh
$ docker-compose up -d
```

## Producer

Run the following commands in order to produce a message in topic `confluent-kafka-go-example`:

```sh
$ go run producer/producer.go -brokers=localhost:9092
```

## Producer

Run the following commands in order to consume a message from topic `confluent-kafka-go-example`:

```sh
$ go run consumer/consumer.go -brokers=localhost:9092
```

## Cleanup

Once you've finished testing these examples, don't forger to cleanup your environment by removing docker containers:

```sh
$ docker-compose down
```
