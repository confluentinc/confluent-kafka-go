# OpenTelemetry Instrumentation

This package provides an instrumentation of [OpenTelemetry](https://github.com/open-telemetry).

Here are the available methods instrumented:

## Producer

```go
Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
ProduceChannel() chan *kafka.Message
```

## Consumer

```go
ReadMessage(timeout time.Duration) (*kafka.Message, error)
Poll(timeoutMs int) kafka.Event
```

Concerning customers, I've also added these 2 following methods in order to trace the consumer handler duration (which is not possible with the original library available methods):

```go
ReadMessageWithHandler(timeout time.Duration, handler ConsumeFunc) (*kafka.Message, error)
PollWithHandler(timeoutMs int, handler ConsumeFunc) kafka.Event
```

Handler function takes the following arguments:

```go
type ConsumeFunc func(consumer *kafka.Consumer, msg *kafka.Message) error
```
