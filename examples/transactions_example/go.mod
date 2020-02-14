module github.com/confluentinc/confluent-kafka-go/examples/transactions_example

go 1.13

require (
	github.com/confluentinc/confluent-kafka-go/kafka v1.3.0
	github.com/gdamore/tcell v1.3.0
)

replace github.com/confluentinc/confluent-kafka-go/kafka v1.3.0 => ../../kafka
