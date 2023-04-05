module github.com/confluentinc/confluent-kafka-go/kafkatest/v2

go 1.14

replace github.com/confluentinc/confluent-kafka-go/v2 => ../

require (
	github.com/alecthomas/kingpin v2.2.6+incompatible
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137 // indirect
	github.com/confluentinc/confluent-kafka-go/v2 v2.1.0
)
