module github.com/confluentinc/confluent-kafka-go/soaktest

go 1.13

replace github.com/confluentinc/confluent-kafka-go => ../

require (
	github.com/DataDog/datadog-go v4.8.3+incompatible
	github.com/confluentinc/confluent-kafka-go v0.0.0-00010101000000-000000000000
	github.com/shirou/gopsutil v3.21.11+incompatible
)
