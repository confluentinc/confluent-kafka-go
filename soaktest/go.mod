module github.com/confluentinc/confluent-kafka-go/soaktest/v2

go 1.21

toolchain go1.21.0

replace github.com/confluentinc/confluent-kafka-go/v2 => ../

require (
	github.com/DataDog/datadog-go v4.8.3+incompatible
	github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
	github.com/shirou/gopsutil v3.21.11+incompatible
)
