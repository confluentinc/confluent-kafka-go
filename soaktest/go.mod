module github.com/confluentinc/confluent-kafka-go/soaktest/v2

go 1.14

replace github.com/confluentinc/confluent-kafka-go/v2 => ../

require (
	github.com/DataDog/datadog-go v4.8.3+incompatible
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/confluentinc/confluent-kafka-go/v2 v2.1.1
	github.com/shirou/gopsutil v3.21.11+incompatible
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
)
