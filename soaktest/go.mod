module github.com/confluentinc/confluent-kafka-go/soaktest/v2

go 1.21

toolchain go1.21.0

replace github.com/confluentinc/confluent-kafka-go/v2 => ../

require (
	github.com/DataDog/datadog-go v4.8.3+incompatible
	github.com/confluentinc/confluent-kafka-go/v2 v2.4.0
	github.com/shirou/gopsutil v3.21.11+incompatible
)

require (
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/stretchr/objx v0.5.1 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	golang.org/x/sys v0.19.0 // indirect
)
