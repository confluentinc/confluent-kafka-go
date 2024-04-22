module github.com/confluentinc/confluent-kafka-go/kafkatest/v2

go 1.21

toolchain go1.21.0

replace github.com/confluentinc/confluent-kafka-go/v2 => ../

require (
	github.com/alecthomas/kingpin v2.2.6+incompatible
	github.com/confluentinc/confluent-kafka-go/v2 v2.4.0-RC2
)

require (
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137 // indirect
)
