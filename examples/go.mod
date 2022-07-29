module github.com/confluentinc/confluent-kafka-go/examples

go 1.13

replace github.com/confluentinc/confluent-kafka-go => ../

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1
	github.com/alecthomas/kingpin v2.2.6+incompatible
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137 // indirect
	github.com/confluentinc/confluent-kafka-go v1.9.2-RC1
	github.com/gdamore/tcell v1.4.0
	google.golang.org/protobuf v1.28.0
)
