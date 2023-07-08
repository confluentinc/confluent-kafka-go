module github.com/confluentinc/confluent-kafka-go/examples/v2

go 1.16

replace github.com/confluentinc/confluent-kafka-go/v2 => ../

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1
	github.com/alecthomas/kingpin v2.2.6+incompatible
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137 // indirect
	github.com/confluentinc/confluent-kafka-go/v2 v2.2.0-RC1
	github.com/gdamore/tcell v1.4.0
	github.com/spiffe/go-spiffe/v2 v2.1.6 // indirect
	google.golang.org/protobuf v1.30.0
)
