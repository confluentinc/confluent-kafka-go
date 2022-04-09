module github.com/confluentinc/confluent-kafka-go/examples

go 1.13

replace github.com/confluentinc/confluent-kafka-go => ../

require (
	github.com/confluentinc/confluent-kafka-go v0.0.0-00010101000000-000000000000
	github.com/gdamore/tcell v1.4.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
)
