module github.com/confluentinc/confluent-kafka-go/instrumentation/otel/example

go 1.17

replace github.com/confluentinc/confluent-kafka-go/instrumentation/otel => ../

require (
	github.com/confluentinc/confluent-kafka-go v1.7.0
	github.com/confluentinc/confluent-kafka-go/instrumentation/otel v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v1.0.1
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.0.1
	go.opentelemetry.io/otel/sdk v1.0.1
)

require (
	go.opentelemetry.io/contrib v1.0.0 // indirect
	go.opentelemetry.io/otel/trace v1.0.1 // indirect
	golang.org/x/sys v0.0.0-20210423185535-09eb48e85fd7 // indirect
)
