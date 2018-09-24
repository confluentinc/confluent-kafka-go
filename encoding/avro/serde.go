package avro

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rnpridgeon/avro"
 )

// Common fields and helper methods for both the serializer and the deserializer.
const magicByte byte = 0x0
const schemaID int = 4

type subjectNameStrategy func(topic *string, record avro.Schema) string
type payloadStrategy func(msg *kafka.Message) interface{}

// topicNameStrategy creates a subject name by appending -[key|value] to the topic name.
func topicNameStrategy(isKey bool) subjectNameStrategy {
	suffix := "-value"
	if isKey {
		suffix = "-key"
	}
	return func(topic *string, _ avro.Schema) string {
		return *topic + suffix
	}
}

// recordNameStrategy derives the subject name from the Avro record name.
func recordNameStrategy(_ *string, record avro.Schema) string {
	return record.GetName()
}

// topicRecordNameStrategy derives the subject name from by concatenating the topic name with the Avro record name.
func topicRecordNameStrategy(topic *string, record avro.Schema) string {
	return *topic + record.GetName()
}

// setNameStrategy returns a function pointer to the desired subject naming strategy.
// For additional information on subject naming strategies see the following link.
// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#subject-name-strategy
func setNameStrategy(conf kafka.ConfigMap, isKey bool) (subjectNameStrategy, *kafka.Error) {
	var strategy subjectNameStrategy

	name, err := conf.Extract("key.subject.name.strategy", "TopicNameStrategy")

	if err != nil {
		return nil, kafka.NewError(kafka.ErrInvalidConfig, err)
	}

	switch name.(string) {
	case "TopicNameStrategy":
		strategy = topicNameStrategy(isKey)
	case "RecordNameStrategy":
		strategy = recordNameStrategy
	case "TopicRecordNameStrategy":
		strategy = topicRecordNameStrategy
	default:
		return nil, kafka.NewError(kafka.ErrInvalidConfig, fmt.Errorf("subject name strategy %s unknown", name))
	}

	return strategy, nil
}

// Common instance  for both the Serializer and the Deserializer.
type avroSerde struct {
	kafka.AbstractSerializer
	client     SchemaRegistryClient
	getSubject subjectNameStrategy
}

// Configure extracts all Serializer related ConfigValues returning a new copy with the difference.
func (s *avroSerde) Configure(conf kafka.ConfigMap, isKey bool) (kafka.ConfigMap, error) {
	// Clone dict first to avoid mutating original
	confCopy := conf.Clone()
	srConf := confCopy.ExtractPrefix("schema.registry.")
	var err error

	s.getSubject, err = setNameStrategy(confCopy, isKey)

	if err != (*kafka.Error)(nil) {
		return confCopy, kafka.NewError(kafka.ErrInvalidConfig, err)
	}

	if s.client, err = NewCachedSchemaRegistryClient(srConf); err != nil {
		return confCopy, kafka.NewError(kafka.ErrInvalidConfig, err)
	}

	s.IsKey = isKey

	return confCopy, nil
}
