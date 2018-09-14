package avro

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rnpridgeon/avro"
	"sync"
)

/**
 * Common fields and helper methods for both the serializer and the deserializer.
 */
const magicByte byte = 0x0
const schemaId int = 4

type subjectNameStrategy func(topic *string, record avro.Schema) (string)

func topicNameStrategy(isKey bool) subjectNameStrategy {
	suffix := "-value"
	if isKey {
		suffix = "-key"
	}
	return func(topic *string, _ avro.Schema) (string) {
		return *topic + suffix
	}
}

func recordNameStrategy(_ *string, record avro.Schema) (string) {
	return record.GetName()
}

func topicRecordNameStrategy(topic *string, record avro.Schema) (string) {
	return *topic + record.GetName()
}

func setNameStrategy(conf kafka.ConfigMap, isKey bool) (subjectNameStrategy, error) {
	var strategy subjectNameStrategy

	name, err := conf.Extract("key.subject.name.strategy", "TopicNameStrategy")

	if err != nil {
		return nil, err
	}

	switch name.(string) {
	case "TopicNameStrategy":
		strategy = topicNameStrategy(isKey)
	case "RecordNameStrategy":
		strategy = recordNameStrategy
	case "TopicRecordNameStrategy":
		strategy = topicRecordNameStrategy
	default:
		err = fmt.Errorf("subject name strategy %s unknown", name)
	}
	return strategy, err
}

type avroSerde struct {
	sync.Mutex
	client     SchemaRegistryClient
	getSubject subjectNameStrategy
}

func (s *avroSerde) Configure(conf kafka.ConfigMap, isKey bool) (kafka.ConfigMap, error) {
	var err error
	confCopy := conf.Clone()
	if s.getSubject, err = setNameStrategy(confCopy, isKey); err != nil {
		return confCopy, err
	}

	if s.client, err = NewCachedSchemaRegistryClient(confCopy); err != nil {
		return confCopy, err
	}

	return confCopy, nil
}

func (s *avroSerde) Close() {
	/* NOP */
}
