package serde

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

type SubjectNameStrategy interface {
	GetSubject(topic string, info schemaregistry.SchemaInfo) (string, error)
	IsKey() bool
}

var _ SubjectNameStrategy = new(TopicNameStrategyFunc)

// TopicNameStrategyFunc TopicNameStrategy: creates a subject name by appending -[key|value] to the topic name.
type TopicNameStrategyFunc struct {
	SerdeType Type
}

func (t TopicNameStrategyFunc) IsKey() bool {
	return t.SerdeType == KeySerde
}

func (t TopicNameStrategyFunc) GetSubject(topic string, _ schemaregistry.SchemaInfo) (string, error) {
	suffix := "-value"
	if t.SerdeType == KeySerde {
		suffix = "-key"
	}
	return topic + suffix, nil
}

var _ SubjectNameStrategy = new(TopicRecordNameStrategyFunc)

// TopicRecordNameStrategyFunc TopicRecordNameStrategy: creates a subject name by concatenating <topic>-<fullyqualifiedname>
type TopicRecordNameStrategyFunc struct {
}

func (t TopicRecordNameStrategyFunc) GetSubject(topic string, info schemaregistry.SchemaInfo) (string, error) {
	return fmt.Sprintf("%s-%s", topic, info.Schema), nil
}

func (t TopicRecordNameStrategyFunc) IsKey() bool {
	return true
}
