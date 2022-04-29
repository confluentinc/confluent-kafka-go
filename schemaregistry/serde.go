package schemaregistry

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/resolver"
	"github.com/actgardner/gogen-avro/v10/schema"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const magicByte byte = 0x0

// MessageFactory is a factory function, which should return a pointer to
// an instance into which we will unmarshal wire data.
// For Avro, the name will be the name of the Avro type if it has one.
// For JSON Schema, the name will be empty.
// For Protobuf, the name will be the name of the message type.
type MessageFactory func(subject string, name string) (interface{}, error)

// Serializer represents a serializer
type Serializer interface {
	Configure(conf *kafka.ConfigMap, isKey bool) error
	Serialize(topic string, msg interface{}) ([]byte, error)
	Close()
}

// Deserializer represents a deserializer
type Deserializer interface {
	Configure(conf *kafka.ConfigMap, isKey bool) error
	// Deserialize will call the MessageFactory to create an object
	// into which we will unmarshal data.
	Deserialize(topic string, payload []byte) (interface{}, error)
	// DeserializeInto will unmarshal data into the given object.
	DeserializeInto(topic string, payload []byte, msg interface{}) error
	MessageFactory() MessageFactory
	SetMessageFactory(factory MessageFactory)
	Close()
}

// serde is a common instance for both the serializers and deserializers
type serde struct {
	client              Client
	conf                *kafka.ConfigMap
	isKey               bool
	subjectNameStrategy SubjectNameStrategy
}

type serializer struct {
	serde
}

type deserializer struct {
	serde
	messageFactory MessageFactory
}

// Configure configures the serde
func (s *serde) Configure(conf *kafka.ConfigMap, isKey bool) error {
	client, err := NewClient(conf)
	if err != nil {
		return err
	}
	s.client = client
	s.conf = conf
	s.isKey = isKey
	s.subjectNameStrategy = TopicNameStrategy
	return nil
}

// SubjectNameStrategy determines the subject for the given parameters
type SubjectNameStrategy func(topic string, isKey bool, schema SchemaInfo) string

// SubjectNameStrategy returns a function pointer to the desired subject naming strategy.
// For additional information on subject naming strategies see the following link.
// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#subject-name-strategy
func (s *serde) SubjectNameStrategy() SubjectNameStrategy {
	return s.subjectNameStrategy
}

// SetSubjectNameStrategy sets the subject naming strategy.
func (s *serde) SetSubjectNameStrategy(strategy SubjectNameStrategy) {
	s.subjectNameStrategy = strategy
}

// TopicNameStrategy creates a subject name by appending -[key|value] to the topic name.
func TopicNameStrategy(topic string, isKey bool, schema SchemaInfo) string {
	suffix := "-value"
	if isKey {
		suffix = "-key"
	}
	return topic + suffix
}

func (s *serializer) getID(topic string, msg interface{}, info SchemaInfo) (int, error) {
	autoRegister, err := s.conf.Get("auto.register.schemas", true)
	if err != nil {
		return -1, err
	}
	useSchemaID, err := s.conf.Get("use.info.id", -1)
	if err != nil {
		return -1, err
	}
	useLatest, err := s.conf.Get("use.latest.version", false)
	if err != nil {
		return -1, err
	}
	normalizeSchema, err := s.conf.Get("normalize.schemas", false)
	if err != nil {
		return -1, err
	}

	var id = -1
	subject := s.subjectNameStrategy(topic, s.isKey, info)
	if autoRegister.(bool) {
		id, err = s.client.Register(subject, info, normalizeSchema.(bool))
		if err != nil {
			return -1, err
		}
	} else if useSchemaID.(int) >= 0 {
		info, err = s.client.GetBySubjectAndID(subject, useSchemaID.(int))
		if err != nil {
			return -1, err
		}
		_, err := s.client.GetID(subject, info, false)
		if err != nil {
			return -1, err
		}
	} else if useLatest.(bool) {
		metadata, err := s.client.GetLatestSchemaMetadata(subject)
		if err != nil {
			return -1, err
		}
		info = SchemaInfo{
			Schema:     metadata.Schema,
			SchemaType: metadata.SchemaType,
			References: metadata.References,
		}
		id, err = s.client.GetID(subject, info, false)
		if err != nil {
			return -1, err
		}
	} else {
		id, err = s.client.GetID(subject, info, normalizeSchema.(bool))
		if err != nil {
			return -1, err
		}
	}
	return id, nil
}

func (s *serializer) writeBytes(id int, msgBytes []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := buf.WriteByte(magicByte)
	if err != nil {
		return nil, err
	}
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, uint32(id))
	_, err = buf.Write(idBytes)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(msgBytes)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *deserializer) MessageFactory() MessageFactory {
	return s.messageFactory
}

func (s *deserializer) SetMessageFactory(factory MessageFactory) {
	s.messageFactory = factory
}

func (s *deserializer) getSchema(topic string, payload []byte) (SchemaInfo, error) {
	info := SchemaInfo{}
	if payload[0] != magicByte {
		return info, fmt.Errorf("unknown magic byte")
	}
	id := binary.BigEndian.Uint32(payload[1:5])
	subject := s.subjectNameStrategy(topic, s.isKey, info)
	return s.client.GetBySubjectAndID(subject, int(id))
}

func resolveReferences(c Client, schema SchemaInfo, deps map[string]string) error {
	for _, ref := range schema.References {
		metadata, err := c.GetSchemaMetadata(ref.Subject, ref.Version)
		if err != nil {
			return err
		}
		info := SchemaInfo{
			Schema:     metadata.Schema,
			SchemaType: metadata.SchemaType,
			References: metadata.References,
		}
		deps[ref.Name] = metadata.Schema
		err = resolveReferences(c, info, deps)
		if err != nil {
			return err
		}
	}
	return nil
}

func resolveAvroReferences(c Client, schema SchemaInfo, ns *parser.Namespace) (schema.AvroType, error) {
	for _, ref := range schema.References {
		metadata, err := c.GetSchemaMetadata(ref.Subject, ref.Version)
		if err != nil {
			return nil, err
		}
		info := SchemaInfo{
			Schema:     metadata.Schema,
			SchemaType: metadata.SchemaType,
			References: metadata.References,
		}
		_, err = resolveAvroReferences(c, info, ns)
		if err != nil {
			return nil, err
		}

	}
	sType, err := ns.TypeForSchema([]byte(schema.Schema))
	if err != nil {
		return nil, err
	}
	for _, def := range ns.Roots {
		if err := resolver.ResolveDefinition(def, ns.Definitions); err != nil {
			return nil, err
		}
	}
	return sType, nil
}

// Close closes the serde
func (s *serde) Close() {
}
