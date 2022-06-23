package serializer

import "C"
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/resolver"
	"github.com/actgardner/gogen-avro/v10/schema"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
)

type SerdeType = int

const (
	// KeySerde denotes a key serde
	KeySerde = 1
	// ValueSerde denotes a value serde
	ValueSerde = 2
)

const (
	// EnableValidation enables validation
	EnableValidation = true
	// DisableValidation disables validation
	DisableValidation = false
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
	configure(conf *schemaregistry.ConfigMap, serdeType SerdeType) error
	Serialize(topic string, msg interface{}) ([]byte, error)
	Close()
}

// Deserializer represents a deserializer
type Deserializer interface {
	configure(conf *schemaregistry.ConfigMap, serdeType SerdeType) error
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
	client              schemaregistry.Client
	conf                *schemaregistry.ConfigMap
	serdeType           SerdeType
	subjectNameStrategy SubjectNameStrategyFunc
}

type serializer struct {
	serde
}

type deserializer struct {
	serde
	messageFactory MessageFactory
}

// configure configures the serde
func (s *serde) configure(conf *schemaregistry.ConfigMap, serdeType SerdeType) error {
	client, err := schemaregistry.NewClient(conf)
	if err != nil {
		return err
	}
	s.client = client
	s.conf = conf
	s.serdeType = serdeType
	s.subjectNameStrategy = TopicNameStrategy
	return nil
}

// SubjectNameStrategyFunc determines the subject for the given parameters
type SubjectNameStrategyFunc func(topic string, serdeType SerdeType, schema schemaregistry.SchemaInfo) (string, error)

// SubjectNameStrategyFunc returns a function pointer to the desired subject naming strategy.
// For additional information on subject naming strategies see the following link.
// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#subject-name-strategy
func (s *serde) SubjectNameStrategy() SubjectNameStrategyFunc {
	return s.subjectNameStrategy
}

// SetSubjectNameStrategy sets the subject naming strategy.
func (s *serde) SetSubjectNameStrategy(strategy SubjectNameStrategyFunc) {
	s.subjectNameStrategy = strategy
}

// TopicNameStrategy creates a subject name by appending -[key|value] to the topic name.
func TopicNameStrategy(topic string, serdeType SerdeType, schema schemaregistry.SchemaInfo) (string, error) {
	suffix := "-value"
	if serdeType == KeySerde {
		suffix = "-key"
	}
	return topic + suffix, nil
}

func (s *serializer) getID(topic string, msg interface{}, info schemaregistry.SchemaInfo) (int, error) {
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
	subject, err := s.subjectNameStrategy(topic, s.serdeType, info)
	if err != nil {
		return -1, err
	}
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
		info = schemaregistry.SchemaInfo{
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

func (s *deserializer) getSchema(topic string, payload []byte) (schemaregistry.SchemaInfo, error) {
	info := schemaregistry.SchemaInfo{}
	if payload[0] != magicByte {
		return info, fmt.Errorf("unknown magic byte")
	}
	id := binary.BigEndian.Uint32(payload[1:5])
	subject, err := s.subjectNameStrategy(topic, s.serdeType, info)
	if err != nil {
		return info, err
	}
	return s.client.GetBySubjectAndID(subject, int(id))
}

func resolveReferences(c schemaregistry.Client, schema schemaregistry.SchemaInfo, deps map[string]string) error {
	for _, ref := range schema.References {
		metadata, err := c.GetSchemaMetadata(ref.Subject, ref.Version)
		if err != nil {
			return err
		}
		info := schemaregistry.SchemaInfo{
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

func resolveAvroReferences(c schemaregistry.Client, schema schemaregistry.SchemaInfo, ns *parser.Namespace) (schema.AvroType, error) {
	for _, ref := range schema.References {
		metadata, err := c.GetSchemaMetadata(ref.Subject, ref.Version)
		if err != nil {
			return nil, err
		}
		info := schemaregistry.SchemaInfo{
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
