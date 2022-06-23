package serde

import "C"
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
)

// Type represents the type of Serde
type Type = int

const (
	// KeySerde denotes a key Serde
	KeySerde = 1
	// ValueSerde denotes a value Serde
	ValueSerde = 2
)

const (
	// EnableValidation enables validation
	EnableValidation = true
	// DisableValidation disables validation
	DisableValidation = false
)

// MagicByte is prepended to the serialized payload
const MagicByte byte = 0x0

// MessageFactory is a factory function, which should return a pointer to
// an instance into which we will unmarshal wire data.
// For Avro, the name will be the name of the Avro type if it has one.
// For JSON Schema, the name will be empty.
// For Protobuf, the name will be the name of the message type.
type MessageFactory func(subject string, name string) (interface{}, error)

// Serializer represents a BaseSerializer
type Serializer interface {
	Configure(conf *schemaregistry.ConfigMap, serdeType Type) error
	Serialize(topic string, msg interface{}) ([]byte, error)
	Close()
}

// Deserializer represents a BaseDeserializer
type Deserializer interface {
	Configure(conf *schemaregistry.ConfigMap, serdeType Type) error
	// Deserialize will call the MessageFactory to create an object
	// into which we will unmarshal data.
	Deserialize(topic string, payload []byte) (interface{}, error)
	// DeserializeInto will unmarshal data into the given object.
	DeserializeInto(topic string, payload []byte, msg interface{}) error
	Close()
}

// Serde is a common instance for both the serializers and deserializers
type Serde struct {
	Client              schemaregistry.Client
	Conf                *schemaregistry.ConfigMap
	SerdeType           Type
	SubjectNameStrategy SubjectNameStrategyFunc
}

// BaseSerializer represents basic serializer info
type BaseSerializer struct {
	Serde
}

// BaseDeserializer represents basic deserializer info
type BaseDeserializer struct {
	Serde
	MessageFactory MessageFactory
}

// Configure configures the Serde
func (s *Serde) Configure(conf *schemaregistry.ConfigMap, serdeType Type) error {
	client, err := schemaregistry.NewClient(conf)
	if err != nil {
		return err
	}
	s.Client = client
	s.Conf = conf
	s.SerdeType = serdeType
	s.SubjectNameStrategy = TopicNameStrategy
	return nil
}

// SubjectNameStrategyFunc determines the subject for the given parameters
type SubjectNameStrategyFunc func(topic string, serdeType Type, schema schemaregistry.SchemaInfo) (string, error)

// TopicNameStrategy creates a subject name by appending -[key|value] to the topic name.
func TopicNameStrategy(topic string, serdeType Type, schema schemaregistry.SchemaInfo) (string, error) {
	suffix := "-value"
	if serdeType == KeySerde {
		suffix = "-key"
	}
	return topic + suffix, nil
}

// GetID returns a schema ID for the given schema
func (s *BaseSerializer) GetID(topic string, msg interface{}, info schemaregistry.SchemaInfo) (int, error) {
	autoRegister, err := s.Conf.Get("auto.register.schemas", true)
	if err != nil {
		return -1, err
	}
	useSchemaID, err := s.Conf.Get("use.info.id", -1)
	if err != nil {
		return -1, err
	}
	useLatest, err := s.Conf.Get("use.latest.version", false)
	if err != nil {
		return -1, err
	}
	normalizeSchema, err := s.Conf.Get("normalize.schemas", false)
	if err != nil {
		return -1, err
	}

	var id = -1
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return -1, err
	}
	if autoRegister.(bool) {
		id, err = s.Client.Register(subject, info, normalizeSchema.(bool))
		if err != nil {
			return -1, err
		}
	} else if useSchemaID.(int) >= 0 {
		info, err = s.Client.GetBySubjectAndID(subject, useSchemaID.(int))
		if err != nil {
			return -1, err
		}
		_, err := s.Client.GetID(subject, info, false)
		if err != nil {
			return -1, err
		}
	} else if useLatest.(bool) {
		metadata, err := s.Client.GetLatestSchemaMetadata(subject)
		if err != nil {
			return -1, err
		}
		info = schemaregistry.SchemaInfo{
			Schema:     metadata.Schema,
			SchemaType: metadata.SchemaType,
			References: metadata.References,
		}
		id, err = s.Client.GetID(subject, info, false)
		if err != nil {
			return -1, err
		}
	} else {
		id, err = s.Client.GetID(subject, info, normalizeSchema.(bool))
		if err != nil {
			return -1, err
		}
	}
	return id, nil
}

// WriteBytes writes the serialized payload prepended by the MagicByte
func (s *BaseSerializer) WriteBytes(id int, msgBytes []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := buf.WriteByte(MagicByte)
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

// GetSchema returns a schema for a payload
func (s *BaseDeserializer) GetSchema(topic string, payload []byte) (schemaregistry.SchemaInfo, error) {
	info := schemaregistry.SchemaInfo{}
	if payload[0] != MagicByte {
		return info, fmt.Errorf("unknown magic byte")
	}
	id := binary.BigEndian.Uint32(payload[1:5])
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return info, err
	}
	return s.Client.GetBySubjectAndID(subject, int(id))
}

// ResolveReferences resolves schema references
func ResolveReferences(c schemaregistry.Client, schema schemaregistry.SchemaInfo, deps map[string]string) error {
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
		err = ResolveReferences(c, info, deps)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the Serde
func (s *Serde) Close() {
}
