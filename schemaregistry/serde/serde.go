/**
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

// magicByte is prepended to the serialized payload
const magicByte byte = 0x0

// MessageFactory is a factory function, which should return a pointer to
// an instance into which we will unmarshal wire data.
// For Avro, the name will be the name of the Avro type if it has one.
// For JSON Schema, the name will be empty.
// For Protobuf, the name will be the name of the message type.
type MessageFactory func(subject string, name string) (interface{}, error)

// Serializer represents a serializer
type Serializer interface {
	ConfigureSerializer(client schemaregistry.Client, serdeType Type, conf *SerializerConfig) error
	// Serialize will serialize the given message, which should be a pointer.
	// For example, in Protobuf, messages are always a pointer to a struct and never just a struct.
	Serialize(topic string, msg interface{}) ([]byte, error)
	Close()
}

// Deserializer represents a deserializer
type Deserializer interface {
	ConfigureDeserializer(client schemaregistry.Client, serdeType Type, conf *DeserializerConfig) error
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
	SerdeType           Type
	SubjectNameStrategy SubjectNameStrategyFunc
}

// BaseSerializer represents basic serializer info
type BaseSerializer struct {
	Serde
	Conf *SerializerConfig
}

// BaseDeserializer represents basic deserializer info
type BaseDeserializer struct {
	Serde
	Conf           *DeserializerConfig
	MessageFactory MessageFactory
}

// ConfigureSerializer configures the Serializer
func (s *BaseSerializer) ConfigureSerializer(client schemaregistry.Client, serdeType Type, conf *SerializerConfig) error {
	if client == nil {
		return fmt.Errorf("schema registry client missing")
	}
	s.Client = client
	s.Conf = conf
	s.SerdeType = serdeType
	s.SubjectNameStrategy = TopicNameStrategy
	return nil
}

// ConfigureDeserializer configures the Deserializer
func (s *BaseDeserializer) ConfigureDeserializer(client schemaregistry.Client, serdeType Type, conf *DeserializerConfig) error {
	if client == nil {
		return fmt.Errorf("schema registry client missing")
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
	autoRegister := s.Conf.AutoRegisterSchemas
	useSchemaID := s.Conf.UseSchemaID
	useLatest := s.Conf.UseLatestVersion
	normalizeSchema := s.Conf.NormalizeSchemas

	var id = -1
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return -1, err
	}
	if autoRegister {
		id, err = s.Client.Register(subject, info, normalizeSchema)
		if err != nil {
			return -1, err
		}
	} else if useSchemaID >= 0 {
		info, err = s.Client.GetBySubjectAndID(subject, useSchemaID)
		if err != nil {
			return -1, err
		}
		id, err = s.Client.GetID(subject, info, false)
		if err != nil {
			return -1, err
		}
		if id != useSchemaID {
			return -1, fmt.Errorf("failed to match schema ID (%d != %d)", id, useSchemaID)
		}
	} else if useLatest {
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
		id, err = s.Client.GetID(subject, info, normalizeSchema)
		if err != nil {
			return -1, err
		}
	}
	return id, nil
}

// WriteBytes writes the serialized payload prepended by the magicByte
func (s *BaseSerializer) WriteBytes(id int, msgBytes []byte) ([]byte, error) {
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

// GetSchema returns a schema for a payload
func (s *BaseDeserializer) GetSchema(topic string, payload []byte) (schemaregistry.SchemaInfo, error) {
	info := schemaregistry.SchemaInfo{}
	if payload[0] != magicByte {
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
