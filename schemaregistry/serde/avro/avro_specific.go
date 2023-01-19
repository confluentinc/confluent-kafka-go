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

package avro

import (
	"bytes"
	"fmt"
	"io"

	"github.com/actgardner/gogen-avro/v10/compiler"
	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/schema"
	"github.com/actgardner/gogen-avro/v10/vm"
	"github.com/actgardner/gogen-avro/v10/vm/types"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

// SpecificSerializer represents a specific Avro serializer
type SpecificSerializer struct {
	serde.BaseSerializer
}

// SpecificDeserializer represents a specific Avro deserializer
type SpecificDeserializer struct {
	serde.BaseDeserializer
}

var _ serde.Serializer = new(SpecificSerializer)
var _ serde.Deserializer = new(SpecificDeserializer)

// SpecificAvroMessage represents a generated Avro class from gogen-avro
type SpecificAvroMessage interface {
	types.Field
	Serialize(w io.Writer) error
	Schema() string
}

// NewSpecificSerializer creates an Avro serializer for Avro-generated objects
func NewSpecificSerializer(client schemaregistry.Client, serdeType serde.Type, conf *SerializerConfig) (*SpecificSerializer, error) {
	s := &SpecificSerializer{}
	err := s.ConfigureSerializer(client, serdeType, &conf.SerializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Serialize implements serialization of specific Avro data
func (s *SpecificSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	var avroMsg SpecificAvroMessage
	switch t := msg.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return nil, fmt.Errorf("serialization target must be an avro message. Got '%v'", t)
	}
	var id = 0
	info := schemaregistry.SchemaInfo{
		Schema: avroMsg.Schema(),
	}
	id, err := s.GetID(topic, avroMsg, info)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = avroMsg.Serialize(&buf)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, buf.Bytes())
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// NewSpecificDeserializer creates an Avro deserializer for Avro-generated objects
func NewSpecificDeserializer(client schemaregistry.Client, serdeType serde.Type, conf *DeserializerConfig) (*SpecificDeserializer, error) {
	s := &SpecificDeserializer{}
	err := s.ConfigureDeserializer(client, serdeType, &conf.DeserializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Deserialize implements deserialization of specific Avro data
func (s *SpecificDeserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return nil, err
	}
	writer, err := s.toAvroType(info)
	if err != nil {
		return nil, err
	}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return nil, err
	}
	msg, err := s.MessageFactory(subject, writer.Name())
	if err != nil {
		return nil, err
	}
	var avroMsg SpecificAvroMessage
	switch t := msg.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return nil, fmt.Errorf("deserialization target must be an avro message. Got '%v'", t)
	}
	reader, err := s.toAvroType(schemaregistry.SchemaInfo{Schema: avroMsg.Schema()})
	if err != nil {
		return nil, err
	}
	deser, err := compiler.Compile(writer, reader)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(payload[5:])

	if err = vm.Eval(r, deser, avroMsg); err != nil {
		return nil, err
	}
	return avroMsg, nil
}

// DeserializeInto implements deserialization of specific Avro data to the given object
func (s *SpecificDeserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
	if payload == nil {
		return nil
	}
	var avroMsg SpecificAvroMessage
	switch t := msg.(type) {
	case SpecificAvroMessage:
		avroMsg = t
	default:
		return fmt.Errorf("serialization target must be an avro message. Got '%v'", t)
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return err
	}
	writer, err := s.toAvroType(info)
	if err != nil {
		return err
	}
	reader, err := s.toAvroType(schemaregistry.SchemaInfo{Schema: avroMsg.Schema()})
	if err != nil {
		return err
	}
	deser, err := compiler.Compile(writer, reader)
	if err != nil {
		return err
	}
	r := bytes.NewReader(payload[5:])
	return vm.Eval(r, deser, avroMsg)
}

func (s *SpecificDeserializer) toAvroType(schema schemaregistry.SchemaInfo) (schema.AvroType, error) {
	ns := parser.NewNamespace(false)
	return resolveAvroReferences(s.Client, schema, ns)
}
