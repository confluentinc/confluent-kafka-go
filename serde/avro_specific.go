package serde

import (
	"bytes"
	"fmt"
	"github.com/actgardner/gogen-avro/v10/compiler"
	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/schema"
	"github.com/actgardner/gogen-avro/v10/vm"
	"github.com/actgardner/gogen-avro/v10/vm/types"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"io"
)

// SpecificAvroSerializer represents a specific Avro serializer
type SpecificAvroSerializer struct {
	serializer
}

// SpecificAvroDeserializer represents a specific Avro deserializer
type SpecificAvroDeserializer struct {
	deserializer
}

var _ Serializer = new(SpecificAvroSerializer)
var _ Deserializer = new(SpecificAvroDeserializer)

// SpecificAvroMessage represents a generated Avro class from gogen-avro
type SpecificAvroMessage interface {
	types.Field
	Serialize(w io.Writer) error
	Schema() string
}

// NewSpecificAvroSerializer creates an Avro serializer for Avro-generated objects
func NewSpecificAvroSerializer(conf *schemaregistry.ConfigMap, isKey bool) (*SpecificAvroSerializer, error) {
	s := &SpecificAvroSerializer{}
	err := s.configure(conf, isKey)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Serialize implements serialization of specific Avro data
func (s *SpecificAvroSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
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
	id, err := s.getID(topic, avroMsg, info)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = avroMsg.Serialize(&buf)
	if err != nil {
		return nil, err
	}
	payload, err := s.writeBytes(id, buf.Bytes())
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// NewSpecificAvroDeserializer creates an Avro deserializer for Avro-generated objects
func NewSpecificAvroDeserializer(conf *schemaregistry.ConfigMap, isKey bool) (*SpecificAvroDeserializer, error) {
	s := &SpecificAvroDeserializer{}
	err := s.configure(conf, isKey)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Deserialize implements deserialization of specific Avro data
func (s *SpecificAvroDeserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	info, err := s.getSchema(topic, payload)
	if err != nil {
		return nil, err
	}
	writer, err := s.toAvroType(info)
	if err != nil {
		return nil, err
	}
	subject := s.subjectNameStrategy(topic, s.isKey, info)
	msg, err := s.messageFactory(subject, writer.Name())
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
	return vm.Eval(r, deser, avroMsg), nil
}

// DeserializeInto implements deserialization of specific Avro data to the given object
func (s *SpecificAvroDeserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
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
	info, err := s.getSchema(topic, payload)
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

func (s *SpecificAvroDeserializer) toAvroType(schema schemaregistry.SchemaInfo) (schema.AvroType, error) {
	ns := parser.NewNamespace(false)
	return resolveAvroReferences(s.client, schema, ns)
}
