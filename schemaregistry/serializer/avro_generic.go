package serializer

import (
	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/schema"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/heetch/avro"
	"reflect"
	"unsafe"
)

// GenericAvroSerializer represents a generic Avro serializer
type GenericAvroSerializer struct {
	serializer
}

// GenericAvroDeserializer represents a generic Avro deserializer
type GenericAvroDeserializer struct {
	deserializer
}

var _ Serializer = new(GenericAvroSerializer)
var _ Deserializer = new(GenericAvroDeserializer)

// NewGenericAvroSerializer creates an Avro serializer for generic objects
func NewGenericAvroSerializer(conf *schemaregistry.ConfigMap, serdeType SerdeType) (*GenericAvroSerializer, error) {
	s := &GenericAvroSerializer{}
	err := s.configure(conf, serdeType)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Serialize implements serialization of generic Avro data
func (s *GenericAvroSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	avroType, err := avro.TypeOf(msg)
	if err != nil {
		return nil, err
	}
	info := schemaregistry.SchemaInfo{
		Schema: avroType.String(),
	}
	id, err := s.getID(topic, msg, info)
	if err != nil {
		return nil, err
	}
	msgBytes, _, err := avro.Marshal(msg)
	if err != nil {
		return nil, err
	}
	payload, err := s.writeBytes(id, msgBytes)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// NewGenericAvroDeserializer creates an Avro deserializer for generic objects
func NewGenericAvroDeserializer(conf *schemaregistry.ConfigMap, serdeType SerdeType) (*GenericAvroDeserializer, error) {
	s := &GenericAvroDeserializer{}
	err := s.configure(conf, serdeType)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Deserialize implements deserialization of generic Avro data
func (s *GenericAvroDeserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	info, err := s.getSchema(topic, payload)
	if err != nil {
		return nil, err
	}
	writer, name, err := s.toType(info)
	if err != nil {
		return nil, err
	}
	subject, err := s.subjectNameStrategy(topic, s.serdeType, info)
	if err != nil {
		return nil, err
	}
	msg, err := s.messageFactory(subject, name)
	if err != nil {
		return nil, err
	}
	_, err = avro.Unmarshal(payload[5:], msg, writer)
	return msg, err
}

// DeserializeInto implements deserialization of generic Avro data to the given object
func (s *GenericAvroDeserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
	if payload == nil {
		return nil
	}
	info, err := s.getSchema(topic, payload)
	if err != nil {
		return err
	}
	writer, _, err := s.toType(info)
	_, err = avro.Unmarshal(payload[5:], msg, writer)
	return err
}

func (s *GenericAvroDeserializer) toType(schema schemaregistry.SchemaInfo) (*avro.Type, string, error) {
	t := avro.Type{}
	avroType, err := s.toAvroType(schema)
	if err != nil {
		return nil, "", err
	}

	// Use reflection to set the private avroType field of avro.Type
	setPrivateAvroType(&t, avroType)

	return &t, avroType.Name(), nil
}

func (s *GenericAvroDeserializer) toAvroType(schema schemaregistry.SchemaInfo) (schema.AvroType, error) {
	ns := parser.NewNamespace(false)
	return resolveAvroReferences(s.client, schema, ns)
}

// From https://stackoverflow.com/questions/42664837/how-to-access-unexported-struct-fields/43918797#43918797
func setPrivateAvroType(t *avro.Type, avroType schema.AvroType) {
	rt := reflect.ValueOf(t).Elem()
	rf := rt.Field(0)
	reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).
		Elem().
		Set(reflect.ValueOf(avroType))
}
