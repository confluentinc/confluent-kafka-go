package avro

import (
	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/schema"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/heetch/avro"
	"reflect"
	"unsafe"
)

// GenericAvroSerializer represents a generic Avro BaseSerializer
type GenericAvroSerializer struct {
	serde.BaseSerializer
}

// GenericAvroDeserializer represents a generic Avro BaseDeserializer
type GenericAvroDeserializer struct {
	serde.BaseDeserializer
}

var _ serde.Serializer = new(GenericAvroSerializer)
var _ serde.Deserializer = new(GenericAvroDeserializer)

// NewGenericAvroSerializer creates an Avro BaseSerializer for generic objects
func NewGenericAvroSerializer(conf *schemaregistry.ConfigMap, serdeType serde.SerdeType) (*GenericAvroSerializer, error) {
	s := &GenericAvroSerializer{}
	err := s.Configure(conf, serdeType)
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
	id, err := s.GetID(topic, msg, info)
	if err != nil {
		return nil, err
	}
	msgBytes, _, err := avro.Marshal(msg)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, msgBytes)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// NewGenericAvroDeserializer creates an Avro BaseDeserializer for generic objects
func NewGenericAvroDeserializer(conf *schemaregistry.ConfigMap, serdeType serde.SerdeType) (*GenericAvroDeserializer, error) {
	s := &GenericAvroDeserializer{}
	err := s.Configure(conf, serdeType)
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
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return nil, err
	}
	writer, name, err := s.toType(info)
	if err != nil {
		return nil, err
	}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return nil, err
	}
	msg, err := s.MessageFactory(subject, name)
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
	info, err := s.GetSchema(topic, payload)
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
	return resolveAvroReferences(s.Client, schema, ns)
}

// From https://stackoverflow.com/questions/42664837/how-to-access-unexported-struct-fields/43918797#43918797
func setPrivateAvroType(t *avro.Type, avroType schema.AvroType) {
	rt := reflect.ValueOf(t).Elem()
	rf := rt.Field(0)
	reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).
		Elem().
		Set(reflect.ValueOf(avroType))
}
