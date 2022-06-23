package avro

import (
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/test"
	"testing"
)

func TestGenericAvroSerdeWithSimple(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewGenericAvroSerializer(&conf, serde.ValueSerde)
	serde.MaybeFail("BaseSerializer configuration", err)

	obj := GenericDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	bytes, err := ser.Serialize("topic1", obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewGenericAvroDeserializer(&conf, serde.ValueSerde)
	serde.MaybeFail("BaseDeserializer configuration", err)
	deser.Client = ser.Client

	var newobj GenericDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

func TestGenericAvroSerdeWithNested(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewGenericAvroSerializer(&conf, serde.ValueSerde)
	serde.MaybeFail("BaseSerializer configuration", err)

	nested := GenericDemoSchema{}
	nested.IntField = 123
	nested.DoubleField = 45.67
	nested.StringField = "hi"
	nested.BoolField = true
	nested.BytesField = []byte{1, 2}
	obj := GenericNestedTestRecord{
		OtherField: nested,
	}

	bytes, err := ser.Serialize("topic1", obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewGenericAvroDeserializer(&conf, serde.ValueSerde)
	serde.MaybeFail("BaseDeserializer configuration", err)
	deser.Client = ser.Client

	var newobj GenericNestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

func TestGenericAvroSerdeWithCycle(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewGenericAvroSerializer(&conf, serde.ValueSerde)
	serde.MaybeFail("BaseSerializer configuration", err)

	nested := GenericLinkedList{
		Value: 456,
	}
	obj := GenericLinkedList{
		Value: 123,
		Next:  &nested,
	}

	bytes, err := ser.Serialize("topic1", obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewGenericAvroDeserializer(&conf, serde.ValueSerde)
	serde.MaybeFail("BaseDeserializer configuration", err)
	deser.Client = ser.Client

	var newobj GenericLinkedList
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

type GenericDemoSchema struct {
	IntField int32 `json:"IntField"`

	DoubleField float64 `json:"DoubleField"`

	StringField string `json:"StringField"`

	BoolField bool `json:"BoolField"`

	BytesField test.Bytes `json:"BytesField"`
}

type GenericNestedTestRecord struct {
	OtherField GenericDemoSchema
}

type GenericLinkedList struct {
	Value int32
	Next  *GenericLinkedList
}
