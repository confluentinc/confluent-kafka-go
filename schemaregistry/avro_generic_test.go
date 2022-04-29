package schemaregistry

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/test"
	"testing"
)

func TestGenericAvroSerdeWithSimple(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := kafka.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser := GenericAvroSerializer{}
	err = ser.Configure(&conf, false)
	maybeFail("serializer configuration", err)

	obj := GenericDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	bytes, err := ser.Serialize("topic1", obj)
	maybeFail("serialization", err)

	deser := GenericAvroDeserializer{}
	err = deser.Configure(&conf, false)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	var newobj GenericDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	maybeFail("deserialization", err, expect(newobj, obj))
}

func TestGenericAvroSerdeWithNested(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := kafka.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser := GenericAvroSerializer{}
	err = ser.Configure(&conf, false)
	maybeFail("serializer configuration", err)

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
	maybeFail("serialization", err)

	deser := GenericAvroDeserializer{}
	err = deser.Configure(&conf, false)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	var newobj GenericNestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	maybeFail("deserialization", err, expect(newobj, obj))
}

func TestGenericAvroSerdeWithCycle(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := kafka.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser := GenericAvroSerializer{}
	err = ser.Configure(&conf, false)
	maybeFail("serializer configuration", err)

	nested := GenericLinkedList{
		Value: 456,
	}
	obj := GenericLinkedList{
		Value: 123,
		Next:  &nested,
	}

	bytes, err := ser.Serialize("topic1", obj)
	maybeFail("serialization", err)

	deser := GenericAvroDeserializer{}
	err = deser.Configure(&conf, false)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	var newobj GenericLinkedList
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	maybeFail("deserialization", err, expect(newobj, obj))
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
