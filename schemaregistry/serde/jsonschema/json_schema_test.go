package jsonschema

import (
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/test"
	"testing"
)

func TestJSONSchemaSerdeWithSimple(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewSerializer(&conf, serde.ValueSerde, serde.EnableValidation)
	serde.MaybeFail("BaseSerializer configuration", err)

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{0, 0, 0, 1}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(&conf, serde.ValueSerde, serde.EnableValidation)
	serde.MaybeFail("BaseDeserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

func TestJSONSchemaSerdeWithNested(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewSerializer(&conf, serde.ValueSerde, serde.EnableValidation)
	serde.MaybeFail("BaseSerializer configuration", err)

	nested := JSONDemoSchema{}
	nested.IntField = 123
	nested.DoubleField = 45.67
	nested.StringField = "hi"
	nested.BoolField = true
	nested.BytesField = []byte{0, 0, 0, 1}
	obj := JSONNestedTestRecord{
		OtherField: nested,
	}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(&conf, serde.ValueSerde, serde.EnableValidation)
	serde.MaybeFail("BaseDeserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONNestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

// invopop/jsonschema does not support cycles
/*
func TestJSONSchemaSerdeWithCycle(t *testing.T) {
	MaybeFail = InitFailFunc(t)
	var err error
	Conf := schemaregistry.ConfigMap{}
	Conf.SetKey("schema.registry.url", "mock://")

	ser := Serializer{
		validate: true,
	}
	err = ser.Configure(&Conf, false)
	MaybeFail("BaseSerializer configuration", err)

	nested := JSONLinkedList{
		Value: 456,
	}
	obj := JSONLinkedList{
		Value: 123,
		Next:  &nested,
	}
	bytes, err := ser.Serialize("topic1", &obj)
	MaybeFail("serialization", err)

	deser := Deserializer{
		validate: true,
	}
	err = deser.Configure(&Conf, false)
	MaybeFail("BaseDeserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONLinkedList
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	MaybeFail("deserialization", err, Expect(newobj, obj))
}
*/

type JSONDemoSchema struct {
	IntField int32 `json:"IntField"`

	DoubleField float64 `json:"DoubleField"`

	StringField string `json:"StringField"`

	BoolField bool `json:"BoolField"`

	BytesField test.Bytes `json:"BytesField"`
}

type JSONNestedTestRecord struct {
	OtherField JSONDemoSchema
}

type JSONLinkedList struct {
	Value int32
	Next  *JSONLinkedList
}
