package serde

import (
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/test"
	"testing"
)

func TestJSONSchemaSerdeWithSimple(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewJSONSchemaSerializer(&conf, false, true)
	maybeFail("serializer configuration", err)

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{0, 0, 0, 1}
	bytes, err := ser.Serialize("topic1", &obj)
	maybeFail("serialization", err)

	deser, err := NewJSONSchemaDeserializer(&conf, ValueSerde, EnableValidation)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	maybeFail("deserialization", err, expect(newobj, obj))
}

func TestJSONSchemaSerdeWithNested(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewJSONSchemaSerializer(&conf, false, true)
	maybeFail("serializer configuration", err)

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
	maybeFail("serialization", err)

	deser, err := NewJSONSchemaDeserializer(&conf, ValueSerde, EnableValidation)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	var newobj JSONNestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	maybeFail("deserialization", err, expect(newobj, obj))
}

// invopop/jsonschema does not support cycles
/*
func TestJSONSchemaSerdeWithCycle(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser := JSONSchemaSerializer{
		validate: true,
	}
	err = ser.configure(&conf, false)
	maybeFail("serializer configuration", err)

	nested := JSONLinkedList{
		Value: 456,
	}
	obj := JSONLinkedList{
		Value: 123,
		Next:  &nested,
	}
	bytes, err := ser.Serialize("topic1", &obj)
	maybeFail("serialization", err)

	deser := JSONSchemaDeserializer{
		validate: true,
	}
	err = deser.configure(&conf, false)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	var newobj JSONLinkedList
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	maybeFail("deserialization", err, expect(newobj, obj))
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
