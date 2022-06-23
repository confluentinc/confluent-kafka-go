package serde

import (
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/test"
	"testing"
)

func TestSpecificAvroSerdeWithSimple(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewSpecificAvroSerializer(&conf, ValueSerde)
	maybeFail("serializer configuration", err)

	obj := test.NewDemoSchema()
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{0, 0, 0, 1}
	bytes, err := ser.Serialize("topic1", &obj)
	maybeFail("serialization", err)

	deser, err := NewSpecificAvroDeserializer(&conf, ValueSerde)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	var newobj test.DemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	maybeFail("deserialization", err, expect(newobj, obj))
}

func TestSpecificAvroSerdeWithNested(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewSpecificAvroSerializer(&conf, ValueSerde)
	maybeFail("serializer configuration", err)

	nested := test.NestedRecord{
		StringField: "hi",
		BoolField:   true,
		BytesField:  []byte{1, 2},
	}
	number := test.NumberRecord{
		IntField:    123,
		LongField:   456,
		FloatField:  1.23,
		DoubleField: 4.56,
	}
	obj := test.NestedTestRecord{
		NumberField: number,
		OtherField:  nested,
	}
	bytes, err := ser.Serialize("topic1", &obj)
	maybeFail("serialization", err)

	deser, err := NewSpecificAvroDeserializer(&conf, ValueSerde)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	var newobj test.NestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	maybeFail("deserialization", err, expect(newobj, obj))
}

func TestSpecificAvroSerdeWithCycle(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewSpecificAvroSerializer(&conf, ValueSerde)
	maybeFail("serializer configuration", err)

	inner := test.RecursiveUnionTestRecord{
		RecursiveField: nil,
	}
	wrapper := test.UnionNullRecursiveUnionTestRecord{
		RecursiveUnionTestRecord: inner,
		UnionType:                1,
	}
	obj := test.RecursiveUnionTestRecord{
		RecursiveField: &wrapper,
	}
	bytes, err := ser.Serialize("topic1", &obj)
	maybeFail("serialization", err)

	deser, err := NewSpecificAvroDeserializer(&conf, ValueSerde)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	var newobj test.RecursiveUnionTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	maybeFail("deserialization", err, expect(newobj, obj))
}
