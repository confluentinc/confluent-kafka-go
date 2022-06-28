package avro

import (
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/test"
	"testing"
)

func TestSpecificAvroSerdeWithSimple(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, NewSerializerConfig(), serde.ValueSerde)
	serde.MaybeFail("BaseSerializer configuration", err)

	obj := test.NewDemoSchema()
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{0, 0, 0, 1}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewSpecificDeserializer(client, NewDeserializerConfig(), serde.ValueSerde)
	serde.MaybeFail("BaseDeserializer configuration", err)
	deser.Client = ser.Client

	var newobj test.DemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

func TestSpecificAvroSerdeWithNested(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, NewSerializerConfig(), serde.ValueSerde)
	serde.MaybeFail("BaseSerializer configuration", err)

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
	serde.MaybeFail("serialization", err)

	deser, err := NewSpecificDeserializer(client, NewDeserializerConfig(), serde.ValueSerde)
	serde.MaybeFail("BaseDeserializer configuration", err)
	deser.Client = ser.Client

	var newobj test.NestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

func TestSpecificAvroSerdeWithCycle(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, NewSerializerConfig(), serde.ValueSerde)
	serde.MaybeFail("BaseSerializer configuration", err)

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
	serde.MaybeFail("serialization", err)

	deser, err := NewSpecificDeserializer(client, NewDeserializerConfig(), serde.ValueSerde)
	serde.MaybeFail("BaseDeserializer configuration", err)
	deser.Client = ser.Client

	var newobj test.RecursiveUnionTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}
