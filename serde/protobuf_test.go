package serde

import (
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/test"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestProtobufSerdeWithSimple(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewProtobufSerializer(&conf, false)
	maybeFail("serializer configuration", err)

	obj := test.Author{
		Name:  "Kafka",
		Id:    123,
		Works: []string{"The Castle", "The Trial"},
	}
	bytes, err := ser.Serialize("topic1", &obj)
	maybeFail("serialization", err)

	deser, err := NewProtobufDeserializer(&conf, ValueSerde)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.Deserialize("topic1", bytes)
	maybeFail("deserialization", err, expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeWithSecondMessage(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewProtobufSerializer(&conf, false)
	maybeFail("serializer configuration", err)

	obj := test.Pizza{
		Size:     "Extra extra large",
		Toppings: []string{"anchovies", "mushrooms"},
	}
	bytes, err := ser.Serialize("topic1", &obj)
	maybeFail("serialization", err)

	deser, err := NewProtobufDeserializer(&conf, ValueSerde)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.Deserialize("topic1", bytes)
	maybeFail("deserialization", err, expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeWithNestedMessage(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewProtobufSerializer(&conf, false)
	maybeFail("serializer configuration", err)

	obj := test.NestedMessage_InnerMessage{
		Id: "inner",
	}
	bytes, err := ser.Serialize("topic1", &obj)
	maybeFail("serialization", err)

	deser, err := NewProtobufDeserializer(&conf, ValueSerde)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.Deserialize("topic1", bytes)
	maybeFail("deserialization", err, expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeWithReference(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewProtobufSerializer(&conf, false)
	maybeFail("serializer configuration", err)

	msg := test.TestMessage{
		TestString:   "hi",
		TestBool:     true,
		TestBytes:    []byte{1, 2},
		TestDouble:   1.23,
		TestFloat:    3.45,
		TestFixed32:  67,
		TestFixed64:  89,
		TestInt32:    100,
		TestInt64:    200,
		TestSfixed32: 300,
		TestSfixed64: 400,
		TestSint32:   500,
		TestSint64:   600,
		TestUint32:   700,
		TestUint64:   800,
	}
	obj := test.DependencyMessage{
		IsActive:     true,
		TestMesssage: &msg,
	}
	bytes, err := ser.Serialize("topic1", &obj)
	maybeFail("serialization", err)

	deser, err := NewProtobufDeserializer(&conf, ValueSerde)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.Deserialize("topic1", bytes)
	maybeFail("deserialization", err, expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeWithCycle(t *testing.T) {
	maybeFail = initFailFunc(t)
	var err error
	conf := schemaregistry.ConfigMap{}
	conf.SetKey("schema.registry.url", "mock://")

	ser, err := NewProtobufSerializer(&conf, false)
	maybeFail("serializer configuration", err)

	inner := test.LinkedList{
		Value: 100,
	}
	obj := test.LinkedList{
		Value: 1,
		Next:  &inner,
	}
	bytes, err := ser.Serialize("topic1", &obj)
	maybeFail("serialization", err)

	deser, err := NewProtobufDeserializer(&conf, ValueSerde)
	maybeFail("deserializer configuration", err)
	deser.client = ser.client

	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.Deserialize("topic1", bytes)
	maybeFail("deserialization", err, expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}
