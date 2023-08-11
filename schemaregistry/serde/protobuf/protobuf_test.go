/**
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package protobuf

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/test"
	"google.golang.org/protobuf/proto"
)

func TestProtobufSerdeWithSimple(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	obj := test.Author{
		Name:  "Kafka",
		Id:    123,
		Works: []string{"The Castle", "The Trial"},
	}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeWithSecondMessage(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	obj := test.Pizza{
		Size:     "Extra extra large",
		Toppings: []string{"anchovies", "mushrooms"},
	}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeWithNestedMessage(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	obj := test.NestedMessage_InnerMessage{
		Id: "inner",
	}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeWithReference(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

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
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeWithCycle(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	inner := test.LinkedList{
		Value: 100,
	}
	obj := test.LinkedList{
		Value: 1,
		Next:  &inner,
	}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeDeserializeRecordName(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	inner := test.LinkedList{
		Value: 100,
	}

	obj := test.Pizza{
		Size:     "Extra extra large",
		Toppings: []string{"anchovies", "mushrooms"},
	}

	innerSubject := "test.LinkedList"
	objSubject := "test.Pizza"

	subjects := make(map[string]interface{})
	subjects[innerSubject] = struct{}{}
	subjects[objSubject] = struct{}{}

	bytesInner, err := ser.Serialize(innerSubject, &inner)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.Serialize(objSubject, &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	deser.ProtoRegistry.RegisterMessage(inner.ProtoReflect().Type())
	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.DeserializeRecordName(subjects, bytesInner)
	serde.MaybeFail("deserializeRecordNameValidSchema", serde.Expect(err, nil))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), inner.ProtoReflect()))

	newobj, err = deser.DeserializeRecordName(subjects, bytesObj)
	serde.MaybeFail("deserializeRecordNameValidSchema", serde.Expect(err, nil))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeDeserializeRecordNameInvalidSchema(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	inner := test.LinkedList{
		Value: 100,
	}

	obj := test.Pizza{
		Size:     "Extra extra large",
		Toppings: []string{"anchovies", "mushrooms"},
	}

	innerSubject := "test.LinkedList"
	objSubject := "test.Author"

	subjects := make(map[string]interface{})
	subjects[innerSubject] = struct{}{}
	subjects[objSubject] = struct{}{}

	bytesInner, err := ser.Serialize(innerSubject, &inner)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.Serialize(objSubject, &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	deser.ProtoRegistry.RegisterMessage(inner.ProtoReflect().Type())
	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	newobj, err := deser.DeserializeRecordName(subjects, bytesInner)
	serde.MaybeFail("deserializeRecordNameValidSchema", serde.Expect(err, nil))
	serde.MaybeFail("deserializeRecordNameValidSchema", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), inner.ProtoReflect()))

	newobj, err = deser.DeserializeRecordName(subjects, bytesObj)
	serde.MaybeFail("deserializeRecordNameInvalidSchema", serde.Expect(err.Error(), "unfound subject declaration for test.Pizza"))
	serde.MaybeFail("deserializeRecordNameInvalidSchema", serde.Expect(newobj, nil))

}

func TestProtobufSerdeDeserializeIntoRecordName(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	inner := test.LinkedList{
		Value: 100,
	}

	obj := test.Pizza{
		Size:     "Extra extra large",
		Toppings: []string{"anchovies", "mushrooms"},
	}

	receiveInner := &test.LinkedList{}
	receiveObj := &test.Pizza{}

	innerSubject := "test.LinkedList"
	objSubject := "test.Pizza"

	subjects := make(map[string]interface{})
	subjects[innerSubject] = receiveInner
	subjects[objSubject] = receiveObj

	bytesInner, err := ser.Serialize(innerSubject, &inner)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.Serialize(objSubject, &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	deser.ProtoRegistry.RegisterMessage(inner.ProtoReflect().Type())
	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	err = deser.DeserializeIntoRecordName(subjects, bytesInner)
	serde.MaybeFail("deserializeIntoRecordNameValidSchema", serde.Expect(err, nil))
	err = deser.DeserializeIntoRecordName(subjects, bytesObj)
	serde.MaybeFail("deserializeIntoRecordNameValidSchema", serde.Expect(err, nil))

	serde.MaybeFail("deserializeIntoRecordNameValidSchema", serde.Expect(receiveInner.Value, int32(100)))

	serde.MaybeFail("deserializeIntoRecordNameValidSchema", serde.Expect(receiveObj.Size, "Extra extra large"))
	serde.MaybeFail("deserializeIntoRecordNameValidSchema", serde.Expect(receiveObj.Toppings[0], "anchovies"))
}

func TestProtobufSerdeDeserializeIntoRecordNameInvalidSchema(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	inner := test.LinkedList{
		Value: 100,
	}

	obj := test.Pizza{
		Size:     "Extra extra large",
		Toppings: []string{"anchovies", "mushrooms"},
	}

	receiveInner := &test.LinkedList{}
	receiveObj := &test.Author{}

	innerSubject := "test.LinkedList"
	objSubject := "test.Author"

	subjects := make(map[string]interface{})
	subjects[innerSubject] = receiveInner
	subjects[objSubject] = receiveObj

	bytesInner, err := ser.Serialize(innerSubject, &inner)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.Serialize(objSubject, &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	deser.ProtoRegistry.RegisterMessage(inner.ProtoReflect().Type())
	deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())

	err = deser.DeserializeIntoRecordName(subjects, bytesInner)
	serde.MaybeFail("deserializeIntoRecordNameValidSchema", serde.Expect(err, nil))

	err = deser.DeserializeIntoRecordName(subjects, bytesObj)
	serde.MaybeFail("deserializeIntoRecordNameInvalidSchema", serde.Expect(err.Error(), "unfound subject declaration for test.Pizza"))

	serde.MaybeFail("deserializeIntoRecordNameInvalidSchema", serde.Expect(receiveObj.Name, ""))
	serde.MaybeFail("deserializeIntoRecordNameInvalidSchema", serde.Expect(receiveObj.Id, int32(0)))
	serde.MaybeFail("deserializeIntoRecordNameInvalidSchema", serde.Expect(len(receiveObj.Works), 0))
}
