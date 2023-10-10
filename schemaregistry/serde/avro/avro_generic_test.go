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

package avro

import (
	"errors"
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/test"
)

func testMessageFactoryGeneric(subject string, name string) (interface{}, error) {
	if subject != "topic1-value" {
		return nil, errors.New("message factory only handles topic1")
	}

	switch name {
	case "GenericDemoSchema":
		return &GenericDemoSchema{}, nil
	case "GenericLinkedList":
		return &GenericLinkedList{}, nil
	case "GenericNestedTestRecord":
		return &GenericNestedTestRecord{}, nil
	}

	return nil, errors.New("schema not found")
}

func TestGenericAvroSerdeWithSimple(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewGenericSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	obj := GenericDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewGenericDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactoryGeneric

	var newobj GenericDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestGenericAvroSerdeWithNested(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewGenericSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	nested := GenericDemoSchema{}
	nested.IntField = 123
	nested.DoubleField = 45.67
	nested.StringField = "hi"
	nested.BoolField = true
	nested.BytesField = []byte{1, 2}
	obj := GenericNestedTestRecord{
		OtherField: nested,
	}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewGenericDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactoryGeneric

	var newobj GenericNestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestGenericAvroSerdeWithCycle(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewGenericSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	nested := GenericLinkedList{
		Value: 456,
	}
	obj := GenericLinkedList{
		Value: 123,
		Next:  &nested,
	}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewGenericDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactoryGeneric

	var newobj GenericLinkedList
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
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

const (
	linkedList    = "avro.LinkedList"
	pizza         = "avro.Pizza"
	invalidSchema = "invalidSchema"
)

type LinkedList struct {
	Value int
}

type Pizza struct {
	Size     string
	Toppings []string
}

type Author struct {
	Name string
}

var (
	inner = LinkedList{
		Value: 100,
	}

	obj = Pizza{
		Size:     "Extra extra large",
		Toppings: []string{"anchovies", "mushrooms"},
	}
)

func TestAvroGenericSerdeDeserializeRecordName(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewGenericSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(&inner)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(&obj, pizza)
	serde.MaybeFail("serialization", err)

	deser, err := NewGenericDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	newobj, err := deser.DeserializeRecordName(bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `map[Value:100]`))
	// access the newobj payload
	if obj, ok := newobj.(map[string]interface{}); ok {
		if value, ok := obj["Value"].(interface{}); ok {
			serde.MaybeFail("deserialization", serde.Expect(value.(int64), int64(100)))
		} else {
			fmt.Println("Value is not of type int")
		}
	}

	newobj, err = deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `map[Size:Extra extra large Toppings:[anchovies mushrooms]]`))
}

func RegisterMessageFactory() func(string, string) (interface{}, error) {
	return func(subject string, name string) (interface{}, error) {
		switch name {
		case linkedList:
			return &LinkedList{}, nil
		case pizza:
			return &Pizza{}, nil
		}
		return nil, fmt.Errorf("No matching receiver")
	}
}

func RegisterMessageFactoryNoReceiver() func(string, string) (interface{}, error) {
	return func(subject string, name string) (interface{}, error) {
		return nil, fmt.Errorf("No matching receiver")
	}
}

func RegisterMessageFactoryInvalidReceiver() func(string, string) (interface{}, error) {
	return func(subject string, name string) (interface{}, error) {
		switch name {
		case pizza:
			return &LinkedList{}, nil
		case linkedList:
			return "", nil
		}
		return nil, fmt.Errorf("No matching receiver")
	}
}

func TestAvroGenericSerdeDeserializeRecordNameWithHandler(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewGenericSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(&inner, linkedList)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(&obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewGenericDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = RegisterMessageFactory()

	newobj, err := deser.DeserializeRecordName(bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*LinkedList).Value, inner.Value))

	newobj, err = deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Size, obj.Size))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Toppings[0], obj.Toppings[0]))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*Pizza).Toppings[1], obj.Toppings[1]))
}

func TestAvroGenericSerdeDeserializeRecordNameWithHandlerNoReceiver(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewGenericSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeRecordName(&obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewGenericDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	// register invalid receiver
	deser.MessageFactory = RegisterMessageFactoryNoReceiver()

	newobj, err := deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "No matching receiver"))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
}

func TestAvroGenericSerdeDeserializeRecordNameWithInvalidSchema(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewGenericSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(&inner)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(&obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewGenericDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	// register invalid schema
	deser.MessageFactory = RegisterMessageFactoryInvalidReceiver()

	newobj, err := deser.DeserializeRecordName(bytesInner)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, ""))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "destination is not a pointer string"))

	newobj, err = deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserializeInvalidReceiver", err)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `&{0}`))
}

func TestAvroGenericSerdeDeserializeIntoRecordName(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewGenericSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(&inner)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(&obj, pizza)
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[linkedList] = &LinkedList{}
	receivers[pizza] = &Pizza{}

	deser, err := NewGenericDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoRecordName(receivers, bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(int(receivers[linkedList].(*LinkedList).Value), 100))

	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(receivers[pizza].(*Pizza).Toppings[0], obj.Toppings[0]))
	serde.MaybeFail("deserialization", err, serde.Expect(receivers[pizza].(*Pizza).Toppings[1], obj.Toppings[1]))
}

func TestAvroGenericSerdeDeserializeIntoRecordNameWithInvalidSchema(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewGenericSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeRecordName(&obj)
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[invalidSchema] = &Pizza{}

	deser, err := NewGenericDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "unfound subject declaration"))
	serde.MaybeFail("deserialization", serde.Expect(receivers[invalidSchema].(*Pizza).Size, ""))
}

func TestAvroGenericSerdeDeserializeIntoRecordNameWithInvalidReceiver(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewGenericSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeRecordName(&obj)
	serde.MaybeFail("serialization", err)

	bytesInner, err := ser.SerializeRecordName(&inner)
	serde.MaybeFail("serialization", err)

	aut := Author{
		Name: "aut",
	}
	bytesAut, err := ser.SerializeRecordName(&aut, "avro.Author")
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[pizza] = &LinkedList{}
	receivers[linkedList] = ""

	deser, err := NewGenericDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprint(receivers[pizza]), `&{0}`))

	err = deser.DeserializeIntoRecordName(receivers, bytesInner)
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "destination is not a pointer string"))
	err = deser.DeserializeIntoRecordName(receivers, bytesAut)
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "unfound subject declaration"))
}

func TestAvroGenericSerdeRecordNamePayloadMismatchSubject(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewGenericSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	_, err = ser.SerializeRecordName(&obj, "test.Pizza")
	serde.MaybeFail("serialization", serde.Expect(err.Error(), "the payload's fullyQualifiedName: 'avro.Pizza' does not match the subject: 'test.Pizza'"))
}
