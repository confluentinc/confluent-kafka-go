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
	rn "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/test/avro/recordname"
)

func testMessageFactorySpecific(subject string, name string) (interface{}, error) {
	if subject != "topic1-value" {
		return nil, errors.New("message factory only handles topic1")
	}

	switch name {
	case "DemoSchema":
		return &test.DemoSchema{}, nil
	case "NestedTestRecord":
		return &test.NestedTestRecord{}, nil
	case "RecursiveUnionTestRecord":
		return &test.RecursiveUnionTestRecord{}, nil
	}

	return nil, errors.New("schema not found")
}

func TestSpecificAvroSerdeWithSimple(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	obj := test.NewDemoSchema()
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{0, 0, 0, 1}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewSpecificDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactorySpecific

	var newobj test.DemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestSpecificAvroSerdeWithNested(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

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

	deser, err := NewSpecificDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactorySpecific

	var newobj test.NestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestSpecificAvroSerdeWithCycle(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

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

	deser, err := NewSpecificDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactorySpecific

	var newobj test.RecursiveUnionTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

// as the avro schema does not define namespace
// use the Go namespace recordname.DemoSchema
var exampleNamespace = "recordname.DemoSchema"
var example = &rn.DemoSchema{
	StringField: "demoSchema from example",
}

// Declare mapBP as a global variable
var mapBP = map[string]rn.BasicPerson{
	"first": {
		Number: &rn.UnionLongNull{Long: 1},
		Name:   rn.UnionString{String: "Flo"},
	},
	"second": {
		Number: &rn.UnionLongNull{Long: 2},
		Name:   rn.UnionString{String: "Paul"},
	},
}

// namespace is python.test.advanced.advanced
var complexDTNamespace = "python.test.advanced.advanced"
var complexDT = &rn.Advanced{
	Number:  &rn.UnionLongNull{Long: 10},
	Name:    rn.UnionString{String: "Ari"},
	Friends: mapBP,
}

func TestAvroSpecificSerdeDeserializeRecordName(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(example, exampleNamespace)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(complexDT, complexDTNamespace)
	serde.MaybeFail("serialization", err)

	deser, err := NewSpecificDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	newobj, err := deser.DeserializeRecordName(bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `map[BoolField:false BytesField:[] DoubleField:0 IntField:0 StringField:demoSchema from example]`))

	newobj, err = deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", newobj), `map[family:map[] friends:map[first:map[name:map[string:Flo] number:map[long:1]] second:map[name:map[string:Paul] number:map[long:2]]] name:map[string:Ari] number:map[long:10]]`))
}

func RegisterMessageFactorySpecific() func(string, string) (interface{}, error) {
	return func(subject string, name string) (interface{}, error) {
		switch name {
		case exampleNamespace:
			return &rn.DemoSchema{}, nil
		case complexDTNamespace:
			return &rn.Advanced{}, nil
		}
		return nil, fmt.Errorf("No matching receiver")
	}
}

func RegisterMessageFactoryNoReceiverSpecific() func(string, string) (interface{}, error) {
	return func(subject string, name string) (interface{}, error) {
		return nil, fmt.Errorf("No matching receiver")
	}
}

func RegisterMessageFactoryInvalidReceiverSpecific() func(string, string) (interface{}, error) {
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

func TestAvroSpecificSerdeDeserializeRecordNameWithHandler(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(example)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(complexDT)
	serde.MaybeFail("serialization", err)

	deser, err := NewSpecificDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = RegisterMessageFactorySpecific()

	newobj, err := deser.DeserializeRecordName(bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*rn.DemoSchema).StringField, example.StringField))

	newobj, err = deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*rn.Advanced).Number.Long, complexDT.Number.Long))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*rn.Advanced).Name.String, complexDT.Name.String))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*rn.Advanced).Friends["first"].Name.String, complexDT.Friends["first"].Name.String))
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*rn.Advanced).Friends["second"].Number.Long, complexDT.Friends["second"].Number.Long))
}

func TestAvroSpecificSerdeDeserializeRecordNameWithHandlerNoReceiver(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeRecordName(example)
	serde.MaybeFail("serialization", err)

	deser, err := NewSpecificDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	// register invalid receiver
	deser.MessageFactory = RegisterMessageFactoryNoReceiverSpecific()

	newobj, err := deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "No matching receiver"))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
}

func TestAvroSpecificSerdeDeserializeRecordNameWithInvalidSchema(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(example)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(complexDT)
	serde.MaybeFail("serialization", err)

	deser, err := NewSpecificDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	// register invalid schema
	deser.MessageFactory = RegisterMessageFactoryInvalidReceiverSpecific()

	newobj, err := deser.DeserializeRecordName(bytesInner)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "No matching receiver"))

	newobj, err = deser.DeserializeRecordName(bytesObj)
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(err.Error(), "No matching receiver"))
	serde.MaybeFail("deserializeInvalidReceiver", serde.Expect(newobj, nil))
}

func TestAvroSpecificSerdeDeserializeIntoRecordName(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesInner, err := ser.SerializeRecordName(example)
	serde.MaybeFail("serialization", err)

	bytesObj, err := ser.SerializeRecordName(complexDT)
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[exampleNamespace] = &rn.DemoSchema{}
	receivers[complexDTNamespace] = &rn.Advanced{}

	deser, err := NewSpecificDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoRecordName(receivers, bytesInner)
	serde.MaybeFail("deserialization", err, serde.Expect(receivers[exampleNamespace].(*rn.DemoSchema).StringField, example.StringField))

	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(receivers[complexDTNamespace].(*rn.Advanced).Number.Long, complexDT.Number.Long))
	serde.MaybeFail("deserialization", err, serde.Expect(receivers[complexDTNamespace].(*rn.Advanced).Name.String, complexDT.Name.String))
	serde.MaybeFail("deserialization", err, serde.Expect(receivers[complexDTNamespace].(*rn.Advanced).Friends["first"].Name.String, complexDT.Friends["first"].Name.String))
	serde.MaybeFail("deserialization", err, serde.Expect(receivers[complexDTNamespace].(*rn.Advanced).Friends["second"].Number.Long, complexDT.Friends["second"].Number.Long))
}

func TestAvroSpecificSerdeDeserializeIntoRecordNameWithInvalidSchema(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeRecordName(example)
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[invalidSchema] = &rn.DemoSchema{}

	deser, err := NewSpecificDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "unfound subject declaration"))
	serde.MaybeFail("deserialization", serde.Expect(fmt.Sprintf("%v", receivers[invalidSchema]), `&{0 0  false []}`))
}

func TestAvroSpecificSerdeDeserializeIntoRecordNameWithInvalidReceiver(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	bytesObj, err := ser.SerializeRecordName(example)
	serde.MaybeFail("serialization", err)

	bytesInner, err := ser.SerializeRecordName(complexDT)
	serde.MaybeFail("serialization", err)

	var receivers = make(map[string]interface{})
	receivers[exampleNamespace] = &rn.Advanced{}
	receivers[complexDTNamespace] = ""

	deser, err := NewGenericDeserializer(client, serde.ValueSerde, NewDeserializerConfig())

	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.DeserializeIntoRecordName(receivers, bytesObj)
	serde.MaybeFail("deserialization", err, serde.Expect(fmt.Sprintf("%v", receivers[exampleNamespace]), `&{<nil> { 0} map[] map[]}`))

	err = deser.DeserializeIntoRecordName(receivers, bytesInner)
	serde.MaybeFail("deserialization", serde.Expect(err.Error(), "destination is not a pointer string"))
	serde.MaybeFail("deserialization", serde.Expect(receivers[complexDTNamespace], ""))
}

func TestAvroSpecificSerdeRecordNamePayloadMismatchSubject(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSpecificSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	_, err = ser.SerializeRecordName(example, "test.Pizza")
	serde.MaybeFail("serialization", serde.Expect(err.Error(), "the payload's fullyQualifiedName: 'recordname.DemoSchema' does not match the subject: 'test.Pizza'"))
}
