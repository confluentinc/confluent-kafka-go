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
	"errors"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/awskms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/azurekms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/gcpkms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/hcvault"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/localkms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/jsonata"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/test"
	"google.golang.org/protobuf/proto"
)

const (
	authorSchema = `
syntax = "proto3";

package test;
option go_package="../test";

import "confluent/meta.proto";

message Author {
  string name = 1 [
   (confluent.field_meta).tags = "PII"
  ];
  int32 id = 2;
  bytes picture = 3 [
   (confluent.field_meta).tags = "PII"
  ];
  repeated string works = 4;
}

message Pizza {
  string size = 1;
  repeated string toppings = 2;
}
`
	widgetSchema = `
syntax = "proto3";

package test;
option go_package="../test";

message Widget {
    string name = 1;
    int32 size = 2;
    int32 version = 3;
}
`
	newWidgetSchema = `
syntax = "proto3";

package test;
option go_package="../test";

message NewWidget {
    string name = 1;
    int32 height = 2;
    int32 version = 3;
}
`
	newerWidgetSchema = `
syntax = "proto3";

package test;
option go_package="../test";

message NewerWidget {
    string name = 1;
    int32 length = 2;
    int32 version = 3;
}
`
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

	err = deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

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

	err = deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

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

	err = deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

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

	err = deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

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

	err = deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeEmptyMessage(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")
	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)
	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)

	_, err = deser.Deserialize("topic1", nil)
	serde.MaybeFail("deserialization", err)
	_, err = deser.Deserialize("topic1", []byte{})
	serde.MaybeFail("deserialization", err)
}

func TestProtobufSerdeWithCELCondition(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	encRule := schemaregistry.Rule{
		Name: "test-cel",
		Kind: "CONDITION",
		Mode: "WRITE",
		Type: "CEL",
		Expr: "message.name == 'Kafka'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     authorSchema,
		SchemaType: "PROTOBUF",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := test.Author{
		Name:    "Kafka",
		Id:      123,
		Picture: []byte{1, 2},
		Works:   []string{"The Castle", "The Trial"},
	}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeWithCELConditionFail(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	encRule := schemaregistry.Rule{
		Name: "test-cel",
		Kind: "CONDITION",
		Mode: "WRITE",
		Type: "CEL",
		Expr: "message.name != 'Kafka'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     authorSchema,
		SchemaType: "PROTOBUF",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := test.Author{
		Name:    "Kafka",
		Id:      123,
		Picture: []byte{1, 2},
		Works:   []string{"The Castle", "The Trial"},
	}

	_, err = ser.Serialize("topic1", &obj)
	var ruleErr serde.RuleConditionErr
	errors.As(err, &ruleErr)
	serde.MaybeFail("serialization", nil, serde.Expect(ruleErr, serde.RuleConditionErr{Rule: &encRule}))
}

func TestProtobufSerdeWithCELFieldTransform(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	encRule := schemaregistry.Rule{
		Name: "test-cel",
		Kind: "TRANSFORM",
		Mode: "WRITE",
		Type: "CEL_FIELD",
		Expr: "name == 'name' ; value + '-suffix'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     authorSchema,
		SchemaType: "PROTOBUF",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := test.Author{
		Name:    "Kafka",
		Id:      123,
		Picture: []byte{1, 2},
		Works:   []string{"The Castle", "The Trial"},
	}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

	obj2 := test.Author{
		Name:    "Kafka-suffix",
		Id:      123,
		Picture: []byte{1, 2},
		Works:   []string{"The Castle", "The Trial"},
	}

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj2.ProtoReflect()))
}

func TestProtobufSerdeWithCELFieldCondition(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	encRule := schemaregistry.Rule{
		Name: "test-cel",
		Kind: "CONDITION",
		Mode: "WRITE",
		Type: "CEL_FIELD",
		Expr: "name == 'name' ; value == 'Kafka'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     authorSchema,
		SchemaType: "PROTOBUF",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := test.Author{
		Name:    "Kafka",
		Id:      123,
		Picture: []byte{1, 2},
		Works:   []string{"The Castle", "The Trial"},
	}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeWithCELFieldConditionFail(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	encRule := schemaregistry.Rule{
		Name: "test-cel",
		Kind: "CONDITION",
		Mode: "WRITE",
		Type: "CEL_FIELD",
		Expr: "name == 'name' ; value == 'hi'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     authorSchema,
		SchemaType: "PROTOBUF",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := test.Author{
		Name:    "Kafka",
		Id:      123,
		Picture: []byte{1, 2},
		Works:   []string{"The Castle", "The Trial"},
	}

	_, err = ser.Serialize("topic1", &obj)
	var ruleErr serde.RuleConditionErr
	errors.As(err, &ruleErr)
	serde.MaybeFail("serialization", nil, serde.Expect(ruleErr, serde.RuleConditionErr{Rule: &encRule}))
}

func TestProtobufSerdeEncryption(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.RuleConfig = map[string]string{
		"secret": "foo",
	}
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	encRule := schemaregistry.Rule{
		Name: "test-encrypt",
		Kind: "TRANSFORM",
		Mode: "WRITEREAD",
		Type: "ENCRYPT",
		Tags: []string{"PII"},
		Params: map[string]string{
			"encrypt.kek.name":   "kek1",
			"encrypt.kms.type":   "local-kms",
			"encrypt.kms.key.id": "mykey",
		},
		OnFailure: "ERROR,NONE",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     authorSchema,
		SchemaType: "PROTOBUF",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := test.Author{
		Name:    "Kafka",
		Id:      123,
		Picture: []byte{1, 2},
		Works:   []string{"The Castle", "The Trial"},
	}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "foo",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	err = deser.ProtoRegistry.RegisterMessage(obj.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), obj.ProtoReflect()))
}

func TestProtobufSerdeJSONataFullyCompatible(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	rule1To2 := "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])"
	rule2To1 := "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])"
	rule2To3 := "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])"
	rule3To2 := "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])"

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	widget := test.Widget{
		Name:    "alice",
		Size:    123,
		Version: 1,
	}

	info := schemaregistry.SchemaInfo{
		Schema:     widgetSchema,
		SchemaType: "PROTOBUF",
		References: nil,
		Metadata: &schemaregistry.Metadata{
			Tags:       nil,
			Properties: map[string]string{"application.version": "v1"},
			Sensitive:  nil,
		},
		Ruleset: nil,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	newWidget := test.NewWidget{
		Name:    "alice",
		Height:  123,
		Version: 1,
	}

	info = schemaregistry.SchemaInfo{
		Schema:     newWidgetSchema,
		SchemaType: "PROTOBUF",
		References: nil,
		Metadata: &schemaregistry.Metadata{
			Tags:       nil,
			Properties: map[string]string{"application.version": "v2"},
			Sensitive:  nil,
		},
		Ruleset: &schemaregistry.RuleSet{
			MigrationRules: []schemaregistry.Rule{
				schemaregistry.Rule{
					Name:      "myRule1",
					Doc:       "",
					Kind:      "TRANSFORM",
					Mode:      "UPGRADE",
					Type:      "JSONATA",
					Tags:      nil,
					Params:    nil,
					Expr:      rule1To2,
					OnSuccess: "",
					OnFailure: "",
					Disabled:  false,
				},
				schemaregistry.Rule{
					Name:      "myRule2",
					Doc:       "",
					Kind:      "TRANSFORM",
					Mode:      "DOWNGRADE",
					Type:      "JSONATA",
					Tags:      nil,
					Params:    nil,
					Expr:      rule2To1,
					OnSuccess: "",
					OnFailure: "",
					Disabled:  false,
				},
			},
			DomainRules: nil,
		},
	}

	id, err = client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	newerWidget := test.NewerWidget{
		Name:    "alice",
		Length:  123,
		Version: 1,
	}

	info = schemaregistry.SchemaInfo{
		Schema:     newerWidgetSchema,
		SchemaType: "PROTOBUF",
		References: nil,
		Metadata: &schemaregistry.Metadata{
			Tags:       nil,
			Properties: map[string]string{"application.version": "v3"},
			Sensitive:  nil,
		},
		Ruleset: &schemaregistry.RuleSet{
			MigrationRules: []schemaregistry.Rule{
				schemaregistry.Rule{
					Name:      "myRule1",
					Doc:       "",
					Kind:      "TRANSFORM",
					Mode:      "UPGRADE",
					Type:      "JSONATA",
					Tags:      nil,
					Params:    nil,
					Expr:      rule2To3,
					OnSuccess: "",
					OnFailure: "",
					Disabled:  false,
				},
				schemaregistry.Rule{
					Name:      "myRule2",
					Doc:       "",
					Kind:      "TRANSFORM",
					Mode:      "DOWNGRADE",
					Type:      "JSONATA",
					Tags:      nil,
					Params:    nil,
					Expr:      rule3To2,
					OnSuccess: "",
					OnFailure: "",
					Disabled:  false,
				},
			},
			DomainRules: nil,
		},
	}

	id, err = client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	serConfig1 := NewSerializerConfig()
	serConfig1.AutoRegisterSchemas = false
	serConfig1.UseLatestVersion = false
	serConfig1.UseLatestWithMetadata = map[string]string{
		"application.version": "v1",
	}

	ser1, err := NewSerializer(client, serde.ValueSerde, serConfig1)
	serde.MaybeFail("Serializer configuration", err)

	bytes, err := ser1.Serialize("topic1", &widget)
	serde.MaybeFail("serialization", err)

	deserializeWithAllVersions(err, client, ser1, bytes, widget, newWidget, newerWidget)

	serConfig2 := NewSerializerConfig()
	serConfig2.AutoRegisterSchemas = false
	serConfig2.UseLatestVersion = false
	serConfig2.UseLatestWithMetadata = map[string]string{
		"application.version": "v2",
	}

	ser2, err := NewSerializer(client, serde.ValueSerde, serConfig2)
	serde.MaybeFail("Serializer configuration", err)

	bytes, err = ser2.Serialize("topic1", &newWidget)
	serde.MaybeFail("serialization", err)

	deserializeWithAllVersions(err, client, ser2, bytes, widget, newWidget, newerWidget)

	serConfig3 := NewSerializerConfig()
	serConfig3.AutoRegisterSchemas = false
	serConfig3.UseLatestVersion = false
	serConfig3.UseLatestWithMetadata = map[string]string{
		"application.version": "v3",
	}

	ser3, err := NewSerializer(client, serde.ValueSerde, serConfig3)
	serde.MaybeFail("Serializer configuration", err)

	bytes, err = ser3.Serialize("topic1", &newerWidget)
	serde.MaybeFail("serialization", err)

	deserializeWithAllVersions(err, client, ser3, bytes, widget, newWidget, newerWidget)
}

func deserializeWithAllVersions(err error, client schemaregistry.Client, ser *Serializer,
	bytes []byte, widget test.Widget, newWidget test.NewWidget, newerWidget test.NewerWidget) {
	deserConfig1 := NewDeserializerConfig()
	deserConfig1.UseLatestWithMetadata = map[string]string{
		"application.version": "v1",
	}

	deser1, err := NewDeserializer(client, serde.ValueSerde, deserConfig1)
	serde.MaybeFail("Deserializer configuration", err)
	deser1.Client = ser.Client
	err = deser1.ProtoRegistry.RegisterMessage(widget.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

	newobj, err := deser1.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), widget.ProtoReflect()))

	deserConfig2 := NewDeserializerConfig()
	deserConfig2.UseLatestWithMetadata = map[string]string{
		"application.version": "v2",
	}

	deser2, err := NewDeserializer(client, serde.ValueSerde, deserConfig2)
	serde.MaybeFail("Deserializer configuration", err)
	deser2.Client = ser.Client
	err = deser2.ProtoRegistry.RegisterMessage(newWidget.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

	newobj, err = deser2.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), newWidget.ProtoReflect()))

	deserConfig3 := NewDeserializerConfig()
	deserConfig3.UseLatestWithMetadata = map[string]string{
		"application.version": "v3",
	}

	deser3, err := NewDeserializer(client, serde.ValueSerde, deserConfig3)
	serde.MaybeFail("Deserializer configuration", err)
	deser3.Client = ser.Client
	err = deser3.ProtoRegistry.RegisterMessage(newerWidget.ProtoReflect().Type())
	serde.MaybeFail("register message", err)

	newobj, err = deser3.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(proto.Message).ProtoReflect(), newerWidget.ProtoReflect()))
}

func BenchmarkProtobufSerWithReference(b *testing.B) {
	serde.MaybeFail = serde.InitFailFuncBenchmark(b)
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ser.Serialize("topic1", &obj)
	}
}

func BenchmarkProtobufSerWithReferenceCached(b *testing.B) {
	serde.MaybeFail = serde.InitFailFuncBenchmark(b)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConf := NewSerializerConfig()
	serConf.CacheSchemas = true
	ser, err := NewSerializer(client, serde.ValueSerde, serConf)
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ser.Serialize("topic1", &obj)
	}
}

func BenchmarkProtobufDeserWithReference(b *testing.B) {
	serde.MaybeFail = serde.InitFailFuncBenchmark(b)
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deser.Deserialize("topic1", bytes)
	}
}
