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

package avrov2

import (
	"errors"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/awskms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/azurekms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/gcpkms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/hcvault"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/localkms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/jsonata"
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

const (
	rootSchema = `
{
  "name": "NestedTestRecord",
  "type": "record",
  "fields": [
    {
      "name": "OtherField",
      "type": "DemoSchema"
    }
  ]
}
`
	demoSchema = `
{
  "name": "DemoSchema",
  "type": "record",
  "fields": [
    {
      "name": "IntField",
      "type": "int"
    },
    {
      "name": "DoubleField",
      "type": "double"
    },
    {
      "name": "StringField",
      "type": "string",
      "confluent:tags": [ "PII" ]
    },
    {
      "name": "BoolField",
      "type": "boolean"
    },
    {
      "name": "BytesField",
      "type": "bytes",
      "confluent:tags": [ "PII" ]
    }
  ]
} 
`
	rootPointerSchema = `
{
  "name": "NestedTestPointerRecord",
  "type": "record",
  "fields": [
    {
      "name": "OtherField",
      "type": ["null", "DemoSchema"]
    }
  ]
}
`
	demoSchemaWithUnion = `
{
  "name": "DemoSchemaWithUnion",
  "type": "record",
  "fields": [
    {
      "name": "IntField",
      "type": "int"
    },
    {
      "name": "DoubleField",
      "type": "double"
    },
    {
      "name": "StringField",
      "type": ["null", "string"],
      "confluent:tags": [ "PII" ]
    },
    {
      "name": "BoolField",
      "type": "boolean"
    },
    {
      "name": "BytesField",
      "type": ["null", "bytes"],
      "confluent:tags": [ "PII" ]
    }
  ]
}
`
	complexSchema = `
{
  "name": "ComplexSchema",
  "type": "record",
  "fields": [
    {
      "name": "ArrayField",
      "type": {
        "type": "array",
        "items": "string"
       }
    },
    {
      "name": "MapField",
      "type": {
        "type": "map",
        "values": "string"
       }
    },
    {
      "name": "UnionField",
      "type": ["null", "string"],
      "confluent:tags": [ "PII" ]
    }
  ]
}
`
)

func testMessageFactory(subject string, name string) (interface{}, error) {
	if subject != "" && subject != "topic1-value" {
		return nil, errors.New("message factory only handles topic1")
	}

	switch name {
	case "DemoSchema":
		return &DemoSchema{}, nil
	case "DemoSchemaWithUnion":
		return &DemoSchemaWithUnion{}, nil
	case "ComplexSchema":
		return &ComplexSchema{}, nil
	case "NestedTestRecord":
		return &NestedTestRecord{}, nil
	case "NestedTestPointerRecord":
		return &NestedTestPointerRecord{}, nil
	case "OldWidget":
		return &OldWidget{}, nil
	case "NewWidget":
		return &NewWidget{}, nil
	case "NewerWidget":
		return &NewerWidget{}, nil
	}

	return nil, errors.New("schema not found")
}

func TestAvroSerdeWithSimple(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj DemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestAvroSerdeWithNested(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	nested := DemoSchema{}
	nested.IntField = 123
	nested.DoubleField = 45.67
	nested.StringField = "hi"
	nested.BoolField = true
	nested.BytesField = []byte{1, 2}
	obj := NestedTestRecord{
		OtherField: nested,
	}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj NestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestAvroSerdeWithReferences(t *testing.T) {
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

	info := schemaregistry.SchemaInfo{
		Schema:     string(demoSchema),
		SchemaType: "AVRO",
	}

	id, err := client.Register("demo-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	info = schemaregistry.SchemaInfo{
		Schema:     string(rootSchema),
		SchemaType: "AVRO",
		References: []schemaregistry.Reference{
			schemaregistry.Reference{
				Name:    "DemoSchema",
				Subject: "demo-value",
				Version: 1,
			},
		},
	}

	id, err = client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	nested := DemoSchema{}
	nested.IntField = 123
	nested.DoubleField = 45.67
	nested.StringField = "hi"
	nested.BoolField = true
	nested.BytesField = []byte{1, 2}
	obj := NestedTestRecord{
		OtherField: nested,
	}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj NestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestAvroSerdeWithCELCondition(t *testing.T) {
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
		Expr: "message.StringField == 'hi'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))
}

func TestAvroSerdeWithCELConditionFail(t *testing.T) {
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
		Expr: "message.StringField != 'hi'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}

	_, err = ser.Serialize("topic1", &obj)
	var ruleErr serde.RuleConditionErr
	errors.As(err, &ruleErr)
	serde.MaybeFail("serialization", nil, serde.Expect(ruleErr, serde.RuleConditionErr{Rule: &encRule}))
}

func TestAvroSerdeWithCELFieldTransform(t *testing.T) {
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
		Expr: "name == 'StringField' ; value + '-suffix'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	obj2 := DemoSchema{}
	obj2.IntField = 123
	obj2.DoubleField = 45.67
	obj2.StringField = "hi-suffix"
	obj2.BoolField = true
	obj2.BytesField = []byte{1, 2}

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj2))
}

func TestAvroSerdeWithCELFieldTransformComplex(t *testing.T) {
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
		Expr: "typeName == 'STRING' ; value + '-suffix'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     complexSchema,
		SchemaType: "AVRO",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	str := "bye"
	obj := ComplexSchema{}
	obj.ArrayField = []string{"hello"}
	obj.MapField = map[string]string{"key": "world"}
	obj.UnionField = &str

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	str2 := "bye-suffix"
	obj2 := ComplexSchema{}
	obj2.ArrayField = []string{"hello-suffix"}
	obj2.MapField = map[string]string{"key": "world-suffix"}
	obj2.UnionField = &str2

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj2))
}

func TestAvroSerdeWithCELFieldTransformComplexWithNil(t *testing.T) {
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
		Expr: "typeName == 'STRING' ; value + '-suffix'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     complexSchema,
		SchemaType: "AVRO",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := ComplexSchema{}
	obj.ArrayField = []string{"hello"}
	obj.MapField = map[string]string{"key": "world"}
	obj.UnionField = nil

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	obj2 := ComplexSchema{}
	obj2.ArrayField = []string{"hello-suffix"}
	obj2.MapField = map[string]string{"key": "world-suffix"}
	obj2.UnionField = nil

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj2))
}

func TestAvroSerdeWithCELFieldCondition(t *testing.T) {
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
		Expr: "name == 'StringField' ; value == 'hi'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))
}

func TestAvroSerdeWithCELFieldConditionFail(t *testing.T) {
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
		Expr: "name == 'StringField' ; value == 'bye'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}

	_, err = ser.Serialize("topic1", &obj)
	var ruleErr serde.RuleConditionErr
	errors.As(err, &ruleErr)
	serde.MaybeFail("serialization", nil, serde.Expect(ruleErr, serde.RuleConditionErr{Rule: &encRule}))
}

func TestAvroSerdeEncryption(t *testing.T) {
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
		Schema:     demoSchema,
		SchemaType: "AVRO",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj.StringField = "hi"
	obj.BytesField = []byte{1, 2}

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "foo",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))
}

func TestAvroSerdeEncryptionWithReferences(t *testing.T) {
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

	info := schemaregistry.SchemaInfo{
		Schema:     string(demoSchema),
		SchemaType: "AVRO",
	}

	id, err := client.Register("demo-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

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

	info = schemaregistry.SchemaInfo{
		Schema:     string(rootSchema),
		SchemaType: "AVRO",
		References: []schemaregistry.Reference{
			schemaregistry.Reference{
				Name:    "DemoSchema",
				Subject: "demo-value",
				Version: 1,
			},
		},
		Ruleset: &ruleSet,
	}

	id, err = client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	nested := DemoSchema{}
	nested.IntField = 123
	nested.DoubleField = 45.67
	nested.StringField = "hi"
	nested.BoolField = true
	nested.BytesField = []byte{1, 2}
	obj := NestedTestRecord{
		OtherField: nested,
	}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj.OtherField.StringField = "hi"
	obj.OtherField.BytesField = []byte{1, 2}

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "foo",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))
}

func TestAvroSerdeEncryptionWithPointerReferences(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	nested := DemoSchema{}
	nested.IntField = 123
	nested.DoubleField = 45.67
	nested.StringField = "hi"
	nested.BoolField = true
	nested.BytesField = []byte{1, 2}
	obj := NestedTestPointerRecord{
		OtherField: &nested,
	}

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
	ser.RegisterTypeFromMessageFactory("DemoSchema", testMessageFactory)

	info := schemaregistry.SchemaInfo{
		Schema:     string(demoSchema),
		SchemaType: "AVRO",
	}

	id, err := client.Register("demo-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

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

	info = schemaregistry.SchemaInfo{
		Schema:     string(rootPointerSchema),
		SchemaType: "AVRO",
		References: []schemaregistry.Reference{
			schemaregistry.Reference{
				Name:    "DemoSchema",
				Subject: "demo-value",
				Version: 1,
			},
		},
		Ruleset: &ruleSet,
	}

	id, err = client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj.OtherField.StringField = "hi"
	obj.OtherField.BytesField = []byte{1, 2}

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "foo",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory
	deser.RegisterTypeFromMessageFactory("DemoSchema", testMessageFactory)

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))
}

func TestAvroSerdeEncryptionWithUnion(t *testing.T) {
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
		Schema:     demoSchemaWithUnion,
		SchemaType: "AVRO",
		Ruleset:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	str := "hi"
	b := []byte{1, 2}
	obj := DemoSchemaWithUnion{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = &str
	obj.BoolField = true
	obj.BytesField = &b

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj.StringField = &str
	obj.BytesField = &b

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "foo",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))
}

func TestAvroSerdeJSONataFullyCompatible(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	rule1To2 := "$merge([$sift($, function($v, $k) {$k != 'Size'}), {'Height': $.'Size'}])"
	rule2To1 := "$merge([$sift($, function($v, $k) {$k != 'Height'}), {'Size': $.'Height'}])"
	rule2To3 := "$merge([$sift($, function($v, $k) {$k != 'Height'}), {'Length': $.'Height'}])"
	rule3To2 := "$merge([$sift($, function($v, $k) {$k != 'Length'}), {'Height': $.'Length'}])"

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	widget := OldWidget{
		Name:    "alice",
		Size:    123,
		Version: 1,
	}
	avroSchema, err := StructToSchema(reflect.TypeOf(widget))
	serde.MaybeFail("StructToSchema", err)

	info := schemaregistry.SchemaInfo{
		Schema:     avroSchema.String(),
		SchemaType: "AVRO",
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

	newWidget := NewWidget{
		Name:    "alice",
		Height:  123,
		Version: 1,
	}
	avroSchema, err = StructToSchema(reflect.TypeOf(newWidget))
	serde.MaybeFail("StructToSchema", err)

	info = schemaregistry.SchemaInfo{
		Schema:     avroSchema.String(),
		SchemaType: "AVRO",
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

	newerWidget := NewerWidget{
		Name:    "alice",
		Length:  123,
		Version: 1,
	}
	avroSchema, err = StructToSchema(reflect.TypeOf(newerWidget))
	serde.MaybeFail("StructToSchema", err)

	info = schemaregistry.SchemaInfo{
		Schema:     avroSchema.String(),
		SchemaType: "AVRO",
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
	bytes []byte, widget OldWidget, newWidget NewWidget, newerWidget NewerWidget) {
	deserConfig1 := NewDeserializerConfig()
	deserConfig1.UseLatestWithMetadata = map[string]string{
		"application.version": "v1",
	}

	deser1, err := NewDeserializer(client, serde.ValueSerde, deserConfig1)
	serde.MaybeFail("Deserializer configuration", err)
	deser1.Client = ser.Client
	deser1.MessageFactory = testMessageFactory

	newobj, err := deser1.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &widget))

	deserConfig2 := NewDeserializerConfig()
	deserConfig2.UseLatestWithMetadata = map[string]string{
		"application.version": "v2",
	}

	deser2, err := NewDeserializer(client, serde.ValueSerde, deserConfig2)
	serde.MaybeFail("Deserializer configuration", err)
	deser2.Client = ser.Client
	deser2.MessageFactory = testMessageFactory

	newobj, err = deser2.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &newWidget))

	deserConfig3 := NewDeserializerConfig()
	deserConfig3.UseLatestWithMetadata = map[string]string{
		"application.version": "v3",
	}

	deser3, err := NewDeserializer(client, serde.ValueSerde, deserConfig3)
	serde.MaybeFail("Deserializer configuration", err)
	deser3.Client = ser.Client
	deser3.MessageFactory = testMessageFactory

	newobj, err = deser3.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &newerWidget))
}

type DemoSchema struct {
	IntField int32 `json:"IntField"`

	DoubleField float64 `json:"DoubleField"`

	StringField string `json:"StringField"`

	BoolField bool `json:"BoolField"`

	BytesField []byte `json:"BytesField"`
}

type DemoSchemaWithUnion struct {
	IntField int32 `json:"IntField"`

	DoubleField float64 `json:"DoubleField"`

	StringField *string `json:"StringField"`

	BoolField bool `json:"BoolField"`

	BytesField *[]byte `json:"BytesField"`
}

type ComplexSchema struct {
	ArrayField []string `json:"ArrayField"`

	MapField map[string]string `json:"DoubleField"`

	UnionField *string `json:"StringField"`
}

type NestedTestRecord struct {
	OtherField DemoSchema
}

type NestedTestPointerRecord struct {
	OtherField *DemoSchema
}

type OldWidget struct {
	Name string `json:"name"`

	Size int `json:"size"`

	Version int `json:"version"`
}

type NewWidget struct {
	Name string `json:"name"`

	Height int `json:"height"`

	Version int `json:"version"`
}

type NewerWidget struct {
	Name string `json:"name"`

	Length int `json:"length"`

	Version int `json:"version"`
}
