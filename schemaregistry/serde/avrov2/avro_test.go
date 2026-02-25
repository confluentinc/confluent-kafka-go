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
	"reflect"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
	"github.com/hamba/avro/v2"

	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/awskms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/azurekms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/gcpkms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/hcvault"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/localkms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/jsonata"

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
	rootSchemaNested = `
{
  "name": "NestedTestRecord",
  "type": "record",
  "fields": [
    {
      "name": "OtherField",
      "type": 
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
	demoSchemaWithMissing = `
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
    },
    {
      "name": "Missing",
      "type": ["null", "string"],
      "default": null
    }
  ]
} 
`
	demoSchemaWithLogicalType = `
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
      "type": {
        "type": "string",
        "logicalType": "uuid"
      },
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
	demoSchemaSingleTag = `
{
  "name": "DemoSchemaSingleTag",
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
      "type": "bytes"
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
	schemaEvolution1 = `
{
  "name": "SchemaEvolution",
  "type": "record",
  "fields": [
    {
      "name": "FieldToDelete",
      "type": "string"
    }
  ]
}
`
	schemaEvolution2 = `
{
  "name": "SchemaEvolution",
  "type": "record",
  "fields": [
    {
      "name": "NewOptionalField",
      "type": "string",
      "default": "optional"
    }
  ]
}
`
	f1Schema = `
{
  "name": "F1Schema",
  "type": "record",
  "fields": [
    {
      "name": "F1",
      "type": "string",
      "confluent:tags": [ "PII" ]
    }
  ]
}
`
	demoWithSchemaFuncSchema = `
{
  "name": "DemoWithSchemaFunc",
  "type": "record",
  "fields": [
    {
      "name": "IntField",
      "type": "int"
    },
    {
      "name": "BoolField",
      "type": "boolean"
    },
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
      "name": "StringField",
      "type": ["null", "string"]
    },
    {
      "name": "EnumField",
      "type": {
        "name": "GreetingsEnum",
        "type": "enum",
        "symbols": ["hey", "bye"]
      }
    },
    {
      "name": "RecordField",
      "type": {
        "name": "GreetingsObj",
        "type": "record",
        "fields": [
          {
            "name": "Hey",
            "type": "string"
          }
        ]
      }
    }
  ]
}
`
	wrappedUnionSchema = `{
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "result",
      "type": [
        "null",
        {
          "fields": [
            {
              "name": "code",
              "type": "int"
            },
            {
              "confluent:tags": [
                "PII"
              ],
              "name": "secret",
              "type": [
                "null",
                "string"
              ]
            }
          ],
          "name": "Data",
          "type": "record"
        },
        {
          "fields": [
            {
              "name": "code",
              "type": "int"
            },
            {
              "name": "reason",
              "type": [
                "null",
                "string"
              ]
            }
          ],
          "name": "Error",
          "type": "record"
        }
      ]
    }
  ],
  "name": "Result",
  "namespace": "com.acme",
  "type": "record"
}
`
)

func testMessageFactory(subject string, name string) (interface{}, error) {
	switch name {
	case "DemoSchema":
		return &DemoSchema{}, nil
	case "DemoSchemaSingleTag":
		return &DemoSchemaSingleTag{}, nil
	case "DemoSchemaWithUnion":
		return &DemoSchemaWithUnion{}, nil
	case "DemoWithSchemaFunc":
		return &DemoWithSchemaFunc{}, nil
	case "ComplexSchema":
		return &ComplexSchema{}, nil
	case "SchemaEvolution":
		return &SchemaEvolution2{}, nil
	case "F1Schema":
		return &F1Schema{}, nil
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

	// serialize second object
	obj = DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "bye"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	bytes, err = ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	msg, err = deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestAvroSerdeWithGuidInHeader(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	ser.SchemaIDSerializer = serde.HeaderSchemaIDSerializer

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	headers, bytes, err := ser.SerializeWithHeaders("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj DemoSchema
	err = deser.DeserializeWithHeadersInto("topic1", headers, bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.DeserializeWithHeaders("topic1", headers, bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))

	// serialize second object
	obj = DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "bye"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	bytes, err = ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	msg, err = deser.DeserializeWithHeaders("topic1", headers, bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestAvroSerdeWithSimpleMap(t *testing.T) {
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
		Schema:     demoSchema,
		SchemaType: "AVRO",
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := make(map[string]interface{})
	obj["IntField"] = 123
	obj["DoubleField"] = 45.67
	obj["StringField"] = "hi"
	obj["BoolField"] = true
	obj["BytesField"] = []byte{1, 2}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj map[string]interface{}
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))
}

func TestAvroSerdeWithPrimitive(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	obj := "Hello, World!"
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj string
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))
}

func TestAvroSerdeWithBytes(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	obj := []byte{0x02, 0x03, 0x04}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)
	serde.MaybeFail("serialization", serde.Expect(bytes, []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04}))

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj []byte
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))
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

func TestAvroSerdeWithNestedMap(t *testing.T) {
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
		Schema:     rootSchemaNested,
		SchemaType: "AVRO",
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	nested := make(map[string]interface{})
	nested["IntField"] = 123
	nested["DoubleField"] = 45.67
	nested["StringField"] = "hi"
	nested["BoolField"] = true
	nested["BytesField"] = []byte{1, 2}
	obj := make(map[string]interface{})
	obj["OtherField"] = nested

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj map[string]interface{}
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))
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
			{
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

func TestAvroSerdeUnionWithReferences(t *testing.T) {
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
	_ = ser.RegisterTypeFromMessageFactory("DemoSchema", testMessageFactory)
	_ = ser.RegisterTypeFromMessageFactory("ComplexSchema", testMessageFactory)

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
		Schema:     string(complexSchema),
		SchemaType: "AVRO",
	}

	id, err = client.Register("complex-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	info = schemaregistry.SchemaInfo{
		Schema:     `[ "DemoSchema", "ComplexSchema" ]`,
		SchemaType: "AVRO",
		References: []schemaregistry.Reference{
			{
				Name:    "DemoSchema",
				Subject: "demo-value",
				Version: 1,
			},
			{
				Name:    "Complexchema",
				Subject: "complex-value",
				Version: 1,
			},
		},
	}

	id, err = client.Register("topic1-value", info, false)
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

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory
	_ = deser.RegisterTypeFromMessageFactory("DemoSchema", testMessageFactory)
	_ = deser.RegisterTypeFromMessageFactory("ComplexSchema", testMessageFactory)

	oldmap := map[string]interface{}{
		"DemoSchema": map[string]interface{}{
			"IntField":    123,
			"DoubleField": 45.67,
			"StringField": "hi",
			"BoolField":   true,
			"BytesField":  []byte{1, 2},
		},
	}

	// deserialize into map
	var newmap map[string]interface{}
	err = deser.DeserializeInto("topic1", bytes, &newmap)
	serde.MaybeFail("deserialization into", err, serde.Expect(newmap, oldmap))

	// deserialize into interface{}
	var newany interface{}
	err = deser.DeserializeInto("topic1", bytes, &newany)
	var newobj = newany.(DemoSchema)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))
}

func TestAvroSchemaEvolution(t *testing.T) {
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
		Schema:     string(schemaEvolution1),
		SchemaType: "AVRO",
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := SchemaEvolution1{}
	obj.FieldToDelete = "bye"

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	info = schemaregistry.SchemaInfo{
		Schema:     string(schemaEvolution2),
		SchemaType: "AVRO",
	}

	id, err = client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	client.ClearLatestCaches()

	deserConfig := NewDeserializerConfig()
	deserConfig.UseLatestVersion = true
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	obj2 := SchemaEvolution2{}
	obj2.NewOptionalField = "optional"

	var newobj SchemaEvolution2
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj2))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj2))
}

func TestAvroSerdeWithEncodingSchemaFunc(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	obj := DemoWithSchemaFunc{
		IntField:    123,
		StringField: nil,
		BoolField:   true,
		ArrayField:  []string{"hello", "world"},
		MapField: map[string]string{
			"hello": "world",
		},
		EnumField: "hey",
		RecordField: struct {
			Hey string `json:"Hey"`
		}{Hey: "bye"},
	}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj DemoWithSchemaFunc
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
		RuleSet:    &ruleSet,
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

func TestAvroSerdeWithCELConditionLogicalType(t *testing.T) {
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

	uuid := "550e8400-e29b-41d4-a716-446655440000"

	encRule := schemaregistry.Rule{
		Name: "test-cel",
		Kind: "CONDITION",
		Mode: "WRITE",
		Type: "CEL",
		Expr: "message.StringField == '" + uuid + "'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchemaWithLogicalType,
		SchemaType: "AVRO",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = uuid
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
		RuleSet:    &ruleSet,
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
	serde.MaybeFail("serialization", nil, serde.Expect(encRule, *ruleErr.Rule))
}

func TestAvroSerdeWithCELConditionIgnoreFail(t *testing.T) {
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
		Name:      "test-cel",
		Kind:      "CONDITION",
		Mode:      "WRITE",
		Type:      "CEL",
		Expr:      "message.StringField != 'hi'",
		OnFailure: "NONE",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
		RuleSet:    &ruleSet,
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

func TestAvroSerdeWithCELFieldTransformDisable(t *testing.T) {
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
		RuleSet:    &ruleSet,
	}

	registry := serde.NewRuleRegistry()
	registry.RegisterExecutor(cel.NewFieldExecutor())
	registry.RegisterOverride(&serde.RuleOverride{
		Type:      "CEL_FIELD",
		OnSuccess: nil,
		OnFailure: nil,
		Disabled:  &[]bool{true}[0],
	})
	ser.RuleRegistry = &registry

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
	serde.MaybeFail("deserialization", err, serde.Expect(newobj.(*DemoSchema).StringField, "hi"))
}

func TestAvroSerdeWithCELFieldTransformMissingProp(t *testing.T) {
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
		Mode: "WRITEREAD",
		Type: "CEL_FIELD",
		Expr: "name == 'StringField' ; value + '-suffix'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchemaWithMissing,
		SchemaType: "AVRO",
		RuleSet:    &ruleSet,
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
	obj2.StringField = "hi-suffix-suffix"
	obj2.BoolField = true
	obj2.BytesField = []byte{1, 2}

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj2))
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
		RuleSet:    &ruleSet,
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
		RuleSet:    &ruleSet,
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
		RuleSet:    &ruleSet,
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
		RuleSet:    &ruleSet,
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
		RuleSet:    &ruleSet,
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

type clock struct{}

func (*clock) NowUnixMilli() int64 {
	return time.Now().UnixMilli()
}

type fakeClock struct {
	now int64
}

func (c *fakeClock) NowUnixMilli() int64 {
	return c.now
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
		"secret": "mysecret",
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
		RuleSet:    &ruleSet,
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
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))
}

func TestAvroSerdePayloadEncryption(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	encRule := schemaregistry.Rule{
		Name: "test-encrypt",
		Kind: "TRANSFORM",
		Mode: "WRITEREAD",
		Type: "ENCRYPT_PAYLOAD",
		Params: map[string]string{
			"encrypt.kek.name":   "kek1",
			"encrypt.kms.type":   "local-kms",
			"encrypt.kms.key.id": "mykey",
		},
		OnFailure: "ERROR,NONE",
	}
	ruleSet := schemaregistry.RuleSet{
		EncodingRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
		RuleSet:    &ruleSet,
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
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))
}

func TestAvroSerdeEncryptionAlternateKeks(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.RuleConfig = map[string]string{
		"secret":                        "mysecret",
		"encrypt.alternate.kms.key.ids": "mykey2,mykey3",
	}
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	encRule := schemaregistry.Rule{
		Name: "test-encrypt",
		Kind: "TRANSFORM",
		Mode: "WRITEREAD",
		Type: "ENCRYPT_PAYLOAD",
		Params: map[string]string{
			"encrypt.kek.name":   "kek1",
			"encrypt.kms.type":   "local-kms",
			"encrypt.kms.key.id": "mykey",
		},
		OnFailure: "ERROR,NONE",
	}
	ruleSet := schemaregistry.RuleSet{
		EncodingRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
		RuleSet:    &ruleSet,
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
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))
}

func TestAvroSerdeEncryptionDeterministic(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
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
			"encrypt.kek.name":      "kek1",
			"encrypt.kms.type":      "local-kms",
			"encrypt.kms.key.id":    "mykey",
			"encrypt.dek.algorithm": "AES256_SIV",
		},
		OnFailure: "ERROR,NONE",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
		RuleSet:    &ruleSet,
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
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))
}

func TestAvroSerdeEncryptionWithSimpleMap(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
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
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := make(map[string]interface{})
	obj["IntField"] = 123
	obj["DoubleField"] = 45.67
	obj["StringField"] = "hi"
	obj["BoolField"] = true
	obj["BytesField"] = []byte{1, 2}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj["StringField"] = "hi"
	obj["BytesField"] = []byte{1, 2}

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj map[string]interface{}
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))
}

func TestAvroSerdeEncryptionWithWrappedUnion(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
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
		Schema:     wrappedUnionSchema,
		SchemaType: "AVRO",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := make(map[string]interface{})
	obj["id"] = 123
	result := make(map[string]interface{})
	result["com.acme.Data"] = map[string]interface{}{
		"code":   456,
		"secret": "mypii",
	}
	obj["result"] = result

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	// Note that we use a wrapped union for secret
	obj["result"].(map[string]interface{})["com.acme.Data"].(map[string]interface{})["secret"] = map[string]interface{}{
		"string": "mypii",
	}

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj map[string]interface{}
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))
}

func TestAvroSerdeEncryptionDekRotation(t *testing.T) {
	f := fakeClock{now: time.Now().UnixMilli()}
	executor := encryption.RegisterFieldExecutorWithClock(&f)

	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
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
			"encrypt.kek.name":        "kek1",
			"encrypt.kms.type":        "local-kms",
			"encrypt.kms.key.id":      "mykey",
			"encrypt.dek.expiry.days": "1",
		},
		OnFailure: "ERROR,NONE",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     demoSchemaSingleTag,
		SchemaType: "AVRO",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := DemoSchemaSingleTag{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj.StringField = "hi"

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))

	dek, err := executor.Executor.Client.GetDekVersion(
		"kek1", "topic1-value", -1, "AES256_GCM", false)
	serde.MaybeFail("DEK retrieval", err, serde.Expect(dek.Version, 1))

	// Advance 2 days
	f.now = time.Now().AddDate(0, 0, 2).UnixMilli()

	obj = DemoSchemaSingleTag{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}

	bytes, err = ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj.StringField = "hi"

	newobj, err = deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))

	dek, err = executor.Executor.Client.GetDekVersion(
		"kek1", "topic1-value", -1, "AES256_GCM", false)
	serde.MaybeFail("DEK retrieval", err, serde.Expect(dek.Version, 2))

	// Advance 2 days
	f.now = time.Now().AddDate(0, 0, 2).UnixMilli()

	obj = DemoSchemaSingleTag{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}

	bytes, err = ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj.StringField = "hi"

	newobj, err = deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))

	dek, err = executor.Executor.Client.GetDekVersion(
		"kek1", "topic1-value", -1, "AES256_GCM", false)
	serde.MaybeFail("DEK retrieval", err, serde.Expect(dek.Version, 3))

	executor.Executor.Client.Close()
}

func TestAvroSerdeEncryptionF1Preserialized(t *testing.T) {
	c := clock{}
	executor := encryption.RegisterFieldExecutorWithClock(&c)

	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

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
		OnFailure: "ERROR,ERROR",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     f1Schema,
		SchemaType: "AVRO",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := F1Schema{}
	obj.F1 = "hello world"

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.MessageFactory = testMessageFactory

	executor.Executor.Client.RegisterKek("kek1", "local-kms", "mykey", make(map[string]string), "", false)
	serde.MaybeFail("Kek registration", err)

	encryptedDek := "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4="
	executor.Executor.Client.RegisterDek("kek1", "topic1-value", "AES256_GCM", encryptedDek)
	serde.MaybeFail("Dek registration", err)

	bytes := []byte{0, 0, 0, 0, 1, 104, 122, 103, 121, 47, 106, 70, 78, 77, 86, 47, 101, 70, 105, 108, 97, 72, 114, 77, 121, 101, 66, 103, 100, 97, 86, 122, 114, 82, 48, 117, 100, 71, 101, 111, 116, 87, 56, 99, 65, 47, 74, 97, 108, 55, 117, 107, 114, 43, 77, 47, 121, 122}
	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))

	executor.Executor.Client.Close()
}

func TestAvroSerdeEncryptionDeterministicF1Preserialized(t *testing.T) {
	c := clock{}
	executor := encryption.RegisterFieldExecutorWithClock(&c)

	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	encRule := schemaregistry.Rule{
		Name: "test-encrypt",
		Kind: "TRANSFORM",
		Mode: "WRITEREAD",
		Type: "ENCRYPT",
		Tags: []string{"PII"},
		Params: map[string]string{
			"encrypt.kek.name":      "kek1",
			"encrypt.kms.type":      "local-kms",
			"encrypt.kms.key.id":    "mykey",
			"encrypt.dek.algorithm": "AES256_SIV",
		},
		OnFailure: "ERROR,ERROR",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     f1Schema,
		SchemaType: "AVRO",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := F1Schema{}
	obj.F1 = "hello world"

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.MessageFactory = testMessageFactory

	executor.Executor.Client.RegisterKek("kek1", "local-kms", "mykey", make(map[string]string), "", false)
	serde.MaybeFail("Kek registration", err)

	encryptedDek := "YSx3DTlAHrmpoDChquJMifmPntBzxgRVdMzgYL82rgWBKn7aUSnG+WIu9ozBNS3y2vXd++mBtK07w4/W/G6w0da39X9hfOVZsGnkSvry/QRht84V8yz3dqKxGMOK5A=="
	executor.Executor.Client.RegisterDek("kek1", "topic1-value", "AES256_SIV", encryptedDek)
	serde.MaybeFail("Dek registration", err)

	bytes := []byte{0, 0, 0, 0, 1, 72, 68, 54, 89, 116, 120, 114, 108, 66, 110, 107, 84, 87, 87, 57, 78, 54, 86, 98, 107, 51, 73, 73, 110, 106, 87, 72, 56, 49, 120, 109, 89, 104, 51, 107, 52, 100}
	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))

	executor.Executor.Client.Close()
}

func TestAvroSerdeEncryptionDekRotationF1Preserialized(t *testing.T) {
	c := clock{}
	executor := encryption.RegisterFieldExecutorWithClock(&c)

	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	encRule := schemaregistry.Rule{
		Name: "test-encrypt",
		Kind: "TRANSFORM",
		Mode: "WRITEREAD",
		Type: "ENCRYPT",
		Tags: []string{"PII"},
		Params: map[string]string{
			"encrypt.kek.name":        "kek1",
			"encrypt.kms.type":        "local-kms",
			"encrypt.kms.key.id":      "mykey",
			"encrypt.dek.expiry.days": "1",
		},
		OnFailure: "ERROR,ERROR",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     f1Schema,
		SchemaType: "AVRO",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := F1Schema{}
	obj.F1 = "hello world"

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.MessageFactory = testMessageFactory

	executor.Executor.Client.RegisterKek("kek1", "local-kms", "mykey", make(map[string]string), "", false)
	serde.MaybeFail("Kek registration", err)

	encryptedDek := "W/v6hOQYq1idVAcs1pPWz9UUONMVZW4IrglTnG88TsWjeCjxmtRQ4VaNe/I5dCfm2zyY9Cu0nqdvqImtUk4="
	executor.Executor.Client.RegisterDek("kek1", "topic1-value", "AES256_GCM", encryptedDek)
	serde.MaybeFail("Dek registration", err)

	bytes := []byte{0, 0, 0, 0, 1, 120, 65, 65, 65, 65, 65, 65, 71, 52, 72, 73, 54, 98, 49, 110, 88, 80, 88, 113, 76, 121, 71, 56, 99, 73, 73, 51, 53, 78, 72, 81, 115, 101, 113, 113, 85, 67, 100, 43, 73, 101, 76, 101, 70, 86, 65, 101, 78, 112, 83, 83, 51, 102, 120, 80, 110, 74, 51, 50, 65, 61}
	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))

	executor.Executor.Client.Close()
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
		"secret": "mysecret",
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
			{
				Name:    "DemoSchema",
				Subject: "demo-value",
				Version: 1,
			},
		},
		RuleSet: &ruleSet,
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
		"secret": "mysecret",
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
		"secret": "mysecret",
	}
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)
	_ = ser.RegisterTypeFromMessageFactory("DemoSchema", testMessageFactory)

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
			{
				Name:    "DemoSchema",
				Subject: "demo-value",
				Version: 1,
			},
		},
		RuleSet: &ruleSet,
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
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory
	_ = deser.RegisterTypeFromMessageFactory("DemoSchema", testMessageFactory)

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
		"secret": "mysecret",
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
		RuleSet:    &ruleSet,
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
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	newobj, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &obj))
}

func TestAvroSerdeJSONataWithCEL(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	rule1To2 := "$merge([$sift($, function($v, $k) {$k != 'Size'}), {'Height': $.'Size'}])"

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
		RuleSet: nil,
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
		RuleSet: &schemaregistry.RuleSet{
			MigrationRules: []schemaregistry.Rule{
				{
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
			},
			DomainRules: []schemaregistry.Rule{
				{
					Name:      "myRule2",
					Doc:       "",
					Kind:      "TRANSFORM",
					Mode:      "READ",
					Type:      "CEL_FIELD",
					Tags:      nil,
					Params:    nil,
					Expr:      "name == 'Name' ; value + '-suffix'",
					OnSuccess: "",
					OnFailure: "",
					Disabled:  false,
				},
			},
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

	deserConfig2 := NewDeserializerConfig()
	deserConfig2.UseLatestWithMetadata = map[string]string{
		"application.version": "v2",
	}

	deser2, err := NewDeserializer(client, serde.ValueSerde, deserConfig2)
	serde.MaybeFail("Deserializer configuration", err)
	deser2.Client = ser1.Client
	deser2.MessageFactory = testMessageFactory

	newWidget2 := NewWidget{
		Name:    "alice-suffix",
		Height:  123,
		Version: 1,
	}

	newobj, err := deser2.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &newWidget2))

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
		RuleSet: nil,
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
		RuleSet: &schemaregistry.RuleSet{
			MigrationRules: []schemaregistry.Rule{
				{
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
				{
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
		RuleSet: &schemaregistry.RuleSet{
			MigrationRules: []schemaregistry.Rule{
				{
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
				{
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

	deserializeWithAllVersions(client, ser1, bytes, widget, newWidget, newerWidget)

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

	deserializeWithAllVersions(client, ser2, bytes, widget, newWidget, newerWidget)

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

	deserializeWithAllVersions(client, ser3, bytes, widget, newWidget, newerWidget)
}

func deserializeWithAllVersions(client schemaregistry.Client, ser *Serializer,
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

type DemoSchemaSingleTag struct {
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

type F1Schema struct {
	F1 string `json:"F1"`
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

type SchemaEvolution1 struct {
	FieldToDelete string `json:"FieldToDelete"`
}

type SchemaEvolution2 struct {
	NewOptionalField string `json:"NewOptionalField"`
}

type DemoWithSchemaFunc struct {
	IntField    int32             `json:"IntField"`
	BoolField   bool              `json:"BoolField"`
	StringField *string           `json:"StringField"`
	ArrayField  []string          `json:"ArrayField"`
	MapField    map[string]string `json:"MapField"`
	EnumField   string            `json:"EnumField"`
	RecordField struct {
		Hey string `json:"Hey"`
	} `json:"RecordField"`
}

func (d *DemoWithSchemaFunc) Schema() avro.Schema {
	return avro.MustParse(demoWithSchemaFuncSchema)
}

func TestAvroSerdeWithRecordNameStrategy(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.SubjectNameStrategyType = serde.RecordNameStrategyType
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deserConfig.SubjectNameStrategyType = serde.RecordNameStrategyType
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj DemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestAvroSerdeWithTopicRecordNameStrategy(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.SubjectNameStrategyType = serde.TopicRecordNameStrategyType
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deserConfig.SubjectNameStrategyType = serde.TopicRecordNameStrategyType
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj DemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestAvroSerdeWithAssociatedNameStrategy(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	// Register schema with a custom subject name
	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
	}

	id, err := client.Register("my-custom-subject", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	// Create association between topic1 and my-custom-subject
	assocRequest := schemaregistry.AssociationCreateOrUpdateRequest{
		ResourceName:      "topic1",
		ResourceNamespace: "-",
		ResourceID:        "lkc-123:topic1",
		ResourceType:      "topic",
		Associations: []schemaregistry.AssociationCreateOrUpdateInfo{
			{
				Subject:         "my-custom-subject",
				AssociationType: "value",
			},
		},
	}
	_, err = client.CreateAssociation(assocRequest)
	serde.MaybeFail("Association creation", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.SubjectNameStrategyType = serde.AssociatedNameStrategyType
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deserConfig.SubjectNameStrategyType = serde.AssociatedNameStrategyType
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	var newobj DemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization into", err, serde.Expect(newobj, obj))

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestAvroSerdeWithAssociatedNameStrategyFallbackToTopic(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	// Register schema with topic name strategy (no association)
	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	// No association created - should fall back to TopicNameStrategy

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.SubjectNameStrategyType = serde.AssociatedNameStrategyType
	// Default fallback is TOPIC
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
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

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestAvroSerdeWithAssociatedNameStrategyFallbackNone(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	// Register schema with some subject (but no association will be created)
	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	// No association created, and fallback is NONE - should error

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.SubjectNameStrategyType = serde.AssociatedNameStrategyType
	serConfig.SubjectNameStrategyConfig = map[string]string{
		serde.FallbackSubjectNameStrategyTypeConfig: "NONE",
	}
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	_, err = ser.Serialize("topic1", &obj)
	if err == nil {
		t.Errorf("Expected error when no association found and fallback is NONE")
	}
}

func TestAvroSerdeWithAssociatedNameStrategyMultipleAssociations(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	// Register two schemas with different subjects
	info1 := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
	}
	id, err := client.Register("subject1", info1, false)
	serde.MaybeFail("Schema registration 1", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	info2 := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
	}
	id, err = client.Register("subject2", info2, false)
	serde.MaybeFail("Schema registration 2", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	// Create first association
	assocRequest1 := schemaregistry.AssociationCreateOrUpdateRequest{
		ResourceName:      "topic1",
		ResourceNamespace: "-",
		ResourceID:        "lkc-123:topic1",
		ResourceType:      "topic",
		Associations: []schemaregistry.AssociationCreateOrUpdateInfo{
			{
				Subject:         "subject1",
				AssociationType: "value",
			},
		},
	}
	_, err = client.CreateAssociation(assocRequest1)
	serde.MaybeFail("Association creation 1", err)

	// Create second association for same topic and association type
	assocRequest2 := schemaregistry.AssociationCreateOrUpdateRequest{
		ResourceName:      "topic1",
		ResourceNamespace: "-",
		ResourceID:        "lkc-456:topic1",
		ResourceType:      "topic",
		Associations: []schemaregistry.AssociationCreateOrUpdateInfo{
			{
				Subject:         "subject2",
				AssociationType: "value",
			},
		},
	}
	_, err = client.CreateAssociation(assocRequest2)
	serde.MaybeFail("Association creation 2", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.SubjectNameStrategyType = serde.AssociatedNameStrategyType
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	_, err = ser.Serialize("topic1", &obj)
	if err == nil {
		t.Errorf("Expected error when multiple associations found")
	}
}

func TestAvroSerdeWithAssociatedNameStrategyWithKafkaClusterID(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	// Register schema with a custom subject name
	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
	}

	id, err := client.Register("my-custom-subject", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	// Create association with specific namespace (kafka cluster id)
	assocRequest := schemaregistry.AssociationCreateOrUpdateRequest{
		ResourceName:      "topic1",
		ResourceNamespace: "lkc-my-cluster",
		ResourceID:        "lkc-my-cluster:topic1",
		ResourceType:      "topic",
		Associations: []schemaregistry.AssociationCreateOrUpdateInfo{
			{
				Subject:         "my-custom-subject",
				AssociationType: "value",
			},
		},
	}
	_, err = client.CreateAssociation(assocRequest)
	serde.MaybeFail("Association creation", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.SubjectNameStrategyType = serde.AssociatedNameStrategyType
	serConfig.SubjectNameStrategyConfig = map[string]string{
		serde.KafkaClusterIDConfig: "lkc-my-cluster",
	}
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deserConfig.SubjectNameStrategyType = serde.AssociatedNameStrategyType
	deserConfig.SubjectNameStrategyConfig = map[string]string{
		serde.KafkaClusterIDConfig: "lkc-my-cluster",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	msg, err := deser.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
}

func TestAvroSerdeWithAssociatedNameStrategyCaching(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	// Register schema with a custom subject name
	info := schemaregistry.SchemaInfo{
		Schema:     demoSchema,
		SchemaType: "AVRO",
	}

	id, err := client.Register("my-cached-subject", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	// Create association
	assocRequest := schemaregistry.AssociationCreateOrUpdateRequest{
		ResourceName:      "topic1",
		ResourceNamespace: "-",
		ResourceID:        "lkc-123:topic1",
		ResourceType:      "topic",
		Associations: []schemaregistry.AssociationCreateOrUpdateInfo{
			{
				Subject:         "my-cached-subject",
				AssociationType: "value",
			},
		},
	}
	_, err = client.CreateAssociation(assocRequest)
	serde.MaybeFail("Association creation", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.SubjectNameStrategyType = serde.AssociatedNameStrategyType
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	obj := DemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = []byte{1, 2}

	deserConfig := NewDeserializerConfig()
	deserConfig.SubjectNameStrategyType = serde.AssociatedNameStrategyType
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client
	deser.MessageFactory = testMessageFactory

	// Serialize multiple times - should use cache after first call
	for i := 0; i < 5; i++ {
		bytes, err := ser.Serialize("topic1", &obj)
		serde.MaybeFail("serialization", err)

		msg, err := deser.Deserialize("topic1", bytes)
		serde.MaybeFail("deserialization", err, serde.Expect(msg, &obj))
	}
}
