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

package jsonschema

import (
	"encoding/base64"
	"errors"

	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/cel"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/awskms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/azurekms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/gcpkms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/hcvault"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/localkms"
	_ "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/jsonata"

	"encoding/json"
	"strings"
	"testing"

	"github.com/invopop/jsonschema"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

const (
	rootSchema = `
{
  "type": "object",
  "properties": {
    "OtherField": { "$ref": "DemoSchema" }
  }
}
`
	demoSchema = `
{
  "type": "object",
  "properties": {
    "IntField": { "type": "integer" },
    "DoubleField": { "type": "number" },
    "StringField": { 
       "type": "string",
       "confluent:tags": [ "PII" ]
    },
    "BoolField": { "type": "boolean" },
    "BytesField": { 
       "type": "string",
       "contentEncoding": "base64",
       "confluent:tags": [ "PII" ]
    }
  }
}
`
	demoSchemaWithNullable = `
{
  "type": "object",
  "properties": {
    "IntField": { "type": "integer" },
    "DoubleField": { "type": "number" },
    "StringField": { 
       "type": ["string", "null"],
       "confluent:tags": [ "PII" ]
    },
    "BoolField": { "type": "boolean" },
    "BytesField": { 
       "type": "string",
       "contentEncoding": "base64",
       "confluent:tags": [ "PII" ]
    }
  }
}
`
	demoSchemaWithUnion = `
{
  "type": "object",
  "properties": {
    "IntField": { "type": "integer" },
    "DoubleField": { "type": "number" },
    "StringField": { 
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ],
      "confluent:tags": [ "PII" ]
    },
    "BoolField": { "type": "boolean" },
    "BytesField": { 
       "type": "string",
       "contentEncoding": "base64",
       "confluent:tags": [ "PII" ]
    }
  }
}
`
	demoSchemaNested = `
{
  "type": "object",
  "properties": {
    "OtherField": {
	  "type": "object",
	  "properties": {
		"IntField": { "type": "integer" },
		"DoubleField": { "type": "number" },
		"StringField": { 
		   "type": "string",
		   "confluent:tags": [ "PII" ]
		},
		"BoolField": { "type": "boolean" },
		"BytesField": { 
		   "type": "string",
		   "contentEncoding": "base64",
		   "confluent:tags": [ "PII" ]
		}
	  }
    }
  }
}
`
	complexSchema = `
{
  "type": "object",
  "properties": {
    "ArrayField": { 
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "ObjectField": { 
      "type": "object",
      "properties": {
        "StringField": { "type": "string" }
      }
    },
    "UnionField": { 
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ],
      "confluent:tags": [ "PII" ]
    }
  }
}
`
	widgetSchema = `
{
  "type": "object",
  "properties": {
    "name": { 
       "type": "string",
       "confluent:tags": [ "PII" ]
    },
    "size": { "type": "number" },
    "version": { "type": "integer" }
  }
}
`
	newWidgetSchema = `
{
  "type": "object",
  "properties": {
    "name": { 
       "type": "string",
       "confluent:tags": [ "PII" ]
    },
    "height": { "type": "number" },
    "version": { "type": "integer" }
  }
}
`
	newerWidgetSchema = `
{
  "type": "object",
  "properties": {
    "name": { 
       "type": "string",
       "confluent:tags": [ "PII" ]
    },
    "length": { "type": "number" },
    "version": { "type": "integer" }
  }
}
`
	defSchema = `
{
	"$schema" : "http://json-schema.org/draft-07/schema#",
	"additionalProperties" : false,
	"definitions" : {
		"Address" : {
			"additionalProperties" : false,
			"properties" : {
				"doornumber" : {
					"type" : "integer"
				},
				"doorpin" : {
					"confluent:tags" : [ "PII" ],
					"type" : "string"
				}
			},
			"type" : "object"
		}
	},
	"properties" : {
		"address" : {
			"$ref" : "#/definitions/Address"
		},
		"name" : {
			"confluent:tags" : [ "PII" ],
			"type" : "string"
		}
	},
	"title" : "Sample Event",
	"type" : "object"
}
`
	messageSchema = `
	{

        "type": "object",
        "properties": {
            "messageType": {
                "type": "string"
            },
            "version": {
                "type": "string"
            },
            "payload": {
                "type": "object",
                "oneOf": [
                    {
                        "$ref": "#/$defs/authentication_request"
                    },
                    {
                        "$ref": "#/$defs/authentication_status"
                    }
                ]
            }
        },
        "required": [
            "payload",
            "messageType",
            "version"
        ],
        "$defs": {
            "authentication_request": {
                "properties": {
                    "messageId": {
                        "type": "string",
                        "confluent:tags": ["PII"]
                    },
                    "timestamp": {
                        "type": "integer",
                        "minimum": 0
                    },
                    "requestId": {
                        "type": "string"
                    }
                },
                "required": [
                    "messageId",
                    "timestamp"
                ]
            },
            "authentication_status": {
                "properties": {
                    "messageId": {
                        "type": "string",
                        "confluent:tags": ["PII"]
                    },
                    "authType": {
                        "type": [
                            "string",
                            "null"
                        ]
                    }
                },
                "required": [
                    "messageId",
                    "authType"
                ]
            }
        }
    }
`
	schemaWithFormats = `
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "email": {
      "type": "string",
      "format": "email"
    },
    "date": {
      "type": "string",
      "format": "date"
    },
    "uri": {
      "type": "string",
      "format": "uri"
    }
  }
}
`

	schemaWithContent = `
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "encodedData": {
      "type": "string",
      "contentEncoding": "base64"
    },
    "jsonData": {
      "type": "string",
      "contentMediaType": "application/json"
    }
  }
}
`

	schemaCombined = `
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "email": {
      "type": "string",
      "format": "email"
    },
    "encodedData": {
      "type": "string",
      "contentEncoding": "base64"
    }
  }
}
`
)

func testMessageFactory1(subject string, name string) (interface{}, error) {
	return &OldWidget{}, nil
}

func testMessageFactory2(subject string, name string) (interface{}, error) {
	return &NewWidget{}, nil
}

func testMessageFactory3(subject string, name string) (interface{}, error) {
	return &NewerWidget{}, nil
}

func TestJSONSchemaSerdeWithSimple(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.EnableValidation = true
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deserConfig.EnableValidation = true
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))

	// serialize second object
	obj = JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "bye"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})
	bytes, err = ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

func TestJSONSchemaSerdeWithGuidInHeader(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	ser.SchemaIDSerializer = serde.HeaderSchemaIDSerializer

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})
	headers, bytes, err := ser.SerializeWithHeaders("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONDemoSchema
	err = deser.DeserializeWithHeadersInto("topic1", headers, bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))

	// serialize second object
	obj = JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "bye"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})
	headers, bytes, err = ser.SerializeWithHeaders("topic1", &obj)
	serde.MaybeFail("serialization", err)

	err = deser.DeserializeWithHeadersInto("topic1", headers, bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

func TestJSONSchemaSerdeWithSimpleMap(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.EnableValidation = true
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	obj := make(map[string]interface{})
	obj["IntField"] = 123
	obj["DoubleField"] = 45.67
	obj["StringField"] = "hi"
	obj["BoolField"] = true
	obj["BytesField"] = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deserConfig.EnableValidation = true
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	// JSON decoding produces floats
	obj["IntField"] = 123.0

	var newobj map[string]interface{}
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

func TestJSONSchemaSerdeWithNested(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	ser, err := NewSerializer(client, serde.ValueSerde, NewSerializerConfig())
	serde.MaybeFail("Serializer configuration", err)

	nested := JSONDemoSchema{}
	nested.IntField = 123
	nested.DoubleField = 45.67
	nested.StringField = "hi"
	nested.BoolField = true
	nested.BytesField = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})
	obj := JSONNestedTestRecord{
		OtherField: nested,
	}
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONNestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

func TestJSONSchemaSerdeWithReferences(t *testing.T) {
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
		SchemaType: "JSON",
	}

	id, err := client.Register("demo-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	info = schemaregistry.SchemaInfo{
		Schema:     rootSchema,
		SchemaType: "JSON",
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

	nested := JSONDemoSchema{}
	nested.IntField = 123
	nested.DoubleField = 45.67
	nested.StringField = "hi"
	nested.BoolField = true
	nested.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})
	obj := JSONNestedTestRecord{}
	obj.OtherField = nested

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONNestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj))
}

func TestFailingJSONSchemaValidationWithSimple(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error
	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.EnableValidation = true
	// We don't want to risk registering one instead of using the already registered one
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
	serde.MaybeFail("Serializer configuration", err)

	obj := JSONDemoSchema{}
	jschema := jsonschema.Reflect(obj)
	raw, err := json.Marshal(jschema)
	serde.MaybeFail("Schema marshalling", err)
	info := schemaregistry.SchemaInfo{
		Schema:     string(raw),
		SchemaType: "JSON",
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	_, err = ser.Serialize("topic1", &obj)
	if err != nil {
		t.Errorf("Expected no validation error, found %s", err)
	}

	diffObj := DifferentJSONDemoSchema{}
	_, err = ser.Serialize("topic1", &diffObj)
	if err == nil || !strings.Contains(err.Error(), "jsonschema") {
		t.Errorf("Expected validation error, found %s", err)
	}
}

func TestJSONSchemaSerializerValidationAssertFlags(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)

	testCases := []struct {
		name          string
		assertFormat  bool
		assertContent bool
		schema        string
		topic         string
		validObj      map[string]interface{}
		invalidObj    map[string]interface{}
		expectError   bool
		errorContains []string
	}{
		{
			name:          "AssertFormat enabled - valid email",
			assertFormat:  true,
			assertContent: false,
			schema:        schemaWithFormats,
			topic:         "assert-format-valid",
			validObj: map[string]interface{}{
				"email": "test@example.com",
				"date":  "2024-01-01",
				"uri":   "https://example.com",
			},
			expectError: false,
		},
		{
			name:          "AssertFormat enabled - invalid email",
			assertFormat:  true,
			assertContent: false,
			schema:        schemaWithFormats,
			topic:         "assert-format-invalid-email",
			invalidObj: map[string]interface{}{
				"email": "not-an-email",
				"date":  "2024-01-01",
				"uri":   "https://example.com",
			},
			expectError:   true,
			errorContains: []string{"jsonschema", "email"},
		},
		{
			name:          "AssertFormat enabled - invalid date",
			assertFormat:  true,
			assertContent: false,
			schema:        schemaWithFormats,
			topic:         "assert-format-invalid-date",
			invalidObj: map[string]interface{}{
				"email": "test@example.com",
				"date":  "not-a-date",
				"uri":   "https://example.com",
			},
			expectError:   true,
			errorContains: []string{"jsonschema", "date"},
		},
		{
			name:          "AssertFormat enabled - invalid URI",
			assertFormat:  true,
			assertContent: false,
			schema:        schemaWithFormats,
			topic:         "assert-format-invalid-uri",
			invalidObj: map[string]interface{}{
				"email": "test@example.com",
				"date":  "2024-01-01",
				"uri":   "not a valid uri",
			},
			expectError:   true,
			errorContains: []string{"jsonschema", "uri"},
		},
		{
			name:          "AssertContent enabled - valid content",
			assertFormat:  false,
			assertContent: true,
			schema:        schemaWithContent,
			topic:         "assert-content-valid",
			validObj: map[string]interface{}{
				"encodedData": base64.StdEncoding.EncodeToString([]byte("test data")),
				"jsonData":    `{"key":"value"}`,
			},
			expectError: false,
		},
		{
			name:          "AssertContent enabled - invalid base64",
			assertFormat:  false,
			assertContent: true,
			schema:        schemaWithContent,
			topic:         "assert-content-invalid-base64",
			invalidObj: map[string]interface{}{
				"encodedData": "not-valid-base64!@#$%",
				"jsonData":    `{"key":"value"}`,
			},
			expectError:   true,
			errorContains: []string{"jsonschema"},
		},
		{
			name:          "AssertContent enabled - invalid JSON",
			assertFormat:  false,
			assertContent: true,
			schema:        schemaWithContent,
			topic:         "assert-content-invalid-json",
			invalidObj: map[string]interface{}{
				"encodedData": base64.StdEncoding.EncodeToString([]byte("test data")),
				"jsonData":    "not valid json {",
			},
			expectError:   true,
			errorContains: []string{"jsonschema"},
		},
		{
			name:          "AssertContent disabled - invalid content",
			assertFormat:  false,
			assertContent: false,
			schema:        schemaWithContent,
			topic:         "assert-content-disabled",
			invalidObj: map[string]interface{}{
				"encodedData": "not-valid-base64!@#$%",
				"jsonData":    "not valid json {",
			},
			expectError: false,
		},
		{
			name:          "Both flags enabled - valid",
			assertFormat:  true,
			assertContent: true,
			schema:        schemaCombined,
			topic:         "both-flags-valid",
			validObj: map[string]interface{}{
				"email":       "test@example.com",
				"encodedData": base64.StdEncoding.EncodeToString([]byte("data")),
			},
			expectError: false,
		},
		{
			name:          "Both flags enabled - invalid email",
			assertFormat:  true,
			assertContent: true,
			schema:        schemaCombined,
			topic:         "both-flags-invalid-email",
			invalidObj: map[string]interface{}{
				"email":       "not-an-email",
				"encodedData": base64.StdEncoding.EncodeToString([]byte("data")),
			},
			expectError: true,
		},
		{
			name:          "Both flags enabled - invalid encoding",
			assertFormat:  true,
			assertContent: true,
			schema:        schemaCombined,
			topic:         "both-flags-invalid-encoding",
			invalidObj: map[string]interface{}{
				"email":       "test@example.com",
				"encodedData": "not-valid-base64!@#",
			},
			expectError: true,
		},
		{
			name:          "Both flags enabled - both invalid",
			assertFormat:  true,
			assertContent: true,
			schema:        schemaCombined,
			topic:         "both-flags-both-invalid",
			invalidObj: map[string]interface{}{
				"email":       "not-an-email",
				"encodedData": "not-valid-base64!@#",
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := schemaregistry.NewConfig("mock://")
			client, err := schemaregistry.NewClient(conf)
			serde.MaybeFail("Schema Registry configuration", err)

			serConfig := NewSerializerConfig()
			serConfig.EnableValidation = true
			serConfig.AssertFormat = tc.assertFormat
			serConfig.AssertContent = tc.assertContent
			serConfig.AutoRegisterSchemas = false
			serConfig.UseLatestVersion = true
			ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
			serde.MaybeFail("Serializer configuration", err)

			info := schemaregistry.SchemaInfo{
				Schema:     tc.schema,
				SchemaType: "JSON",
			}

			id, err := client.Register(tc.topic+"-value", info, false)
			serde.MaybeFail("Schema registration", err)
			if id <= 0 {
				t.Errorf("Expected valid schema id, found %d", id)
			}

			var obj map[string]interface{}
			if tc.validObj != nil {
				obj = tc.validObj
			} else {
				obj = tc.invalidObj
			}

			_, err = ser.Serialize(tc.topic, &obj)

			if tc.expectError {
				if err == nil {
					t.Errorf("Expected validation error but got none")
				} else if len(tc.errorContains) > 0 {
					foundAny := false
					for _, substring := range tc.errorContains {
						if strings.Contains(err.Error(), substring) {
							foundAny = true
							break
						}
					}
					if !foundAny {
						t.Errorf("Expected error to contain one of %v, found: %s", tc.errorContains, err)
					}
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error, found: %s", err)
				}
			}
		})
	}
}

func TestJSONSchemaDeserializerValidationAssertFlags(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)

	testCases := []struct {
		name          string
		assertFormat  bool
		assertContent bool
		schema        string
		topic         string
		validObj      map[string]interface{}
		invalidObj    map[string]interface{}
		expectError   bool
		errorContains []string
	}{
		{
			name:          "AssertFormat enabled - valid email",
			assertFormat:  true,
			assertContent: false,
			schema:        schemaWithFormats,
			topic:         "deser-assert-format-valid",
			validObj: map[string]interface{}{
				"email": "test@example.com",
				"date":  "2024-01-01",
				"uri":   "https://example.com",
			},
			expectError: false,
		},
		{
			name:          "AssertFormat enabled - invalid email",
			assertFormat:  true,
			assertContent: false,
			schema:        schemaWithFormats,
			topic:         "deser-assert-format-invalid-email",
			invalidObj: map[string]interface{}{
				"email": "not-an-email",
				"date":  "2024-01-01",
				"uri":   "https://example.com",
			},
			expectError:   true,
			errorContains: []string{"jsonschema", "email"},
		},
		{
			name:          "AssertFormat enabled - invalid date",
			assertFormat:  true,
			assertContent: false,
			schema:        schemaWithFormats,
			topic:         "deser-assert-format-invalid-date",
			invalidObj: map[string]interface{}{
				"email": "test@example.com",
				"date":  "not-a-date",
				"uri":   "https://example.com",
			},
			expectError:   true,
			errorContains: []string{"jsonschema", "date"},
		},
		{
			name:          "AssertFormat enabled - invalid URI",
			assertFormat:  true,
			assertContent: false,
			schema:        schemaWithFormats,
			topic:         "deser-assert-format-invalid-uri",
			invalidObj: map[string]interface{}{
				"email": "test@example.com",
				"date":  "2024-01-01",
				"uri":   "not a valid uri",
			},
			expectError:   true,
			errorContains: []string{"jsonschema", "uri"},
		},
		{
			name:          "AssertContent enabled - valid content",
			assertFormat:  false,
			assertContent: true,
			schema:        schemaWithContent,
			topic:         "deser-assert-content-valid",
			validObj: map[string]interface{}{
				"encodedData": base64.StdEncoding.EncodeToString([]byte("test data")),
				"jsonData":    `{"key":"value"}`,
			},
			expectError: false,
		},
		{
			name:          "AssertContent enabled - invalid base64",
			assertFormat:  false,
			assertContent: true,
			schema:        schemaWithContent,
			topic:         "deser-assert-content-invalid-base64",
			invalidObj: map[string]interface{}{
				"encodedData": "not-valid-base64!@#$%",
				"jsonData":    `{"key":"value"}`,
			},
			expectError:   true,
			errorContains: []string{"jsonschema"},
		},
		{
			name:          "AssertContent enabled - invalid JSON",
			assertFormat:  false,
			assertContent: true,
			schema:        schemaWithContent,
			topic:         "deser-assert-content-invalid-json",
			invalidObj: map[string]interface{}{
				"encodedData": base64.StdEncoding.EncodeToString([]byte("test data")),
				"jsonData":    "not valid json {",
			},
			expectError:   true,
			errorContains: []string{"jsonschema"},
		},
		{
			name:          "AssertContent disabled - invalid content",
			assertFormat:  false,
			assertContent: false,
			schema:        schemaWithContent,
			topic:         "deser-assert-content-disabled",
			invalidObj: map[string]interface{}{
				"encodedData": "not-valid-base64!@#$%",
				"jsonData":    "not valid json {",
			},
			expectError: false,
		},
		{
			name:          "Both flags enabled - valid",
			assertFormat:  true,
			assertContent: true,
			schema:        schemaCombined,
			topic:         "deser-both-flags-valid",
			validObj: map[string]interface{}{
				"email":       "test@example.com",
				"encodedData": base64.StdEncoding.EncodeToString([]byte("data")),
			},
			expectError: false,
		},
		{
			name:          "Both flags enabled - invalid email",
			assertFormat:  true,
			assertContent: true,
			schema:        schemaCombined,
			topic:         "deser-both-flags-invalid-email",
			invalidObj: map[string]interface{}{
				"email":       "not-an-email",
				"encodedData": base64.StdEncoding.EncodeToString([]byte("data")),
			},
			expectError: true,
		},
		{
			name:          "Both flags enabled - invalid encoding",
			assertFormat:  true,
			assertContent: true,
			schema:        schemaCombined,
			topic:         "deser-both-flags-invalid-encoding",
			invalidObj: map[string]interface{}{
				"email":       "test@example.com",
				"encodedData": "not-valid-base64!@#",
			},
			expectError: true,
		},
		{
			name:          "Both flags enabled - both invalid",
			assertFormat:  true,
			assertContent: true,
			schema:        schemaCombined,
			topic:         "deser-both-flags-both-invalid",
			invalidObj: map[string]interface{}{
				"email":       "not-an-email",
				"encodedData": "not-valid-base64!@#",
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := schemaregistry.NewConfig("mock://")
			client, err := schemaregistry.NewClient(conf)
			serde.MaybeFail("Schema Registry configuration", err)

			// First serialize the data without validation to get serialized bytes
			serConfig := NewSerializerConfig()
			serConfig.EnableValidation = false
			serConfig.AutoRegisterSchemas = false
			serConfig.UseLatestVersion = true
			ser, err := NewSerializer(client, serde.ValueSerde, serConfig)
			serde.MaybeFail("Serializer configuration", err)

			info := schemaregistry.SchemaInfo{
				Schema:     tc.schema,
				SchemaType: "JSON",
			}

			id, err := client.Register(tc.topic+"-value", info, false)
			serde.MaybeFail("Schema registration", err)
			if id <= 0 {
				t.Errorf("Expected valid schema id, found %d", id)
			}

			var obj map[string]interface{}
			if tc.validObj != nil {
				obj = tc.validObj
			} else {
				obj = tc.invalidObj
			}

			bytes, err := ser.Serialize(tc.topic, &obj)
			serde.MaybeFail("serialization", err)

			deserConfig := NewDeserializerConfig()
			deserConfig.EnableValidation = true
			deserConfig.AssertFormat = tc.assertFormat
			deserConfig.AssertContent = tc.assertContent
			deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
			serde.MaybeFail("Deserializer configuration", err)
			deser.Client = ser.Client

			var newobj map[string]interface{}
			err = deser.DeserializeInto(tc.topic, bytes, &newobj)

			if tc.expectError {
				if err == nil {
					t.Errorf("Expected validation error but got none")
				} else if len(tc.errorContains) > 0 {
					foundAny := false
					for _, substring := range tc.errorContains {
						if strings.Contains(err.Error(), substring) {
							foundAny = true
							break
						}
					}
					if !foundAny {
						t.Errorf("Expected error to contain one of %v, found: %s", tc.errorContains, err)
					}
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error, found: %s", err)
				}
			}
		})
	}
}

func TestJSONSchemaSerdeWithCELCondition(t *testing.T) {
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
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj))
}

func TestJSONSchemaSerdeWithCELConditionFail(t *testing.T) {
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
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	_, err = ser.Serialize("topic1", &obj)
	var ruleErr serde.RuleConditionErr
	errors.As(err, &ruleErr)
	serde.MaybeFail("serialization", nil, serde.Expect(encRule, *ruleErr.Rule))
}

func TestJSONSchemaSerdeWithCELFieldTransform(t *testing.T) {
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
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	obj2 := JSONDemoSchema{}
	obj2.IntField = 123
	obj2.DoubleField = 45.67
	obj2.StringField = "hi-suffix"
	obj2.BoolField = true
	obj2.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj2))
}

func TestJSONSchemaSerdeWithCELFieldTransformWithNullable(t *testing.T) {
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
		Schema:     demoSchemaWithNullable,
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	obj2 := JSONDemoSchema{}
	obj2.IntField = 123
	obj2.DoubleField = 45.67
	obj2.StringField = "hi-suffix"
	obj2.BoolField = true
	obj2.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj2))
}

func TestJSONSchemaSerdeWithCELFieldTransformWithUnionOfRefs(t *testing.T) {
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
		Expr: "name == 'messageId' ; value + '-suffix'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     messageSchema,
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	payload := Payload{}
	payload.MessageID = "12345"
	payload.Timestamp = 12345
	obj := Message{}
	obj.MessageType = "authentication_request"
	obj.Version = "1.0"
	obj.Payload = payload

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	payload2 := Payload{}
	payload2.MessageID = "12345-suffix"
	payload2.Timestamp = 12345
	obj.Payload = payload2

	var newobj Message
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj))
}

func TestJSONSchemaSerdeWithCELFieldTransformWithDef(t *testing.T) {
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
		Tags: []string{"PII"},
		Expr: "value + '-suffix'",
	}
	ruleSet := schemaregistry.RuleSet{
		DomainRules: []schemaregistry.Rule{encRule},
	}

	info := schemaregistry.SchemaInfo{
		Schema:     defSchema,
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	addr := Address{}
	addr.DoorNumber = 123
	addr.DoorPin = "1234"
	obj := JSONPerson{}
	obj.Name = "bob"
	obj.Address = addr

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	addr2 := Address{}
	addr2.DoorNumber = 123
	addr2.DoorPin = "1234-suffix"
	obj2 := JSONPerson{}
	obj2.Name = "bob-suffix"
	obj2.Address = addr2

	var newobj JSONPerson
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj2))
}

func TestJSONSchemaSerdeWithCELFieldTransformWithSimpleMap(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	serConfig := NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serConfig.EnableValidation = true
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
		SchemaType: "JSON",
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
	obj["BytesField"] = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})
	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deserConfig.EnableValidation = true
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	obj2 := JSONDemoSchema{}
	// JSON decoding produces floats
	obj2.IntField = 123.0
	obj2.DoubleField = 45.67
	obj2.StringField = "hi-suffix"
	obj2.BoolField = true
	obj2.BytesField = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj2))
}

func TestJSONSchemaSerdeWithCELFieldTransformWithNestedMap(t *testing.T) {
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
		Schema:     demoSchemaNested,
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
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
	nested["BytesField"] = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})
	obj := make(map[string]interface{})
	obj["OtherField"] = nested

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deser, err := NewDeserializer(client, serde.ValueSerde, NewDeserializerConfig())
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	nested2 := JSONDemoSchema{}
	// JSON decoding produces floats
	nested2.IntField = 123.0
	nested2.DoubleField = 45.67
	nested2.StringField = "hi-suffix"
	nested2.BoolField = true
	nested2.BytesField = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})
	obj2 := JSONNestedTestRecord{nested2}

	var newobj JSONNestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj2))
}

func TestJSONSchemaSerdeWithCELFieldTransformComplex(t *testing.T) {
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
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	str := "bye"
	obj := JSONComplexSchema{}
	obj.ArrayField = []string{"hello"}
	obj.ObjectField = NestedSchema{StringField: "world"}
	obj.UnionField = &str

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	str2 := "bye-suffix"
	obj2 := JSONComplexSchema{}
	obj2.ArrayField = []string{"hello-suffix"}
	obj2.ObjectField = NestedSchema{StringField: "world-suffix"}
	obj2.UnionField = &str2

	var newobj JSONComplexSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj2))
}

func TestJSONSchemaSerdeWithCELFieldTransformComplexWithNil(t *testing.T) {
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
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := JSONComplexSchema{}
	obj.ArrayField = []string{"hello"}
	obj.ObjectField = NestedSchema{StringField: "world"}
	obj.UnionField = nil

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	obj2 := JSONComplexSchema{}
	obj2.ArrayField = []string{"hello-suffix"}
	obj2.ObjectField = NestedSchema{StringField: "world-suffix"}
	obj2.UnionField = nil

	var newobj JSONComplexSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj2))
}

func TestJSONSchemaSerdeWithCELFieldCondition(t *testing.T) {
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
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj))
}

func TestJSONSchemaSerdeWithCELFieldConditionFail(t *testing.T) {
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
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	_, err = ser.Serialize("topic1", &obj)
	var ruleErr serde.RuleConditionErr
	errors.As(err, &ruleErr)
	serde.MaybeFail("serialization", nil, serde.Expect(ruleErr, serde.RuleConditionErr{Rule: &encRule}))
}

func TestJSONSchemaSerdeEncryptionWithSimpleMap(t *testing.T) {
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
	serConfig.EnableValidation = true
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
		SchemaType: "JSON",
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
	obj["BytesField"] = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj["StringField"] = "hi"
	obj["BytesField"] = base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 1})

	// JSON decoding produces floats
	obj["IntField"] = 123.0

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deserConfig.EnableValidation = true
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj map[string]interface{}
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, obj))
}

func TestJSONSchemaSerdeEncryption(t *testing.T) {
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
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj.StringField = "hi"
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj))
}

func TestJSONSchemaSerdePayloadEncryption(t *testing.T) {
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
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj))
}

func TestJSONSchemaSerdeEncryptionWithUnion(t *testing.T) {
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
		SchemaType: "JSON",
		RuleSet:    &ruleSet,
	}

	id, err := client.Register("topic1-value", info, false)
	serde.MaybeFail("Schema registration", err)
	if id <= 0 {
		t.Errorf("Expected valid schema id, found %d", id)
	}

	obj := JSONDemoSchema{}
	obj.IntField = 123
	obj.DoubleField = 45.67
	obj.StringField = "hi"
	obj.BoolField = true
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj.StringField = "hi"
	obj.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONDemoSchema
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj))
}

func TestJSONSchemaSerdeEncryptionWithReferences(t *testing.T) {
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
		Schema:     demoSchema,
		SchemaType: "JSON",
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
		Schema:     rootSchema,
		SchemaType: "JSON",
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

	nested := JSONDemoSchema{}
	nested.IntField = 123
	nested.DoubleField = 45.67
	nested.StringField = "hi"
	nested.BoolField = true
	nested.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})
	obj := JSONNestedTestRecord{}
	obj.OtherField = nested

	bytes, err := ser.Serialize("topic1", &obj)
	serde.MaybeFail("serialization", err)

	// Reset encrypted field
	obj.OtherField.StringField = "hi"
	obj.OtherField.BytesField = base64.StdEncoding.EncodeToString([]byte{1, 2})

	deserConfig := NewDeserializerConfig()
	deserConfig.RuleConfig = map[string]string{
		"secret": "mysecret",
	}
	deser, err := NewDeserializer(client, serde.ValueSerde, deserConfig)
	serde.MaybeFail("Deserializer configuration", err)
	deser.Client = ser.Client

	var newobj JSONNestedTestRecord
	err = deser.DeserializeInto("topic1", bytes, &newobj)
	serde.MaybeFail("deserialization", err, serde.Expect(&newobj, &obj))
}

func TestJSONSchemaSerdeJSONataFullyCompatible(t *testing.T) {
	serde.MaybeFail = serde.InitFailFunc(t)
	var err error

	rule1To2 := "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])"
	rule2To1 := "$merge([$sift($, function($v, $k) {$k != 'height'}), {'size': $.'height'}])"
	rule2To3 := "$merge([$sift($, function($v, $k) {$k != 'height'}), {'length': $.'height'}])"
	rule3To2 := "$merge([$sift($, function($v, $k) {$k != 'length'}), {'height': $.'length'}])"

	conf := schemaregistry.NewConfig("mock://")

	client, err := schemaregistry.NewClient(conf)
	serde.MaybeFail("Schema Registry configuration", err)

	widget := OldWidget{
		Name:    "alice",
		Size:    123,
		Version: 1,
	}

	info := schemaregistry.SchemaInfo{
		Schema:     widgetSchema,
		SchemaType: "JSON",
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

	info = schemaregistry.SchemaInfo{
		Schema:     newWidgetSchema,
		SchemaType: "JSON",
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

	info = schemaregistry.SchemaInfo{
		Schema:     newerWidgetSchema,
		SchemaType: "JSON",
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
	deser1.MessageFactory = testMessageFactory1

	newobj, err := deser1.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &widget))

	deserConfig2 := NewDeserializerConfig()
	deserConfig2.UseLatestWithMetadata = map[string]string{
		"application.version": "v2",
	}

	deser2, err := NewDeserializer(client, serde.ValueSerde, deserConfig2)
	serde.MaybeFail("Deserializer configuration", err)
	deser2.Client = ser.Client
	deser2.MessageFactory = testMessageFactory2

	newobj, err = deser2.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &newWidget))

	deserConfig3 := NewDeserializerConfig()
	deserConfig3.UseLatestWithMetadata = map[string]string{
		"application.version": "v3",
	}

	deser3, err := NewDeserializer(client, serde.ValueSerde, deserConfig3)
	serde.MaybeFail("Deserializer configuration", err)
	deser3.Client = ser.Client
	deser3.MessageFactory = testMessageFactory3

	newobj, err = deser3.Deserialize("topic1", bytes)
	serde.MaybeFail("deserialization", err, serde.Expect(newobj, &newerWidget))
}

type DifferentJSONDemoSchema struct {
	IntField int32 `json:"IntField"`

	ExtraStringField string `json:"ExtraStringField"`

	DoubleField float64 `json:"DoubleField"`

	StringField string `json:"StringField"`

	BoolFieldThatsActuallyString string `json:"BoolField"`

	BytesField string `json:"BytesField"`
}

type JSONDemoSchema struct {
	IntField int32 `json:"IntField"`

	DoubleField float64 `json:"DoubleField"`

	StringField string `json:"StringField"`

	BoolField bool `json:"BoolField"`

	BytesField string `json:"BytesField"`
}

type JSONComplexSchema struct {
	ArrayField []string `json:"ArrayField"`

	ObjectField NestedSchema `json:"ObjectField"`

	UnionField *string `json:"UnionField"`
}

type NestedSchema struct {
	StringField string `json:"StringField"`
}

type JSONNestedTestRecord struct {
	OtherField JSONDemoSchema
}

type JSONLinkedList struct {
	Value int32
	Next  *JSONLinkedList
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

type Address struct {
	DoorNumber int `json:"doornumber"`

	DoorPin string `json:"doorpin"`
}

type JSONPerson struct {
	Name string `json:"name"`

	Address Address `json:"address"`
}

type Message struct {
	MessageType string `json:"messageType"`

	Version string `json:"version"`

	Payload Payload `json:"payload"`
}

type Payload struct {
	MessageID string `json:"messageId"`

	Timestamp int `json:"timestamp"`
}
