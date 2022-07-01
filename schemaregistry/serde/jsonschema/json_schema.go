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
	"encoding/json"
	"io"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/invopop/jsonschema"
	jsonschema2 "github.com/santhosh-tekuri/jsonschema/v5"
)

// Serializer represents a JSON Schema serializer
type Serializer struct {
	serde.BaseSerializer
	validate bool
}

// Deserializer represents a JSON Schema deserializer
type Deserializer struct {
	serde.BaseDeserializer
	validate bool
}

var _ serde.Serializer = new(Serializer)
var _ serde.Deserializer = new(Deserializer)

// NewSerializer creates a JSON serializer for generic objects
func NewSerializer(client schemaregistry.Client, serdeType serde.Type, conf *SerializerConfig) (*Serializer, error) {
	s := &Serializer{
		validate: conf.EnableValidation,
	}
	err := s.ConfigureSerializer(client, serdeType, &conf.SerializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Serialize implements serialization of generic data to JSON
func (s *Serializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	jschema := jsonschema.Reflect(msg)
	raw, err := json.Marshal(jschema)
	if err != nil {
		return nil, err
	}
	info := schemaregistry.SchemaInfo{
		Schema:     string(raw),
		SchemaType: "JSON",
	}
	id, err := s.GetID(topic, msg, info)
	if err != nil {
		return nil, err
	}
	raw, err = json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(raw, &obj)
		if err != nil {
			return nil, err
		}
		jschema, err := toJSONSchema(s.Client, info)
		if err != nil {
			return nil, err
		}
		err = jschema.Validate(obj)
		if err != nil {
			return nil, err
		}
	}
	payload, err := s.WriteBytes(id, raw)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// NewDeserializer creates a JSON deserializer for generic objects
func NewDeserializer(client schemaregistry.Client, serdeType serde.Type, conf *DeserializerConfig) (*Deserializer, error) {
	s := &Deserializer{
		validate: conf.EnableValidation,
	}
	err := s.ConfigureDeserializer(client, serdeType, &conf.DeserializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Deserialize implements deserialization of generic data from JSON
func (s *Deserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return nil, err
	}
	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(payload[5:], &obj)
		if err != nil {
			return nil, err
		}
		jschema, err := toJSONSchema(s.Client, info)
		if err != nil {
			return nil, err
		}
		err = jschema.Validate(obj)
		if err != nil {
			return nil, err
		}
	}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return nil, err
	}
	msg, err := s.MessageFactory(subject, "")
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(payload[5:], msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// DeserializeInto implements deserialization of generic data from JSON to the given object
func (s *Deserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
	if payload == nil {
		return nil
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return err
	}
	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(payload[5:], &obj)
		if err != nil {
			return err
		}
		jschema, err := toJSONSchema(s.Client, info)
		if err != nil {
			return err
		}
		err = jschema.Validate(obj)
		if err != nil {
			return err
		}
	}
	err = json.Unmarshal(payload[5:], msg)
	if err != nil {
		return err
	}
	return nil
}

func toJSONSchema(c schemaregistry.Client, schema schemaregistry.SchemaInfo) (*jsonschema2.Schema, error) {
	deps := make(map[string]string)
	err := serde.ResolveReferences(c, schema, deps)
	if err != nil {
		return nil, err
	}
	compiler := jsonschema2.NewCompiler()
	compiler.LoadURL = func(url string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(deps[url])), nil
	}
	url := "schema.json"
	if err := compiler.AddResource(url, strings.NewReader(schema.Schema)); err != nil {
		return nil, err
	}
	return compiler.Compile(url)
}
