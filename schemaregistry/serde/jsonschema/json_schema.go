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
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/cache"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/invopop/jsonschema"
	jsonschema2 "github.com/santhosh-tekuri/jsonschema/v5"
)

const (
	// SchemaType is the JSON Schema type
	SchemaType     = "JSON"
	defaultBaseURL = "mem://input/"
)

// Serializer represents a JSON Schema serializer
type Serializer struct {
	serde.BaseSerializer
	*Serde
}

// Deserializer represents a JSON Schema deserializer
type Deserializer struct {
	serde.BaseDeserializer
	*Serde
}

// Serde represents a JSON Schema serde
type Serde struct {
	validate              bool
	schemaToTypeCache     cache.Cache
	schemaToTypeCacheLock sync.RWMutex
}

var _ serde.Serializer = new(Serializer)
var _ serde.Deserializer = new(Deserializer)

// NewSerializer creates a JSON serializer for generic objects
func NewSerializer(client schemaregistry.Client, serdeType serde.Type, conf *SerializerConfig) (*Serializer, error) {
	schemaToTypeCache, err := cache.NewLRUCache(1000)
	if err != nil {
		return nil, err
	}
	sr := &Serde{
		validate:          conf.EnableValidation,
		schemaToTypeCache: schemaToTypeCache,
	}
	s := &Serializer{
		Serde: sr,
	}
	err = s.ConfigureSerializer(client, serdeType, &conf.SerializerConfig)
	if err != nil {
		return nil, err
	}
	fieldTransformer := func(ctx serde.RuleContext, fieldTransform serde.FieldTransform, msg interface{}) (interface{}, error) {
		return s.FieldTransform(s.Client, ctx, fieldTransform, msg)
	}
	s.FieldTransformer = fieldTransformer
	err = s.SetRuleRegistry(serde.GlobalRuleRegistry(), conf.RuleConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Serialize implements serialization of generic data to JSON
func (s *Serializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	_, payload, err := s.SerializeWithHeaders(topic, msg)
	return payload, err
}

// SerializeWithHeaders implements serialization of generic data to JSON
func (s *Serializer) SerializeWithHeaders(topic string, msg interface{}) ([]kafka.Header, []byte, error) {
	if msg == nil {
		return nil, nil, nil
	}
	var info schemaregistry.SchemaInfo
	var err error
	// Don't derive the schema if it is being looked up in the following ways
	if s.Conf.UseSchemaID == -1 &&
		!s.Conf.UseLatestVersion &&
		len(s.Conf.UseLatestWithMetadata) == 0 {
		jschema := jsonschema.Reflect(msg)
		raw, err := json.Marshal(jschema)
		if err != nil {
			return nil, nil, err
		}
		info = schemaregistry.SchemaInfo{
			Schema:     string(raw),
			SchemaType: "JSON",
		}
	}
	schemaID, err := s.GetSchemaID(SchemaType, topic, msg, &info)
	if err != nil {
		return nil, nil, err
	}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return nil, nil, err
	}
	msg, err = s.ExecuteRules(subject, topic, schemaregistry.Write, nil, &info, msg)
	if err != nil {
		return nil, nil, err
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		return nil, nil, err
	}
	if s.validate {
		// Need to unmarshal to pure interface
		var obj interface{}
		err = json.Unmarshal(raw, &obj)
		if err != nil {
			return nil, nil, err
		}
		jschema, err := s.toJSONSchema(s.Client, info)
		if err != nil {
			return nil, nil, err
		}
		err = jschema.Validate(obj)
		if err != nil {
			return nil, nil, err
		}
	}
	msg, err = s.ExecuteRulesWithPhase(subject, topic,
		schemaregistry.EncodingPhase, schemaregistry.Write, nil, &info, raw)
	if err != nil {
		return nil, nil, err
	}
	return s.SchemaIDSerializer(topic, s.SerdeType, msg.([]byte), schemaID)
}

// NewDeserializer creates a JSON deserializer for generic objects
func NewDeserializer(client schemaregistry.Client, serdeType serde.Type, conf *DeserializerConfig) (*Deserializer, error) {
	schemaToTypeCache, err := cache.NewLRUCache(1000)
	if err != nil {
		return nil, err
	}
	sr := &Serde{
		validate:          conf.EnableValidation,
		schemaToTypeCache: schemaToTypeCache,
	}
	s := &Deserializer{
		Serde: sr,
	}
	err = s.ConfigureDeserializer(client, serdeType, &conf.DeserializerConfig)
	if err != nil {
		return nil, err
	}
	fieldTransformer := func(ctx serde.RuleContext, fieldTransform serde.FieldTransform, msg interface{}) (interface{}, error) {
		return s.FieldTransform(s.Client, ctx, fieldTransform, msg)
	}
	s.FieldTransformer = fieldTransformer
	err = s.SetRuleRegistry(serde.GlobalRuleRegistry(), conf.RuleConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Deserialize implements deserialization of generic data from JSON
func (s *Deserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	return s.DeserializeWithHeaders(topic, nil, payload)
}

// DeserializeWithHeaders implements deserialization of generic data from JSON
func (s *Deserializer) DeserializeWithHeaders(topic string, headers []kafka.Header, payload []byte) (interface{}, error) {
	return s.deserialize(topic, headers, payload, nil)
}

// DeserializeInto implements deserialization of generic data from JSON to the given object
func (s *Deserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
	return s.DeserializeWithHeadersInto(topic, nil, payload, msg)
}

// DeserializeWithHeadersInto implements deserialization of generic data from JSON to the given object
func (s *Deserializer) DeserializeWithHeadersInto(topic string, headers []kafka.Header, payload []byte, msg interface{}) error {
	_, err := s.deserialize(topic, headers, payload, msg)
	return err
}

func (s *Deserializer) deserialize(topic string, headers []kafka.Header, payload []byte, result interface{}) (interface{}, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	schemaID := serde.SchemaID{SchemaType: SchemaType}
	info, bytesRead, err := s.GetWriterSchema(topic, headers, payload, &schemaID)
	if err != nil {
		return nil, err
	}
	payload = payload[bytesRead:]
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return nil, err
	}
	var msg interface{}
	msg, err = s.ExecuteRulesWithPhase(subject, topic,
		schemaregistry.EncodingPhase, schemaregistry.Read, nil, &info, payload)
	if err != nil {
		return nil, err
	}
	payload = msg.([]byte)
	readerMeta, err := s.GetReaderSchema(subject)
	if err != nil {
		return nil, err
	}
	var migrations []serde.Migration
	if readerMeta != nil {
		migrations, err = s.GetMigrations(subject, topic, &info, readerMeta, payload)
		if err != nil {
			return nil, err
		}
	}
	bytes := payload
	if len(migrations) > 0 {
		err = json.Unmarshal(bytes, &msg)
		if err != nil {
			return nil, err
		}
		msg, err = s.ExecuteMigrations(migrations, subject, topic, msg)
		if err != nil {
			return nil, err
		}
		bytes, err = json.Marshal(msg)
		if err != nil {
			return nil, err
		}
	}
	if result == nil {
		msg, err = s.MessageFactory(subject, "")
		if err != nil {
			return nil, err
		}
	} else {
		msg = result
	}
	err = json.Unmarshal(bytes, msg)
	if err != nil {
		return nil, err
	}
	var target *schemaregistry.SchemaInfo
	if readerMeta != nil {
		target = &readerMeta.SchemaInfo
	} else {
		target = &info
	}
	msg, err = s.ExecuteRules(subject, topic, schemaregistry.Read, nil, target, msg)
	if err != nil {
		return nil, err
	}
	if s.validate {
		jschema, err := s.toJSONSchema(s.Client, info)
		if err != nil {
			return nil, err
		}
		err = jschema.Validate(msg)
		if err != nil {
			return nil, err
		}
	}
	return msg, nil
}

// FieldTransform transforms the field value using the rule
func (s *Serde) FieldTransform(client schemaregistry.Client, ctx serde.RuleContext, fieldTransform serde.FieldTransform, msg interface{}) (interface{}, error) {
	schema, err := s.toJSONSchema(client, *ctx.Target)
	if err != nil {
		return nil, err
	}
	val := reflect.ValueOf(msg)
	newVal, err := transform(ctx, schema, "$", &val, fieldTransform)
	if err != nil {
		return nil, err
	}
	return newVal.Interface(), nil
}

func (s *Serde) toJSONSchema(c schemaregistry.Client, schema schemaregistry.SchemaInfo) (*jsonschema2.Schema, error) {
	s.schemaToTypeCacheLock.RLock()
	value, ok := s.schemaToTypeCache.Get(schema.Schema)
	s.schemaToTypeCacheLock.RUnlock()
	if ok {
		jsonType := value.(*jsonschema2.Schema)
		return jsonType, nil
	}
	deps := make(map[string]string)
	err := serde.ResolveReferences(c, schema, deps)
	if err != nil {
		return nil, err
	}
	compiler := jsonschema2.NewCompiler()
	compiler.RegisterExtension("confluent:tags", tagsMeta, tagsCompiler{})
	compiler.LoadURL = func(url string) (io.ReadCloser, error) {
		url = strings.TrimPrefix(url, defaultBaseURL)
		return io.NopCloser(strings.NewReader(deps[url])), nil
	}
	if err := compiler.AddResource(defaultBaseURL, strings.NewReader(schema.Schema)); err != nil {
		return nil, err
	}
	jsonType, err := compiler.Compile(defaultBaseURL)
	if err != nil {
		return nil, err
	}
	s.schemaToTypeCacheLock.Lock()
	s.schemaToTypeCache.Put(schema.Schema, jsonType)
	s.schemaToTypeCacheLock.Unlock()
	return jsonType, nil
}

var tagsMeta = jsonschema2.MustCompileString("tags.json", `{
	"properties" : {
		"confluent:tags": {
			"type": "array",
            "items": { "type": "string" }
		}
	}
}`)

type tagsCompiler struct{}

func (tagsCompiler) Compile(ctx jsonschema2.CompilerContext, m map[string]interface{}) (jsonschema2.ExtSchema, error) {
	if prop, ok := m["confluent:tags"]; ok {
		val, ok2 := prop.([]interface{})
		if ok2 {
			tags := make([]string, len(val))
			for i, v := range val {
				tags[i] = fmt.Sprint(v)
			}
			return tagsSchema(tags), nil
		}
	}
	return nil, nil
}

type tagsSchema []string

func (s tagsSchema) Validate(ctx jsonschema2.ValidationContext, v interface{}) error {
	return nil
}
