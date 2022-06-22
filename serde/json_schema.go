package serde

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/invopop/jsonschema"
	jsonschema2 "github.com/santhosh-tekuri/jsonschema/v5"
	"io"
	"strings"
)

// JSONSchemaSerializer represents a JSON Schema serializer
type JSONSchemaSerializer struct {
	serializer
	validate bool
}

// JSONSchemaDeserializer represents a JSON Schema deserializer
type JSONSchemaDeserializer struct {
	deserializer
	validate bool
}

var _ Serializer = new(JSONSchemaSerializer)
var _ Deserializer = new(JSONSchemaDeserializer)

// NewJSONSchemaSerializer creates a JSON serializer for generic objects
func NewJSONSchemaSerializer(conf *schemaregistry.ConfigMap, isKey bool, validate bool) (*JSONSchemaSerializer, error) {
	s := &JSONSchemaSerializer{
		validate: validate,
	}
	err := s.configure(conf, isKey)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Serialize implements serialization of generic data to JSON
func (s *JSONSchemaSerializer) Serialize(topic string, msg interface{}) ([]byte, error) {
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
	id, err := s.getID(topic, msg, info)
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
		jschema, err := toJSONSchema(s.client, info)
		if err != nil {
			return nil, err
		}
		err = jschema.Validate(obj)
		if err != nil {
			return nil, err
		}
	}
	payload, err := s.writeBytes(id, raw)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// NewJSONSchemaDeserializer creates a JSON deserializer for generic objects
func NewJSONSchemaDeserializer(conf *schemaregistry.ConfigMap, isKey bool, validate bool) (*JSONSchemaDeserializer, error) {
	s := &JSONSchemaDeserializer{
		validate: validate,
	}
	err := s.configure(conf, isKey)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Deserialize implements deserialization of generic data from JSON
func (s *JSONSchemaDeserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	info, err := s.getSchema(topic, payload)
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
		jschema, err := toJSONSchema(s.client, info)
		if err != nil {
			return nil, err
		}
		err = jschema.Validate(obj)
		if err != nil {
			return nil, err
		}
	}
	subject := s.subjectNameStrategy(topic, s.isKey, info)
	msg, err := s.messageFactory(subject, "")
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
func (s *JSONSchemaDeserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
	if payload == nil {
		return nil
	}
	info, err := s.getSchema(topic, payload)
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
		jschema, err := toJSONSchema(s.client, info)
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
	err := resolveReferences(c, schema, deps)
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
