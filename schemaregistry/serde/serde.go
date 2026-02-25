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

package serde

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/cache"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rest"
)

// Type represents the type of Serde
type Type = int

const (
	// KeySerde denotes a key Serde
	KeySerde = 1
	// ValueSerde denotes a value Serde
	ValueSerde = 2
)

const (
	// EnableValidation enables validation
	EnableValidation = true
	// DisableValidation disables validation
	DisableValidation = false
)

// MagicByte is prepended to a schema ID
const MagicByte byte = 0x0

// MagicByteV0 is prepended to a schema ID
const MagicByteV0 = MagicByte

// MagicByteV1 is prepended to a schema GUID
const MagicByteV1 byte = 0x1

// KeySchemaIDHeader is the key schema ID header
const KeySchemaIDHeader = "__key_schema_id"

// ValueSchemaIDHeader is the value schema ID header
const ValueSchemaIDHeader = "__value_schema_id"

// SchemaID represents a schema ID or GUID
type SchemaID struct {
	SchemaType     string
	ID             int
	GUID           uuid.UUID
	MessageIndexes []int
}

// NewSchemaID creates a new SchemaID
func NewSchemaID(schemaType string, id int, guid string) (*SchemaID, error) {
	var err error
	u := uuid.Nil
	if len(guid) > 0 {
		u, err = uuid.Parse(guid)
		if err != nil {
			return nil, err
		}
	}
	return &SchemaID{
		SchemaType:     schemaType,
		ID:             id,
		GUID:           u,
		MessageIndexes: nil,
	}, nil
}

// FromBytes converts the bytes to the SchemaID
func (s *SchemaID) FromBytes(payload []byte) (int, error) {
	var totalBytesRead int
	magicByte := payload[0]
	if magicByte == MagicByteV0 {
		s.ID = int(binary.BigEndian.Uint32(payload[1:5]))
		totalBytesRead = 5
	} else if magicByte == MagicByteV1 {
		guid, err := uuid.FromBytes(payload[1:17])
		if err != nil {
			return 0, err
		}
		s.GUID = guid
		totalBytesRead = 17
	} else {
		return 0, fmt.Errorf("unknown magic byte %d", magicByte)
	}
	if s.SchemaType == "PROTOBUF" {
		bytesRead, msgIndexes, err := readMessageIndexes(payload[totalBytesRead:])
		if err != nil {
			return 0, err
		}
		s.MessageIndexes = msgIndexes
		totalBytesRead += bytesRead
	}
	return totalBytesRead, nil
}

// IDToBytes converts the schema ID to bytes
func (s *SchemaID) IDToBytes() ([]byte, error) {
	if s.ID == 0 {
		return nil, fmt.Errorf("schema ID is not set")
	}
	var buf bytes.Buffer
	err := buf.WriteByte(MagicByteV0)
	if err != nil {
		return nil, err
	}
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, uint32(s.ID))
	_, err = buf.Write(idBytes)
	if err != nil {
		return nil, err
	}
	if len(s.MessageIndexes) > 0 {
		buf.Write(writeMessageIndexes(s.MessageIndexes))
	}
	return buf.Bytes(), nil
}

// GUIDToBytes converts the schema GUID to bytes
func (s *SchemaID) GUIDToBytes() ([]byte, error) {
	if s.GUID == uuid.Nil {
		return nil, fmt.Errorf("schema GUID is not set")
	}
	var buf bytes.Buffer
	err := buf.WriteByte(MagicByteV1)
	if err != nil {
		return nil, err
	}
	guidBytes, err := s.GUID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(guidBytes)
	if err != nil {
		return nil, err
	}
	if len(s.MessageIndexes) > 0 {
		buf.Write(writeMessageIndexes(s.MessageIndexes))
	}
	return buf.Bytes(), nil
}

func readMessageIndexes(payload []byte) (int, []int, error) {
	arrayLen, bytesRead := binary.Varint(payload)
	if bytesRead <= 0 {
		return bytesRead, nil, fmt.Errorf("unable to read message indexes")
	}
	if arrayLen < 0 {
		return bytesRead, nil, fmt.Errorf("parsed invalid message index count")
	}
	if arrayLen == 0 {
		// Handle the optimization for the first message in the schema
		return bytesRead, []int{0}, nil
	}
	msgIndexes := make([]int, arrayLen)
	for i := 0; i < int(arrayLen); i++ {
		idx, read := binary.Varint(payload[bytesRead:])
		if read <= 0 {
			return bytesRead, nil, fmt.Errorf("unable to read message indexes")
		}
		bytesRead += read
		msgIndexes[i] = int(idx)
	}
	return bytesRead, msgIndexes, nil
}

func writeMessageIndexes(msgIndexes []int) []byte {
	if len(msgIndexes) == 1 && msgIndexes[0] == 0 {
		// Handle the optimization for the first message in the schema
		return []byte{0}
	}
	buf := make([]byte, (1+len(msgIndexes))*binary.MaxVarintLen64)
	length := binary.PutVarint(buf, int64(len(msgIndexes)))

	for _, element := range msgIndexes {
		length += binary.PutVarint(buf[length:], int64(element))
	}
	return buf[0:length]
}

// MessageFactory is a factory function, which should return a pointer to
// an instance into which we will unmarshal wire data.
// For Avro, the name will be the name of the Avro type if it has one.
// For JSON Schema, the name will be empty/F.
// For Protobuf, the name will be the name of the message type.
type MessageFactory func(subject string, name string) (interface{}, error)

// Serializer represents a serializer
type Serializer interface {
	ConfigureSerializer(client schemaregistry.Client, serdeType Type,
		conf *SerializerConfig) error
	// Serialize will serialize the given message, which should be a pointer.
	// For example, in Protobuf, messages are always a pointer to a struct and never just a struct.
	Serialize(topic string, msg interface{}) ([]byte, error)
	SerializeWithHeaders(topic string, msg interface{}) ([]kafka.Header, []byte, error)
	Close() error
}

// Deserializer represents a deserializer
type Deserializer interface {
	ConfigureDeserializer(client schemaregistry.Client, serdeType Type,
		conf *DeserializerConfig) error
	// Deserialize will call the MessageFactory to create an object
	// into which we will unmarshal data.
	Deserialize(topic string, payload []byte) (interface{}, error)
	DeserializeWithHeaders(topic string, headers []kafka.Header, payload []byte) (interface{}, error)
	// DeserializeInto will unmarshal data into the given object.
	DeserializeInto(topic string, payload []byte, msg interface{}) error
	DeserializeWithHeadersInto(topic string, headers []kafka.Header, payload []byte, msg interface{}) error
	Close() error
}

// Serde is a common instance for both the serializers and deserializers
type Serde struct {
	Client              schemaregistry.Client
	SerdeType           Type
	SubjectNameStrategy SubjectNameStrategyFunc
	MessageFactory      MessageFactory
	FieldTransformer    FieldTransformer
	RuleRegistry        *RuleRegistry
}

// BaseSerializer represents basic serializer info
type BaseSerializer struct {
	Serde
	SchemaIDSerializer SchemaIDSerializerFunc
	Conf               *SerializerConfig
}

// BaseDeserializer represents basic deserializer info
type BaseDeserializer struct {
	Serde
	SchemaIDDeserializer SchemaIDDeserializerFunc
	Conf                 *DeserializerConfig
}

// RuleContext represents a rule context
type RuleContext struct {
	Source           *schemaregistry.SchemaInfo
	Target           *schemaregistry.SchemaInfo
	Subject          string
	Topic            string
	IsKey            bool
	RuleMode         schemaregistry.RuleMode
	Rule             *schemaregistry.Rule
	Index            int
	Rules            []schemaregistry.Rule
	FieldTransformer FieldTransformer
	fieldContexts    []FieldContext
}

// GetParameter returns a parameter by name
func (r *RuleContext) GetParameter(name string) *string {
	params := r.Rule.Params
	value, ok := params[name]
	if ok {
		return &value
	}
	metadata := r.Target.Metadata
	if metadata != nil {
		value, ok = metadata.Properties[name]
		if ok {
			return &value
		}
	}
	return nil
}

// CurrentField returns the current field context
func (r *RuleContext) CurrentField() *FieldContext {
	size := len(r.fieldContexts)
	if size == 0 {
		return nil
	}
	return &r.fieldContexts[size-1]
}

// EnterField enters a field context
func (r *RuleContext) EnterField(containingMessage interface{}, fullName string,
	name string, fieldType FieldType, tags []string) (FieldContext, bool) {
	allTags := make(map[string]bool)
	for _, v := range tags {
		allTags[v] = true
	}
	for k, v := range r.GetTags(fullName) {
		allTags[k] = v
	}
	fieldContext := FieldContext{
		ContainingMessage: containingMessage,
		FullName:          fullName,
		Name:              name,
		Type:              fieldType,
		Tags:              allTags,
	}
	r.fieldContexts = append(r.fieldContexts, fieldContext)
	return fieldContext, true
}

// GetTags returns tags for a full name
func (r *RuleContext) GetTags(fullName string) map[string]bool {
	tags := make(map[string]bool)
	metadata := r.Target.Metadata
	if metadata != nil && metadata.Tags != nil {
		for k, v := range metadata.Tags {
			if match(fullName, k) {
				for _, tag := range v {
					tags[tag] = true
				}
			}
		}
	}
	return tags
}

// LeaveField leaves a field context
func (r *RuleContext) LeaveField() {
	size := len(r.fieldContexts) - 1
	r.fieldContexts = r.fieldContexts[:size]
}

// RuleBase represents a rule base
type RuleBase interface {
	Configure(clientConfig *schemaregistry.Config, config map[string]string) error
	Type() string
	Close() error
}

// RuleExecutor represents a rule executor
type RuleExecutor interface {
	RuleBase
	Transform(ctx RuleContext, msg interface{}) (interface{}, error)
}

// FieldTransformer represents a field transformer
type FieldTransformer func(ctx RuleContext, fieldTransform FieldTransform, msg interface{}) (interface{}, error)

// FieldTransform represents a field transform
type FieldTransform interface {
	Transform(ctx RuleContext, fieldCtx FieldContext, fieldValue interface{}) (interface{}, error)
}

// FieldRuleExecutor represents a field rule executor
type FieldRuleExecutor interface {
	RuleExecutor
	NewTransform(ctx RuleContext) (FieldTransform, error)
}

// AbstractFieldRuleExecutor represents an abstract field rule executor
type AbstractFieldRuleExecutor struct {
	FieldRuleExecutor
}

// Transform transforms the message using the rule
func (a *AbstractFieldRuleExecutor) Transform(ctx RuleContext, msg interface{}) (interface{}, error) {
	// TODO preserve source?
	switch ctx.RuleMode {
	case schemaregistry.Write, schemaregistry.Upgrade:
		for i := 0; i < ctx.Index; i++ {
			otherRule := ctx.Rules[i]
			if areTransformsWithSameTag(*ctx.Rule, otherRule) {
				// ignore this transform if an earlier one has the same tag
				return msg, nil
			}
		}
	case schemaregistry.Read, schemaregistry.Downgrade:
		for i := ctx.Index + 1; i < len(ctx.Rules); i++ {
			otherRule := ctx.Rules[i]
			if areTransformsWithSameTag(*ctx.Rule, otherRule) {
				// ignore this transform if a later one has the same tag
				return msg, nil
			}
		}
	}

	fieldTransform, err := a.NewTransform(ctx)
	if err != nil {
		return nil, err
	}
	// TODO preserve source?
	return ctx.FieldTransformer(ctx, fieldTransform, msg)
}

func areTransformsWithSameTag(rule1 schemaregistry.Rule, rule2 schemaregistry.Rule) bool {
	return len(rule1.Tags) > 0 && rule1.Kind == "TRANSFORM" && rule1.Kind == rule2.Kind && rule1.Mode == rule2.Mode &&
		rule1.Type == rule2.Type && reflect.DeepEqual(rule1.Tags, rule2.Tags)
}

// FieldContext represents a field context
type FieldContext struct {
	ContainingMessage interface{}
	FullName          string
	Name              string
	Type              FieldType
	Tags              map[string]bool
}

// FieldType represents the field type
type FieldType = int

const (
	// TypeRecord represents a record
	TypeRecord = 1
	// TypeEnum represents an enum
	TypeEnum = 2
	// TypeArray represents an array
	TypeArray = 3
	// TypeMap represents a map
	TypeMap = 4
	// TypeCombined represents a combined
	TypeCombined = 5
	// TypeFixed represents a fixed
	TypeFixed = 6
	// TypeString represents a string
	TypeString = 7
	// TypeBytes represents bytes
	TypeBytes = 8
	// TypeInt represents an int
	TypeInt = 9
	// TypeLong represents a long
	TypeLong = 10
	// TypeFloat represents a float
	TypeFloat = 11
	// TypeDouble represents a double
	TypeDouble = 12
	// TypeBoolean represents a Boolean
	TypeBoolean = 13
	// TypeNull represents a null
	TypeNull = 14
)

// IsPrimitive returns true if the field is a primitive
func (f *FieldContext) IsPrimitive() bool {
	t := f.Type
	return t == TypeString || t == TypeBytes || t == TypeInt || t == TypeLong ||
		t == TypeFloat || t == TypeDouble || t == TypeBoolean || t == TypeNull
}

// TypeName returns the type name
func (f *FieldContext) TypeName() string {
	switch f.Type {
	case TypeRecord:
		return "RECORD"
	case TypeEnum:
		return "ENUM"
	case TypeArray:
		return "ARRAY"
	case TypeMap:
		return "MAP"
	case TypeCombined:
		return "COMBINED"
	case TypeFixed:
		return "FIXED"
	case TypeString:
		return "STRING"
	case TypeBytes:
		return "BYTES"
	case TypeInt:
		return "INT"
	case TypeLong:
		return "LONG"
	case TypeFloat:
		return "FLOAT"
	case TypeDouble:
		return "DOUBLE"
	case TypeBoolean:
		return "BOOLEAN"
	case TypeNull:
		return "NULL"
	}
	return ""
}

// RuleAction represents a rule action
type RuleAction interface {
	RuleBase
	Run(ctx RuleContext, msg interface{}, err error) error
}

// ErrorAction represents an error action
type ErrorAction struct {
}

// NoneAction represents a no-op action
type NoneAction struct {
}

// RuleConditionErr represents a rule condition error
type RuleConditionErr struct {
	Rule *schemaregistry.Rule
	Err  error
}

// Error returns the error message
func (re RuleConditionErr) Error() string {
	errMsg := re.Rule.Doc
	if errMsg == "" {
		if re.Rule.Expr != "" {
			return "Expr failed: '" + re.Rule.Expr + "'"
		}
		return "Condition failed: '" + re.Rule.Name + "'"
	}
	return errMsg
}

// ConfigureSerializer configures the Serializer
func (s *BaseSerializer) ConfigureSerializer(client schemaregistry.Client, serdeType Type,
	conf *SerializerConfig) error {
	if client == nil {
		return fmt.Errorf("schema registry client missing")
	}
	s.Client = client
	s.Conf = conf
	s.SerdeType = serdeType
	s.SubjectNameStrategy = TopicNameStrategy
	s.SchemaIDSerializer = PrefixSchemaIDSerializer
	return nil
}

// ConfigureDeserializer configures the Deserializer
func (s *BaseDeserializer) ConfigureDeserializer(client schemaregistry.Client, serdeType Type,
	conf *DeserializerConfig) error {
	if client == nil {
		return fmt.Errorf("schema registry client missing")
	}
	s.Client = client
	s.Conf = conf
	s.SerdeType = serdeType
	s.SubjectNameStrategy = TopicNameStrategy
	s.SchemaIDDeserializer = DualSchemaIDDeserializer
	return nil
}

// SubjectNameStrategyType determines the type of subject name strategy
type SubjectNameStrategyType int

const (
	// NoStrategyType uses the strategy set in SubjectNameStrategy
	NoStrategyType SubjectNameStrategyType = iota
	// TopicNameStrategyType creates a subject name by appending -[key|value] to the topic name
	TopicNameStrategyType
	// RecordNameStrategyType creates a subject name from the record name
	RecordNameStrategyType
	// TopicRecordNameStrategyType creates a subject name from the topic and record name
	TopicRecordNameStrategyType
	// AssociatedNameStrategyType retrieves the associated subject name from schema registry
	AssociatedNameStrategyType
)

const (
	// KafkaClusterIDConfig is the configuration key for the Kafka cluster ID
	KafkaClusterIDConfig = "kafka.cluster.id"
	// NamespaceWildcard is the default namespace when no Kafka cluster ID is configured
	NamespaceWildcard = "-"
	// FallbackSubjectNameStrategyTypeConfig is the configuration key for the fallback subject name strategy
	FallbackSubjectNameStrategyTypeConfig = "fallback.subject.name.strategy.type"
	// DefaultCacheCapacity is the default capacity for the association cache
	DefaultCacheCapacity = 1000
)

// SubjectNameStrategyFunc determines the subject for the given parameters
type SubjectNameStrategyFunc func(topic string, serdeType Type, schema schemaregistry.SchemaInfo) (string, error)

// RecordNameFunc extracts the record name from a schema
type RecordNameFunc func(schema schemaregistry.SchemaInfo) (string, error)

// ParseSubjectNameStrategyType parses a string to SubjectNameStrategyType.
// An empty string returns TopicNameStrategyType (the default).
// An unrecognized non-empty string returns an error.
func ParseSubjectNameStrategyType(s string) (SubjectNameStrategyType, error) {
	switch strings.ToUpper(s) {
	case "", "TOPIC":
		return TopicNameStrategyType, nil
	case "RECORD":
		return RecordNameStrategyType, nil
	case "TOPIC_RECORD":
		return TopicRecordNameStrategyType, nil
	case "ASSOCIATED":
		return AssociatedNameStrategyType, nil
	case "NONE":
		return NoStrategyType, nil
	default:
		return NoStrategyType, fmt.Errorf("unrecognized subject name strategy type: %q", s)
	}
}

// StrategyFunc returns the SubjectNameStrategyFunc for the given strategy type.
// Note: This does not handle AssociatedNameStrategyType as it requires additional parameters.
func StrategyFunc(strategyType SubjectNameStrategyType, getRecordName RecordNameFunc) (SubjectNameStrategyFunc, error) {
	switch strategyType {
	case TopicNameStrategyType:
		return TopicNameStrategy, nil
	case RecordNameStrategyType:
		if getRecordName == nil {
			return nil, fmt.Errorf("getRecordName is required for RecordNameStrategyType")
		}
		return RecordNameStrategy(getRecordName), nil
	case TopicRecordNameStrategyType:
		if getRecordName == nil {
			return nil, fmt.Errorf("getRecordName is required for TopicRecordNameStrategyType")
		}
		return TopicRecordNameStrategy(getRecordName), nil
	case NoStrategyType:
		return nil, nil
	default:
		return TopicNameStrategy, nil
	}
}

// ConfigureSubjectNameStrategy configures the subject name strategy based on the strategy type
func (s *Serde) ConfigureSubjectNameStrategy(strategyType SubjectNameStrategyType, config map[string]string, getRecordName RecordNameFunc) error {
	if strategyType == AssociatedNameStrategyType {
		strategy, err := AssociatedNameStrategy(s.Client, config, getRecordName)
		if err != nil {
			return err
		}
		s.SubjectNameStrategy = strategy
	} else {
		strategy, err := StrategyFunc(strategyType, getRecordName)
		if err != nil {
			return err
		}
		s.SubjectNameStrategy = strategy
		if s.SubjectNameStrategy == nil {
			// NoStrategyType should default to TopicNameStrategy for main strategy
			s.SubjectNameStrategy = TopicNameStrategy
		}
	}
	return nil
}

// TopicNameStrategy creates a subject name by appending -[key|value] to the topic name.
func TopicNameStrategy(topic string, serdeType Type, schema schemaregistry.SchemaInfo) (string, error) {
	suffix := "-value"
	if serdeType == KeySerde {
		suffix = "-key"
	}
	return topic + suffix, nil
}

// RecordNameStrategy creates a subject name from the record name.
// If the schema is empty, an empty string is returned to allow deferred resolution.
func RecordNameStrategy(getRecordName RecordNameFunc) SubjectNameStrategyFunc {
	return func(topic string, serdeType Type, schema schemaregistry.SchemaInfo) (string, error) {
		if schema.Schema == "" {
			return "", nil
		}
		return getRecordName(schema)
	}
}

// TopicRecordNameStrategy creates a subject name from the topic and record name.
// If the schema is empty, an empty string is returned to allow deferred resolution.
func TopicRecordNameStrategy(getRecordName RecordNameFunc) SubjectNameStrategyFunc {
	return func(topic string, serdeType Type, schema schemaregistry.SchemaInfo) (string, error) {
		if schema.Schema == "" {
			return "", nil
		}
		recordName, err := getRecordName(schema)
		if err != nil {
			return "", err
		}
		return topic + "-" + recordName, nil
	}
}

// subjectCacheKey is the cache key for subject lookups
type subjectCacheKey struct {
	topic  string
	isKey  bool
	schema string
}

// AssociatedNameStrategy returns a strategy that retrieves the associated subject name from schema registry.
// The topic is passed as the resource name to schema registry. If there is a configuration property named
// "kafka.cluster.id", then its value will be passed as the resource namespace; otherwise the value "-"
// will be passed as the resource namespace.
// If more than one subject is returned from the query, an error will be returned.
// If no subjects are returned from the query, then the behavior will fall back to TopicNameStrategy,
// unless the configuration property "fallback.subject.name.strategy.type" is set to "RECORD",
// "TOPIC_RECORD", or "NONE".
func AssociatedNameStrategy(client schemaregistry.Client, config map[string]string, getRecordName RecordNameFunc) (SubjectNameStrategyFunc, error) {
	// Get kafka cluster ID from config, default to wildcard
	kafkaClusterID := NamespaceWildcard
	if id, ok := config[KafkaClusterIDConfig]; ok && id != "" {
		kafkaClusterID = id
	}

	// Determine fallback strategy
	fallbackType, err := ParseSubjectNameStrategyType(config[FallbackSubjectNameStrategyTypeConfig])
	if err != nil {
		return nil, err
	}
	fallbackStrategy, err := StrategyFunc(fallbackType, getRecordName)
	if err != nil {
		return nil, err
	}

	// Create LRU cache for subject names
	subjectNameCache, err := cache.NewLRUCache(DefaultCacheCapacity)
	if err != nil {
		return nil, fmt.Errorf("associated name strategy: failed to create LRU cache: %w", err)
	}
	var subjectNameCacheLock sync.RWMutex

	return func(topic string, serdeType Type, schema schemaregistry.SchemaInfo) (string, error) {
		if topic == "" {
			return "", nil
		}

		isKey := serdeType == KeySerde

		// Create cache key using topic, isKey, and schema string
		cacheKey := subjectCacheKey{
			topic:  topic,
			isKey:  isKey,
			schema: schema.Schema,
		}

		// Check cache first with read lock
		subjectNameCacheLock.RLock()
		cached, ok := subjectNameCache.Get(cacheKey)
		subjectNameCacheLock.RUnlock()
		if ok {
			return cached.(string), nil
		}

		// Load subject name from schema registry (without holding lock)
		subject, err := loadAssociatedSubjectName(client, topic, kafkaClusterID, isKey, schema, fallbackStrategy, serdeType)
		if err != nil {
			return "", err
		}

		// Store in cache
		subjectNameCacheLock.Lock()
		subjectNameCache.Put(cacheKey, subject)
		subjectNameCacheLock.Unlock()

		return subject, nil
	}, nil
}

// loadAssociatedSubjectName loads the subject name from schema registry associations
func loadAssociatedSubjectName(client schemaregistry.Client, topic string, kafkaClusterID string,
	isKey bool, schema schemaregistry.SchemaInfo, fallbackStrategy SubjectNameStrategyFunc,
	serdeType Type) (string, error) {

	associationType := "value"
	if isKey {
		associationType = "key"
	}

	associations, err := client.GetAssociationsByResourceName(
		topic,
		kafkaClusterID,
		"topic",
		[]string{associationType},
		"",
		0,
		-1,
	)
	if err != nil {
		// Handle 404 errors by falling back to the fallback strategy
		var restErr *rest.Error
		if errors.As(err, &restErr) && strings.HasPrefix(strconv.Itoa(restErr.Code), "404") {
			associations = nil
		} else {
			return "", fmt.Errorf("failed to get associations for topic %s: %w", topic, err)
		}
	}

	if len(associations) > 1 {
		return "", fmt.Errorf("multiple associated subjects found for topic %s", topic)
	} else if len(associations) == 1 {
		return associations[0].Subject, nil
	} else if fallbackStrategy != nil {
		return fallbackStrategy(topic, serdeType, schema)
	} else {
		return "", fmt.Errorf("no associated subject found for topic %s", topic)
	}
}

// SchemaIDSerializerFunc determines how to serialize a schema ID/GUID
type SchemaIDSerializerFunc func(topic string, serdeType Type,
	payload []byte, schemaID SchemaID) ([]kafka.Header, []byte, error)

// HeaderSchemaIDSerializer serializes a schema GUID to the header
func HeaderSchemaIDSerializer(topic string, serdeType Type,
	payload []byte, schemaID SchemaID) ([]kafka.Header, []byte, error) {
	headerKey := ValueSchemaIDHeader
	if serdeType == KeySerde {
		headerKey = KeySchemaIDHeader
	}
	headerValue, err := schemaID.GUIDToBytes()
	if err != nil {
		return nil, nil, err
	}
	headers := []kafka.Header{{Key: headerKey, Value: headerValue}}
	return headers, payload, nil
}

// PrefixSchemaIDSerializer serializes a schema ID to the payload prefix
func PrefixSchemaIDSerializer(topic string, serdeType Type,
	payload []byte, schemaID SchemaID) ([]kafka.Header, []byte, error) {
	var buf bytes.Buffer
	idBytes, err := schemaID.IDToBytes()
	if err != nil {
		return nil, nil, err
	}
	_, err = buf.Write(idBytes)
	if err != nil {
		return nil, nil, err
	}
	_, err = buf.Write(payload)
	if err != nil {
		return nil, nil, err
	}
	return nil, buf.Bytes(), nil
}

// SchemaIDDeserializerFunc determines how to deserialize a schema ID/GUID
// and returns the number of bytes read from the payload
type SchemaIDDeserializerFunc func(topic string, serdeType Type, headers []kafka.Header,
	payload []byte, schemaID *SchemaID) (int, error)

// DualSchemaIDDeserializer either deserializes a schema GUID from the header or
// a schema ID from the payload prefix and returns the number of bytes read from the payload
func DualSchemaIDDeserializer(topic string, serdeType Type, headers []kafka.Header,
	payload []byte, schemaID *SchemaID) (int, error) {
	headerKey := ValueSchemaIDHeader
	if serdeType == KeySerde {
		headerKey = KeySchemaIDHeader
	}
	// find the schema ID in the headers
	for _, header := range headers {
		if header.Key == headerKey {
			if len(header.Value) != 0 {
				_, err := schemaID.FromBytes(header.Value)
				if err == nil {
					return 0, nil
				}
			}
		}
	}
	return schemaID.FromBytes(payload)
}

// PrefixSchemaIDDeserializer deserializes a schema ID from the payload prefix
// and returns the number of bytes read from the payload
func PrefixSchemaIDDeserializer(topic string, serdeType Type, headers []kafka.Header,
	payload []byte, schemaID SchemaID) (int, error) {
	return schemaID.FromBytes(payload)
}

// GetID returns a schema ID for the given schema
// Deprecated: Use GetSchemaID instead
func (s *BaseSerializer) GetID(topic string, msg interface{}, info *schemaregistry.SchemaInfo) (int, error) {
	schemaID, err := s.GetSchemaID("", topic, msg, info)
	if err != nil {
		return -1, err
	}
	return schemaID.ID, nil
}

// GetSchemaID returns a schema ID for the given schema
func (s *BaseSerializer) GetSchemaID(schemaType string, topic string, msg interface{}, info *schemaregistry.SchemaInfo) (SchemaID, error) {
	autoRegister := s.Conf.AutoRegisterSchemas
	useSchemaID := s.Conf.UseSchemaID
	useLatestWithMetadata := s.Conf.UseLatestWithMetadata
	useLatest := s.Conf.UseLatestVersion
	normalizeSchema := s.Conf.NormalizeSchemas

	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, *info)
	if err != nil {
		return SchemaID{
			ID: -1,
		}, err
	}
	var metadata schemaregistry.SchemaMetadata
	if autoRegister {
		metadata, err = s.Client.RegisterFullResponse(subject, *info, normalizeSchema)
		if err != nil {
			return SchemaID{
				ID: -1,
			}, err
		}
	} else if useSchemaID >= 0 {
		*info, err = s.Client.GetBySubjectAndID(subject, useSchemaID)
		if err != nil {
			return SchemaID{
				ID: -1,
			}, err
		}
		metadata, err = s.Client.GetIDFullResponse(subject, *info, normalizeSchema)
		if err != nil {
			return SchemaID{
				ID: -1,
			}, err
		}
	} else if len(useLatestWithMetadata) != 0 {
		metadata, err = s.Client.GetLatestWithMetadata(subject, useLatestWithMetadata, true)
		if err != nil {
			return SchemaID{
				ID: -1,
			}, err
		}
		*info = metadata.SchemaInfo
	} else if useLatest {
		metadata, err = s.Client.GetLatestSchemaMetadata(subject)
		if err != nil {
			return SchemaID{
				ID: -1,
			}, err
		}
		*info = metadata.SchemaInfo
	} else {
		metadata, err = s.Client.GetIDFullResponse(subject, *info, normalizeSchema)
		if err != nil {
			return SchemaID{
				ID: -1,
			}, err
		}
	}
	schemaID, err := NewSchemaID(schemaType, metadata.ID, metadata.GUID)
	if err != nil {
		return SchemaID{
			ID: -1,
		}, err
	}
	return *schemaID, nil
}

// SetRuleRegistry sets the rule registry
func (s *Serde) SetRuleRegistry(registry *RuleRegistry, ruleConfig map[string]string) error {
	s.RuleRegistry = registry
	for _, rule := range registry.GetExecutors() {
		err := rule.Configure(s.Client.Config(), ruleConfig)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetMigrations returns the migration rules for the given subject
func (s *Serde) GetMigrations(subject string, topic string, sourceInfo *schemaregistry.SchemaInfo,
	target *schemaregistry.SchemaMetadata, msg interface{}) ([]Migration, error) {
	version, err := s.Client.GetVersionIncludeDeleted(subject, *sourceInfo, false, true)
	if err != nil {
		return nil, err
	}
	source := &schemaregistry.SchemaMetadata{
		SchemaInfo: *sourceInfo,
		Version:    version,
	}
	var migrationMode schemaregistry.RuleMode
	var migrations []Migration
	var first *schemaregistry.SchemaMetadata
	var last *schemaregistry.SchemaMetadata
	if source.Version < target.Version {
		migrationMode = schemaregistry.Upgrade
		first = source
		last = target
	} else if source.Version > target.Version {
		migrationMode = schemaregistry.Downgrade
		first = target
		last = source
	} else {
		return migrations, nil
	}
	var previous *schemaregistry.SchemaMetadata
	versions, err := s.getSchemasBetween(subject, first, last)
	if err != nil {
		return nil, err
	}
	for i, version := range versions {
		if i == 0 {
			previous = version
			continue
		}
		if version.RuleSet != nil && version.RuleSet.HasRules(schemaregistry.MigrationPhase, migrationMode) {
			var m Migration
			if migrationMode == schemaregistry.Upgrade {
				m = Migration{
					RuleMode: migrationMode,
					Source:   previous,
					Target:   version,
				}
			} else {
				m = Migration{
					RuleMode: migrationMode,
					Source:   version,
					Target:   previous,
				}
			}
			migrations = append(migrations, m)
		}
		previous = version
	}
	if migrationMode == schemaregistry.Downgrade {
		// Reverse the order of migrations for symmetry
		for i, j := 0, len(migrations)-1; i < j; i, j = i+1, j-1 {
			migrations[i], migrations[j] = migrations[j], migrations[i]
		}
	}
	return migrations, nil
}

func (s *Serde) getSchemasBetween(subject string, first *schemaregistry.SchemaMetadata,
	last *schemaregistry.SchemaMetadata) ([]*schemaregistry.SchemaMetadata, error) {
	if last.Version-first.Version <= 1 {
		return []*schemaregistry.SchemaMetadata{first, last}, nil
	}
	version1 := first.Version
	version2 := last.Version
	result := []*schemaregistry.SchemaMetadata{first}
	for i := version1 + 1; i < version2; i++ {
		meta, err := s.Client.GetSchemaMetadataIncludeDeleted(subject, i, true)
		if err != nil {
			return nil, err
		}
		result = append(result, &meta)
	}
	result = append(result, last)
	return result, nil
}

// Migration represents a migration
type Migration struct {
	RuleMode schemaregistry.RuleMode
	Source   *schemaregistry.SchemaMetadata
	Target   *schemaregistry.SchemaMetadata
}

// ExecuteMigrations executes the given migrations
func (s *Serde) ExecuteMigrations(migrations []Migration, subject string, topic string, msg interface{}) (interface{}, error) {
	var err error
	for _, migration := range migrations {
		msg, err = s.ExecuteRulesWithPhase(subject, topic,
			schemaregistry.MigrationPhase, migration.RuleMode,
			&migration.Source.SchemaInfo, &migration.Target.SchemaInfo, msg)
		if err != nil {
			return nil, err
		}
	}
	return msg, nil
}

// ExecuteRules executes the given rules
func (s *Serde) ExecuteRules(subject string, topic string, ruleMode schemaregistry.RuleMode,
	source *schemaregistry.SchemaInfo, target *schemaregistry.SchemaInfo, msg interface{}) (interface{}, error) {
	return s.ExecuteRulesWithPhase(subject, topic, schemaregistry.DomainPhase, ruleMode, source, target, msg)
}

// ExecuteRulesWithPhase executes the given rules
func (s *Serde) ExecuteRulesWithPhase(subject string, topic string,
	rulePhase schemaregistry.RulePhase, ruleMode schemaregistry.RuleMode,
	source *schemaregistry.SchemaInfo, target *schemaregistry.SchemaInfo, msg interface{}) (interface{}, error) {
	if msg == nil || target == nil {
		return msg, nil
	}
	var rules []schemaregistry.Rule
	switch ruleMode {
	case schemaregistry.Upgrade:
		if target.RuleSet != nil {
			rules = target.RuleSet.MigrationRules
		}
	case schemaregistry.Downgrade:
		if source.RuleSet != nil {
			// Execute downgrade rules in reverse order for symmetry
			rules = reverseRules(source.RuleSet.MigrationRules)
		}
	default:
		if target.RuleSet != nil {
			if rulePhase == schemaregistry.EncodingPhase {
				rules = target.RuleSet.EncodingRules
			} else {
				rules = target.RuleSet.DomainRules
			}
			if ruleMode == schemaregistry.Read {
				// Execute read rules in reverse order for symmetry
				rules = reverseRules(rules)
			}
		}
	}
	for i, rule := range rules {
		if s.isDisabled(rule) {
			continue
		}
		mode, ok := schemaregistry.ParseMode(rule.Mode)
		if !ok {
			continue
		}
		switch mode {
		case schemaregistry.WriteRead:
			if ruleMode != schemaregistry.Write && ruleMode != schemaregistry.Read {
				continue
			}
		case schemaregistry.UpDown:
			if ruleMode != schemaregistry.Upgrade && ruleMode != schemaregistry.Downgrade {
				continue
			}
		default:
			if mode != ruleMode {
				continue
			}
		}
		ctx := RuleContext{
			Source:           source,
			Target:           target,
			Subject:          subject,
			Topic:            topic,
			IsKey:            s.SerdeType == KeySerde,
			RuleMode:         ruleMode,
			Rule:             &rule,
			Index:            i,
			Rules:            rules,
			FieldTransformer: s.FieldTransformer,
		}
		ruleExecutor := s.RuleRegistry.GetExecutor(rule.Type)
		if ruleExecutor == nil {
			err := s.runAction(ctx, ruleMode, rule, s.getOnFailure(rule), msg,
				fmt.Errorf("could not find rule executor of type %s", rule.Type), "ERROR")
			if err != nil {
				return nil, err
			}
			return msg, nil
		}
		var err error
		result, err := ruleExecutor.Transform(ctx, msg)
		if result == nil || err != nil {
			err = s.runAction(ctx, ruleMode, rule, s.getOnFailure(rule), msg, err, "ERROR")
			if err != nil {
				return nil, err
			}
		} else {
			switch rule.Kind {
			case "CONDITION":
				condResult, ok2 := result.(bool)
				if ok2 && !condResult {
					err = s.runAction(ctx, ruleMode, rule, s.getOnFailure(rule), msg, err, "ERROR")
					if err != nil {
						return nil, RuleConditionErr{
							Rule: ctx.Rule,
							Err:  err,
						}
					}
				}
			case "TRANSFORM":
				msg = result
			}
			// ignore error, since rule succeeded
			_ = s.runAction(ctx, ruleMode, rule, s.getOnSuccess(rule), msg, nil, "NONE")
		}
	}
	return msg, nil
}

func (s *Serde) getOnSuccess(rule schemaregistry.Rule) string {
	override := s.RuleRegistry.GetOverride(rule.Type)
	if override != nil && override.OnSuccess != nil {
		return *override.OnSuccess
	}
	return rule.OnSuccess
}

func (s *Serde) getOnFailure(rule schemaregistry.Rule) string {
	override := s.RuleRegistry.GetOverride(rule.Type)
	if override != nil && override.OnFailure != nil {
		return *override.OnFailure
	}
	return rule.OnFailure
}

func (s *Serde) isDisabled(rule schemaregistry.Rule) bool {
	override := s.RuleRegistry.GetOverride(rule.Type)
	if override != nil && override.Disabled != nil {
		return *override.Disabled
	}
	return rule.Disabled
}

func reverseRules(rules []schemaregistry.Rule) []schemaregistry.Rule {
	newRules := make([]schemaregistry.Rule, len(rules))
	copy(newRules, rules)
	// Execute downgrade rules in reverse order for symmetry
	for i, j := 0, len(newRules)-1; i < j; i, j = i+1, j-1 {
		newRules[i], newRules[j] = newRules[j], newRules[i]
	}
	return newRules
}

func (s *Serde) runAction(ctx RuleContext, ruleMode schemaregistry.RuleMode, rule schemaregistry.Rule,
	action string, msg interface{}, err error, defaultAction string) error {
	actionName := s.getRuleActionName(rule, ruleMode, action)
	if actionName == nil {
		actionName = &defaultAction
	}
	ruleAction := s.getRuleAction(ctx, *actionName)
	if ruleAction == nil {
		log.Printf("could not find rule action of type %s\n", *actionName)
		return fmt.Errorf("could not find rule action of type %s", *actionName)
	}
	e := ruleAction.Run(ctx, msg, err)
	if e != nil {
		log.Printf("WARN: could not run post-rule action %s: %v\n", *actionName, e)
		return e
	}
	return nil
}

func (s *Serde) getRuleActionName(rule schemaregistry.Rule, ruleMode schemaregistry.RuleMode, actionName string) *string {
	if actionName == "" {
		return nil
	}
	mode, ok := schemaregistry.ParseMode(rule.Mode)
	if !ok {
		return nil
	}
	if (mode == schemaregistry.WriteRead || mode == schemaregistry.UpDown) && strings.Contains(actionName, ",") {
		parts := strings.Split(actionName, ",")
		switch ruleMode {
		case schemaregistry.Write, schemaregistry.Upgrade:
			return &parts[0]
		case schemaregistry.Read, schemaregistry.Downgrade:
			return &parts[1]
		default:
			return nil
		}
	}
	return &actionName
}

func (s *Serde) getRuleAction(_ RuleContext, actionName string) RuleAction {
	if actionName == "ERROR" {
		return ErrorAction{}
	} else if actionName == "NONE" {
		return NoneAction{}
	} else {
		return s.RuleRegistry.GetAction(actionName)
	}
}

// WriteBytes writes the serialized payload prepended by the MagicByteV0
// Deprecated: Use SchemaIdSerializer instead
func (s *BaseSerializer) WriteBytes(id int, msgBytes []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := buf.WriteByte(MagicByteV0)
	if err != nil {
		return nil, err
	}
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, uint32(id))
	_, err = buf.Write(idBytes)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(msgBytes)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GetSchema returns a schema for a payload
// Deprecated: Use GetWriterSchema instead
func (s *BaseDeserializer) GetSchema(topic string, payload []byte) (schemaregistry.SchemaInfo, error) {
	info := schemaregistry.SchemaInfo{}
	if payload[0] != MagicByteV0 {
		return info, fmt.Errorf("unknown magic byte")
	}
	id := binary.BigEndian.Uint32(payload[1:5])
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return info, err
	}
	return s.Client.GetBySubjectAndID(subject, int(id))
}

// GetWriterSchema returns a schema for the given headers and payload
func (s *BaseDeserializer) GetWriterSchema(topic string, headers []kafka.Header,
	payload []byte, schemaID *SchemaID) (schemaregistry.SchemaInfo, int, error) {
	info := schemaregistry.SchemaInfo{}
	bytesRead, err := s.SchemaIDDeserializer(topic, s.SerdeType, headers, payload, schemaID)
	if err != nil {
		return info, 0, err
	}
	var subject string
	if schemaID.ID > 0 {
		subject, err = s.SubjectNameStrategy(topic, s.SerdeType, info)
		if err != nil {
			return info, 0, err
		}
		info, err = s.Client.GetBySubjectAndID(subject, int(schemaID.ID))
		if err != nil {
			return info, 0, err
		}
		return info, bytesRead, nil
	} else if schemaID.GUID != uuid.Nil {
		info, err = s.Client.GetByGUID(schemaID.GUID.String())
		if err != nil {
			return info, 0, err
		}
		return info, bytesRead, nil
	} else {
		return info, 0, fmt.Errorf("unknown schema ID")
	}
}

// GetReaderSchema returns a schema for reading
func (s *BaseDeserializer) GetReaderSchema(subject string) (*schemaregistry.SchemaMetadata, error) {
	useLatestWithMetadata := s.Conf.UseLatestWithMetadata
	useLatest := s.Conf.UseLatestVersion
	if len(useLatestWithMetadata) != 0 {
		meta, err := s.Client.GetLatestWithMetadata(subject, useLatestWithMetadata, true)
		if err != nil {
			return nil, err
		}
		return &meta, nil
	}
	if useLatest {
		meta, err := s.Client.GetLatestSchemaMetadata(subject)
		if err != nil {
			return nil, err
		}
		return &meta, nil
	}
	return nil, nil
}

// ResolveReferences resolves schema references
func ResolveReferences(c schemaregistry.Client, schema schemaregistry.SchemaInfo, deps map[string]string) error {
	for _, ref := range schema.References {
		metadata, err := c.GetSchemaMetadataIncludeDeleted(ref.Subject, ref.Version, true)
		if err != nil {
			return err
		}
		info := metadata.SchemaInfo
		deps[ref.Name] = metadata.Schema
		err = ResolveReferences(c, info, deps)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the Serde
func (s *Serde) Close() error {
	return nil
}

// Configure configures the action
func (a ErrorAction) Configure(clientConfig *schemaregistry.Config, config map[string]string) error {
	return nil
}

// Type returns the type
func (a ErrorAction) Type() string {
	return "ERROR"
}

// Run runs the action
func (a ErrorAction) Run(ctx RuleContext, msg interface{}, err error) error {
	return fmt.Errorf("rule %s failed: %w", ctx.Rule.Name, err)
}

// Close closes the action
func (a ErrorAction) Close() error {
	return nil
}

// Configure configures the action
func (a NoneAction) Configure(clientConfig *schemaregistry.Config, config map[string]string) error {
	return nil
}

// Type returns the type
func (a NoneAction) Type() string {
	return "NONE"
}

// Run runs the action
func (a NoneAction) Run(ctx RuleContext, msg interface{}, err error) error {
	return nil
}

// Close closes the action
func (a NoneAction) Close() error {
	return nil
}
