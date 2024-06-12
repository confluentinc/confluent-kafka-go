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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/cache"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	protoV1 "github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/desc/protoprint"
	"google.golang.org/genproto/googleapis/type/calendarperiod"
	"google.golang.org/genproto/googleapis/type/color"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/datetime"
	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/genproto/googleapis/type/fraction"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/genproto/googleapis/type/money"
	"google.golang.org/genproto/googleapis/type/month"
	"google.golang.org/genproto/googleapis/type/postaladdress"
	"google.golang.org/genproto/googleapis/type/quaternion"
	"google.golang.org/genproto/googleapis/type/timeofday"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/apipb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/sourcecontextpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/typepb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Serializer represents a Protobuf serializer
type Serializer struct {
	serde.BaseSerializer
	*Serde
	Conf                  *SerializerConfig
	descToSchemaCache     cache.Cache
	descToSchemaCacheLock sync.RWMutex
}

// Deserializer represents a Protobuf deserializer
type Deserializer struct {
	serde.BaseDeserializer
	*Serde
	ProtoRegistry *protoregistry.Types
}

// Serde represents a Protobuf serde
type Serde struct {
	schemaToDescCache     cache.Cache
	schemaToDescCacheLock sync.RWMutex
}

var _ serde.Serializer = new(Serializer)
var _ serde.Deserializer = new(Deserializer)

var builtInDeps = make(map[string]string)

func init() {
	builtins := map[string]protoreflect.FileDescriptor{
		"confluent/meta.proto":                 confluent.File_confluent_meta_proto,
		"confluent/type/decimal.proto":         types.File_confluent_types_decimal_proto,
		"google/type/calendar_period.proto":    calendarperiod.File_google_type_calendar_period_proto,
		"google/type/color.proto":              color.File_google_type_color_proto,
		"google/type/date.proto":               date.File_google_type_date_proto,
		"google/type/datetime.proto":           datetime.File_google_type_datetime_proto,
		"google/type/dayofweek.proto":          dayofweek.File_google_type_dayofweek_proto,
		"google/type/expr.proto":               expr.File_google_type_expr_proto,
		"google/type/fraction.proto":           fraction.File_google_type_fraction_proto,
		"google/type/latlng.proto":             latlng.File_google_type_latlng_proto,
		"google/type/money.proto":              money.File_google_type_money_proto,
		"google/type/month.proto":              month.File_google_type_month_proto,
		"google/type/postal_address.proto":     postaladdress.File_google_type_postal_address_proto,
		"google/type/quaternion.proto":         quaternion.File_google_type_quaternion_proto,
		"google/type/timeofday.proto":          timeofday.File_google_type_timeofday_proto,
		"google/protobuf/any.proto":            anypb.File_google_protobuf_any_proto,
		"google/protobuf/api.proto":            apipb.File_google_protobuf_api_proto,
		"google/protobuf/descriptor.proto":     descriptorpb.File_google_protobuf_descriptor_proto,
		"google/protobuf/duration.proto":       durationpb.File_google_protobuf_duration_proto,
		"google/protobuf/empty.proto":          emptypb.File_google_protobuf_empty_proto,
		"google/protobuf/field_mask.proto":     fieldmaskpb.File_google_protobuf_field_mask_proto,
		"google/protobuf/source_context.proto": sourcecontextpb.File_google_protobuf_source_context_proto,
		"google/protobuf/struct.proto":         structpb.File_google_protobuf_struct_proto,
		"google/protobuf/timestamp.proto":      timestamppb.File_google_protobuf_timestamp_proto,
		"google/protobuf/type.proto":           typepb.File_google_protobuf_type_proto,
		"google/protobuf/wrappers.proto":       wrapperspb.File_google_protobuf_wrappers_proto,
	}
	var fds []*descriptorpb.FileDescriptorProto
	for _, value := range builtins {
		fd := protodesc.ToFileDescriptorProto(value)
		fds = append(fds, fd)
	}
	fdMap, err := desc.CreateFileDescriptors(fds)
	if err != nil {
		log.Fatalf("Could not create fds")
	}
	printer := protoprint.Printer{OmitComments: protoprint.CommentsAll}
	for key, value := range fdMap {
		var writer strings.Builder
		err = printer.PrintProtoFile(value, &writer)
		if err != nil {
			log.Fatalf("Could not print %s", key)
		}
		builtInDeps[key] = writer.String()
	}
}

// NewSerializer creates a Protobuf serializer for Protobuf-generated objects
func NewSerializer(client schemaregistry.Client, serdeType serde.Type, conf *SerializerConfig) (*Serializer, error) {
	descToSchemaCache, err := cache.NewLRUCache(1000)
	if err != nil {
		return nil, err
	}
	schemaToDescCache, err := cache.NewLRUCache(1000)
	if err != nil {
		return nil, err
	}
	ps := &Serde{
		schemaToDescCache: schemaToDescCache,
	}
	s := &Serializer{
		Serde:             ps,
		descToSchemaCache: descToSchemaCache,
	}
	err = s.ConfigureSerializer(client, serdeType, &conf.SerializerConfig)
	s.Conf = conf
	fieldTransformer := func(ctx serde.RuleContext, fieldTransform serde.FieldTransform, msg interface{}) (interface{}, error) {
		return s.FieldTransform(s.Client, ctx, fieldTransform, msg)
	}
	s.FieldTransformer = fieldTransformer

	if err != nil {
		return nil, err
	}
	for _, rule := range serde.GetRuleExecutors() {
		err = rule.Configure(client.Config(), conf.RuleConfig)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

// ConfigureDeserializer configures the Protobuf deserializer
func (s *Deserializer) ConfigureDeserializer(client schemaregistry.Client, serdeType serde.Type,
	conf *serde.DeserializerConfig) error {
	if client == nil {
		return fmt.Errorf("schema registry client missing")
	}
	if err := s.BaseDeserializer.ConfigureDeserializer(client, serdeType, conf); err != nil {
		return err
	}
	s.MessageFactory = s.protoMessageFactory
	s.ProtoRegistry = new(protoregistry.Types)
	return nil
}

// Serialize implements serialization of Protobuf data
func (s *Serializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	var protoMsg proto.Message
	switch t := msg.(type) {
	case proto.Message:
		protoMsg = t
	default:
		return nil, fmt.Errorf("serialization target must be a protobuf message. Got '%v'", t)
	}
	info, err := s.getSchemaInfo(protoMsg)
	if err != nil {
		return nil, err
	}
	id, err := s.GetID(topic, protoMsg, info)
	if err != nil {
		return nil, err
	}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, *info)
	if err != nil {
		return nil, err
	}
	msg, err = s.ExecuteRules(subject, topic, schemaregistry.Write, nil, info, protoMsg)
	if err != nil {
		return nil, err
	}
	switch t := msg.(type) {
	case proto.Message:
		protoMsg = t
	default:
		return nil, fmt.Errorf("serialization target must be a protobuf message. Got '%v'", t)
	}
	msgIndexBytes := toMessageIndexBytes(protoMsg.ProtoReflect().Descriptor())
	msgBytes, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, append(msgIndexBytes, msgBytes...))
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (s *Serializer) getSchemaInfo(protoMsg proto.Message) (*schemaregistry.SchemaInfo, error) {
	messageDesc, err := desc.LoadMessageDescriptorForMessage(protoV1.MessageV1(protoMsg))
	if err != nil {
		return nil, err
	}
	fileDesc := messageDesc.GetFile()
	if s.Conf.CacheSchemas {
		s.descToSchemaCacheLock.RLock()
		value, ok := s.descToSchemaCache.Get(fileDesc.GetName())
		s.descToSchemaCacheLock.RUnlock()
		if ok {
			return value.(*schemaregistry.SchemaInfo), nil
		}
	}
	deps, err := s.toProtobufSchema(fileDesc)
	if err != nil {
		return nil, err
	}
	autoRegister := s.Conf.AutoRegisterSchemas
	normalize := s.Conf.NormalizeSchemas
	metadata, err := s.resolveDependencies(fileDesc, deps, "", autoRegister, normalize)
	if err != nil {
		return nil, err
	}
	info := &metadata.SchemaInfo
	if s.Conf.CacheSchemas {
		s.descToSchemaCacheLock.Lock()
		s.descToSchemaCache.Put(fileDesc.GetName(), info)
		s.descToSchemaCacheLock.Unlock()
	}
	return info, nil
}

func (s *Serializer) toProtobufSchema(fileDesc *desc.FileDescriptor) (map[string]string, error) {
	deps := make(map[string]string)
	err := s.toDependencies(fileDesc, deps)
	if err != nil {
		return nil, err
	}
	return deps, nil
}

func (s *Serializer) toDependencies(fileDesc *desc.FileDescriptor, deps map[string]string) error {
	printer := protoprint.Printer{OmitComments: protoprint.CommentsAll}
	var writer strings.Builder
	err := printer.PrintProtoFile(fileDesc, &writer)
	if err != nil {
		return err
	}
	deps[fileDesc.GetName()] = writer.String()
	for _, d := range fileDesc.GetDependencies() {
		if ignoreFile(d.GetName()) {
			continue
		}
		err = s.toDependencies(d, deps)
		if err != nil {
			return err
		}
	}
	for _, d := range fileDesc.GetPublicDependencies() {
		if ignoreFile(d.GetName()) {
			continue
		}
		err = s.toDependencies(d, deps)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Serializer) resolveDependencies(fileDesc *desc.FileDescriptor, deps map[string]string, subject string, autoRegister bool, normalize bool) (schemaregistry.SchemaMetadata, error) {
	refs := make([]schemaregistry.Reference, 0, len(fileDesc.GetDependencies())+len(fileDesc.GetPublicDependencies()))
	for _, d := range fileDesc.GetDependencies() {
		if ignoreFile(d.GetName()) {
			continue
		}
		ref, err := s.resolveDependencies(d, deps, d.GetName(), autoRegister, normalize)
		if err != nil {
			return schemaregistry.SchemaMetadata{}, err
		}
		refs = append(refs, schemaregistry.Reference{
			Name:    d.GetName(),
			Subject: ref.Subject,
			Version: ref.Version,
		})
	}
	for _, d := range fileDesc.GetPublicDependencies() {
		if ignoreFile(d.GetName()) {
			continue
		}
		ref, err := s.resolveDependencies(d, deps, d.GetName(), autoRegister, normalize)
		if err != nil {
			return schemaregistry.SchemaMetadata{}, err
		}
		refs = append(refs, schemaregistry.Reference{
			Name:    d.GetName(),
			Subject: ref.Subject,
			Version: ref.Version,
		})
	}
	info := schemaregistry.SchemaInfo{
		Schema:     deps[fileDesc.GetName()],
		SchemaType: "PROTOBUF",
		References: refs,
	}
	var id = -1
	var err error
	var version = 0
	if subject != "" {
		if autoRegister {
			id, err = s.Client.Register(subject, info, normalize)
			if err != nil {
				return schemaregistry.SchemaMetadata{}, err
			}
		} else {
			id, err = s.Client.GetID(subject, info, normalize)
			if err != nil {
				return schemaregistry.SchemaMetadata{}, err
			}
		}
		version, err = s.Client.GetVersion(subject, info, normalize)
		if err != nil {
			return schemaregistry.SchemaMetadata{}, err
		}
	}
	metadata := schemaregistry.SchemaMetadata{
		SchemaInfo: info,
		ID:         id,
		Subject:    subject,
		Version:    version,
	}
	return metadata, nil
}

func toMessageIndexBytes(descriptor protoreflect.Descriptor) []byte {
	if descriptor.Index() == 0 {
		switch descriptor.Parent().(type) {
		case protoreflect.FileDescriptor:
			// This is an optimization for the first message in the schema
			return []byte{0}
		}
	}
	msgIndexes := toMessageIndexes(descriptor, 0)
	buf := make([]byte, (1+len(msgIndexes))*binary.MaxVarintLen64)
	length := binary.PutVarint(buf, int64(len(msgIndexes)))

	for _, element := range msgIndexes {
		length += binary.PutVarint(buf[length:], int64(element))
	}
	return buf[0:length]
}

// Adapted from ideasculptor, see https://github.com/riferrei/srclient/issues/17
func toMessageIndexes(descriptor protoreflect.Descriptor, count int) []int {
	index := descriptor.Index()
	switch v := descriptor.Parent().(type) {
	case protoreflect.FileDescriptor:
		// parent is FileDescriptor, we reached the top of the stack, so we are
		// done. Allocate an array large enough to hold count+1 entries and
		// populate first value with index
		msgIndexes := make([]int, count+1)
		msgIndexes[0] = index
		return msgIndexes[0:1]
	default:
		// parent is another MessageDescriptor.  We were nested so get that
		// descriptor's indexes and append the index of this one
		msgIndexes := toMessageIndexes(v, count+1)
		return append(msgIndexes, index)
	}
}

func ignoreFile(name string) bool {
	return strings.HasPrefix(name, "confluent/") ||
		strings.HasPrefix(name, "google/protobuf/") ||
		strings.HasPrefix(name, "google/type/")
}

// FieldTransform transforms the field value using the rule
func (s *Serde) FieldTransform(client schemaregistry.Client, ctx serde.RuleContext, fieldTransform serde.FieldTransform, msg interface{}) (interface{}, error) {
	fd, err := s.toFileDesc(client, *ctx.Target)
	if err != nil {
		return nil, err
	}
	m := msg.(proto.Message)
	md := fd.FindMessage(string(m.ProtoReflect().Descriptor().FullName()))
	return transform(ctx, md.Unwrap(), msg, fieldTransform)
}

func (s *Serde) toFileDesc(client schemaregistry.Client, info schemaregistry.SchemaInfo) (*desc.FileDescriptor, error) {
	s.schemaToDescCacheLock.RLock()
	value, ok := s.schemaToDescCache.Get(info.Schema)
	s.schemaToDescCacheLock.RUnlock()
	if ok {
		return value.(*desc.FileDescriptor), nil
	}
	fd, err := parseFileDesc(client, info)
	if err != nil {
		return nil, err
	}
	s.schemaToDescCacheLock.Lock()
	s.schemaToDescCache.Put(info.Schema, fd)
	s.schemaToDescCacheLock.Unlock()
	return fd, nil
}

func parseFileDesc(client schemaregistry.Client, info schemaregistry.SchemaInfo) (*desc.FileDescriptor, error) {
	deps := make(map[string]string)
	err := serde.ResolveReferences(client, info, deps)
	if err != nil {
		return nil, err
	}
	parser := protoparse.Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			var schema string
			if filename == "." {
				schema = info.Schema
			} else {
				schema = deps[filename]
			}
			if schema == "" {
				schema = builtInDeps[filename]
			}
			return io.NopCloser(strings.NewReader(schema)), nil
		},
	}

	fileDescriptors, err := parser.ParseFiles(".")
	if err != nil {
		return nil, err
	}

	if len(fileDescriptors) != 1 {
		return nil, fmt.Errorf("could not resolve schema")
	}
	fd := fileDescriptors[0]
	return fd, nil
}

// NewDeserializer creates a Protobuf deserializer for Protobuf-generated objects
func NewDeserializer(client schemaregistry.Client, serdeType serde.Type, conf *DeserializerConfig) (*Deserializer, error) {
	schemaToDescCache, err := cache.NewLRUCache(1000)
	if err != nil {
		return nil, err
	}
	ps := &Serde{
		schemaToDescCache: schemaToDescCache,
	}
	s := &Deserializer{
		Serde: ps,
	}
	err = s.ConfigureDeserializer(client, serdeType, &conf.DeserializerConfig)
	if err != nil {
		return nil, err
	}
	fieldTransformer := func(ctx serde.RuleContext, fieldTransform serde.FieldTransform, msg interface{}) (interface{}, error) {
		return s.FieldTransform(s.Client, ctx, fieldTransform, msg)
	}
	s.FieldTransformer = fieldTransformer
	for _, rule := range serde.GetRuleExecutors() {
		err = rule.Configure(client.Config(), conf.RuleConfig)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Deserialize implements deserialization of Protobuf data
func (s *Deserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	return s.deserialize(topic, payload, nil)
}

// DeserializeInto implements deserialization of Protobuf data to the given object
func (s *Deserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
	_, err := s.deserialize(topic, payload, msg)
	return err
}

func (s *Deserializer) deserialize(topic string, payload []byte, result interface{}) (interface{}, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return nil, err
	}
	fd, err := s.toFileDesc(s.Client, info)
	if err != nil {
		return nil, err
	}
	bytesRead, msgIndexes, err := readMessageIndexes(payload[5:])
	if err != nil {
		return nil, err
	}
	messageDesc, err := toMessageDesc(fd, msgIndexes)
	if err != nil {
		return nil, err
	}
	name := messageDesc.GetFullyQualifiedName()
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return nil, err
	}
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
	var msg interface{}
	var protoMsg proto.Message
	if len(migrations) > 0 {
		dynamicMsg := dynamicpb.NewMessage(messageDesc.UnwrapMessage())
		err = proto.Unmarshal(payload[5+bytesRead:], dynamicMsg)
		if err != nil {
			return nil, err
		}
		jsonBytes, err := protojson.Marshal(dynamicMsg)
		if err != nil {
			return nil, err
		}
		var jsonMsg interface{}
		err = json.Unmarshal(jsonBytes, &jsonMsg)
		if err != nil {
			return nil, err
		}
		jsonMsg, err = s.ExecuteMigrations(migrations, subject, topic, jsonMsg)
		if err != nil {
			return nil, err
		}
		readerFd, err := s.toFileDesc(s.Client, readerMeta.SchemaInfo)
		if err != nil {
			return nil, err
		}
		name, err = toMessageName(readerFd, name)
		if err != nil {
			return nil, err
		}
		jsonBytes, err = json.Marshal(jsonMsg)
		if err != nil {
			return nil, err
		}
		if result == nil {
			msg, err = s.MessageFactory(subject, name)
			if err != nil {
				return nil, err
			}
		} else {
			msg = result
		}
		switch t := msg.(type) {
		case proto.Message:
			protoMsg = t
		default:
			return nil, fmt.Errorf("deserialization target must be a protobuf message. Got '%v'", t)
		}
		err = protojson.Unmarshal(jsonBytes, protoMsg)
		if err != nil {
			return nil, err
		}
	} else {
		if result == nil {
			msg, err = s.MessageFactory(subject, name)
			if err != nil {
				return nil, err
			}
		} else {
			msg = result
		}
		switch t := msg.(type) {
		case proto.Message:
			protoMsg = t
		default:
			return nil, fmt.Errorf("deserialization target must be a protobuf message. Got '%v'", t)
		}
		err = proto.Unmarshal(payload[5+bytesRead:], protoMsg)
		if err != nil {
			return nil, err
		}
	}
	var target *schemaregistry.SchemaInfo
	if readerMeta != nil {
		target = &readerMeta.SchemaInfo
	} else {
		target = &info
	}
	msg, err = s.ExecuteRules(subject, topic, schemaregistry.Read, nil, target, protoMsg)
	if err != nil {
		return nil, err
	}
	switch t := msg.(type) {
	case proto.Message:
		protoMsg = t
	default:
		return nil, fmt.Errorf("deserialization target must be a protobuf message. Got '%v'", t)
	}
	return protoMsg, err
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

func toMessageDesc(descriptor desc.Descriptor, msgIndexes []int) (*desc.MessageDescriptor, error) {
	index := msgIndexes[0]

	switch v := descriptor.(type) {
	case *desc.FileDescriptor:
		if len(msgIndexes) == 1 {
			return v.GetMessageTypes()[index], nil
		}
		return toMessageDesc(v.GetMessageTypes()[index], msgIndexes[1:])
	case *desc.MessageDescriptor:
		if len(msgIndexes) == 1 {
			return v.GetNestedMessageTypes()[index], nil
		}
		return toMessageDesc(v.GetNestedMessageTypes()[index], msgIndexes[1:])
	default:
		return nil, fmt.Errorf("unexpected type")
	}
}

// toMessageName returns the given message name if it exists, otherwise the first message name
func toMessageName(fd *desc.FileDescriptor, name string) (string, error) {
	var first string
	for i, md := range fd.GetMessageTypes() {
		if md.GetFullyQualifiedName() == name {
			return name, nil
		}
		if i == 0 {
			first = md.GetFullyQualifiedName()
		}
	}
	return first, nil
}

func (s *Deserializer) protoMessageFactory(subject string, name string) (interface{}, error) {
	mt, err := s.ProtoRegistry.FindMessageByName(protoreflect.FullName(name))
	if mt == nil {
		err = fmt.Errorf("unable to find MessageType %s", name)
	}
	if err != nil {
		return nil, err
	}
	msg := mt.New()
	return msg.Interface(), nil
}
