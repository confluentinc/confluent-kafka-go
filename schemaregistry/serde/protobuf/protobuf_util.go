/**
 * Copyright 2024 Confluent Inc.
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
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func transform(ctx serde.RuleContext, descriptor protoreflect.Descriptor, msg interface{},
	fieldTransform serde.FieldTransform) (interface{}, error) {
	if msg == nil || descriptor == nil {
		return msg, nil
	}
	m, ok := msg.(proto.Message)
	if !ok {
		return msg, nil
	}
	desc, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return msg, nil
	}
	clone := proto.Clone(m)
	fields := clone.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		err := transformField(ctx, fd, desc, msg, clone, fieldTransform)
		if err != nil {
			return nil, err
		}
	}
	return clone, nil
}

func transformField(ctx serde.RuleContext, fd protoreflect.FieldDescriptor, desc protoreflect.MessageDescriptor,
	msg interface{}, clone proto.Message, fieldTransform serde.FieldTransform) error {
	// The schema-side descriptor is the one carrying the inline tags; only the runtime
	// field can read the value off the message. Resolve it by number, not by name:
	// protobuf identifies a field by its number, and renaming a field at the same number
	// is a compatible change, so with use.latest.version the registered schema's name for
	// a field can differ from the message's. A field the schema does not declare carries
	// no tags.
	schemaFd := desc.Fields().ByNumber(fd.Number())
	if schemaFd == nil {
		return nil
	}
	defer ctx.LeaveField()
	// The names come from the registered schema alongside the tags: rules and metadata
	// tags are written against it. The value is still read through the runtime field.
	ctx.EnterField(msg, string(schemaFd.FullName()), string(schemaFd.Name()), getType(fd),
		getInlineTags(schemaFd))
	// Skip-on-null, as in the validation walk: a field with explicit presence that is
	// unset has no value to transform, and writing one back would materialize it - turning
	// an absent message or unset optional scalar into a present one carrying a transformed
	// default. HasPresence covers oneof members too.
	if fd.HasPresence() && !clone.ProtoReflect().Has(fd) {
		return nil
	}
	newValue, err := transformFieldValue(ctx, fd, schemaFd, clone, fieldTransform)
	if err != nil {
		return err
	}
	if ctx.Rule.Kind == "CONDITION" {
		// A condition rule reports on the value rather than replacing it, and
		// transformLeaf has already turned a false result into an error.
		return nil
	}
	clone.ProtoReflect().Set(fd, newValue)
	return nil
}

// transformFieldValue transforms a single field's value, descending exactly the way the
// validation walk does: into a message-valued field with that field's own descriptor, into
// each element of a repeated field, and into each value of a message-valued map. A scalar
// is handed to the field transform.
func transformFieldValue(ctx serde.RuleContext, fd protoreflect.FieldDescriptor,
	schemaFd protoreflect.FieldDescriptor, clone proto.Message,
	fieldTransform serde.FieldTransform) (protoreflect.Value, error) {
	value := clone.ProtoReflect().Get(fd)
	switch {
	case fd.IsMap():
		if !isMessageKind(fd.MapValue()) || !isMessageKind(schemaFd.MapValue()) {
			// Scalar map values have no tags of their own to act on, which is also why
			// the validation walk does not descend into them.
			return value, nil
		}
		newMap := clone.ProtoReflect().NewField(fd).Map()
		var rangeErr error
		value.Map().Range(func(key protoreflect.MapKey, entry protoreflect.Value) bool {
			transformed, err := transformMessage(ctx, schemaFd.MapValue().Message(), entry,
				fieldTransform)
			if err != nil {
				rangeErr = err
				return false
			}
			newMap.Set(key, transformed)
			return true
		})
		if rangeErr != nil {
			return value, rangeErr
		}
		return protoreflect.ValueOfMap(newMap), nil
	case fd.IsList():
		list := value.List()
		newList := clone.ProtoReflect().NewField(fd).List()
		for i := 0; i < list.Len(); i++ {
			var newValue protoreflect.Value
			var err error
			if isMessageKind(fd) && isMessageKind(schemaFd) {
				newValue, err = transformMessage(ctx, schemaFd.Message(), list.Get(i), fieldTransform)
			} else {
				newValue, err = transformLeaf(ctx, list.Get(i), fieldTransform)
			}
			if err != nil {
				return value, err
			}
			newList.Append(newValue)
		}
		return protoreflect.ValueOfList(newList), nil
	case isMessageKind(fd) && isMessageKind(schemaFd):
		return transformMessage(ctx, schemaFd.Message(), value, fieldTransform)
	default:
		return transformLeaf(ctx, value, fieldTransform)
	}
}

// transformMessage descends into a nested message with the given descriptor.
func transformMessage(ctx serde.RuleContext, desc protoreflect.MessageDescriptor,
	value protoreflect.Value, fieldTransform serde.FieldTransform) (protoreflect.Value, error) {
	transformed, err := transform(ctx, desc, value.Message().Interface(), fieldTransform)
	if err != nil {
		return value, err
	}
	newMessage, ok := transformed.(proto.Message)
	if !ok {
		return value, nil
	}
	return protoreflect.ValueOfMessage(newMessage.ProtoReflect()), nil
}

// transformLeaf hands a scalar value to the field transform, when the rule's tags overlap
// the field's.
func transformLeaf(ctx serde.RuleContext, value protoreflect.Value,
	fieldTransform serde.FieldTransform) (protoreflect.Value, error) {
	fieldCtx := ctx.CurrentField()
	if fieldCtx == nil {
		return value, nil
	}
	ruleTags := ctx.Rule.Tags
	if len(ruleTags) != 0 && disjoint(ruleTags, fieldCtx.Tags) {
		return value, nil
	}
	newValue, err := fieldTransform.Transform(ctx, *fieldCtx, value.Interface())
	if err != nil {
		return value, err
	}
	if ctx.Rule.Kind == "CONDITION" {
		// The result is a verdict on the value, not a replacement for it, so the value is
		// returned unchanged - and a value of a different type is never written back.
		newBool, ok := newValue.(bool)
		if ok && !newBool {
			return value, serde.RuleConditionErr{
				Rule: ctx.Rule,
			}
		}
		return value, nil
	}
	return protoreflect.ValueOf(newValue), nil
}

func isMessageKind(fd protoreflect.FieldDescriptor) bool {
	return fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind
}

func getType(fd protoreflect.FieldDescriptor) serde.FieldType {
	if fd.IsMap() {
		return serde.TypeMap
	}
	switch fd.Kind() {
	case protoreflect.MessageKind:
		return serde.TypeRecord
	case protoreflect.EnumKind:
		return serde.TypeEnum
	case protoreflect.StringKind:
		return serde.TypeString
	case protoreflect.BytesKind:
		return serde.TypeBytes
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Uint32Kind,
		protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		return serde.TypeInt
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Uint64Kind,
		protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		return serde.TypeLong
	case protoreflect.FloatKind:
		return serde.TypeFloat
	case protoreflect.DoubleKind:
		return serde.TypeDouble
	case protoreflect.BoolKind:
		return serde.TypeBoolean
	default:
		return serde.TypeNull
	}
}

func getInlineTags(fd protoreflect.FieldDescriptor) []string {
	options := fd.Options()
	if proto.HasExtension(options, confluent.E_FieldMeta) {
		option := proto.GetExtension(options, confluent.E_FieldMeta)
		meta, ok := option.(*confluent.Meta)
		if ok {
			return meta.Tags
		}
	}
	return nil
}

func disjoint(slice1 []string, map1 map[string]bool) bool {
	for _, v := range slice1 {
		if map1[v] {
			return false
		}
	}
	return true
}
