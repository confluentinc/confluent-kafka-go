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
	"reflect"
)

func transform(ctx serde.RuleContext, descriptor protoreflect.Descriptor, msg interface{},
	fieldTransform serde.FieldTransform) (interface{}, error) {
	if msg == nil || descriptor == nil {
		return msg, nil
	}
	m, ok := msg.(proto.Message)
	if ok {
		desc := descriptor.(protoreflect.MessageDescriptor)
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
	fieldCtx := ctx.CurrentField()
	if fieldCtx != nil {
		desc := descriptor.(protoreflect.MessageDescriptor)
		fd := desc.Fields().ByName(protoreflect.Name(fieldCtx.Name))
		val := msg.(protoreflect.Value)

		// val.Interface() returns a pointer for list and map
		if reflect.ValueOf(val.Interface()).Kind() == reflect.Pointer {
			if fd.IsList() {
				v := val.List()
				var result []interface{}
				for i := 0; i < v.Len(); i++ {
					newmsg, err := transform(ctx, descriptor, v.Get(i), fieldTransform)
					if err != nil {
						return nil, err
					}
					result = append(result, newmsg)
				}
				return result, nil
			} else if fd.IsMap() {
				return msg, nil
			}
		}

		ruleTags := ctx.Rule.Tags
		if (len(ruleTags) == 0) || !disjoint(ruleTags, fieldCtx.Tags) {
			newVal, err := fieldTransform.Transform(ctx, *fieldCtx, val.Interface())
			if err != nil {
				return nil, err
			}
			return protoreflect.ValueOf(newVal), nil
		}
	}
	return msg, nil
}

func transformField(ctx serde.RuleContext, fd protoreflect.FieldDescriptor, desc protoreflect.MessageDescriptor,
	msg interface{}, clone proto.Message, fieldTransform serde.FieldTransform) error {
	name := fd.Name()
	fullName := fd.FullName()
	schemaFd := desc.Fields().ByName(name)
	defer ctx.LeaveField()
	ctx.EnterField(msg, string(fullName), string(name), getType(fd), getInlineTags(schemaFd))
	if fd.ContainingOneof() != nil && !clone.ProtoReflect().Has(fd) {
		// skip oneof fields that are not set
		return nil
	}
	value := clone.ProtoReflect().Get(fd)
	d := desc
	md, ok := desc.(protoreflect.MessageDescriptor)
	if ok {
		// Pass the schema-based descriptor which has the tags
		d = md
	}
	newValue, err := transform(ctx, d, value, fieldTransform)
	if err != nil {
		return err
	}
	if ctx.Rule.Kind == "CONDITION" {
		newProtoValue, ok := newValue.(protoreflect.Value)
		if ok {
			i := newProtoValue.Interface()
			newBool, ok := i.(bool)
			if ok && !newBool {
				return serde.RuleConditionErr{
					Rule: ctx.Rule,
				}
			}
		}
	} else {
		newProtoValue, ok := newValue.(protoreflect.Value)
		if ok {
			clone.ProtoReflect().Set(fd, newProtoValue)
		} else {
			if fd.IsList() {
				newValues := newValue.([]interface{})
				list := clone.ProtoReflect().NewField(fd).List()
				for i := 0; i < len(newValues); i++ {
					list.Append(newValues[i].(protoreflect.Value))
				}
				clone.ProtoReflect().Set(fd, protoreflect.ValueOfList(list))
			} else {
				clone.ProtoReflect().Set(fd, protoreflect.ValueOf(newValue))
			}
		}
	}
	return nil
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
