/**
 * Copyright 2026 Confluent Inc.
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
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// validateMessage walks msg against descriptor, evaluating every inline validation rule
// declared in the confluent.Meta extension and collecting all failures. Read-only — the
// message is not modified.
//
// Two kinds of rules are evaluated:
//   - Message-level (confluent.message_meta rules) — `this` is the message.
//   - Field-level (confluent.field_meta rules) — `this` is the field value; for repeated
//     and map fields that is the whole collection. Honors the skip-on-null contract: an
//     unset field with explicit presence (proto3 optional, message fields, oneof members)
//     does not have its rules invoked.
//
// Failures are returned with their dotted-path location (e.g. addr.zip, items[3],
// labels["k"]). The walk continues after each failure unless failFast is set.
//
// Only message_meta and field_meta rules are evaluated; rules on files, enums and enum
// values are ignored, matching the JVM client.
func validateMessage(executor serde.ValidationRuleExecutor, descriptor protoreflect.MessageDescriptor,
	msg interface{}, failFast bool) ([]serde.ValidationRuleError, error) {
	var violations []serde.ValidationRuleError
	if executor == nil || descriptor == nil || msg == nil {
		return violations, nil
	}
	m, ok := msg.(proto.Message)
	if !ok {
		return violations, nil
	}
	err := validate(executor, descriptor, "", m, failFast, &violations)
	if err != nil {
		return nil, err
	}
	return violations, nil
}

// validate mirrors transform's dispatch shape, walking the descriptor's fields and
// descending into message-valued fields, map values and repeated elements.
func validate(executor serde.ValidationRuleExecutor, descriptor protoreflect.MessageDescriptor, path string,
	msg proto.Message, failFast bool, out *[]serde.ValidationRuleError) error {
	if descriptor == nil || msg == nil {
		return nil
	}
	// Message-level rules: this = the message.
	for _, rule := range getMessageValidationRules(descriptor) {
		if err := serde.EvaluateValidationRule(executor, rule, descriptor, msg, path, out); err != nil {
			return err
		}
		if failFast && len(*out) > 0 {
			return nil
		}
	}
	reflectMsg := msg.ProtoReflect()
	fields := reflectMsg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		// Use the schema-side field descriptor, which carries the Meta options.
		schemaFd := descriptor.Fields().ByName(fd.Name())
		if schemaFd == nil {
			continue
		}
		// Skip-on-null: a field with explicit presence that is unset does not invoke the
		// executor. Repeated and map fields have no presence and are never unset.
		if fd.HasPresence() && !reflectMsg.Has(fd) {
			continue
		}
		value := reflectMsg.Get(fd)
		childPath := string(fd.Name())
		if path != "" {
			childPath = path + "." + string(fd.Name())
		}
		for _, rule := range getFieldValidationRules(schemaFd) {
			if err := serde.EvaluateValidationRule(
				executor, rule, schemaFd, celFieldValue(fd, value), childPath, out); err != nil {
				return err
			}
			if failFast && len(*out) > 0 {
				return nil
			}
		}
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() != protoreflect.MessageKind {
				continue
			}
			var mapErr error
			value.Map().Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
				if failFast && len(*out) > 0 {
					return false
				}
				mapErr = validate(executor, schemaFd.MapValue().Message(),
					fmt.Sprintf("%s[%q]", childPath, k.String()), v.Message().Interface(), failFast, out)
				return mapErr == nil
			})
			if mapErr != nil {
				return mapErr
			}
		case fd.IsList():
			if fd.Kind() != protoreflect.MessageKind {
				continue
			}
			list := value.List()
			for j := 0; j < list.Len(); j++ {
				err := validate(executor, schemaFd.Message(), fmt.Sprintf("%s[%d]", childPath, j),
					list.Get(j).Message().Interface(), failFast, out)
				if err != nil {
					return err
				}
				if failFast && len(*out) > 0 {
					return nil
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			err := validate(executor, schemaFd.Message(), childPath, value.Message().Interface(), failFast, out)
			if err != nil {
				return err
			}
		}
		if failFast && len(*out) > 0 {
			return nil
		}
	}
	return nil
}

// celFieldValue converts a field value into the form a rule expects `this` to be in.
// protoreflect.Value.Interface() hands back reflection wrappers for messages, lists and
// maps, which an expression cannot index, size or read fields from, so those are unwrapped
// into a proto.Message and Go slices and maps of already-unwrapped values.
func celFieldValue(fd protoreflect.FieldDescriptor, value protoreflect.Value) interface{} {
	switch {
	case fd.IsMap():
		result := make(map[interface{}]interface{}, value.Map().Len())
		value.Map().Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			result[k.Interface()] = celElementValue(fd.MapValue(), v)
			return true
		})
		return result
	case fd.IsList():
		list := value.List()
		result := make([]interface{}, 0, list.Len())
		for i := 0; i < list.Len(); i++ {
			result = append(result, celElementValue(fd, list.Get(i)))
		}
		return result
	default:
		return celElementValue(fd, value)
	}
}

// celElementValue converts a single (non-collection) value of the given field's type.
func celElementValue(fd protoreflect.FieldDescriptor, value protoreflect.Value) interface{} {
	if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
		return value.Message().Interface()
	}
	return value.Interface()
}

func getMessageValidationRules(desc protoreflect.MessageDescriptor) []serde.ValidationRule {
	options := desc.Options()
	if proto.HasExtension(options, confluent.E_MessageMeta) {
		option := proto.GetExtension(options, confluent.E_MessageMeta)
		if meta, ok := option.(*confluent.Meta); ok {
			return toValidationRules(meta.Rules)
		}
	}
	return nil
}

func getFieldValidationRules(fd protoreflect.FieldDescriptor) []serde.ValidationRule {
	options := fd.Options()
	if proto.HasExtension(options, confluent.E_FieldMeta) {
		option := proto.GetExtension(options, confluent.E_FieldMeta)
		if meta, ok := option.(*confluent.Meta); ok {
			return toValidationRules(meta.Rules)
		}
	}
	return nil
}

func toValidationRules(rules []*confluent.Rule) []serde.ValidationRule {
	result := make([]serde.ValidationRule, 0, len(rules))
	for _, rule := range rules {
		if rule == nil {
			continue
		}
		result = append(result, serde.ValidationRule{
			Name: rule.GetName(),
			Doc:  rule.GetDoc(),
			Expr: rule.GetExpr(),
			SQL:  rule.GetSql(),
		})
	}
	return result
}
