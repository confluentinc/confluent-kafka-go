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
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
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
	// The walk is driven by the caller's message throughout: it decides which fields exist,
	// which are absent, and what the values are. A rule that binds `this` to a message needs
	// one more thing - a view of that message in the schema's terms, since a rule's CEL
	// environment is built from the schema and `this.renamed` cannot read a field the
	// caller's class calls something else. Protobuf pairs fields by number on the wire, so
	// re-reading the message through the registered descriptor produces exactly that view.
	//
	// Whether that is needed is decided once per descriptor pair (see needsSchemaView) rather
	// than per record. A generated type describing the same fields as the registered schema
	// skips it entirely, even though the two descriptors are distinct. A type that has fallen
	// behind the schema does not: under use.latest.version the schema may declare a field the
	// type has never heard of, and a rule that binds `this` can read the schema's default for
	// it, so those producers re-read every record. That cost is the price of evaluating rules
	// in the schema's terms, not an accident.
	var schemaMsg proto.Message
	if needsSchemaView(descriptor, m.ProtoReflect().Descriptor()) {
		bytes, err := proto.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("could not read message %s through the registered schema: %w",
				descriptor.FullName(), err)
		}
		dynMsg := dynamicpb.NewMessage(descriptor)
		if err := proto.Unmarshal(bytes, dynMsg); err != nil {
			return nil, fmt.Errorf("could not read message %s through the registered schema: %w",
				descriptor.FullName(), err)
		}
		schemaMsg = dynMsg
	}
	err := validate(executor, descriptor, "", m, schemaMsg, failFast, &violations)
	if err != nil {
		return nil, err
	}
	return violations, nil
}

// descriptorPair keys the memo below: the registered schema's descriptor paired with the
// runtime descriptor of a message handed to validateMessage.
type descriptorPair struct {
	schema  protoreflect.MessageDescriptor
	runtime protoreflect.MessageDescriptor
}

// schemaViewNeeded memoizes needsSchemaView. Both descriptors are stable for the lifetime of
// a serializer, so this is one lookup per record rather than a tree comparison. The answer is
// no only for a type that describes the same fields as the registered schema; a type that has
// fallen behind it re-reads every record.
var schemaViewNeeded sync.Map

// needsSchemaView reports whether a message whose runtime descriptor is runtimeDesc has to be
// re-read through descriptor before rules can bind `this` to it - true when the two disagree
// about any field a rule could observe: its name, its kind, or whether it is a list or a map,
// at any depth.
//
// Presence deliberately does not count. Whether an unset field is absent is decided by the
// producer's field on the producer's message, which the walk reads directly, so a schema that
// only moved a field into or out of a oneof needs no re-read.
//
// A field the schema declares and the caller's type does not does count, which means a type
// running behind the registered schema - the use.latest.version case - re-reads every record.
// Only an exact match skips the re-read. Narrowing that to the rules that could actually
// observe the added field is possible but not simple: a rule binding `this` at any ancestor
// can traverse into the field, and a field-level rule on a message-valued field binds `this`
// to a type that need not declare rules of its own, so a per-descriptor test for
// message-level rules would be wrong in both directions.
func needsSchemaView(descriptor protoreflect.MessageDescriptor,
	runtimeDesc protoreflect.MessageDescriptor) bool {
	if runtimeDesc == descriptor {
		return false
	}
	key := descriptorPair{schema: descriptor, runtime: runtimeDesc}
	if cached, ok := schemaViewNeeded.Load(key); ok {
		return cached.(bool)
	}
	needed := !presentsSameValues(descriptor, runtimeDesc, map[string]bool{})
	schemaViewNeeded.Store(key, needed)
	return needed
}

// presentsSameValues reports whether the two descriptors present every field they share -
// paired by number, which is how protobuf identifies a field - under the same name, kind and
// cardinality, recursively through message-valued fields.
//
// A field the registered schema declares and the caller's does not counts as a difference:
// adding a field is a compatible change, and a message-level rule may reference the added
// field expecting the schema's default for it, which only a message read through the schema
// can supply. Fields only the caller declares are ignored - no rule can name them, and the
// walk skips them.
//
// visited holds the descriptor pairs already compared, so a self-referential message type
// terminates.
func presentsSameValues(descriptor protoreflect.MessageDescriptor,
	runtimeDesc protoreflect.MessageDescriptor, visited map[string]bool) bool {
	pair := string(descriptor.FullName()) + "\x00" + string(runtimeDesc.FullName())
	if visited[pair] {
		// Already compared on another path, or cycling back to it. Either way this pair
		// contributes no new disagreement.
		return true
	}
	visited[pair] = true
	schemaFields := descriptor.Fields()
	for i := 0; i < schemaFields.Len(); i++ {
		if runtimeDesc.Fields().ByNumber(schemaFields.Get(i).Number()) == nil {
			return false
		}
	}
	fields := runtimeDesc.Fields()
	for i := 0; i < fields.Len(); i++ {
		runtimeFd := fields.Get(i)
		schemaFd := descriptor.Fields().ByNumber(runtimeFd.Number())
		if schemaFd == nil {
			continue
		}
		if schemaFd.Name() != runtimeFd.Name() || schemaFd.Kind() != runtimeFd.Kind() ||
			schemaFd.IsList() != runtimeFd.IsList() || schemaFd.IsMap() != runtimeFd.IsMap() {
			return false
		}
		if runtimeFd.Kind() == protoreflect.MessageKind && schemaFd.Kind() == protoreflect.MessageKind {
			if !presentsSameValues(schemaFd.Message(), runtimeFd.Message(), visited) {
				return false
			}
		}
	}
	return true
}

// validate mirrors transform's dispatch shape, walking the message's fields and descending
// into message-valued fields, map values and repeated elements.
//
// The walk is driven by msg, the caller's message: it decides which fields exist, which are
// absent, and what the values are. Each field is paired to descriptor by number, and the
// schema's field supplies the rules and the name used in the reported path. schemaMsg is the
// same message read through descriptor, or nil when the two descriptors present it
// identically; where it exists, it is what rules see.
func validate(executor serde.ValidationRuleExecutor, descriptor protoreflect.MessageDescriptor, path string,
	msg proto.Message, schemaMsg proto.Message, failFast bool, out *[]serde.ValidationRuleError) error {
	if descriptor == nil || msg == nil {
		return nil
	}
	// Message-level rules: this = the message, read as the schema names it.
	thisMsg := msg
	if schemaMsg != nil {
		thisMsg = schemaMsg
	}
	for _, rule := range getMessageValidationRules(descriptor) {
		if err := serde.EvaluateValidationRule(executor, rule, descriptor, thisMsg, path, out); err != nil {
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
		// Use the schema-side field descriptor, which carries the Meta options. Resolve it
		// by number, not by name: protobuf identifies a field by its number, and renaming a
		// field at the same number is a compatible change, so with use.latest.version the
		// registered schema's name for a field can differ from the message's.
		schemaFd := descriptor.Fields().ByNumber(fd.Number())
		if schemaFd == nil {
			continue
		}
		// Skip-on-null: a field with explicit presence that is unset does not invoke the
		// executor. Repeated and map fields have no presence and are never unset.
		//
		// Both halves are read from the caller's message: whether an unset field counts as
		// absent is decided by the type that wrote it, not by the registered schema, and the
		// two can disagree - moving a field into or out of a oneof is a compatible change.
		if fd.HasPresence() && !reflectMsg.Has(fd) {
			continue
		}
		value := reflectMsg.Get(fd)
		// Where a schema view exists, values come from it: the two descriptors can disagree
		// about representation as well as naming. bytes and string are interchangeable at the
		// same number - a compatible change - and a rule authored as `this == 'hello'` cannot
		// match a []byte.
		schemaValue := value
		if schemaMsg != nil {
			schemaValue = schemaMsg.ProtoReflect().Get(schemaFd)
		}
		// Paths and names come from the registered schema, which is what a rule refers to.
		childPath := string(schemaFd.Name())
		if path != "" {
			childPath = path + "." + string(schemaFd.Name())
		}
		for _, rule := range getFieldValidationRules(schemaFd) {
			if err := serde.EvaluateValidationRule(
				executor, rule, schemaFd, celFieldValue(schemaFd, schemaValue), childPath, out); err != nil {
				return err
			}
			if failFast && len(*out) > 0 {
				return nil
			}
		}
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() != protoreflect.MessageKind ||
				schemaFd.MapValue().Kind() != protoreflect.MessageKind {
				continue
			}
			var mapErr error
			value.Map().Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
				if failFast && len(*out) > 0 {
					return false
				}
				// Map values pair by key rather than position.
				var schemaEntry proto.Message
				if schemaMsg != nil {
					if entry := schemaValue.Map().Get(k); entry.IsValid() {
						schemaEntry = entry.Message().Interface()
					}
				}
				mapErr = validate(executor, schemaFd.MapValue().Message(),
					fmt.Sprintf("%s[%q]", childPath, k.String()), v.Message().Interface(), schemaEntry,
					failFast, out)
				return mapErr == nil
			})
			if mapErr != nil {
				return mapErr
			}
		case fd.IsList():
			if fd.Kind() != protoreflect.MessageKind || schemaFd.Kind() != protoreflect.MessageKind {
				continue
			}
			list := value.List()
			for j := 0; j < list.Len(); j++ {
				// Both lists came from the same bytes, so they line up; the guard is for safety.
				var schemaElement proto.Message
				if schemaMsg != nil && j < schemaValue.List().Len() {
					schemaElement = schemaValue.List().Get(j).Message().Interface()
				}
				err := validate(executor, schemaFd.Message(), fmt.Sprintf("%s[%d]", childPath, j),
					list.Get(j).Message().Interface(), schemaElement, failFast, out)
				if err != nil {
					return err
				}
				if failFast && len(*out) > 0 {
					return nil
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			if schemaFd.Kind() != protoreflect.MessageKind {
				continue
			}
			var schemaNested proto.Message
			if schemaMsg != nil {
				schemaNested = schemaValue.Message().Interface()
			}
			err := validate(executor, schemaFd.Message(), childPath, value.Message().Interface(),
				schemaNested, failFast, out)
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
