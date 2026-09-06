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

package cel

import (
	"fmt"
	"time"

	"github.com/cockroachdb/apd/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	prototypes "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/variant"
)

// decimalTypeName and variantTypeName are declared alongside their own bindings, in
// decimal.go and variant.go.
const timestampTypeName = "google.protobuf.Timestamp"

// writeBackProtobuf rebuilds a protobuf message from the map a message-level CEL transform
// returned, or returns result unchanged when it is not that shape.
//
// A CEL rule returning a map is returning the whole new message: the transform has replace
// semantics, not merge. Three consequences a rule author needs to know, and every client has
// to match:
//
//   - a field the rule does not name is dropped, so a rule naming only the field it changes
//     discards the rest;
//   - a null in the map clears its field;
//   - echoing a field that was absent materialises it, because reading it produced a value.
//     Preserve absence with has(x) ? x : null.
//
// Without this the executor handed back a raw map[interface{}]interface{}, which the protobuf
// serializer cannot write.
//
// Mechanism note: the JVM client rebuilds by rendering the result to JSON and parsing it back.
// This builds the message directly through protoreflect instead - Go has no fromJson on the
// schema, and a JSON round trip would base64 every bytes field and format every timestamp
// only to parse them straight back. The behaviours the JVM client gets free from the JSON
// mapping (null clearing a field, a key matching the declared or the JSON name) are
// reproduced explicitly below.
func writeBackProtobuf(result interface{}, msg interface{}) (interface{}, error) {
	values, ok := asStringMap(result)
	if !ok {
		return result, nil
	}
	src, ok := msg.(proto.Message)
	if !ok {
		return result, nil
	}
	// A fresh message of the input's own type, so the caller gets back what it passed in.
	out := src.ProtoReflect().New()
	if err := fillMessage(out, values); err != nil {
		return nil, err
	}
	return out.Interface(), nil
}

// asStringMap normalises the two map shapes cel-go's native conversion produces.
func asStringMap(result interface{}) (map[string]interface{}, bool) {
	switch m := result.(type) {
	case map[string]interface{}:
		return m, true
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(m))
		for k, v := range m {
			key, ok := k.(string)
			if !ok {
				return nil, false
			}
			out[key] = v
		}
		return out, true
	default:
		return nil, false
	}
}

// isNull covers both shapes a CEL null takes once converted to a native value: an untyped
// nil, and the structpb.NullValue that cel-go's own conversion produces.
func isNull(value interface{}) bool {
	if value == nil {
		return true
	}
	_, ok := value.(structpb.NullValue)
	return ok
}

func fillMessage(out protoreflect.Message, values map[string]interface{}) error {
	desc := out.Descriptor()
	for key, value := range values {
		fd := findField(desc, key)
		if fd == nil {
			// A key the schema does not declare has nowhere to go. Dropping it matches the
			// JVM client, whose JSON parse ignores unknown fields.
			continue
		}
		if isNull(value) {
			// An explicit null clears the field, which is how a rule preserves an absent
			// value across a transform that echoes it.
			out.Clear(fd)
			continue
		}
		if err := setField(out, fd, value); err != nil {
			return fmt.Errorf("field %s: %w", fd.Name(), err)
		}
	}
	return nil
}

// findField resolves a result key by declared name, then by JSON name: a rule may
// legitimately return either, so matching only the declared name would silently skip a field
// like total_amount.
func findField(desc protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	if fd := desc.Fields().ByName(protoreflect.Name(name)); fd != nil {
		return fd
	}
	return desc.Fields().ByJSONName(name)
}

func setField(out protoreflect.Message, fd protoreflect.FieldDescriptor, value interface{}) error {
	switch {
	case fd.IsMap():
		return setMapField(out, fd, value)
	case fd.IsList():
		return setListField(out, fd, value)
	case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
		m := out.NewField(fd).Message()
		if err := setMessageValue(m, fd, value); err != nil {
			return err
		}
		out.Set(fd, protoreflect.ValueOfMessage(m))
		return nil
	default:
		v, err := scalarValue(fd, value)
		if err != nil {
			return err
		}
		out.Set(fd, v)
		return nil
	}
}

func setMapField(out protoreflect.Message, fd protoreflect.FieldDescriptor, value interface{}) error {
	entries, ok := asStringMap(value)
	if !ok {
		return nil
	}
	mp := out.Mutable(fd).Map()
	valueFd := fd.MapValue()
	for k, v := range entries {
		if isNull(v) {
			continue
		}
		key := protoreflect.ValueOfString(k).MapKey()
		if valueFd.Kind() == protoreflect.MessageKind {
			m := mp.NewValue().Message()
			if err := setMessageValue(m, valueFd, v); err != nil {
				return err
			}
			mp.Set(key, protoreflect.ValueOfMessage(m))
			continue
		}
		sv, err := scalarValue(valueFd, v)
		if err != nil {
			return err
		}
		mp.Set(key, sv)
	}
	return nil
}

func setListField(out protoreflect.Message, fd protoreflect.FieldDescriptor, value interface{}) error {
	items, ok := value.([]interface{})
	if !ok {
		return nil
	}
	list := out.Mutable(fd).List()
	for _, item := range items {
		if isNull(item) {
			continue
		}
		if fd.Kind() == protoreflect.MessageKind {
			m := list.NewElement().Message()
			if err := setMessageValue(m, fd, item); err != nil {
				return err
			}
			list.Append(protoreflect.ValueOfMessage(m))
			continue
		}
		sv, err := scalarValue(fd, item)
		if err != nil {
			return err
		}
		list.Append(sv)
	}
	return nil
}

// setMessageValue writes one message-valued field, inverting how the CEL binding read it.
//
// The three value types do not arrive as maps of their own fields once a rule has touched
// one: this client binds a decimal as *apd.Decimal and a timestamp as time.Time on the way
// in, so that is what comes back out whether the rule computed a new value or merely echoed
// the field. A variant comes back as the proto message when echoed and as variant.Variant
// when computed.
func setMessageValue(out protoreflect.Message, fd protoreflect.FieldDescriptor, value interface{}) error {
	fullName := string(fd.Message().FullName())

	// A message echoed straight through: copy it.
	if pm, ok := value.(proto.Message); ok {
		proto.Merge(out.Interface(), pm)
		return nil
	}

	switch v := value.(type) {
	case *apd.Decimal:
		if fullName != decimalTypeName {
			return fmt.Errorf("cannot write a decimal to %s", fullName)
		}
		d, err := decimalToProto(v)
		if err != nil {
			return err
		}
		proto.Merge(out.Interface(), d)
		return nil
	case time.Time:
		if fullName != timestampTypeName {
			return fmt.Errorf("cannot write a timestamp to %s", fullName)
		}
		proto.Merge(out.Interface(), timestamppb.New(v))
		return nil
	case variant.Variant:
		if fullName != variantTypeName {
			return fmt.Errorf("cannot write a variant to %s", fullName)
		}
		proto.Merge(out.Interface(), &prototypes.Variant{
			Metadata: v.MetadataBytes(),
			Value:    v.ValueBytes(),
		})
		return nil
	}

	// A nested message the rule rebuilt field by field.
	if nested, ok := asStringMap(value); ok {
		return fillMessage(out, nested)
	}
	return fmt.Errorf("cannot write %T to %s", value, fullName)
}

// scalarValue narrows a cel-go native value to what the field's kind accepts. cel-go widens
// every integer to int64 and every float to float64, so a narrower field needs converting
// back rather than rejecting.
func scalarValue(fd protoreflect.FieldDescriptor, value interface{}) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		b, ok := value.(bool)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected a bool, got %T", value)
		}
		return protoreflect.ValueOfBool(b), nil
	case protoreflect.StringKind:
		s, ok := value.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected a string, got %T", value)
		}
		return protoreflect.ValueOfString(s), nil
	case protoreflect.BytesKind:
		switch b := value.(type) {
		case []byte:
			return protoreflect.ValueOfBytes(b), nil
		case string:
			return protoreflect.ValueOfBytes([]byte(b)), nil
		}
		return protoreflect.Value{}, fmt.Errorf("expected bytes, got %T", value)
	case protoreflect.FloatKind:
		f, err := toFloat(value)
		return protoreflect.ValueOfFloat32(float32(f)), err
	case protoreflect.DoubleKind:
		f, err := toFloat(value)
		return protoreflect.ValueOfFloat64(f), err
	case protoreflect.EnumKind:
		i, err := toInt(value)
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(i)), err
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		i, err := toInt(value)
		return protoreflect.ValueOfInt32(int32(i)), err
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		i, err := toInt(value)
		return protoreflect.ValueOfInt64(i), err
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		i, err := toInt(value)
		return protoreflect.ValueOfUint32(uint32(i)), err
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		i, err := toInt(value)
		return protoreflect.ValueOfUint64(uint64(i)), err
	}
	return protoreflect.Value{}, fmt.Errorf("unsupported field kind %s", fd.Kind())
}

func toInt(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	}
	return 0, fmt.Errorf("expected an integer, got %T", value)
}

func toFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	}
	return 0, fmt.Errorf("expected a float, got %T", value)
}
