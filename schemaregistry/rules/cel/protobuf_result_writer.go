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
	"math"
	"time"

	"github.com/cockroachdb/apd/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	typepb "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/type"
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

// asMapEntries keeps the keys in their native types, for setMapField to narrow through the
// field's own key descriptor. asStringMap above stays for the *top-level* result map, whose
// keys are field names and so really are strings.
func asMapEntries(result interface{}) (map[interface{}]interface{}, bool) {
	switch m := result.(type) {
	case map[interface{}]interface{}:
		return m, true
	case map[string]interface{}:
		out := make(map[interface{}]interface{}, len(m))
		for k, v := range m {
			out[k] = v
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

// fillMessage applies a result map to out, one entry per declared field.
//
// Two entries can name the same slot, and applying both leaves the outcome to the order they
// are visited in - which for a Go map is not even stable between runs. JsonFormat refuses both
// shapes, and the two have *opposite* null handling, which is the part worth stating:
//
//   - The same field twice. findField accepts a field's declared name and its JSON name, so
//     `amount_map` and `amountMap` are one field. mergeField tests builder.hasField before its
//     null early-return, so a null after a value is refused ("Field p.M.total_amount has
//     already been set.") while a null after a null is not.
//   - Two members of one oneof. Setting a member clears its siblings, so applying both kept
//     whichever came last. mergeOneofField refuses this ("Cannot set field p.M.b because
//     another field p.M.a belonging to the same oneof has already been set"), but only after
//     returning early for a null, so a null does *not* count - which agrees with this writer's
//     own rule that a null clears rather than sets.
//
// Measured against protobuf-java. A proto3 optional field sits in a synthetic oneof of exactly
// one member, which ContainingOneof reports as synthetic and is skipped, so it can never
// collide with a sibling. The names in each message are sorted because ranging over a map
// visits them in an arbitrary order, and a diagnostic that varies run to run is worse than
// useless.
//
// One deliberate strengthening, as in C++ and Rust. The JVM's hasField test makes a null count
// only when it follows a value, an order a randomised Go map cannot reproduce - measured, one
// pair was refused 264 times in 2000 runs. So a duplicate is refused whichever entry is null.
func fillMessage(out protoreflect.Message, values map[string]interface{}) error {
	desc := out.Descriptor()
	// field number -> the result key that set it; oneof -> the member that filled it.
	setBy := make(map[protoreflect.FieldNumber]string, len(values))
	oneofBy := make(map[protoreflect.FullName]string)
	for key, value := range values {
		fd := findField(desc, key)
		if fd == nil {
			// A key the schema does not declare has nowhere to go. Dropping it matches the
			// JVM client, whose JSON parse ignores unknown fields.
			continue
		}
		if first, ok := setBy[fd.Number()]; ok {
			a, b := sortedPair(first, key)
			return fmt.Errorf("result names field %s twice, as %s and %s", fd.FullName(), a, b)
		}
		// Before the null branch, so the verdict does not depend on iteration order.
		setBy[fd.Number()] = key
		if isNull(value) {
			// An explicit null clears the field, which is how a rule preserves an absent
			// value across a transform that echoes it. It sets nothing, so it does not count
			// towards a oneof collision.
			out.Clear(fd)
			continue
		}
		if oneof := fd.ContainingOneof(); oneof != nil && !oneof.IsSynthetic() {
			if sibling, ok := oneofBy[oneof.FullName()]; ok && sibling != string(fd.Name()) {
				a, b := sortedPair(sibling, string(fd.Name()))
				return fmt.Errorf("result sets more than one member of oneof %s: %s and %s",
					oneof.FullName(), a, b)
			}
			oneofBy[oneof.FullName()] = string(fd.Name())
		}
		if err := setField(out, fd, value); err != nil {
			return fmt.Errorf("field %s: %w", fd.Name(), err)
		}
	}
	return nil
}

// sortedPair returns two names in a stable order, so an error does not vary with the map
// iteration order that produced it.
func sortedPair(x, y string) (string, string) {
	if x <= y {
		return x, y
	}
	return y, x
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
	// A shape mismatch is an error, not a no-op. The message is rebuilt field by field, so
	// returning nil here left the map *empty* - the rule's data silently discarded. The
	// reference rejects the same mismatch, because its message-level write-back goes through a
	// protobuf JSON parse: measured against protobuf-java 4.34.0's JsonFormat,
	// `{"m": "notamap"}` is "Expect a map object but found: \"notamap\"".
	entries, ok := asMapEntries(value)
	if !ok {
		return fmt.Errorf("cannot write %T to map field %s", value, fd.FullName())
	}
	mp := out.Mutable(fd).Map()
	keyFd := fd.MapKey()
	valueFd := fd.MapValue()
	for k, v := range entries {
		if isNull(v) {
			// Dropping the entry reported success while deleting it. A protobuf map value
			// cannot be null, and the reference's write-back parse says exactly that -
			// measured, `{"amount_map": {"a": null}}` is "Map value cannot be null."
			return fmt.Errorf("cannot write a null value to map field %s", fd.FullName())
		}
		// The key is narrowed through its own descriptor, as every other client in the family
		// does. Only string keys were handled, and `ValueOfString` was used unconditionally -
		// so echoing a `map<int32, V>` or `map<bool, V>` through a message-level transform
		// wrote back an *empty* map, because asStringMap rejected the shape and the function
		// then reported success. protobuf permits bool and every integral type as a map key.
		kv, err := scalarValue(keyFd, k)
		if err != nil {
			return fmt.Errorf("map field %s: %w", fd.FullName(), err)
		}
		key := kv.MapKey()
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
	// The same reasoning as setMapField, which was fixed first and left this one behind: the
	// message is rebuilt field by field, so returning nil left the list *empty* and silently
	// discarded the rule's data. protobuf-java's JsonFormat rejects the same mismatch on the
	// reference's write-back path - measured, `{"r": "notalist"}` is "Expected an array for r
	// but found \"notalist\"".
	items, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("cannot write %T to repeated field %s", value, fd.FullName())
	}
	list := out.Mutable(fd).List()
	for i, item := range items {
		if isNull(item) {
			// Skipping changed the list's length and still reported success, so `[1, null, 2]`
			// came back with two elements. protobuf has no null to store, and the reference
			// says "Repeated field elements cannot be null in field: X".
			return fmt.Errorf("cannot write null to element %d of repeated field %s",
				i, fd.FullName())
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
// mergeMessage copies src into out, refusing a type mismatch instead of panicking.
//
// Two distinct problems, both of which `proto.Merge` alone gets wrong:
//
// A *different message type* used to reach Merge unchecked. Merge panics on a descriptor
// mismatch, and cel-go's recover turns that into an opaque "internal error" naming neither
// field nor type - so a rule assigning one message-typed field to another said nothing useful.
// The JVM reports a named rule error there, and the C++ and Rust clients check the descriptor
// too.
//
// And Merge requires descriptor *identity*, not an equal name: two descriptors for the same
// message type are not interchangeable. That is reachable here, because the serde parses the
// schema text at runtime while the values written back (decimalToProto, timestamppb.New, the
// Variant above) are built from the *generated* descriptors - so a same-named pair from two
// registries would panic exactly as a mismatched type does. The wire format is the portable
// bridge between them.
func mergeMessage(out protoreflect.Message, src proto.Message, fullName string) error {
	sd := src.ProtoReflect().Descriptor()
	if string(sd.FullName()) != fullName {
		return fmt.Errorf("cannot write %s to %s", sd.FullName(), fullName)
	}
	if sd == out.Descriptor() {
		proto.Merge(out.Interface(), src)
		return nil
	}
	b, err := proto.Marshal(src)
	if err != nil {
		return fmt.Errorf("cannot write %s to %s: %w", sd.FullName(), fullName, err)
	}
	return (proto.UnmarshalOptions{Merge: true}).Unmarshal(b, out.Interface())
}

func setMessageValue(out protoreflect.Message, fd protoreflect.FieldDescriptor, value interface{}) error {
	fullName := string(fd.Message().FullName())

	// A message echoed straight through: copy it - but only if it is the same message type.
	if pm, ok := value.(proto.Message); ok {
		return mergeMessage(out, pm, fullName)
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
		return mergeMessage(out, d, fullName)
	case time.Time:
		if fullName != timestampTypeName {
			return fmt.Errorf("cannot write a timestamp to %s", fullName)
		}
		// Not range-checked here on purpose. google.protobuf.Timestamp is defined for
		// 0001-9999 and timestamppb.New does not validate, but cel-go is the gate: both
		// `timestamp(...)` and timestamp arithmetic refuse to leave the range ("timestamp
		// overflow"), so no rule can hand this an out-of-range time.Time. Measured, and the
		// same holds for cel-cpp and cel-rust; the reference gets there differently, its
		// write-back going through a protobuf JSON printer that rejects an invalid Timestamp.
		// A check here would be untestable through any rule.
		return mergeMessage(out, timestamppb.New(v), fullName)
	case variant.Variant:
		if fullName != variantTypeName {
			return fmt.Errorf("cannot write a variant to %s", fullName)
		}
		return mergeMessage(out, &typepb.Variant{
			Metadata: v.MetadataBytes(),
			// Slice from this node's offset, not from 0. Trailing sibling bytes are
			// kept so the encoding matches the Java reference, which writes
			// ByteBuffer position..limit.
			Value: v.StandaloneValueBytes(),
		}, fullName)
	}

	// A nested message the rule rebuilt field by field.
	if nested, ok := asStringMap(value); ok {
		return fillMessage(out, nested)
	}
	return fmt.Errorf("cannot write %T to %s", value, fullName)
}

// scalarValue narrows a cel-go native value to what the field's kind accepts. cel-go widens
// every integer to int64 and every float to float64, so a narrower field needs converting
// back rather than rejecting - but only where the conversion is exact. The JVM's write-back
// parses the result map with protobuf's own JSON parser, so that parser's rejections are the
// contract; measured against protobuf-java 4.35.1:
//
//	int32 <- 1.9        -> "Not an int32 value: 1.9"     (int64(1.9) silently gave 1)
//	int32 <- 2147483648 -> "Not an int32 value"          (int32(i) silently wrapped)
//	int32 <- 2.0        -> 2                             (an exact conversion is fine)
//	float <- 1.0e40     -> "Out of range float value"    (float32(f) silently gave +Inf)
//
// A value of the wrong kind entirely is an error, not a coercion. The JVM stringifies a
// number into a string field and base64-decodes a string into a bytes field, both artifacts
// of crossing a JSON transport that this writer does not cross; following them would turn a
// rule-authoring mistake into silently wrong data.
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
		// Bytes only. A string was written as its own raw bytes, so a rule returning base64
		// stored the text "YWI=" rather than the two bytes it encodes.
		b, ok := value.([]byte)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected bytes, got %T", value)
		}
		return protoreflect.ValueOfBytes(b), nil
	case protoreflect.FloatKind:
		f, err := toFloat(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		narrowed, err := narrowToFloat32(f)
		return protoreflect.ValueOfFloat32(narrowed), err
	case protoreflect.DoubleKind:
		f, err := toFloat(value)
		return protoreflect.ValueOfFloat64(f), err
	case protoreflect.EnumKind:
		i, err := toBoundedInt(value, math.MinInt32, math.MaxInt32)
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(i)), err
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		i, err := toBoundedInt(value, math.MinInt32, math.MaxInt32)
		return protoreflect.ValueOfInt32(int32(i)), err
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		i, err := toInt(value)
		return protoreflect.ValueOfInt64(i), err
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		u, err := toBoundedUint(value, math.MaxUint32)
		return protoreflect.ValueOfUint32(uint32(u)), err
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		u, err := toUint(value)
		return protoreflect.ValueOfUint64(u), err
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
		// A uint64 above math.MaxInt64 has no int64 form, and the conversion would wrap it
		// to a negative.
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("value %d is out of range for a signed field", v)
		}
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case float64:
		return floatToInt(v)
	}
	return 0, fmt.Errorf("expected an integer, got %T", value)
}

// toUint is kept separate from toInt because routing an unsigned value through int64 would
// reject everything above math.MaxInt64 - half the protobuf uint64 domain, which an identity
// transform has to round-trip.
func toUint(value interface{}) (uint64, error) {
	switch v := value.(type) {
	case uint64:
		return v, nil
	case uint32:
		return uint64(v), nil
	}
	i, err := toInt(value)
	if err != nil {
		return 0, err
	}
	if i < 0 {
		return 0, fmt.Errorf("value %d is out of range for an unsigned field", i)
	}
	return uint64(i), nil
}

func toBoundedInt(value interface{}, min int64, max int64) (int64, error) {
	i, err := toInt(value)
	if err != nil {
		return 0, err
	}
	if i < min || i > max {
		return 0, fmt.Errorf("value %d is out of range for the field", i)
	}
	return i, nil
}

func toBoundedUint(value interface{}, max uint64) (uint64, error) {
	u, err := toUint(value)
	if err != nil {
		return 0, err
	}
	if u > max {
		return 0, fmt.Errorf("value %d is out of range for the field", u)
	}
	return u, nil
}

// floatToInt is a float as an integer, only when it is exactly integral and inside the int64
// range. A fractional value is a rule-authoring mistake rather than something to truncate.
//
// The upper bound is exclusive of 2^63: float64(math.MaxInt64) rounds *up* to 2^63, so
// comparing against it would admit 2^63 itself, which the conversion then saturates to
// math.MaxInt64 - silently changing the value. -float64(math.MinInt64) is exactly 2^63.
// NaN fails the integral test (NaN != NaN) and an infinity fails the range test.
func floatToInt(f float64) (int64, error) {
	truncated := math.Trunc(f)
	if truncated != f {
		return 0, fmt.Errorf("cannot write non-integral %v to an integer field", f)
	}
	if truncated < math.MinInt64 || truncated >= -float64(math.MinInt64) {
		return 0, fmt.Errorf("value %v is out of range for an integer field", f)
	}
	return int64(truncated), nil
}

// narrowToFloat32 narrows a float64 the way JsonFormat.parseFloat does: a finite value outside
// the float range is an error rather than an infinity, with the same 1e-6 slack that method
// allows. NaN and the infinities pass through - it accepts those explicitly.
func narrowToFloat32(d float64) (float32, error) {
	const epsilon = 1e-6
	limit := float64(math.MaxFloat32) * (1 + epsilon)
	if !math.IsInf(d, 0) && !math.IsNaN(d) && (d > limit || d < -limit) {
		return 0, fmt.Errorf("out of range float value: %v", d)
	}
	return float32(d), nil
}

func toFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	}
	return 0, fmt.Errorf("expected a float, got %T", value)
}
