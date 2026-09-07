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
	"fmt"
	"math/big"
	"time"

	"github.com/cockroachdb/apd/v3"
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
			switch {
			case isMessageKind(fd) && isCelLeafMessage(fd.Message()):
				// A repeated decimal or timestamp is a list of single values, not a list of
				// records. This case has to precede the descend-into-message one below, which
				// is where a repeated value type used to go: the walk reached value/scale one
				// at a time, so the rule's result was never written and the field came back
				// unchanged with no error. The same #4538 reasoning as the scalar case at
				// isCelLeafMessage below, which a list never reached.
				newValue, err = transformValueTypeLeaf(ctx, fd, list.Get(i), fieldTransform,
					/*dropVerdict=*/ true)
			case isMessageKind(fd) && isMessageKind(schemaFd):
				newValue, err = transformMessage(ctx, schemaFd.Message(), list.Get(i), fieldTransform)
			default:
				// An element of a repeated field: evaluated, verdict dropped.
				newValue, err = transformLeaf(ctx, list.Get(i), fieldTransform,
					/*dropVerdict=*/ true)
			}
			if err != nil {
				return value, err
			}
			newList.Append(newValue)
		}
		return protoreflect.ValueOfList(newList), nil
	case isMessageKind(fd) && isCelLeafMessage(fd.Message()):
		// A decimal or a timestamp is a single value to a rule, not a record to descend
		// into. Without this the walk reached value/scale and seconds/nanos one at a time,
		// so a rule tagged for the field never fired and the message came back unchanged
		// with no error. Ported from the JVM client's #4538.
		return transformValueTypeLeaf(ctx, fd, value, fieldTransform, false)
	case isMessageKind(fd) && isMessageKind(schemaFd):
		return transformMessage(ctx, schemaFd.Message(), value, fieldTransform)
	default:
		return transformLeaf(ctx, value, fieldTransform, false)
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

// Message types a CEL rule works with as a single value rather than as a record.
//
// Avro carries the same concepts as logical types on a primitive, so the field is a leaf there
// and a CEL_FIELD rule reaches it. Variant is deliberately not included: it is a record in Avro
// too, so skipping it is the behaviour that matches, and a variant is reached with a
// message-level CEL rule instead.
const (
	celDecimalTypeName   = "confluent.type.Decimal"
	celTimestampTypeName = "google.protobuf.Timestamp"
)

func isCelLeafMessage(desc protoreflect.MessageDescriptor) bool {
	if desc == nil {
		return false
	}
	name := string(desc.FullName())
	return name == celDecimalTypeName || name == celTimestampTypeName
}

// transformValueTypeLeaf hands the whole decimal or timestamp message to the field transform
// and encodes whatever the rule returns back into it.
//
// dropVerdict is set for one element of a repeated field; see transformLeaf.
//
// This was two functions, and the copy drifted: it omitted the tag check below, so a condition
// evaluated against every repeated value-type field regardless of the rule's tags - a rule tagged
// for a repeated string was handed a Decimal and failed to compile. One function with a flag
// cannot drift that way.
func transformValueTypeLeaf(ctx serde.RuleContext, fd protoreflect.FieldDescriptor,
	value protoreflect.Value, fieldTransform serde.FieldTransform,
	dropVerdict bool) (protoreflect.Value, error) {
	fieldCtx := ctx.CurrentField()
	if fieldCtx == nil {
		return value, nil
	}
	ruleTags := ctx.Rule.Tags
	if len(ruleTags) != 0 && disjoint(ruleTags, fieldCtx.Tags) {
		return value, nil
	}
	// The concrete message, not the protoreflect wrapper: the CEL boundary switches on
	// *types.Decimal and *timestamppb.Timestamp.
	newValue, err := fieldTransform.Transform(ctx, *fieldCtx, value.Message().Interface())
	if err != nil {
		return value, err
	}
	if ctx.Rule.Kind == "CONDITION" {
		// A verdict on the value, not a replacement for it, so the value is returned
		// unchanged - and a false verdict is a violation, exactly as on the scalar path,
		// unless this is one element of a container.
		if newBool, ok := newValue.(bool); ok && !newBool && !dropVerdict {
			return value, serde.RuleConditionErr{Rule: ctx.Rule}
		}
		return value, nil
	}
	rebuilt, err := rebuildValueType(ctx, fd, value.Message(), newValue)
	if err != nil {
		return value, err
	}
	return rebuilt, nil
}

// rebuildValueType encodes what a CEL_FIELD rule returned back into the field's message.
//
// An identity rule hands back the message it was given; a computed rule hands back an
// *apd.Decimal or a time.Time. Anything else is a rule-authoring mistake and is named as one
// rather than written back as a default.
func rebuildValueType(ctx serde.RuleContext, fd protoreflect.FieldDescriptor,
	existing protoreflect.Message, value interface{}) (protoreflect.Value, error) {
	desc := fd.Message()
	if value == nil {
		return protoreflect.Value{}, valueTypeError(ctx, fd, "null", "a decimal or timestamp")
	}
	if pm, ok := value.(proto.Message); ok &&
		pm.ProtoReflect().Descriptor().FullName() == desc.FullName() {
		// Already the right message, which is what an identity rule produces.
		return protoreflect.ValueOfMessage(pm.ProtoReflect()), nil
	}

	// A new message of the *field's own* kind, taken from the value already there: a message
	// parsed dynamically from a registered schema must be written back as a dynamic message,
	// and a generated one as its generated type - the parent's reflection rejects the other.
	// This is what the JVM client gets from parent.newBuilderForField(fd).
	out := existing.New()
	setNamed := func(name string, v protoreflect.Value) {
		if f := desc.Fields().ByName(protoreflect.Name(name)); f != nil {
			out.Set(f, v)
		}
	}

	if string(desc.FullName()) == celDecimalTypeName {
		dec, ok := value.(*apd.Decimal)
		if !ok {
			return protoreflect.Value{}, valueTypeError(
				ctx, fd, fmt.Sprintf("%T", value), "a decimal")
		}
		encoded, err := decimalToProtoParts(dec)
		if err != nil {
			return protoreflect.Value{}, err
		}
		setNamed("value", protoreflect.ValueOfBytes(encoded.unscaled))
		setNamed("precision", protoreflect.ValueOfUint32(encoded.precision))
		setNamed("scale", protoreflect.ValueOfInt32(encoded.scale))
		return protoreflect.ValueOfMessage(out), nil
	}

	ts, ok := value.(time.Time)
	if !ok {
		return protoreflect.Value{}, valueTypeError(
			ctx, fd, fmt.Sprintf("%T", value), "a timestamp")
	}
	setNamed("seconds", protoreflect.ValueOfInt64(ts.Unix()))
	setNamed("nanos", protoreflect.ValueOfInt32(int32(ts.Nanosecond())))
	return protoreflect.ValueOfMessage(out), nil
}

type decimalParts struct {
	unscaled  []byte
	precision uint32
	scale     int32
}

// decimalToProtoParts encodes a decimal the way confluent.type.Decimal stores one. Precision
// and scale describe the value itself rather than a declared column width, matching the JVM
// client's DecimalUtils.fromBigDecimal and the reverse of how a decimal is read back.
func decimalToProtoParts(d *apd.Decimal) (decimalParts, error) {
	if d.Form != apd.Finite {
		return decimalParts{}, fmt.Errorf("cannot write a non-finite decimal to %s",
			celDecimalTypeName)
	}
	scale := -d.Exponent
	unscaled := new(big.Int).Set(d.Coeff.MathBigInt())
	if d.Negative {
		unscaled.Neg(unscaled)
	}
	if scale < 0 {
		// A positive exponent (1E+3) has no scale of its own; normalise it into the digits
		// rather than writing a negative scale.
		unscaled.Mul(unscaled, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-scale)), nil))
		scale = 0
	}
	return decimalParts{
		unscaled:  signedBytes(unscaled),
		precision: uint32(len(d.Coeff.MathBigInt().String())),
		scale:     scale,
	}, nil
}

func valueTypeError(ctx serde.RuleContext, fd protoreflect.FieldDescriptor,
	actual string, expected string) error {
	return fmt.Errorf("rule %s returned %s for field '%s', which is a %s; expected %s",
		ctx.Rule.Name, actual, fd.FullName(), fd.Message().FullName(), expected)
}

// transformLeaf hands a scalar value to the field transform, when the rule's tags overlap
// the field's.
// transformLeaf hands a scalar to the field transform.
//
// dropVerdict is set by the caller walking a repeated field's elements. The reference maps a
// repeated field to a new list of the per-element results and then tests
// `Boolean.FALSE.equals(newValue)` on it - which a list never is - so a CEL_FIELD condition does
// not apply to a container field, whatever its element type. Raising per element instead made a
// repeated scalar diverge.
//
// One parameter rather than a near-copy: the two would have to agree about tag matching and about
// which results are written back.
func transformLeaf(ctx serde.RuleContext, value protoreflect.Value,
	fieldTransform serde.FieldTransform, dropVerdict bool) (protoreflect.Value, error) {
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
		if ok && !newBool && !dropVerdict {
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
		// Report the same primitive type the Avro counterpart does, so that CEL_FIELD applies
		// to the field and a rule written against one format ports to the other.
		if isCelLeafMessage(fd.Message()) {
			if string(fd.Message().FullName()) == celDecimalTypeName {
				return serde.TypeBytes
			}
			return serde.TypeLong
		}
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
