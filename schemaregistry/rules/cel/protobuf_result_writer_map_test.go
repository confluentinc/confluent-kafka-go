package cel

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"cel.dev/cel-go/common/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	typepb "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/type"
)

// mapKeyDesc builds `message M { map<K,string> m = 1; }` at runtime, so a non-string map key
// can be exercised without regenerating any checked-in .pb.go.
func mapKeyDesc(t *testing.T, keyType descriptorpb.FieldDescriptorProto_Type) protoreflect.MessageDescriptor {
	t.Helper()
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	rep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	entry := &descriptorpb.DescriptorProto{
		Name: proto.String("MEntry"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("key"), Number: proto.Int32(1), Type: &keyType, Label: &opt},
			{Name: proto.String("value"), Number: proto.Int32(2), Type: &str, Label: &opt},
		},
		Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
	}
	msgType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("mapkey.proto"),
		Package: proto.String("mk"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:       proto.String("M"),
			NestedType: []*descriptorpb.DescriptorProto{entry},
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("m"), Number: proto.Int32(1),
				Type: &msgType, TypeName: proto.String(".mk.M.MEntry"), Label: &rep,
			}},
		}},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	return fd.Messages().Get(0)
}

// A protobuf map may be keyed by bool or any integral type, not only by string. Only string
// keys were handled - and `protoreflect.ValueOfString` was used unconditionally - so echoing a
// `map<int32, V>` or `map<bool, V>` through a message-level transform wrote back an *empty*
// map: the shape was rejected and the function then reported success. Every other client in
// the family narrows the key through its own descriptor.
func TestMapKeysOfEveryPermittedType(t *testing.T) {
	cases := []struct {
		name    string
		keyType descriptorpb.FieldDescriptorProto_Type
		entries map[interface{}]interface{}
		want    map[string]string // stringified key -> value, for comparison
	}{
		{"string", descriptorpb.FieldDescriptorProto_TYPE_STRING,
			map[interface{}]interface{}{"a": "x"}, map[string]string{"a": "x"}},
		{"int32", descriptorpb.FieldDescriptorProto_TYPE_INT32,
			map[interface{}]interface{}{int64(7): "x"}, map[string]string{"7": "x"}},
		{"int64", descriptorpb.FieldDescriptorProto_TYPE_INT64,
			map[interface{}]interface{}{int64(9): "x"}, map[string]string{"9": "x"}},
		{"uint32", descriptorpb.FieldDescriptorProto_TYPE_UINT32,
			map[interface{}]interface{}{uint64(5): "x"}, map[string]string{"5": "x"}},
		{"bool", descriptorpb.FieldDescriptorProto_TYPE_BOOL,
			map[interface{}]interface{}{true: "x"}, map[string]string{"true": "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			desc := mapKeyDesc(t, tc.keyType)
			out := dynamicpb.NewMessage(desc)
			fd := desc.Fields().ByName("m")
			if err := setMapField(out, fd, tc.entries); err != nil {
				t.Fatalf("setMapField: %v", err)
			}
			got := map[string]string{}
			out.Get(fd).Map().Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
				got[k.String()] = v.String()
				return true
			})
			if len(got) != len(tc.want) {
				t.Fatalf("wrote %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("key %q: got %q, want %q (all: %v)", k, got[k], v, got)
				}
			}
		})
	}
}

// A wrong-typed key is a rule error, not an empty map.
func TestAMapKeyOfTheWrongTypeIsReported(t *testing.T) {
	desc := mapKeyDesc(t, descriptorpb.FieldDescriptorProto_TYPE_INT32)
	out := dynamicpb.NewMessage(desc)
	fd := desc.Fields().ByName("m")
	err := setMapField(out, fd, map[interface{}]interface{}{"nota number": "x"})
	if err == nil {
		t.Fatal("expected an error for a string key on a map<int32, string>")
	}
	if !strings.Contains(err.Error(), "map field") {
		t.Errorf("error should name the field, got %v", err)
	}
}

// A non-map value for a map field is a rule error too. Returning nil left the field empty,
// silently discarding the rule's data; the reference rejects the same mismatch through its
// protobuf JSON write-back ("Expect a map object but found: ...").
func TestAWrongShapeForAMapFieldIsReported(t *testing.T) {
	desc := mapKeyDesc(t, descriptorpb.FieldDescriptorProto_TYPE_STRING)
	fd := desc.Fields().ByName("m")
	for _, v := range []interface{}{"notamap", int64(7), []interface{}{1, 2}} {
		out := dynamicpb.NewMessage(desc)
		if err := setMapField(out, fd, v); err == nil {
			t.Errorf("expected an error for %T on a map field", v)
		}
	}
}

// proto.Merge panics on a descriptor mismatch, and cel-go's recover turns that into an opaque
// "internal error" naming neither field nor type. A rule assigning one message-typed field to
// another must get a named rule error instead - as it does on the JVM, and in the C++ and Rust
// clients.
func TestAMessageOfTheWrongTypeIsReported(t *testing.T) {
	// The field is a confluent.type.Decimal; hand it a google.protobuf.Timestamp.
	msg := (&typepb.Decimal{}).ProtoReflect()
	descMsg := msg.Descriptor()
	_ = descMsg
	valueTypes := (&typepb.Decimal{}).ProtoReflect().Descriptor()
	_ = valueTypes

	// Build `message Holder { .confluent.type.Decimal amount = 1; }` via the generated
	// descriptor's own file, so fd.Message() really is confluent.type.Decimal.
	out := dynamicpb.NewMessage((&typepb.Decimal{}).ProtoReflect().Descriptor())
	_ = out

	// setMessageValue takes the destination message and the field descriptor; use the Decimal
	// message's own `value` field's parent as the destination type by writing into a fresh
	// Decimal through a Decimal-typed field on a holder built at runtime.
	holderDesc := decimalHolderDesc(t)
	holder := dynamicpb.NewMessage(holderDesc)
	fd := holderDesc.Fields().ByName("amount")
	nested := holder.Mutable(fd).Message()

	err := setMessageValue(nested, fd, timestamppb.New(timeNow()))
	if err == nil {
		t.Fatal("expected an error writing a Timestamp to a Decimal field")
	}
	if !strings.Contains(err.Error(), "confluent.type.Decimal") {
		t.Errorf("error should name the destination type, got %v", err)
	}

	// The must-fail twin: the right message type still copies.
	fresh := dynamicpb.NewMessage(holderDesc)
	target := fresh.Mutable(fd).Message()
	if err := setMessageValue(target, fd, &typepb.Decimal{Scale: 2, Precision: 4}); err != nil {
		t.Fatalf("echoing a Decimal should work: %v", err)
	}
}

// decimalHolderDesc builds `message Holder { .confluent.type.Decimal amount = 1; }` at runtime,
// reusing the generated confluent.type.Decimal file so the field's message type is the real one.
func decimalHolderDesc(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	decFile := protodesc.ToFileDescriptorProto(
		(&typepb.Decimal{}).ProtoReflect().Descriptor().ParentFile())
	msgType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	holder := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("holder.proto"),
		Package:    proto.String("hold"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{decFile.GetName()},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Holder"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("amount"), Number: proto.Int32(1),
				Type: &msgType, TypeName: proto.String(".confluent.type.Decimal"), Label: &opt,
			}},
		}},
	}
	files := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{decFile, holder}}
	reg, err := protodesc.NewFiles(files)
	if err != nil {
		t.Fatalf("build files: %v", err)
	}
	fd, err := reg.FindFileByPath("holder.proto")
	if err != nil {
		t.Fatalf("find holder: %v", err)
	}
	return fd.Messages().Get(0)
}

func timeNow() time.Time { return time.Unix(1700000000, 0).UTC() }

// A wrong shape for a repeated field is a rule error, not an empty list. setMapField was fixed
// first and left this one behind: the message is rebuilt field by field, so returning nil
// silently discarded the rule's data. protobuf-java's JsonFormat rejects the same mismatch on
// the reference's write-back path - measured, `{"r": "notalist"}` is "Expected an array for r
// but found \"notalist\"".
func TestAWrongShapeForARepeatedFieldIsReported(t *testing.T) {
	desc := repeatedStringDesc(t)
	fd := desc.Fields().ByName("r")
	for _, v := range []interface{}{
		"notalist",
		int64(7),
		map[interface{}]interface{}{"a": "b"},
	} {
		out := dynamicpb.NewMessage(desc)
		if err := setListField(out, fd, v); err == nil {
			t.Errorf("expected an error for %T on a repeated field", v)
		}
	}
	// The must-fail twin: a list still writes, and an empty one is a valid shape rather than a
	// mismatch - clearing a repeated field is a legitimate result.
	out := dynamicpb.NewMessage(desc)
	if err := setListField(out, fd, []interface{}{"a", "b"}); err != nil {
		t.Fatalf("a list should write: %v", err)
	}
	if got := out.Get(fd).List().Len(); got != 2 {
		t.Errorf("wrote %d elements, want 2", got)
	}
	empty := dynamicpb.NewMessage(desc)
	if err := setListField(empty, fd, []interface{}{}); err != nil {
		t.Fatalf("an empty list should write: %v", err)
	}
}

// repeatedStringDesc builds `message R { repeated string r = 1; }` at runtime.
func repeatedStringDesc(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING
	rep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("rep.proto"),
		Package: proto.String("rp"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("R"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("r"), Number: proto.Int32(1), Type: &str, Label: &rep},
			},
		}},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	return fd.Messages().Get(0)
}

// A nested CEL map keeps its native key types on the way out of cel-go, so the write-back can
// narrow each through the field's own key descriptor. nativeMap required *strings* and errored
// otherwise, which mattered only for a map whose values also have no native form - a
// `map<int32, Decimal>` - because that is when whole-map conversion fails and the entry-by-entry
// path is the only one left. The map then arrived as the raw cel-go value, which the writer
// cannot consume.
//
// Asserted through nativeValue rather than a full transform, because the shape it produces is
// exactly the contract setMapField is written against.
func TestNestedMapKeepsNativeKeyTypes(t *testing.T) {
	reg, err := types.NewRegistry()
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	inner := types.NewDynamicMap(reg, map[int64]string{7: "x", 9: "y"})
	got := nativeValue(inner, reflect.TypeOf(map[string]interface{}{}))

	entries, ok := got.(map[interface{}]interface{})
	if !ok {
		t.Fatalf("nativeValue gave %T, want map[interface{}]interface{}", got)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2: %v", len(entries), entries)
	}
	for k := range entries {
		if _, isString := k.(string); isString {
			t.Errorf("key %v was stringified; the descriptor has to narrow it", k)
		}
	}

	// And the top-level result map still requires field-name strings, which is what
	// nativeStringMap is for.
	top := types.NewStringInterfaceMap(reg, map[string]interface{}{"a": int64(1)})
	if _, err := nativeStringMap(top, reflect.TypeOf(map[string]interface{}{})); err != nil {
		t.Errorf("a string-keyed top-level map should convert: %v", err)
	}
	if _, err := nativeStringMap(inner, reflect.TypeOf(map[string]interface{}{})); err == nil {
		t.Error("a non-string key at the top level should still be refused")
	}
}

// A null inside a container is a rule error, not an element to skip. Skipping changed the
// list's length (or dropped a map entry) and still reported success. protobuf has no null to
// store, and the reference's write-back parse refuses the document - measured against
// protobuf-java's JsonFormat, `{"amounts": [null]}` is "Repeated field elements cannot be null
// in field: ..." and `{"amount_map": {"a": null}}` is "Map value cannot be null."
func TestANullInsideAContainerIsReported(t *testing.T) {
	repDesc := repeatedStringDesc(t)
	repFd := repDesc.Fields().ByName("r")
	for _, items := range [][]interface{}{
		{nil},
		{"a", nil},
		{"a", structpb.NullValue(0)},
	} {
		out := dynamicpb.NewMessage(repDesc)
		err := setListField(out, repFd, items)
		if err == nil {
			t.Errorf("expected an error for a null element in %v", items)
			continue
		}
		if !strings.Contains(err.Error(), "repeated field") {
			t.Errorf("error should name the field, got %v", err)
		}
	}

	mapDesc := mapKeyDesc(t, descriptorpb.FieldDescriptorProto_TYPE_STRING)
	mapFd := mapDesc.Fields().ByName("m")
	for _, entries := range []map[interface{}]interface{}{
		{"a": nil},
		{"a": structpb.NullValue(0)},
	} {
		out := dynamicpb.NewMessage(mapDesc)
		err := setMapField(out, mapFd, entries)
		if err == nil {
			t.Errorf("expected an error for a null value in %v", entries)
			continue
		}
		if !strings.Contains(err.Error(), "map field") {
			t.Errorf("error should name the field, got %v", err)
		}
	}

	// The must-fail twin: a non-null element still writes, so the guard is about null alone.
	out := dynamicpb.NewMessage(repDesc)
	if err := setListField(out, repFd, []interface{}{"a", "b"}); err != nil {
		t.Fatalf("a list of non-null elements should write: %v", err)
	}
}
