package cel

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	prototypes "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
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
	msg := (&prototypes.Decimal{}).ProtoReflect()
	descMsg := msg.Descriptor()
	_ = descMsg
	valueTypes := (&prototypes.Decimal{}).ProtoReflect().Descriptor()
	_ = valueTypes

	// Build `message Holder { .confluent.type.Decimal amount = 1; }` via the generated
	// descriptor's own file, so fd.Message() really is confluent.type.Decimal.
	out := dynamicpb.NewMessage((&prototypes.Decimal{}).ProtoReflect().Descriptor())
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
	if err := setMessageValue(target, fd, &prototypes.Decimal{Scale: 2, Precision: 4}); err != nil {
		t.Fatalf("echoing a Decimal should work: %v", err)
	}
}

// decimalHolderDesc builds `message Holder { .confluent.type.Decimal amount = 1; }` at runtime,
// reusing the generated confluent.type.Decimal file so the field's message type is the real one.
func decimalHolderDesc(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	decFile := protodesc.ToFileDescriptorProto(
		(&prototypes.Decimal{}).ProtoReflect().Descriptor().ParentFile())
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
