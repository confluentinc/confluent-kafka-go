package types

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"

	typepb "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/type"
)

// This package is a compatibility surface and nothing inside the client uses it, so without
// these assertions it could stop compiling or stop registering without anything noticing.
// confluent.type.Decimal was generated here until it moved to the canonical confluent/type
// path; a public-import stub keeps the type name and the descriptor variable alive.

// The type is an alias, not a copy, so a value built through the old name is the same type the
// rest of the client now works with - assignable with no conversion, and marshalling with the
// canonical descriptor.
func TestDecimalIsAnAliasOfTheCanonicalType(t *testing.T) {
	var d *typepb.Decimal = &Decimal{Value: []byte{0x04, 0xd2}, Precision: 4, Scale: 2}

	if got := d.ProtoReflect().Descriptor().FullName(); got != "confluent.type.Decimal" {
		t.Errorf("full name = %q, want confluent.type.Decimal", got)
	}
	b, err := proto.Marshal(d)
	if err != nil || len(b) == 0 {
		t.Fatalf("marshal through the alias: %v (%d bytes)", err, len(b))
	}
}

// The descriptor variable this package exported before the move. It names the old path, and the
// file it points at declares nothing and publicly imports the canonical one - which is how the
// old path can be registered at all without a second declaration of confluent.type.Decimal.
func TestLegacyDescriptorVarRegistersTheOldPath(t *testing.T) {
	if got := File_confluent_types_decimal_proto.Path(); got != "confluent/types/decimal.proto" {
		t.Errorf("path = %q, want confluent/types/decimal.proto", got)
	}
	if got := File_confluent_types_decimal_proto.Messages().Len(); got != 0 {
		t.Errorf("stub declares %d messages, want 0 - a declaration here would conflict", got)
	}

	for _, path := range []string{"confluent/type/decimal.proto", "confluent/types/decimal.proto"} {
		if _, err := protoregistry.GlobalFiles.FindFileByPath(path); err != nil {
			t.Errorf("FindFileByPath(%q): %v", path, err)
		}
	}
	// The symbol resolves once, from the canonical file, however it was reached.
	md, err := protoregistry.GlobalTypes.FindMessageByName("confluent.type.Decimal")
	if err != nil {
		t.Fatalf("FindMessageByName: %v", err)
	}
	if got := md.Descriptor().ParentFile().Path(); got != "confluent/type/decimal.proto" {
		t.Errorf("declared in %q, want confluent/type/decimal.proto", got)
	}
}

// Every method and field the shipped package exported, touched so the alias cannot silently
// lose part of the surface. Extracted from v2.15.0's generated decimal.pb.go: fields
// Value/Precision/Scale, methods Reset/String/ProtoReflect/Descriptor/GetValue/GetPrecision/
// GetScale, and ProtoMessage.
func TestTheShippedDecimalSurfaceStillCompiles(t *testing.T) {
	d := &Decimal{Value: []byte{0x04, 0xd2}, Precision: 4, Scale: 2}

	if got := d.GetValue(); len(got) != 2 {
		t.Errorf("GetValue() = %v", got)
	}
	if got := d.GetPrecision(); got != 4 {
		t.Errorf("GetPrecision() = %d, want 4", got)
	}
	if got := d.GetScale(); got != 2 {
		t.Errorf("GetScale() = %d, want 2", got)
	}
	if d.String() == "" {
		t.Error("String() is empty")
	}
	if d.ProtoReflect() == nil {
		t.Error("ProtoReflect() is nil")
	}
	if _, idx := d.Descriptor(); len(idx) == 0 {
		t.Error("Descriptor() returned no index path")
	}
	d.ProtoMessage()
	d.Reset()
	if d.GetScale() != 0 {
		t.Error("Reset() did not clear the value")
	}
}

// Variant deliberately has no stub here: it had not shipped under this path, so nothing can be
// importing it. Pinned so removing the guard in codegen.sh is a deliberate act.
func TestVariantIsNotReExportedHere(t *testing.T) {
	if _, err := protoregistry.GlobalFiles.FindFileByPath("confluent/types/variant.proto"); err == nil {
		t.Error("confluent/types/variant.proto is registered; it never shipped and needs no stub")
	}
}
