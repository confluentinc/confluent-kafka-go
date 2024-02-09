package schema

import (
	"fmt"

	"github.com/actgardner/gogen-avro/v10/util"
)

type BytesField struct {
	PrimitiveField
}

func NewBytesField(definition interface{}) *BytesField {
	return &BytesField{PrimitiveField{
		definition:       definition,
		name:             "Bytes",
		goType:           "Bytes",
		serializerMethod: "vm.WriteBytes",
		unionKey:         "bytes",
	}}
}

func (s *BytesField) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	if _, ok := rvalue.(string); !ok {
		return "", fmt.Errorf("Expected string as default for field %v, got %q", lvalue, rvalue)
	}

	b := util.DecodeByteString(rvalue.(string))

	return fmt.Sprintf("%v = []byte(%q)", lvalue, b), nil
}

func (s *BytesField) WrapperType() string {
	return "BytesWrapper"
}

func (s *BytesField) IsReadableBy(f AvroType) bool {
	if union, ok := f.(*UnionField); ok {
		for _, t := range union.AvroTypes() {
			if s.IsReadableBy(t) {
				return true
			}
		}
	}
	if _, ok := f.(*BytesField); ok {
		return true
	}
	if _, ok := f.(*StringField); ok {
		return true
	}
	return false
}
