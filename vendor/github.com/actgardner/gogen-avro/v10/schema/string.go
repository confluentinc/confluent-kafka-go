package schema

import (
	"fmt"
)

type StringField struct {
	PrimitiveField
}

func NewStringField(definition interface{}) *StringField {
	return &StringField{PrimitiveField{
		definition:       definition,
		name:             "String",
		goType:           "string",
		serializerMethod: "vm.WriteString",
		unionKey:         "string",
	}}
}

func (s *StringField) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	if _, ok := rvalue.(string); !ok {
		return "", fmt.Errorf("Expected string as default for field %v, got %q", lvalue, rvalue)
	}

	return fmt.Sprintf("%v = %q", lvalue, rvalue), nil
}

func (s *StringField) WrapperType() string {
	return "types.String"
}

func (s *StringField) IsReadableBy(f AvroType) bool {
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
