package schema

import (
	"fmt"
)

type IntField struct {
	PrimitiveField
}

func NewIntField(definition interface{}) *IntField {
	return &IntField{PrimitiveField{
		definition:       definition,
		name:             "Int",
		goType:           "int32",
		serializerMethod: "vm.WriteInt",
		unionKey:         "int",
	}}
}

func (s *IntField) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	if _, ok := rvalue.(float64); !ok {
		return "", fmt.Errorf("Expected number as default for field %v, got %q", lvalue, rvalue)
	}

	return fmt.Sprintf("%v = %v", lvalue, rvalue), nil
}

func (s *IntField) WrapperType() string {
	return "types.Int"
}

func (s *IntField) IsReadableBy(f AvroType) bool {
	if union, ok := f.(*UnionField); ok {
		for _, t := range union.AvroTypes() {
			if s.IsReadableBy(t) {
				return true
			}
		}
	}
	if _, ok := f.(*IntField); ok {
		return true
	}
	if _, ok := f.(*LongField); ok {
		return true
	}
	if _, ok := f.(*FloatField); ok {
		return true
	}
	if _, ok := f.(*DoubleField); ok {
		return true
	}
	return false
}
