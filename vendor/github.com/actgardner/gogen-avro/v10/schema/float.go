package schema

import (
	"fmt"
)

type FloatField struct {
	PrimitiveField
}

func NewFloatField(definition interface{}) *FloatField {
	return &FloatField{PrimitiveField{
		definition:       definition,
		name:             "Float",
		goType:           "float32",
		serializerMethod: "vm.WriteFloat",
		unionKey:         "float",
	}}
}

func (s *FloatField) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	if _, ok := rvalue.(float64); !ok {
		return "", fmt.Errorf("Expected float as default for field %v, got %q", lvalue, rvalue)
	}

	return fmt.Sprintf("%v = %v", lvalue, rvalue), nil
}

func (s *FloatField) WrapperType() string {
	return "types.Float"
}

func (s *FloatField) IsReadableBy(f AvroType) bool {
	if union, ok := f.(*UnionField); ok {
		for _, t := range union.AvroTypes() {
			if s.IsReadableBy(t) {
				return true
			}
		}
	}
	if _, ok := f.(*FloatField); ok {
		return true
	}
	if _, ok := f.(*DoubleField); ok {
		return true
	}
	return false
}
