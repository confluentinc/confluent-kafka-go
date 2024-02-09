package schema

import (
	"fmt"
)

type LongField struct {
	PrimitiveField
}

func NewLongField(definition interface{}) *LongField {
	return &LongField{PrimitiveField{
		definition:       definition,
		name:             "Long",
		goType:           "int64",
		serializerMethod: "vm.WriteLong",
		unionKey:         "long",
	}}
}

func (s *LongField) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	if _, ok := rvalue.(float64); !ok {
		return "", fmt.Errorf("Expected number as default for Field %v, got %q", lvalue, rvalue)
	}

	return fmt.Sprintf("%v = %v", lvalue, rvalue), nil
}

func (s *LongField) WrapperType() string {
	return "types.Long"
}

func (s *LongField) IsReadableBy(f AvroType) bool {
	if union, ok := f.(*UnionField); ok {
		for _, t := range union.AvroTypes() {
			if s.IsReadableBy(t) {
				return true
			}
		}
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
