package schema

type NullField struct {
	PrimitiveField
}

func NewNullField(definition interface{}) *NullField {
	return &NullField{PrimitiveField{
		definition:       definition,
		name:             "Null",
		goType:           "*types.NullVal",
		serializerMethod: "vm.WriteNull",
		unionKey:         "null",
	}}
}

func (s *NullField) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	return "", nil
}

func (s *NullField) WrapperType() string {
	return ""
}

func (s *NullField) IsReadableBy(f AvroType) bool {
	if union, ok := f.(*UnionField); ok {
		for _, t := range union.AvroTypes() {
			if s.IsReadableBy(t) {
				return true
			}
		}
	}
	_, ok := f.(*NullField)
	return ok
}
