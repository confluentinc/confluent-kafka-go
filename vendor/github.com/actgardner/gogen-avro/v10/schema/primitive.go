package schema

// Common methods for all primitive types
type PrimitiveField struct {
	definition       interface{}
	name             string
	goType           string
	serializerMethod string
	unionKey         string
}

func (s *PrimitiveField) Name() string {
	return s.name
}

func (s *PrimitiveField) GoType() string {
	return s.goType
}

func (s *PrimitiveField) SerializerMethod() string {
	return s.serializerMethod
}

func (s *PrimitiveField) Attribute(name string) interface{} {
	definition, _ := s.definition.(map[string]interface{})
	return definition[name]
}

func (s *PrimitiveField) Definition(_ map[QualifiedName]interface{}) (interface{}, error) {
	return s.definition, nil
}

func (s *PrimitiveField) Children() []AvroType {
	return []AvroType{}
}

func (s *PrimitiveField) UnionKey() string {
	return s.unionKey
}

func (s *PrimitiveField) WrapperPointer() bool { return false }
