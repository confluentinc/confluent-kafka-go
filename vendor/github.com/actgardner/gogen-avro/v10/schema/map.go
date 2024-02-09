package schema

import (
	"fmt"

	"github.com/actgardner/gogen-avro/v10/generator"
)

type MapField struct {
	itemType   AvroType
	definition map[string]interface{}
}

func NewMapField(itemType AvroType, definition map[string]interface{}) *MapField {
	return &MapField{
		itemType:   itemType,
		definition: definition,
	}
}

func (s *MapField) ItemType() AvroType {
	return s.itemType
}

func (s *MapField) Name() string {
	return "Map" + s.itemType.Name()
}

func (s *MapField) GoType() string {
	return fmt.Sprintf("map[string]%v", s.itemType.GoType())
}

func (s *MapField) SerializerMethod() string {
	return fmt.Sprintf("write%v", s.Name())
}

func (s *MapField) filename() string {
	return generator.ToSnake(s.Name()) + ".go"
}

func (s *MapField) Attribute(name string) interface{} {
	return s.definition[name]
}

func (s *MapField) Definition(scope map[QualifiedName]interface{}) (interface{}, error) {
	def := copyDefinition(s.definition)
	var err error
	def["values"], err = s.itemType.Definition(scope)
	if err != nil {
		return nil, err
	}
	return def, nil
}

func (s *MapField) ConstructorMethod() string {
	return fmt.Sprintf("make(%v)", s.GoType())
}

func (s *MapField) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	items, ok := rvalue.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("Expected map as default for %v, got %v", lvalue, rvalue)
	}
	setters := ""

	for k, v := range items {
		setter, err := s.itemType.DefaultValue(fmt.Sprintf("%v[%q]", lvalue, k), v)
		if err != nil {
			return "", err
		}
		setters += setter + "\n"
	}
	return setters, nil
}

func (s *MapField) WrapperType() string {
	return fmt.Sprintf("%vWrapper", s.Name())
}

func (s *MapField) WrapperPointer() bool {
	return true
}

func (s *MapField) IsReadableBy(f AvroType) bool {
	if union, ok := f.(*UnionField); ok {
		for _, t := range union.AvroTypes() {
			if s.IsReadableBy(t) {
				return true
			}
		}
	}
	if reader, ok := f.(*MapField); ok {
		return s.ItemType().IsReadableBy(reader.ItemType())
	}
	return false
}

func (s *MapField) ItemConstructable() string {
	if constructor, ok := getConstructableForType(s.itemType); ok {
		return fmt.Sprintf("v = %v\n", constructor.ConstructorMethod())
	}
	return ""
}

func (s *MapField) Children() []AvroType {
	return []AvroType{s.itemType}
}

func (s *MapField) UnionKey() string {
	return "map"
}
