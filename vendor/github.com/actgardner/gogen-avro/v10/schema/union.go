package schema

import (
	"encoding/json"
	"fmt"

	"github.com/actgardner/gogen-avro/v10/generator"
)

type UnionField struct {
	name       string
	itemType   []AvroType
	definition []interface{}
	nullIndex  int
}

func NewUnionField(name string, itemType []AvroType, definition []interface{}) *UnionField {
	nullIndex := -1
	for i, t := range itemType {
		if _, ok := t.(*NullField); ok {
			nullIndex = i
			break
		}
	}

	return &UnionField{
		name:       name,
		itemType:   itemType,
		definition: definition,
		nullIndex:  nullIndex,
	}
}

func (s *UnionField) compositeFieldName() string {
	var UnionFields = "Union"
	for _, i := range s.itemType {
		UnionFields += i.Name()
	}
	return UnionFields
}

func (s *UnionField) Name() string {
	if s.name == "" {
		return generator.ToPublicName(s.compositeFieldName())
	}
	return generator.ToPublicName(s.name)
}

func (s *UnionField) AvroTypes() []AvroType {
	return s.itemType
}

func (s *UnionField) GoType() string {
	if s.nullIndex == -1 {
		return s.Name()
	}
	return "*" + s.Name()
}

func (s *UnionField) UnionEnumType() string {
	return fmt.Sprintf("%vTypeEnum", s.Name())
}

func (s *UnionField) ItemName(item AvroType) string {
	return s.UnionEnumType() + item.Name()
}

func (s *UnionField) ItemTypes() []AvroType {
	return s.itemType
}

func (s *UnionField) filename() string {
	return generator.ToSnake(s.Name()) + ".go"
}

func (s *UnionField) SerializerMethod() string {
	return fmt.Sprintf("write%v", s.Name())
}

func (s *UnionField) ItemConstructor(f AvroType) string {
	if constructor, ok := getConstructableForType(f); ok {
		return constructor.ConstructorMethod()
	}
	return ""
}

func (s *UnionField) Attribute(name string) interface{} {
	return nil
}

func (s *UnionField) Schema() (string, error) {
	def, err := s.Definition(make(map[QualifiedName]interface{}))
	if err != nil {
		return "", err
	}
	jsonBytes, err := json.Marshal(def)
	return string(jsonBytes), err
}

func (s *UnionField) Definition(scope map[QualifiedName]interface{}) (interface{}, error) {
	def := make([]interface{}, len(s.definition))
	var err error
	for i, item := range s.itemType {
		def[i], err = item.Definition(scope)
		if err != nil {
			return nil, err
		}
	}
	return def, nil
}

func (s *UnionField) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	defaultType := s.itemType[0]
	if _, ok := defaultType.(*NullField); ok {
		return fmt.Sprintf("%v = nil", lvalue), nil
	}
	init := fmt.Sprintf("%v = %v\n", lvalue, s.ConstructorMethod())
	lvalue = fmt.Sprintf("%v.%v", lvalue, defaultType.Name())
	constructorCall := ""
	if constructor, ok := getConstructableForType(defaultType); ok {
		constructorCall = fmt.Sprintf("%v = %v\n", lvalue, constructor.ConstructorMethod())
	}
	assignment, err := defaultType.DefaultValue(lvalue, rvalue)
	return init + constructorCall + assignment, err
}

func (s *UnionField) WrapperType() string {
	if s.NullIndex() == -1 {
		return "types.Record"
	}
	return ""
}

func (s *UnionField) WrapperPointer() bool {
	return false
}

func (s *UnionField) IsReadableBy(f AvroType) bool {
	// Report if *any* writer type could be deserialized by the reader
	for _, t := range s.AvroTypes() {
		if readerUnion, ok := f.(*UnionField); ok {
			for _, rt := range readerUnion.AvroTypes() {
				if t.IsReadableBy(rt) {
					return true
				}
			}
		} else {
			if t.IsReadableBy(f) {
				return true
			}
		}
	}
	return false
}

func (s *UnionField) ConstructorMethod() string {
	return fmt.Sprintf("New%v()", s.Name())
}

func (s *UnionField) Equals(reader *UnionField) bool {
	if len(reader.AvroTypes()) != len(s.AvroTypes()) {
		return false
	}

	for i, t := range s.AvroTypes() {
		readerType := reader.AvroTypes()[i]
		if writerRef, ok := t.(*Reference); ok {
			if readerRef, ok := readerType.(*Reference); ok {
				if readerRef.TypeName != writerRef.TypeName {
					return false
				}
			} else {
				return false
			}
		} else if t != readerType {
			return false
		}
	}
	return true
}

func (s *UnionField) Children() []AvroType {
	return s.itemType
}

func (s *UnionField) NullIndex() int {
	return s.nullIndex
}

func (s *UnionField) UnionKey() string {
	panic("Unions within unions are not supported")
}

func (s *UnionField) GetReference() bool {
	return s.nullIndex == -1
}
