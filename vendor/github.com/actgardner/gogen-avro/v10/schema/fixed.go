package schema

import (
	"fmt"

	"github.com/actgardner/gogen-avro/v10/generator"
)

type FixedDefinition struct {
	name       QualifiedName
	aliases    []QualifiedName
	sizeBytes  int
	definition map[string]interface{}
}

func NewFixedDefinition(name QualifiedName, aliases []QualifiedName, sizeBytes int, definition map[string]interface{}) *FixedDefinition {
	return &FixedDefinition{
		name:       name,
		aliases:    aliases,
		sizeBytes:  sizeBytes,
		definition: definition,
	}
}

func (s *FixedDefinition) Name() string {
	return s.GoType()
}

func (s *FixedDefinition) AvroName() QualifiedName {
	return s.name
}

func (s *FixedDefinition) Aliases() []QualifiedName {
	return s.aliases
}

func (s *FixedDefinition) GoType() string {
	return generator.ToPublicName(s.name.String())
}

func (s *FixedDefinition) SizeBytes() int {
	return s.sizeBytes
}

func (s *FixedDefinition) filename() string {
	return generator.ToSnake(s.GoType()) + ".go"
}

func (s *FixedDefinition) SerializerMethod() string {
	return fmt.Sprintf("write%v", s.GoType())
}

func (s *FixedDefinition) Attribute(name string) interface{} {
	return s.definition[name]
}

func (s *FixedDefinition) Definition(scope map[QualifiedName]interface{}) (interface{}, error) {
	if _, ok := scope[s.name]; ok {
		return s.name.String(), nil
	}
	scope[s.name] = 1
	return s.definition, nil
}

func (s *FixedDefinition) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	if _, ok := rvalue.(string); !ok {
		return "", fmt.Errorf("Expected string as default for field %v, got %q", lvalue, rvalue)
	}

	return fmt.Sprintf("copy(%v[:], []byte(%q))", lvalue, rvalue), nil
}

func (s *FixedDefinition) IsReadableBy(d Definition) bool {
	if fixed, ok := d.(*FixedDefinition); ok {
		return fixed.sizeBytes == s.sizeBytes && hasMatchingName(s.name, d)
	}
	return false
}

func (s *FixedDefinition) WrapperType() string {
	return fmt.Sprintf("%vWrapper", s.GoType())
}

func (s *FixedDefinition) WrapperPointer() bool { return false }

func (s *FixedDefinition) Children() []AvroType {
	return []AvroType{}
}
