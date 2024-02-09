package schema

import (
	"fmt"
	"strings"

	"github.com/actgardner/gogen-avro/v10/generator"
)

type EnumDefinition struct {
	name         QualifiedName
	aliases      []QualifiedName
	symbols      []string
	doc          string
	defaultValue string
	definition   map[string]interface{}
}

func NewEnumDefinition(name QualifiedName, aliases []QualifiedName, symbols []string, doc string, defaultValue string, definition map[string]interface{}) *EnumDefinition {
	return &EnumDefinition{
		name:         name,
		aliases:      aliases,
		symbols:      symbols,
		doc:          doc,
		defaultValue: defaultValue,
		definition:   definition,
	}
}

func (e *EnumDefinition) Name() string {
	return e.GoType()
}

func (e *EnumDefinition) Doc() string {
	return strings.ReplaceAll(e.doc, "\n", " ")
}

func (e *EnumDefinition) AvroName() QualifiedName {
	return e.name
}

func (e *EnumDefinition) Aliases() []QualifiedName {
	return e.aliases
}

func (e *EnumDefinition) SymbolIndex(symbol string) int {
	for i, s := range e.symbols {
		if s == symbol {
			return i
		}
	}
	return -1
}

func (e *EnumDefinition) Symbols() []string {
	return e.symbols
}

func (e *EnumDefinition) SymbolName(symbol string) string {
	return generator.ToPublicName(e.GoType() + strings.Title(symbol))
}

func (e *EnumDefinition) GoType() string {
	return generator.ToPublicName(e.name.String())
}

func (e *EnumDefinition) SerializerMethod() string {
	return "write" + e.GoType()
}

func (e *EnumDefinition) FromStringMethod() string {
	return "New" + e.GoType() + "Value"
}

func (e *EnumDefinition) filename() string {
	return generator.ToSnake(e.GoType()) + ".go"
}

func (s *EnumDefinition) Attribute(name string) interface{} {
	return s.definition[name]
}

func (s *EnumDefinition) Definition(scope map[QualifiedName]interface{}) (interface{}, error) {
	if _, ok := scope[s.name]; ok {
		return s.name.String(), nil
	}
	scope[s.name] = 1
	return s.definition, nil
}

func (s *EnumDefinition) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	if _, ok := rvalue.(string); !ok {
		return "", fmt.Errorf("Expected string as default for field %v, got %q", lvalue, rvalue)
	}

	return fmt.Sprintf("%v = %v", lvalue, generator.ToPublicName(s.GoType()+strings.Title(rvalue.(string)))), nil
}

func (s *EnumDefinition) IsReadableBy(d Definition) bool {
	_, ok := d.(*EnumDefinition)
	return ok && hasMatchingName(s.AvroName(), d)
}

func (s *EnumDefinition) WrapperType() string {
	return fmt.Sprintf("%vWrapper", s.GoType())
}

func (s *EnumDefinition) WrapperPointer() bool { return false }

func (s *EnumDefinition) Children() []AvroType {
	return []AvroType{}
}

func (s *EnumDefinition) Default() string {
	return s.defaultValue
}
