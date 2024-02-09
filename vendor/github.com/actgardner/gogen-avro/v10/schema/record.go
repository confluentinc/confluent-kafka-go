package schema

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/actgardner/gogen-avro/v10/generator"
)

type RecordDefinition struct {
	name     QualifiedName
	aliases  []QualifiedName
	fields   []*Field
	doc      string
	metadata map[string]interface{}
}

func NewRecordDefinition(name QualifiedName, aliases []QualifiedName, fields []*Field, doc string, metadata map[string]interface{}) *RecordDefinition {
	return &RecordDefinition{
		name:     name,
		aliases:  aliases,
		fields:   fields,
		doc:      doc,
		metadata: metadata,
	}
}

func (r *RecordDefinition) AvroName() QualifiedName {
	return r.name
}

func (r *RecordDefinition) Name() string {
	return generator.ToPublicName(r.name.String())
}

func (r *RecordDefinition) GoType() string {
	return r.Name()
}

func (r *RecordDefinition) Aliases() []QualifiedName {
	return r.aliases
}

func (r *RecordDefinition) SerializerMethod() string {
	return fmt.Sprintf("write%v", r.Name())
}

func (r *RecordDefinition) NewWriterMethod() string {
	return fmt.Sprintf("New%vWriter", r.Name())
}

func (s *RecordDefinition) Attribute(name string) interface{} {
	return s.metadata[name]
}

func (r *RecordDefinition) Definition(scope map[QualifiedName]interface{}) (interface{}, error) {
	if _, ok := scope[r.name]; ok {
		return r.name.String(), nil
	}
	metadata := copyDefinition(r.metadata)
	scope[r.name] = 1
	fields := make([]map[string]interface{}, 0)
	for _, f := range r.fields {
		def, err := f.Definition(scope)
		if err != nil {
			return nil, err
		}
		fields = append(fields, def)
	}

	metadata["fields"] = fields
	return metadata, nil
}

func (r *RecordDefinition) ConstructorMethod() string {
	return fmt.Sprintf("New%v()", r.Name())
}

func (r *RecordDefinition) DefaultForField(f *Field) (string, error) {
	return f.Type().DefaultValue(fmt.Sprintf("r.%v", f.GoName()), f.Default())
}

func (r *RecordDefinition) ConstructableForField(f *Field) string {
	if constructor, ok := getConstructableForType(f.Type()); ok {
		return fmt.Sprintf("r.%v = %v\n", f.GoName(), constructor.ConstructorMethod())
	}
	return ""
}

func (r *RecordDefinition) RecordReaderTypeName() string {
	return r.Name() + "Reader"
}

// FieldByName finds a field in the reader schema whose name or aliases match a name in the writer schema.
func (r *RecordDefinition) FieldByName(field string) *Field {
	for _, f := range r.fields {
		if f.NameMatchesAliases(field) {
			return f
		}
	}
	return nil
}

func (r *RecordDefinition) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	items := rvalue.(map[string]interface{})
	fieldSetters := ""
	for k, v := range items {
		field := r.FieldByName(k)
		fieldSetter, err := field.Type().DefaultValue(fmt.Sprintf("%v.%v", lvalue, field.GoName()), v)
		if err != nil {
			return "", err
		}

		fieldSetters += fieldSetter + "\n"
	}
	return fieldSetters, nil
}

func (r *RecordDefinition) Fields() []*Field {
	return r.fields
}

func (s *RecordDefinition) IsReadableBy(d Definition) bool {
	_, ok := d.(*RecordDefinition)
	return ok && hasMatchingName(s.AvroName(), d)
}

func (s *RecordDefinition) WrapperType() string {
	return "types.Record"
}

func (s *RecordDefinition) WrapperPointer() bool {
	return false
}

func (s *RecordDefinition) Doc() string {
	return strings.ReplaceAll(s.doc, "\n", " ")
}

func (s *RecordDefinition) Schema() (string, error) {
	def0, err := s.Definition(make(map[QualifiedName]interface{}))
	if err != nil {
		return "", err
	}
	def := def0.(map[string]interface{})
	delete(def, "namespace")
	def["name"] = s.name.String()
	jsonBytes, err := json.Marshal(def)
	return string(jsonBytes), err
}

func (s *RecordDefinition) Children() []AvroType {
	children := make([]AvroType, len(s.fields))
	for i, field := range s.fields {
		children[i] = field.Type()
	}
	return children
}

func (s *RecordDefinition) GetReference() bool {
	return true
}
