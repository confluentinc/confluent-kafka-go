package schema

// FileRoot represents the Avro type at the root of a given schema file, and implements Definition.
// This is necessary for files which contain a union, array, map, etc. at the top level since these types don't otherwise have a Definition which would result in code being generated.
type FileRoot struct {
	Type AvroType
}

func (f *FileRoot) AvroName() QualifiedName {
	return QualifiedName{}
}

func (f *FileRoot) Aliases() []QualifiedName {
	return nil
}

func (f *FileRoot) Name() string {
	return ""
}

func (f *FileRoot) GoType() string {
	return ""
}

func (f *FileRoot) SerializerMethod() string {
	return ""
}

func (f *FileRoot) Children() []AvroType {
	return []AvroType{f.Type}
}

func (f *FileRoot) Attribute(name string) interface{} {
	return nil
}

func (f *FileRoot) Definition(scope map[QualifiedName]interface{}) (interface{}, error) {
	return nil, nil
}

func (f *FileRoot) DefaultValue(lvalue string, rvalue interface{}) (string, error) {
	return "", nil
}

func (f *FileRoot) IsReadableBy(_ Definition) bool {
	return false
}

func (f *FileRoot) WrapperType() string {
	return ""
}

func (f *FileRoot) WrapperPointer() bool { return false }
