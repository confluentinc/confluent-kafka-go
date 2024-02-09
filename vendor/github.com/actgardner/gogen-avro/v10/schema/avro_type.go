package schema

type AvroType interface {
	Name() string
	GoType() string
	// The key to use in JSON-encoding a union with this value
	UnionKey() string

	// The name of the method which writes this field onto the wire
	SerializerMethod() string

	Children() []AvroType

	Attribute(name string) interface{}
	Definition(scope map[QualifiedName]interface{}) (interface{}, error)
	DefaultValue(lvalue string, rvalue interface{}) (string, error)

	// WrapperType is the VM type to wrap this value in, if applicable
	WrapperType() string
	// WrapperPointer is whether the VM wrapper type needs to be a pointer
	WrapperPointer() bool
	IsReadableBy(f AvroType) bool
}
