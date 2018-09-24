package avro

import (
	"reflect"
	"github.com/rnpridgeon/avro"
	"io/ioutil"
)

const (
	String = iota + 1
	// Bytes schema type constant
	Bytes
	// Int schema type constant
	Int
	// Long schema type constant
	Long
	// Float schema type constant
	Float
	// Double schema type constant
	Double
	// Boolean schema type constant
	Boolean
	// Null schema type constant
	Null
)

var primitiveENUM = []Schema{
	&avro.StringSchema{},
	&avro.BytesSchema{},
	&avro.IntSchema{},
	&avro.FloatSchema{},
	&avro.DoubleSchema{},
	&avro.BooleanSchema{},
	&avro.NullSchema{},
}

// Record(go-avro:AvroRecord) and Schema Interfaces derived from https://github.com/elodina/go-avro
// Record is an interface for anything that has an Avro schema and can be serialized/deserialized by this library.
type Record interface {
	// Schema returns an Avro schema for this Record.
	Schema() avro.Schema
}

// GenericRecord is a generic instance of a record schema.
// Fields are accessible with methods Get() and Set()
type GenericRecord interface {
	avro.AvroRecord
	Get(name string) interface{}
	Set(name string, value interface{})
}

// Schema is an interface representing a single Avro schema (both primitive and complex).
type Schema interface {
	// Returns an integer constant representing this schema type.
	Type() int

	// If this is a record, enum or fixed, returns its name, otherwise the name of the primitive type.
	GetName() string

	// Gets a custom non-reserved property from this schema and a bool representing if it exists.
	Prop(key string) (interface{}, bool)

	// Converts this schema to its JSON representation.
	String() string

	// Checks whether the given value is writeable to this schema.
	Validate(v reflect.Value) bool
}

// NewGenericRecord returns a new GenericRecord which can be serialized by this Serializer
func NewGenericRecord(schema Schema) GenericRecord {
	return avro.NewGenericRecord(schema)
}

// Parse creates a new Schema from the s
func Parse(schema string) (Schema, error) {
	return avro.ParseSchema(schema)
}

// ParseFile parses the contents of a file
func ParseFile(pathname string) (Schema, error) {
	buff, err := ioutil.ReadFile(pathname)
	if err != nil {
		panic(err)
	}

	return avro.ParseSchema(string(buff))
}
