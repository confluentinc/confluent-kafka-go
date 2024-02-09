// Package avrotypegen holds types that are used by generated Avro Go code.
// This is an implementation detail and this might change over time.
package avrotypegen

import "fmt"

// AvroRecord is implemented by Go types generated
// by the avrogo command.
type AvroRecord interface {
	AvroRecord() RecordInfo
}

// RecordInfo holds information about how a Go type relates
// to an Avro schema.
type RecordInfo struct {
	// Schema holds the Avro schema of the record.
	Schema string

	// Required holds whether fields are required.
	// If a field is required, it has no default value.
	Required []bool

	// Defaults holds default values for the fields.
	// Each item corresponds to the field at that index and returns
	// a newly created default value for the field.
	// An entry is only consulted if Required is false for that field.
	// Missing or nil entries are assumed to default to the zero
	// value for the type.
	Defaults []func() interface{}

	// Unions holds entries for union fields.
	// Each item corresponds to the field at that index
	// and holds slice with one value for each member
	// of the union, of type *T, where T is the type used
	// for that member of the union.
	Unions []UnionInfo
}

type UnionInfo struct {
	// Type holds a value of type *T where T is
	// the type described by the TypeInfo,
	// except when the TypeInfo represents the null
	// type, in which case Type will be nil.
	Type interface{}

	// When the UnionInfo describes a union,
	// Union holds an entry for each member
	// of the union.
	// The info can be omitted if Type is a pointer
	// and the union is ["null", T].
	Union []UnionInfo
}

// Null represents the Avro null type. Its only JSON representation is null.
type Null struct{}

// UnmarshalJSON implements json.Unmarshaler by requiring
// the JSON value to be null.
func (Null) UnmarshalJSON(data []byte) error {
	if string(data) != "null" {
		return fmt.Errorf("cannot unmarshal %q into avro.Null", data)
	}
	return nil
}

// MarshalJSON implements json.Marshaler by returning "null".
func (Null) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}
