package avro

import (
	"context"
	"reflect"
	"sync"
)

// EncodingRegistry is used by SingleEncoder to find
// ids for schemas encoded in messages.
type EncodingRegistry interface {
	// AppendSchemaID appends the given schema ID header to buf
	// and returns the resulting slice.
	AppendSchemaID(buf []byte, id int64) []byte

	// IDForSchema returns an ID for the given schema.
	IDForSchema(ctx context.Context, schema *Type) (int64, error)
}

// SingleEncoder encodes messages in Avro binary format.
// Each message includes a header or wrapper that indicates the schema.
type SingleEncoder struct {
	registry EncodingRegistry
	names    *Names
	// ids holds a map from Go type (reflect.Type) to schema ID (int64)
	ids sync.Map
}

// NewSingleEncoder returns a SingleEncoder instance that encodes single
// messages along with their schema identifier.
//
// Go values unmarshaled through Marshal will have their Avro schemas
// translated with the given Names instance. If names is nil, the global
// namespace will be used.
func NewSingleEncoder(r EncodingRegistry, names *Names) *SingleEncoder {
	if names == nil {
		names = globalNames
	}
	return &SingleEncoder{
		registry: r,
		names:    names,
	}
}

// CheckMarshalType checks that the given type can be marshaled with the encoder.
// It also caches any type information obtained from the EncodingRegistry from the
// type, so future calls to Marshal with that type won't call it.
func (enc *SingleEncoder) CheckMarshalType(ctx context.Context, x interface{}) error {
	_, err := enc.idForType(ctx, reflect.TypeOf(x))
	return err
}

// Marshal returns x marshaled as using the Avro binary encoding,
// along with an identifier that records the type that it was encoded
// with.
func (enc *SingleEncoder) Marshal(ctx context.Context, x interface{}) ([]byte, error) {
	xv := reflect.ValueOf(x)
	id, err := enc.idForType(ctx, xv.Type())
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 100)
	buf = enc.registry.AppendSchemaID(buf, id)
	data, _, err := marshalAppend(enc.names, buf, xv)
	return data, err
}

func (enc *SingleEncoder) idForType(ctx context.Context, t reflect.Type) (int64, error) {
	id, ok := enc.ids.Load(t)
	if ok {
		return id.(int64), nil
	}
	avroType, err := avroTypeOf(enc.names, t)
	if err != nil {
		return 0, err
	}
	id1, err := enc.registry.IDForSchema(ctx, avroType)
	if err != nil {
		return 0, err
	}
	enc.ids.LoadOrStore(t, id1)
	return id1, nil
}
