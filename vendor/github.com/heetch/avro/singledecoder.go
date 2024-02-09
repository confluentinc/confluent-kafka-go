package avro

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// DecodingRegistry is used by SingleDecoder to find information
// about schema identifiers in messages.
type DecodingRegistry interface {
	// DecodeSchemaID returns the schema ID header of the message
	// and the bare message without schema information.
	// A schema ID is specific to the DecodingRegistry instance - within
	// a given DecodingRegistry instance (only), a given schema ID
	// must always correspond to the same schema.
	//
	// If the message isn't valid, DecodeSchemaID should return (0, nil).
	DecodeSchemaID(msg []byte) (int64, []byte)

	// SchemaForID returns the schema for the given ID.
	SchemaForID(ctx context.Context, id int64) (*Type, error)
}

type decoderSchemaPair struct {
	t        reflect.Type
	schemaID int64
}

// SingleDecoder decodes messages in Avro binary format.
// Each message includes a header or wrapper that indicates the schema
// used to encode the message.
//
// A DecodingRegistry is used to retrieve the schema for a given message
// or to find the encoding for a given schema.
//
// To encode or decode a stream of messages that all use the same
// schema, use StreamEncoder or StreamDecoder instead.
type SingleDecoder struct {
	registry DecodingRegistry

	names *Names

	// mu protects the fields below.
	// We might be better off with a couple of sync.Maps here, but this is a bit easier on the brain.
	mu sync.RWMutex

	// writerTypes holds a cache of the schemas previously encountered when
	// decoding messages.
	writerTypes map[int64]*Type

	// programs holds the programs previously created when decoding.
	programs map[decoderSchemaPair]*decodeProgram
}

// NewSingleDecoder returns a new SingleDecoder that uses g to determine
// the schema of each message that's marshaled or unmarshaled.
//
// Go values unmarshaled through Unmarshal will have their Avro schemas
// translated with the given Names instance. If names is nil, the global
// namespace will be used.
func NewSingleDecoder(r DecodingRegistry, names *Names) *SingleDecoder {
	if names == nil {
		names = globalNames
	}
	return &SingleDecoder{
		registry:    r,
		writerTypes: make(map[int64]*Type),
		programs:    make(map[decoderSchemaPair]*decodeProgram),
		names:       names,
	}
}

// Unmarshal unmarshals the given message into x. The body
// of the message is unmarshaled as with the Unmarshal function.
//
// It needs the context argument because it might end up
// fetching schema data over the network via the DecodingRegistry.
//
// Unmarshal returns the actual type that was decoded into.
func (c *SingleDecoder) Unmarshal(ctx context.Context, data []byte, x interface{}) (*Type, error) {
	v := reflect.ValueOf(x)
	if v.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("cannot decode into non-pointer value %T", x)
	}
	v = v.Elem()
	vt := v.Type()
	wID, body := c.registry.DecodeSchemaID(data)
	if wID == 0 && body == nil {
		return nil, fmt.Errorf("cannot get schema ID from message")
	}
	prog, err := c.getProgram(ctx, vt, wID)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal: %v", err)
	}
	return unmarshal(nil, body, prog, v)
}

func (c *SingleDecoder) getProgram(ctx context.Context, vt reflect.Type, wID int64) (*decodeProgram, error) {
	c.mu.RLock()
	if prog := c.programs[decoderSchemaPair{vt, wID}]; prog != nil {
		c.mu.RUnlock()
		return prog, nil
	}
	if debugging {
		debugf("no hit found for program %T schemaID %v", vt, wID)
	}
	wType := c.writerTypes[wID]
	c.mu.RUnlock()

	var err error
	if wType != nil {
		if es, ok := wType.avroType.(errorSchema); ok {
			return nil, es.err
		}
	} else {
		// We haven't seen the writer schema before, so try to fetch it.
		wType, err = c.registry.SchemaForID(ctx, wID)
		// TODO look at the SchemaForID error
		// and return an error without caching it if it's temporary?
		// See https://github.com/heetch/avro/issues/39
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.writerTypes[wID] = &Type{
			avroType: errorSchema{err: err},
		}
		return nil, err
	}
	if prog := c.programs[decoderSchemaPair{vt, wID}]; prog != nil {
		// Someone else got there first.
		return prog, nil
	}

	prog, err := compileDecoder(c.names, vt, wType)
	if err != nil {
		c.writerTypes[wID] = &Type{
			avroType: errorSchema{err: err},
		}
		return nil, err
	}
	c.programs[decoderSchemaPair{vt, wID}] = prog
	return prog, nil
}
